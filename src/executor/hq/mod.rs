pub mod client;
pub mod worker;

pub use client::{HqBackend, HqClient};
pub use worker::{run_worker_if_invoked, run_worker_if_invoked_with};

use std::collections::{HashMap, HashSet, VecDeque};
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use petgraph::graph::NodeIndex;

use crate::error::{Result, RplError};
use crate::executor::scheduler::{BatchScheduler, TaggedHandle};
use crate::executor::{BatchLineage, Executor, OutputBatch, PathStep, SourceGenerator, next_exec_id};
use crate::graph::PipelineGraph;
use crate::task::BatchMode;
use crate::transport::DataTransport;
use crate::transport::file::FileTransport;

use client::CompletedTask;

/// HyperQueue-based executor for distributed pipeline execution.
///
/// Generic over:
/// - `B`: the HQ backend (real CLI or mock)
/// - `T`: the data transport (file-based, Arrow Flight, etc.)
///
/// Submits tasks to HyperQueue as an open job. The same binary serves as
/// both driver and worker — HQ invokes `current_exe() --rpl-worker ...`
/// to run individual tasks on worker nodes.
pub struct HqExecutor<B = HqClient, T: DataTransport = FileTransport> {
    client: B,
    transport: Arc<T>,
    max_batches: Option<usize>,
    poll_interval: Duration,
    /// How many in-flight HQ tasks to allow before pausing source feeding.
    source_buffer: usize,
}

impl HqExecutor<HqClient, FileTransport> {
    /// Create an executor using the real HQ CLI and file-based transport.
    ///
    /// Uses `"hq"` from PATH by default; call
    /// [`with_hq_binary`](Self::with_hq_binary) to override.
    pub fn new(
        staging_dir: impl Into<PathBuf>,
    ) -> Result<Self> {
        let transport = Arc::new(FileTransport::new(staging_dir)?);
        Ok(HqExecutor {
            client: HqClient::new("hq"),
            transport,
            max_batches: None,
            poll_interval: Duration::from_millis(250),
            source_buffer: 16,
        })
    }

    /// Override the HQ binary path (default: `"hq"` from PATH).
    pub fn with_hq_binary(mut self, hq_binary: impl Into<PathBuf>) -> Self {
        self.client = HqClient::new(hq_binary);
        self
    }
}

impl<B: HqBackend, T: DataTransport> HqExecutor<B, T> {
    /// Create an executor with a custom backend and transport.
    pub fn with_backend(backend: B, transport: T) -> Self {
        HqExecutor {
            client: backend,
            transport: Arc::new(transport),
            max_batches: None,
            poll_interval: Duration::from_millis(250),
            source_buffer: 16,
        }
    }

    /// Create an executor with a custom backend and a shared transport.
    ///
    /// Use this when the transport instance is shared with other components
    /// (e.g. a mock backend that executes tasks inline).
    pub fn with_backend_shared(backend: B, transport: Arc<T>) -> Self {
        HqExecutor {
            client: backend,
            transport,
            max_batches: None,
            poll_interval: Duration::from_millis(250),
            source_buffer: 16,
        }
    }

    /// Limit the number of source batches generated.
    pub fn with_max_batches(mut self, n: usize) -> Self {
        self.max_batches = Some(n);
        self
    }

    /// Set the interval between polling HQ for completed tasks.
    pub fn with_poll_interval(mut self, interval: Duration) -> Self {
        self.poll_interval = interval;
        self
    }

    /// Set the source buffer size: how many in-flight HQ tasks to allow
    /// before pausing source batch feeding. Default: 16.
    pub fn with_source_buffer(mut self, n: usize) -> Self {
        self.source_buffer = n;
        self
    }
}

impl<B: HqBackend, T: DataTransport> Executor for HqExecutor<B, T> {
    fn run<'a>(
        &'a mut self,
        graph: &'a PipelineGraph,
        source: &'a mut dyn SourceGenerator,
    ) -> Result<Box<dyn Iterator<Item = Result<OutputBatch>> + 'a>> {
        graph.validate(source)?;

        let transport = self.transport.clone();
        let scheduler = BatchScheduler::new(graph, transport);

        // Compute priority for each node: sink = 0, others = -(rank).
        let ranks = graph.rank_from_sink();
        let priorities: HashMap<NodeIndex, i32> = ranks
            .into_iter()
            .map(|(node, rank)| (node, -(rank as i32)))
            .collect();

        // Create an open job for this run.
        let job_id = self.client.create_open_job("rpl-pipeline")?;

        let worker_binary = std::env::current_exe()
            .map_err(|e| RplError::Hq(format!("failed to get current executable: {e}")))?;

        Ok(Box::new(HqIter {
            graph,
            scheduler,
            source,
            client: &self.client,
            worker_binary,
            job_id,
            priorities,
            max_batches: self.max_batches,
            batch_count: 0,
            source_buffer: self.source_buffer,
            source_hq_ids: HashSet::new(),
            in_flight: HashMap::new(),
            completed_ids: HashSet::new(),
            pending_source_count: 0,
            pending_outputs: VecDeque::new(),
            done: false,
            poll_interval: self.poll_interval,
        }))
    }
}

/// Tracks an HQ task that has been submitted but not yet completed.
enum InFlightTask<H, OT> {
    /// A normal task execution.
    Execute {
        node: NodeIndex,
        input_handles: Vec<TaggedHandle<H>>,
        output_token: OT,
        submitted_at: Instant,
        lineage: BatchLineage,
    },
    /// A split-only task targeting a specific successor.
    Split {
        /// The node whose output is being split (the "from" node).
        from: NodeIndex,
        /// The successor node this split is feeding.
        target: NodeIndex,
        /// Handle to the unsplit batch (released after split completes).
        source_handle: H,
        output_token: OT,
        lineage: BatchLineage,
    },
}

/// Lazy iterator driving the HQ executor pipeline.
struct HqIter<'a, B: HqBackend, T: DataTransport> {
    graph: &'a PipelineGraph,
    scheduler: BatchScheduler<Arc<T>>,
    source: &'a mut dyn SourceGenerator,
    client: &'a B,
    worker_binary: PathBuf,
    job_id: u64,
    priorities: HashMap<NodeIndex, i32>,
    max_batches: Option<usize>,
    batch_count: usize,
    source_buffer: usize,
    /// HQ task IDs that correspond to source node executions.
    source_hq_ids: HashSet<u64>,
    in_flight: HashMap<u64, InFlightTask<T::Handle, T::OutputToken>>,
    /// Track which HQ task IDs we've already processed (since poll returns all finished).
    completed_ids: HashSet<u64>,
    /// Number of source tasks submitted to HQ that are not yet running.
    /// Updated each poll cycle. Starts at 0 so the first step feeds eagerly.
    pending_source_count: usize,
    pending_outputs: VecDeque<Result<OutputBatch>>,
    done: bool,
    poll_interval: Duration,
}

impl<B: HqBackend, T: DataTransport> HqIter<'_, B, T> {
    /// Run one scheduling round.
    fn step(&mut self) -> bool {
        if self.done {
            return false;
        }

        // 1. Feed source batches while pending source tasks < buffer.
        let mut source_exhausted = false;

        while self.pending_source_count < self.source_buffer {
            let under_limit = match self.max_batches {
                Some(limit) => self.batch_count < limit,
                None => true,
            };
            if !under_limit {
                source_exhausted = true;
                break;
            }
            if let Some(batch) = self.source.next_batch() {
                if let Err(e) = self.scheduler.enqueue_source_batch(&batch) {
                    self.pending_outputs.push_back(Err(e));
                    self.done = true;
                    return true;
                }
                self.batch_count += 1;

                // Drain scheduler after each source batch to submit HQ tasks.
                while let Some(ready) = self.scheduler.next_ready_task() {
                    if let Err(e) =
                        self.submit_hq_task(ready.node, ready.handles, ready.lineage)
                    {
                        self.pending_outputs.push_back(Err(e));
                        self.done = true;
                        return true;
                    }
                }
            } else {
                source_exhausted = true;
                break;
            }
        }

        if source_exhausted {
            self.scheduler.mark_flush();
        }

        // 2. Drain any remaining ready tasks.
        while let Some(ready) = self.scheduler.next_ready_task() {
            if let Err(e) = self.submit_hq_task(ready.node, ready.handles, ready.lineage) {
                self.pending_outputs.push_back(Err(e));
                self.done = true;
                return true;
            }
        }

        // 3. If nothing in-flight, we're done.
        if self.in_flight.is_empty() {
            self.finish();
            return !self.pending_outputs.is_empty();
        }

        // 4. Poll HQ for completions.
        if let Err(e) = self.poll_completions() {
            self.pending_outputs.push_back(Err(e));
            self.done = true;
            return true;
        }

        true
    }

    fn submit_hq_task(
        &mut self,
        node: NodeIndex,
        handles: Vec<TaggedHandle<T::Handle>>,
        lineage: BatchLineage,
    ) -> Result<()> {
        let task = self.graph.task(node);
        let priority = self.priorities[&node];

        // Serialize input handles and origins to JSON.
        let input_handles_json = serde_json::to_string(
            &handles.iter().map(|h| &h.handle).collect::<Vec<_>>(),
        )
        .map_err(|e| RplError::Hq(format!("failed to serialize input handles: {e}")))?;

        let origins_json = serde_json::to_string(&lineage.origins)
            .map_err(|e| RplError::Hq(format!("failed to serialize origins: {e}")))?;

        // Prepare an output token for this task.
        let output_token = self.scheduler.transport().prepare_output()?;
        let output_token_json = serde_json::to_string(&output_token)
            .map_err(|e| RplError::Hq(format!("failed to serialize output token: {e}")))?;

        let command = vec![
            self.worker_binary.to_string_lossy().to_string(),
            "--rpl-worker".to_string(),
            "--task-name".to_string(),
            task.name.clone(),
            "--input-handles".to_string(),
            input_handles_json,
            "--origins".to_string(),
            origins_json,
            "--output-token".to_string(),
            output_token_json,
        ];

        let hq_task_id =
            self.client
                .submit_task(self.job_id, &command, priority, &task.resources)?;

        // Track source tasks separately for buffer management.
        if self.graph.is_source(node) {
            self.source_hq_ids.insert(hq_task_id);
            self.pending_source_count += 1;
        }

        self.in_flight.insert(
            hq_task_id,
            InFlightTask::Execute {
                node,
                input_handles: handles,
                output_token,
                submitted_at: Instant::now(),
                lineage,
            },
        );

        Ok(())
    }

    /// Submit a split-only HQ task for a specific successor.
    fn submit_split_task(
        &mut self,
        from: NodeIndex,
        target: NodeIndex,
        handle: T::Handle,
        lineage: BatchLineage,
        max_rows: usize,
    ) -> Result<()> {
        let priority = self.priorities[&target];

        let input_handles_json = serde_json::to_string(&vec![&handle])
            .map_err(|e| RplError::Hq(format!("failed to serialize handle: {e}")))?;

        let origins_json = serde_json::to_string(&lineage.origins)
            .map_err(|e| RplError::Hq(format!("failed to serialize origins: {e}")))?;

        let output_token = self.scheduler.transport().prepare_output()?;
        let output_token_json = serde_json::to_string(&output_token)
            .map_err(|e| RplError::Hq(format!("failed to serialize output token: {e}")))?;

        let command = vec![
            self.worker_binary.to_string_lossy().to_string(),
            "--rpl-worker".to_string(),
            "--split".to_string(),
            "--max-rows".to_string(),
            max_rows.to_string(),
            "--input-handles".to_string(),
            input_handles_json,
            "--origins".to_string(),
            origins_json,
            "--output-token".to_string(),
            output_token_json,
        ];

        // Split tasks use minimal resources.
        let resources = crate::task::Resources::default();
        let hq_task_id =
            self.client
                .submit_task(self.job_id, &command, priority, &resources)?;

        self.in_flight.insert(
            hq_task_id,
            InFlightTask::Split {
                from,
                target,
                source_handle: handle,
                output_token,
                lineage,
            },
        );

        Ok(())
    }

    fn poll_completions(&mut self) -> Result<()> {
        // Sleep briefly to avoid busy-spinning.
        thread::sleep(self.poll_interval);

        let in_flight_ids: Vec<u64> = self.in_flight.keys().copied().collect();
        let poll_result = self.client.poll_tasks(self.job_id, &in_flight_ids)?;

        let newly_completed: Vec<CompletedTask> = poll_result
            .completed
            .into_iter()
            .filter(|c| self.completed_ids.insert(c.task_id))
            .collect();

        // Remove completed source IDs before recomputing pending count.
        for completed_task in &newly_completed {
            self.source_hq_ids.remove(&completed_task.task_id);
        }

        // Recompute pending source count.
        let running_source: usize = poll_result
            .running_ids
            .iter()
            .filter(|id| self.source_hq_ids.contains(id))
            .count();
        self.pending_source_count = self.source_hq_ids.len().saturating_sub(running_source);

        for completed_task in newly_completed {
            if let Some(flight) = self.in_flight.remove(&completed_task.task_id) {
                match flight {
                    InFlightTask::Execute {
                        node,
                        input_handles,
                        output_token,
                        submitted_at,
                        lineage,
                    } => {
                        if !completed_task.success {
                            let task_name = &self.graph.task(node).name;
                            return Err(RplError::Hq(format!(
                                "HQ task for '{task_name}' (id={}) failed",
                                completed_task.task_id
                            )));
                        }

                        let wall_duration = submitted_at.elapsed();

                        // Release input handles.
                        self.scheduler.release_handles(&input_handles)?;

                        let entries = self.scheduler.transport().collect_output(&output_token)?;
                        let is_sink = self.graph.is_sink(node);

                        // Build the path step for this task.
                        let exec_duration = entries
                            .first()
                            .and_then(|e| e.exec_duration_ms)
                            .map(Duration::from_millis)
                            .unwrap_or_default();
                        let step = PathStep {
                            exec_id: next_exec_id(),
                            task: self.graph.task(node).name.clone(),
                            exec_duration,
                            wall_duration,
                        };
                        let mut new_lineage = lineage;
                        new_lineage.path.push(step);

                        if is_sink {
                            for entry in entries {
                                new_lineage.origins = entry.origins;
                                let data = self.scheduler.transport().load(&entry.handle)?;
                                self.scheduler.transport().release(&entry.handle)?;
                                self.pending_outputs.push_back(Ok(OutputBatch {
                                    data,
                                    task: self.graph.task(node).name.clone(),
                                    lineage: new_lineage.clone(),
                                }));
                            }
                        } else {
                            for entry in entries {
                                new_lineage.origins = entry.origins;
                                self.route_to_successors(
                                    node,
                                    entry.handle,
                                    new_lineage.clone(),
                                    entry.num_rows,
                                )?;
                            }
                        }
                    }

                    InFlightTask::Split {
                        from,
                        target,
                        source_handle,
                        output_token,
                        lineage,
                    } => {
                        if !completed_task.success {
                            return Err(RplError::Hq(format!(
                                "split task (id={}) failed",
                                completed_task.task_id
                            )));
                        }

                        // Release the original unsplit handle.
                        self.scheduler.transport().release(&source_handle)?;

                        let entries = self.scheduler.transport().collect_output(&output_token)?;

                        for entry in entries {
                            let mut entry_lineage = lineage.clone();
                            entry_lineage.origins = entry.origins;
                            self.scheduler.deliver_to_successor(
                                from,
                                target,
                                entry.handle,
                                entry_lineage,
                                entry.num_rows,
                            );
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Route a task's output to each successor individually.
    fn route_to_successors(
        &mut self,
        from: NodeIndex,
        handle: T::Handle,
        lineage: BatchLineage,
        num_rows: usize,
    ) -> Result<()> {
        let succs: Vec<NodeIndex> = self.scheduler.successors_of(from).to_vec();

        if succs.is_empty() {
            self.scheduler.transport().release(&handle)?;
            return Ok(());
        }

        let additional = succs.len().saturating_sub(1);
        if additional > 0 {
            self.scheduler
                .transport()
                .add_consumers(&handle, additional)?;
        }

        for &succ in &succs {
            let needs_split = match &self.graph.task(succ).batch_mode {
                BatchMode::MaxRows(n) => num_rows > *n,
                _ => false,
            };

            if needs_split {
                let max_rows = self.graph.task(succ).batch_mode.max_rows().unwrap();
                self.submit_split_task(from, succ, handle.clone(), lineage.clone(), max_rows)?;
            } else {
                self.scheduler.deliver_to_successor(
                    from,
                    succ,
                    handle.clone(),
                    lineage.clone(),
                    num_rows,
                );
            }
        }

        Ok(())
    }

    fn finish(&mut self) {
        if !self.done {
            let _ = self.client.close_job(self.job_id);
        }
        self.done = true;
    }
}

impl<B: HqBackend, T: DataTransport> Drop for HqIter<'_, B, T> {
    fn drop(&mut self) {
        self.finish();
    }
}

impl<B: HqBackend, T: DataTransport> Iterator for HqIter<'_, B, T> {
    type Item = Result<OutputBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(output) = self.pending_outputs.pop_front() {
            return Some(output);
        }

        while self.step() {
            if let Some(output) = self.pending_outputs.pop_front() {
                return Some(output);
            }
        }

        self.pending_outputs.pop_front()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::batch_ext::RecordBatchExt;
    use crate::executor::{DefaultGenerator, Executor, SourceGenerator};
    use crate::graph::PipelineGraph;
    use crate::registry::TaskRegistry;
    use crate::schema::schema_of;
    use crate::task::{BatchMode, TaskDef};
    use arrow::array::{Float64Array, Int64Array};
    use arrow::datatypes::{DataType, Schema};
    use arrow::record_batch::RecordBatch;
    use std::cell::{Cell, RefCell};
    use std::collections::HashSet;
    use std::sync::Arc;

    use client::{CompletedTask, HqBackend, JobPollResult};

    /// Mock HQ backend that executes worker logic inline (synchronously).
    ///
    /// Every submitted task is run immediately via `run_worker`, so the
    /// manifest file is ready by the time `poll_tasks` reports it complete.
    struct MockHqBackend {
        registry: TaskRegistry,
        transport: Arc<FileTransport>,
        next_task_id: Cell<u64>,
        /// Task IDs submitted but not yet reported as completed.
        pending: RefCell<Vec<u64>>,
    }

    impl MockHqBackend {
        fn new(registry: TaskRegistry, transport: Arc<FileTransport>) -> Self {
            MockHqBackend {
                registry,
                transport,
                next_task_id: Cell::new(1),
                pending: RefCell::new(Vec::new()),
            }
        }
    }

    impl HqBackend for MockHqBackend {
        fn create_open_job(&self, _name: &str) -> crate::error::Result<u64> {
            Ok(1)
        }

        fn submit_task(
            &self,
            _job_id: u64,
            command: &[String],
            _priority: i32,
            _resources: &crate::task::Resources,
        ) -> crate::error::Result<u64> {
            // Run the worker logic inline using the shared transport.
            worker::run_worker(command, &self.registry, &*self.transport)?;

            let id = self.next_task_id.get();
            self.next_task_id.set(id + 1);
            self.pending.borrow_mut().push(id);
            Ok(id)
        }

        fn poll_tasks(&self, _job_id: u64, _task_ids: &[u64]) -> crate::error::Result<JobPollResult> {
            let pending = self.pending.borrow_mut().drain(..).collect::<Vec<_>>();
            Ok(JobPollResult {
                completed: pending
                    .into_iter()
                    .map(|id| CompletedTask {
                        task_id: id,
                        success: true,
                    })
                    .collect(),
                running_ids: HashSet::new(),
            })
        }

        fn close_job(&self, _job_id: u64) -> crate::error::Result<()> {
            Ok(())
        }
    }

    /// Build an HqExecutor backed by MockHqBackend in a temp directory.
    fn mock_executor(
        graph: &PipelineGraph,
    ) -> (HqExecutor<MockHqBackend, FileTransport>, tempfile::TempDir) {
        let registry = TaskRegistry::from(graph);
        let tmp = tempfile::tempdir().unwrap();
        let transport = Arc::new(FileTransport::new(tmp.path()).unwrap());
        let executor = HqExecutor::with_backend_shared(
            MockHqBackend::new(registry, transport.clone()),
            transport,
        )
        .with_poll_interval(Duration::ZERO);
        (executor, tmp)
    }

    #[test]
    fn simple_linear_pipeline() {
        let mut graph = PipelineGraph::new();
        graph
            .add_linear(vec![TaskDef::new(
                "add_value",
                Schema::empty(),
                schema_of(&[("value", DataType::Float64)]),
                |batch| {
                    let len = batch.num_rows();
                    let values: Vec<f64> = (0..len).map(|i| i as f64 * 1.5).collect();
                    batch.append_column("value", Arc::new(Float64Array::from(values)))
                },
            )])
            .unwrap();

        let (executor, _tmp) = mock_executor(&graph);
        let mut executor = executor.with_max_batches(3);
        let mut source = DefaultGenerator::new();
        let results: Vec<_> = executor.run(&graph, &mut source).unwrap().collect();

        assert_eq!(results.len(), 3);
        for result in &results {
            let batch = &result.as_ref().unwrap().data;
            assert_eq!(batch.num_columns(), 2); // id + value
            assert!(batch.schema().field_with_name("value").is_ok());
        }
    }

    #[test]
    fn diamond_graph() {
        let mut graph = PipelineGraph::new();
        let b = graph.add_task(TaskDef::new(
            "branch_b",
            Schema::empty(),
            schema_of(&[("from_b", DataType::Float64)]),
            |batch| {
                let len = batch.num_rows();
                batch.append_column("from_b", Arc::new(Float64Array::from(vec![1.0; len])))
            },
        ));
        let c = graph.add_task(TaskDef::new(
            "branch_c",
            Schema::empty(),
            schema_of(&[("from_c", DataType::Float64)]),
            |batch| {
                let len = batch.num_rows();
                batch.append_column("from_c", Arc::new(Float64Array::from(vec![2.0; len])))
            },
        ));
        let d = graph.add_task(TaskDef::passthrough("merge", |b| Ok(b)));
        graph.add_edge(b, d).unwrap();
        graph.add_edge(c, d).unwrap();

        let (executor, _tmp) = mock_executor(&graph);
        let mut executor = executor.with_max_batches(1);
        let mut source = DefaultGenerator::new();
        let results: Vec<_> = executor.run(&graph, &mut source).unwrap().collect();

        // d receives one batch from b and one from c => 2 sink outputs.
        assert_eq!(results.len(), 2);
        for r in &results {
            assert_eq!(r.as_ref().unwrap().task, "merge");
        }
    }

    #[test]
    fn max_rows_splitting() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let id_schema = schema_of(&[("id", DataType::Int64)]);
        let max_seen = Arc::new(AtomicUsize::new(0));
        let max_seen_clone = max_seen.clone();

        let mut graph = PipelineGraph::new();
        graph
            .add_linear(vec![
                TaskDef::passthrough("source", |b| Ok(b)),
                TaskDef::passthrough("process", move |b: RecordBatch| {
                    max_seen_clone.fetch_max(b.num_rows(), Ordering::Relaxed);
                    Ok(b)
                })
                .with_batch_mode(BatchMode::MaxRows(3)),
            ])
            .unwrap();

        struct BigSource {
            schema: Schema,
            emitted: bool,
        }
        impl SourceGenerator for BigSource {
            fn produces(&self) -> &Schema {
                &self.schema
            }
            fn next_batch(&mut self) -> Option<RecordBatch> {
                if self.emitted {
                    return None;
                }
                self.emitted = true;
                let ids: Vec<i64> = (0..10).collect();
                Some(
                    RecordBatch::try_new(
                        Arc::new(self.schema.clone()),
                        vec![Arc::new(Int64Array::from(ids))],
                    )
                    .unwrap(),
                )
            }
        }

        let (mut executor, _tmp) = mock_executor(&graph);
        let mut source = BigSource {
            schema: id_schema,
            emitted: false,
        };
        let results: Vec<_> = executor.run(&graph, &mut source).unwrap().collect();

        assert_eq!(results.len(), 4);
        let total_rows: usize = results
            .iter()
            .map(|r| r.as_ref().unwrap().data.num_rows())
            .sum();
        assert_eq!(total_rows, 10);
        assert!(max_seen.load(Ordering::Relaxed) <= 3);
    }

    #[test]
    fn source_buffer_limits_feeding() {
        let mut graph = PipelineGraph::new();
        graph
            .add_linear(vec![TaskDef::passthrough("passthrough", |b| Ok(b))])
            .unwrap();

        let (executor, _tmp) = mock_executor(&graph);
        let mut executor = executor.with_max_batches(5).with_source_buffer(2);
        let mut source = DefaultGenerator::new();
        let results: Vec<_> = executor.run(&graph, &mut source).unwrap().collect();

        assert_eq!(results.len(), 5);
    }

    #[test]
    fn task_failure_propagates() {
        let mut graph = PipelineGraph::new();
        graph
            .add_linear(vec![TaskDef::passthrough("fail", |_| {
                Err(crate::error::RplError::Hq("intentional failure".into()))
            })])
            .unwrap();

        let (executor, _tmp) = mock_executor(&graph);
        let mut executor = executor.with_max_batches(1);
        let mut source = DefaultGenerator::new();
        let results: Vec<_> = executor.run(&graph, &mut source).unwrap().collect();

        assert_eq!(results.len(), 1);
        assert!(results[0].is_err());
    }

    #[test]
    fn fanout_mixed_batch_modes() {
        let id_schema = schema_of(&[("id", DataType::Int64)]);

        let mut graph = PipelineGraph::new();
        let src = graph.add_task(TaskDef::passthrough("source", |b| Ok(b)));
        let a = graph.add_task(TaskDef::passthrough("sink_a", |b| Ok(b)));
        let b = graph.add_task(
            TaskDef::passthrough("sink_b", |b| Ok(b))
                .with_batch_mode(BatchMode::MaxRows(2)),
        );
        graph.add_edge(src, a).unwrap();
        graph.add_edge(src, b).unwrap();

        struct FiveRowSource {
            schema: Schema,
            emitted: bool,
        }
        impl SourceGenerator for FiveRowSource {
            fn produces(&self) -> &Schema {
                &self.schema
            }
            fn next_batch(&mut self) -> Option<RecordBatch> {
                if self.emitted {
                    return None;
                }
                self.emitted = true;
                let ids: Vec<i64> = (0..5).collect();
                Some(
                    RecordBatch::try_new(
                        Arc::new(self.schema.clone()),
                        vec![Arc::new(Int64Array::from(ids))],
                    )
                    .unwrap(),
                )
            }
        }

        let (mut executor, _tmp) = mock_executor(&graph);
        let mut source = FiveRowSource {
            schema: id_schema,
            emitted: false,
        };
        let results: Vec<_> = executor.run(&graph, &mut source).unwrap().collect();

        let mut a_rows = Vec::new();
        let mut b_rows = Vec::new();
        for r in &results {
            let out = r.as_ref().unwrap();
            match out.task.as_str() {
                "sink_a" => a_rows.push(out.data.num_rows()),
                "sink_b" => b_rows.push(out.data.num_rows()),
                other => panic!("unexpected task: {other}"),
            }
        }

        // sink_a gets the full batch (passthrough).
        assert_eq!(a_rows.iter().sum::<usize>(), 5);
        assert_eq!(a_rows.len(), 1);

        // sink_b gets split chunks, each <= 2 rows, totalling 5.
        assert_eq!(b_rows.iter().sum::<usize>(), 5);
        assert!(b_rows.iter().all(|&n| n <= 2));
        assert_eq!(b_rows.len(), 3); // 2+2+1
    }
}
