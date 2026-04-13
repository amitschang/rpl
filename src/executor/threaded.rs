use std::collections::VecDeque;
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use arrow::record_batch::RecordBatch;
use petgraph::graph::NodeIndex;

use crate::batch_ext::RecordBatchExt;
use crate::error::Result;
use crate::executor::scheduler::BatchScheduler;
use crate::executor::{
    BatchLineage, Executor, OutputBatch, PathStep, SourceGenerator, next_exec_id,
};
use crate::graph::PipelineGraph;
use crate::task::BatchMode;
use crate::transport::DataTransport;
use crate::transport::memory::InMemoryTransport;

/// Multi-threaded executor that runs tasks concurrently using OS threads.
///
/// Dispatches ready tasks to worker threads, bounded by available CPU
/// resources. Each task's [`Resources::num_cpus`](crate::task::Resources::num_cpus)
/// is respected — a task is only dispatched when enough CPU slots are free.
/// When nothing is in flight, one task is always allowed to proceed
/// (preventing deadlock from tasks that exceed the CPU budget).
///
/// Returns a lazy iterator — each call to `next()` advances the pipeline
/// until a sink task produces output, supporting indefinite streaming
/// pipelines.
pub struct ThreadExecutor {
    max_batches: Option<usize>,
    num_cpus: usize,
}

impl ThreadExecutor {
    pub fn new() -> Self {
        let num_cpus = thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);
        ThreadExecutor {
            max_batches: None,
            num_cpus,
        }
    }

    /// Limit the number of source batches generated.
    pub fn with_max_batches(mut self, n: usize) -> Self {
        self.max_batches = Some(n);
        self
    }

    /// Set the number of CPU slots available for concurrent execution.
    ///
    /// Defaults to the number of available hardware threads.
    /// Clamped to a minimum of 1.
    pub fn with_num_cpus(mut self, n: usize) -> Self {
        self.num_cpus = n.max(1);
        self
    }
}

impl Default for ThreadExecutor {
    fn default() -> Self {
        Self::new()
    }
}

impl Executor for ThreadExecutor {
    fn run<'a>(
        &'a mut self,
        graph: &'a PipelineGraph,
        source: &'a mut dyn SourceGenerator,
    ) -> Result<Box<dyn Iterator<Item = Result<OutputBatch>> + 'a>> {
        graph.validate(source)?;

        let transport = InMemoryTransport::new();
        let scheduler = BatchScheduler::new(graph, transport);
        let (tx, rx) = mpsc::channel();

        Ok(Box::new(ThreadedIter {
            graph,
            scheduler,
            source,
            max_batches: self.max_batches,
            max_cpus: self.num_cpus as u32,
            batch_count: 0,
            cpus_in_flight: 0,
            source_exhausted: false,
            flushing: false,
            pending_outputs: VecDeque::new(),
            done: false,
            tx,
            rx,
        }))
    }
}

/// Result sent back from a worker thread after executing a task chunk.
struct CompletedChunk {
    node: NodeIndex,
    output: Result<RecordBatch>,
    lineage: BatchLineage,
    task_name: String,
    is_sink: bool,
    exec_duration: Duration,
    cpus: u32,
}

/// Lazy iterator that drives the threaded pipeline.
///
/// Each call to `next()` feeds sources, dispatches ready tasks to worker
/// threads (within CPU budget), and waits for results until a sink
/// produces output or the pipeline is exhausted. Worker threads are
/// regular `thread::spawn` threads — they only touch `'static` data
/// (cloned `TaskDef` + `RecordBatch`), so no scoped threads are needed.
struct ThreadedIter<'a> {
    graph: &'a PipelineGraph,
    scheduler: BatchScheduler<InMemoryTransport>,
    source: &'a mut dyn SourceGenerator,
    max_batches: Option<usize>,
    max_cpus: u32,
    batch_count: usize,
    cpus_in_flight: u32,
    source_exhausted: bool,
    flushing: bool,
    pending_outputs: VecDeque<Result<OutputBatch>>,
    done: bool,
    tx: mpsc::Sender<CompletedChunk>,
    rx: mpsc::Receiver<CompletedChunk>,
}

impl ThreadedIter<'_> {
    /// Run one scheduling round: feed sources, dispatch ready tasks,
    /// wait for results, and buffer any sink outputs.
    /// Returns `true` if progress was made.
    fn step(&mut self) -> bool {
        if self.done {
            return false;
        }

        // 1. Feed one source batch per step.
        if !self.source_exhausted {
            match super::feed_source(
                &mut *self.source,
                &mut self.scheduler,
                self.max_batches,
                &mut self.batch_count,
            ) {
                super::FeedResult::Fed => {}
                super::FeedResult::Exhausted => self.source_exhausted = true,
                super::FeedResult::Error(e) => {
                    self.pending_outputs.push_back(Err(e));
                    self.done = true;
                    return true;
                }
            }
        }

        // 2. Dispatch ready tasks while CPU budget allows.
        let mut dispatched = false;
        loop {
            if self.cpus_in_flight > 0 && self.cpus_in_flight >= self.max_cpus {
                break;
            }

            let Some(ready) = self.scheduler.next_ready_task() else {
                break;
            };

            let node_idx = ready.node;
            let task = self.graph.task(node_idx);
            let cpus = task.resources.num_cpus;
            let is_sink = self.graph.is_sink(node_idx);
            let task_clone = task.clone();

            let input = match self.scheduler.load_and_concat(&ready.handles) {
                Ok(batch) => batch,
                Err(e) => {
                    self.pending_outputs.push_back(Err(e));
                    self.done = true;
                    return true;
                }
            };
            if let Err(e) = self.scheduler.release_handles(&ready.handles) {
                self.pending_outputs.push_back(Err(e));
                self.done = true;
                return true;
            }

            let chunks: Vec<RecordBatch> = match &task.batch_mode {
                BatchMode::MaxRows(n) if input.num_rows() > *n => input.split(*n),
                _ => vec![input],
            };

            for chunk in chunks {
                let chunk_tx = self.tx.clone();
                let chunk_task = task_clone.clone();
                let chunk_lineage = ready.lineage.clone();
                let chunk_name = task_clone.name.clone();

                self.cpus_in_flight += cpus;
                thread::spawn(move || {
                    let started = Instant::now();
                    let output = chunk_task.execute(chunk);
                    let elapsed = started.elapsed();
                    let _ = chunk_tx.send(CompletedChunk {
                        node: node_idx,
                        output,
                        lineage: chunk_lineage,
                        task_name: chunk_name,
                        is_sink,
                        exec_duration: elapsed,
                        cpus,
                    });
                });
                dispatched = true;
            }
        }

        // 3. If nothing in flight and nothing dispatched:
        //    enable flush mode on first idle after source exhaustion,
        //    otherwise we're done.
        if self.cpus_in_flight == 0 && !dispatched {
            if self.source_exhausted && !self.flushing {
                self.flushing = true;
                self.scheduler.mark_flush();
                return true; // retry — flush may unlock MaxRows tasks
            }
            self.done = true;
            return false;
        }

        // 4. Wait for at least one result when blocked (nothing dispatched
        //    or CPU budget full).
        if !dispatched || self.cpus_in_flight >= self.max_cpus {
            match self.rx.recv() {
                Ok(chunk) => {
                    self.cpus_in_flight -= chunk.cpus;
                    self.process_chunk(chunk);
                }
                Err(_) => {
                    self.done = true;
                    return false;
                }
            }
        }

        // Drain any additional immediately-available results.
        while let Ok(chunk) = self.rx.try_recv() {
            self.cpus_in_flight -= chunk.cpus;
            self.process_chunk(chunk);
        }

        true
    }

    /// Process a completed chunk: build lineage and either buffer as sink
    /// output or deliver to downstream tasks via the scheduler.
    fn process_chunk(&mut self, chunk: CompletedChunk) {
        let output = match chunk.output {
            Ok(batch) => batch,
            Err(e) => {
                self.pending_outputs.push_back(Err(e));
                return;
            }
        };

        let step = PathStep {
            exec_id: next_exec_id(),
            task: chunk.task_name.clone(),
            exec_duration: chunk.exec_duration,
            wall_duration: chunk.exec_duration,
        };
        let mut lineage = chunk.lineage;
        lineage.path.push(step);

        let num_rows = output.num_rows();

        if chunk.is_sink {
            self.pending_outputs.push_back(Ok(OutputBatch {
                data: output,
                task: chunk.task_name,
                lineage,
            }));
        } else {
            let handle = match self.scheduler.transport().store(&output) {
                Ok(h) => h,
                Err(e) => {
                    self.pending_outputs.push_back(Err(e));
                    self.done = true;
                    return;
                }
            };
            if let Err(e) = self
                .scheduler
                .deliver_output(chunk.node, handle, lineage, num_rows)
            {
                self.pending_outputs.push_back(Err(e));
                self.done = true;
            }
        }
    }
}

impl Iterator for ThreadedIter<'_> {
    type Item = Result<OutputBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        crate::executor::drain_step_loop!(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::DefaultGenerator;
    use crate::executor::test_fixtures;
    use crate::schema::schema_of;
    use crate::task::TaskDef;
    use arrow::array::Float64Array;
    use arrow::datatypes::{DataType, Schema};
    use std::sync::Arc;

    #[test]
    fn simple_linear_pipeline() {
        let mut executor = ThreadExecutor::new().with_max_batches(3).with_num_cpus(2);
        test_fixtures::assert_linear_pipeline(&mut executor);
    }

    #[test]
    fn diamond_graph_execution() {
        let mut executor = ThreadExecutor::new().with_max_batches(1).with_num_cpus(4);
        test_fixtures::assert_diamond_graph(&mut executor);
    }

    #[test]
    fn respects_cpu_budget() {
        use std::sync::atomic::{AtomicU32, Ordering};

        for max_cpus in [1, 2] {
            let peak = Arc::new(AtomicU32::new(0));
            let active = Arc::new(AtomicU32::new(0));

            let peak_c = peak.clone();
            let active_c = active.clone();

            let mut graph = PipelineGraph::new();
            graph
                .add_linear(vec![TaskDef::new(
                    "slow",
                    Schema::empty(),
                    schema_of(&[("out", DataType::Float64)]),
                    move |batch| {
                        let prev = active_c.fetch_add(1, Ordering::SeqCst);
                        peak_c.fetch_max(prev + 1, Ordering::SeqCst);
                        std::thread::sleep(std::time::Duration::from_millis(50));
                        active_c.fetch_sub(1, Ordering::SeqCst);
                        let len = batch.num_rows();
                        batch.append_column("out", Arc::new(Float64Array::from(vec![0.0; len])))
                    },
                )])
                .unwrap();

            let mut executor = ThreadExecutor::new()
                .with_max_batches(8)
                .with_num_cpus(max_cpus);
            let mut source = DefaultGenerator::new();
            let results: Vec<_> = executor.run(&graph, &mut source).unwrap().collect();

            assert_eq!(results.len(), 8, "max_cpus={max_cpus}");
            assert!(
                peak.load(Ordering::SeqCst) <= max_cpus as u32,
                "max_cpus={max_cpus}: peak {} exceeded budget",
                peak.load(Ordering::SeqCst),
            );
        }
    }

    #[test]
    fn max_rows_accumulation() {
        let mut executor = ThreadExecutor::new().with_max_batches(5).with_num_cpus(2);
        test_fixtures::assert_max_rows_accumulation(&mut executor);
    }

    #[test]
    fn input_splitting() {
        let mut executor = ThreadExecutor::new().with_num_cpus(2);
        test_fixtures::assert_input_splitting(&mut executor);
    }
}
