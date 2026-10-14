//! Backend-agnostic batch scheduler.
//!
//! [`BatchScheduler`] manages per-task queues, origin tracking, batch
//! accumulation, and flush semantics. It is generic over the data
//! transport and shared by all executor backends so that local and
//! distributed runs use identical scheduling logic.

use std::collections::{BTreeSet, HashMap, VecDeque};

use arrow::compute::concat_batches;
use arrow::record_batch::RecordBatch;
use petgraph::graph::NodeIndex;

use crate::error::{Result, RplError};
use crate::executor::BatchLineage;
use crate::graph::PipelineGraph;
use crate::task::BatchMode;
use crate::transport::DataTransport;

/// Per-predecessor input queues for CommonOrigin nodes.
type OriginQueues<H> = HashMap<NodeIndex, HashMap<NodeIndex, VecDeque<TaggedHandle<H>>>>;

/// A batch reference annotated with lineage.
///
/// `lineage.origins` tracks which source batch IDs contributed to this batch.
/// `lineage.path` records the sequence of tasks this batch passed through.
/// When `MaxRows` merges multiple batches, their lineages are merged.
#[derive(Debug, Clone)]
pub struct TaggedHandle<H> {
    pub handle: H,
    pub lineage: BatchLineage,
    pub num_rows: usize,
}

/// A task ready to execute, with the batch handles to load and concatenate.
pub struct ReadyTask<H> {
    pub node: NodeIndex,
    pub handles: Vec<TaggedHandle<H>>,
    /// Merged lineage across all input handles.
    pub lineage: BatchLineage,
}

/// Sink output buffered for the executor to drain.
pub struct SinkOutput<H> {
    pub handle: H,
    pub lineage: BatchLineage,
    pub task_name: String,
}

/// Transport-generic batch scheduler.
///
/// Manages per-node queues, origin tracking, and batch accumulation
/// for both `MaxRows` and `CommonOrigin` modes. Does NOT execute tasks —
/// the executor loads batches, runs the task function, stores results,
/// and calls back into the scheduler to route outputs.
///
/// Generic over `T: DataTransport` so the same logic works with
/// `InMemoryTransport` (local executor) or `FileTransport` (distributed).
pub struct BatchScheduler<T: DataTransport> {
    transport: T,
    /// Ranked node indices (closest to sink first) for processing order.
    ranked_order: Vec<NodeIndex>,
    /// Source node indices.
    source_indices: Vec<NodeIndex>,
    /// Sink node indices.
    sink_indices: Vec<NodeIndex>,
    /// The batch mode for each node.
    batch_modes: HashMap<NodeIndex, BatchMode>,
    /// Per-stream successor mapping: (from_node, stream) -> list of target nodes.
    stream_successors: HashMap<(NodeIndex, u32), Vec<NodeIndex>>,

    // --- Queues ---
    /// Input queues for non-CommonOrigin nodes.
    queues: HashMap<NodeIndex, VecDeque<TaggedHandle<T::Handle>>>,
    /// Per-predecessor input queues for CommonOrigin nodes.
    /// Key: (receiving node) -> (predecessor node) -> queue of tagged handles.
    origin_queues: OriginQueues<T::Handle>,

    /// Monotonic counter for assigning origin IDs to source batches.
    next_origin: u64,
    /// Buffered sink outputs.
    pending_sink: VecDeque<SinkOutput<T::Handle>>,
    /// When true, MaxRows nodes flush partial accumulations.
    flushing: bool,
}

impl<T: DataTransport> BatchScheduler<T> {
    /// Create a scheduler from a validated graph.
    ///
    /// The graph must already be validated. The scheduler precomputes
    /// structural information (ranked order, predecessors, successors,
    /// batch modes) for efficient scheduling.
    pub fn new(graph: &PipelineGraph, transport: T) -> Self {
        let ranked_order: Vec<NodeIndex> = graph.ranked_nodes().iter().map(|n| n.index).collect();
        let source_indices = graph.source_tasks();
        let sink_indices = graph.sink_tasks();

        let mut batch_modes = HashMap::new();
        let mut stream_successors: HashMap<(NodeIndex, u32), Vec<NodeIndex>> = HashMap::new();
        let mut queues = HashMap::new();
        let mut origin_queues = HashMap::new();

        for node in graph.node_indices() {
            let task = graph.task(node);
            batch_modes.insert(node, task.batch_mode.clone());

            // Build per-stream successor map from edges.
            for (target, streams) in graph.successors_with_streams(node) {
                for stream in streams {
                    stream_successors
                        .entry((node, stream))
                        .or_default()
                        .push(target);
                }
            }

            let preds = graph.predecessors(node);

            match &task.batch_mode {
                BatchMode::CommonOrigin if preds.len() >= 2 => {
                    let mut pred_queues = HashMap::new();
                    for &pred in &preds {
                        pred_queues.insert(pred, VecDeque::new());
                    }
                    origin_queues.insert(node, pred_queues);
                }
                BatchMode::CommonOrigin => {
                    // With 0 or 1 predecessors, CommonOrigin degrades to Passthrough.
                    batch_modes.insert(node, BatchMode::Passthrough);
                    queues.insert(node, VecDeque::new());
                }
                BatchMode::MaxRows(_) | BatchMode::Passthrough => {
                    queues.insert(node, VecDeque::new());
                }
            }
        }

        BatchScheduler {
            transport,
            ranked_order,
            source_indices,
            sink_indices,
            batch_modes,
            stream_successors,
            queues,
            origin_queues,
            next_origin: 0,
            pending_sink: VecDeque::new(),
            flushing: false,
        }
    }

    /// Reference to the transport (for loading batches in the executor).
    pub fn transport(&self) -> &T {
        &self.transport
    }

    /// Source node indices.
    pub fn source_indices(&self) -> &[NodeIndex] {
        &self.source_indices
    }

    /// Store a source batch, assign an origin ID, and enqueue to all source nodes.
    pub fn enqueue_source_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        let origin_id = self.next_origin;
        self.next_origin += 1;

        let handle = self.transport.store(batch)?;
        let num_rows = batch.num_rows();

        // If multiple source nodes, each gets a reference.
        let additional = self.source_indices.len().saturating_sub(1);
        if additional > 0 {
            self.transport.add_consumers(&handle, additional)?;
        }

        let origins = BTreeSet::from([origin_id]);
        let source_indices = self.source_indices.clone();
        for &src in &source_indices {
            let tagged = TaggedHandle {
                handle: handle.clone(),
                lineage: BatchLineage {
                    origins: origins.clone(),
                    path: Vec::new(),
                },
                num_rows,
            };
            self.enqueue_to_node(src, None, tagged);
        }

        Ok(())
    }

    /// Enqueue a tagged handle to a node's input queue.
    ///
    /// For CommonOrigin nodes, `from` must be Some(predecessor).
    /// For MaxRows nodes, `from` is ignored.
    fn enqueue_to_node(
        &mut self,
        node: NodeIndex,
        from: Option<NodeIndex>,
        tagged: TaggedHandle<T::Handle>,
    ) {
        match &self.batch_modes[&node] {
            BatchMode::CommonOrigin => {
                let from = from.expect("CommonOrigin node must receive from a known predecessor");
                self.origin_queues
                    .get_mut(&node)
                    .unwrap()
                    .get_mut(&from)
                    .unwrap()
                    .push_back(tagged);
            }
            BatchMode::MaxRows(_) | BatchMode::Passthrough => {
                self.queues.get_mut(&node).unwrap().push_back(tagged);
            }
        }
    }

    /// Find the next task ready to execute.
    ///
    /// Walks nodes in ranked order (closest to sink first). Returns the
    /// first node whose accumulation condition is met, along with the
    /// handles to load and concatenate.
    pub fn next_ready_task(&mut self) -> Option<ReadyTask<T::Handle>> {
        for &node in &self.ranked_order.clone() {
            match &self.batch_modes[&node].clone() {
                BatchMode::Passthrough => {
                    if let Some(ready) = self.check_passthrough(node) {
                        return Some(ready);
                    }
                }
                BatchMode::MaxRows(max_rows) => {
                    if let Some(ready) = self.check_max_rows(node, *max_rows) {
                        return Some(ready);
                    }
                }
                BatchMode::CommonOrigin => {
                    if let Some(ready) = self.check_common_origin(node) {
                        return Some(ready);
                    }
                }
            }
        }
        None
    }

    /// Check if a Passthrough node is ready to fire.
    ///
    /// Returns the next single batch from the queue — no accumulation.
    fn check_passthrough(&mut self, node: NodeIndex) -> Option<ReadyTask<T::Handle>> {
        let queue = self.queues.get_mut(&node)?;
        let tagged = queue.pop_front()?;
        let lineage = tagged.lineage.clone();
        Some(ReadyTask {
            node,
            handles: vec![tagged],
            lineage,
        })
    }

    /// Check if a MaxRows node is ready to fire.
    fn check_max_rows(&mut self, node: NodeIndex, max_rows: usize) -> Option<ReadyTask<T::Handle>> {
        let queue = self.queues.get(&node)?;
        if queue.is_empty() {
            return None;
        }

        let total_rows: usize = queue.iter().map(|t| t.num_rows).sum();
        let ready = if total_rows >= max_rows {
            true
        } else {
            // In flush mode, drain whatever is available.
            self.flushing
        };

        if !ready {
            return None;
        }

        // Drain batches up to the threshold, never exceeding max_rows.
        // A single oversized batch is still returned alone (the executor
        // will split it before execution).
        let queue = self.queues.get_mut(&node).unwrap();
        let mut handles = Vec::new();
        let mut collected_rows = 0;
        let mut lineage = BatchLineage::default();

        while let Some(front) = queue.front() {
            if !handles.is_empty() && collected_rows + front.num_rows > max_rows {
                break;
            }
            let tagged = queue.pop_front().unwrap();
            collected_rows += tagged.num_rows;
            lineage
                .origins
                .extend(tagged.lineage.origins.iter().copied());
            // Merge paths: take the longest path as representative when
            // accumulating multiple batches (they share the same route).
            if tagged.lineage.path.len() > lineage.path.len() {
                lineage.path = tagged.lineage.path.clone();
            }
            handles.push(tagged);
        }

        Some(ReadyTask {
            node,
            handles,
            lineage,
        })
    }

    /// Check if a CommonOrigin node is ready to fire.
    ///
    /// Scans all pending origins across predecessor queues and fires the
    /// first complete group — not necessarily the oldest. This avoids
    /// head-of-line blocking in distributed executors where completion
    /// order is non-deterministic.
    ///
    /// Algorithm:
    /// 1. Collect all candidate origin IDs from all predecessor queues.
    /// 2. For each candidate, attempt to build a complete group.
    /// 3. Return the first complete group found.
    fn check_common_origin(&mut self, node: NodeIndex) -> Option<ReadyTask<T::Handle>> {
        let pred_queues = self.origin_queues.get(&node)?;

        // All predecessor queues must be non-empty.
        if pred_queues.values().any(|q| q.is_empty()) {
            return None;
        }

        // Collect all distinct origin IDs present in any predecessor queue.
        let mut candidate_origins: BTreeSet<u64> = BTreeSet::new();
        for queue in pred_queues.values() {
            for tagged in queue.iter() {
                candidate_origins.extend(tagged.lineage.origins.iter().copied());
            }
        }

        // Try each candidate origin — return the first complete group.
        for &candidate in &candidate_origins {
            if let Some(ready) = self.try_match_origin(node, candidate) {
                return Some(ready);
            }
        }

        None
    }

    /// Attempt to build a complete origin group starting from `seed_origin`.
    ///
    /// Returns Some(ReadyTask) if every predecessor covers every origin
    /// in the expanded group, None otherwise.
    fn try_match_origin(
        &mut self,
        node: NodeIndex,
        seed_origin: u64,
    ) -> Option<ReadyTask<T::Handle>> {
        let pred_queues = self.origin_queues.get(&node)?;

        // Step 1: For each predecessor, find the first batch containing seed_origin.
        let mut group_origins = BTreeSet::new();
        let mut match_indices: HashMap<NodeIndex, Vec<usize>> = HashMap::new();

        for (&pred, queue) in pred_queues {
            let mut pred_indices = Vec::new();
            for (i, tagged) in queue.iter().enumerate() {
                if tagged.lineage.origins.contains(&seed_origin) {
                    group_origins.extend(tagged.lineage.origins.iter().copied());
                    pred_indices.push(i);
                    break; // Take first match per predecessor.
                }
            }
            if pred_indices.is_empty() {
                return None; // This predecessor doesn't have seed_origin yet.
            }
            match_indices.insert(pred, pred_indices);
        }

        // Step 2: Expand — pull in additional batches whose origins overlap
        // with the group until stable.
        loop {
            let mut expanded = false;
            for (&pred, queue) in pred_queues {
                let indices = match_indices.get_mut(&pred).unwrap();
                for (i, tagged) in queue.iter().enumerate() {
                    if indices.contains(&i) {
                        continue;
                    }
                    if tagged
                        .lineage
                        .origins
                        .iter()
                        .any(|o| group_origins.contains(o))
                    {
                        group_origins.extend(tagged.lineage.origins.iter().copied());
                        indices.push(i);
                        expanded = true;
                    }
                }
            }
            if !expanded {
                break;
            }
        }

        // Step 3: Verify full coverage — every predecessor must cover every
        // origin in the group.
        for (&pred, queue) in pred_queues {
            let indices = &match_indices[&pred];
            let covered: BTreeSet<u64> = indices
                .iter()
                .flat_map(|&i| queue[i].lineage.origins.iter().copied())
                .collect();
            if !group_origins.is_subset(&covered) {
                return None;
            }
        }

        // Step 4: Pop matched batches (in reverse index order to avoid shifting).
        let pred_queues = self.origin_queues.get_mut(&node).unwrap();
        let mut all_handles = Vec::new();

        for (pred, indices) in &mut match_indices {
            indices.sort_unstable();
            indices.reverse();
            let queue = pred_queues.get_mut(pred).unwrap();
            for &i in indices.iter() {
                let tagged = queue.remove(i).unwrap();
                all_handles.push(tagged);
            }
        }

        // Merge lineages from all matched handles.
        let mut lineage = BatchLineage {
            origins: group_origins,
            path: Vec::new(),
        };
        for h in &all_handles {
            if h.lineage.path.len() > lineage.path.len() {
                lineage.path = h.lineage.path.clone();
            }
        }

        Some(ReadyTask {
            node,
            handles: all_handles,
            lineage,
        })
    }

    /// Route a single stream's output to the successors that receive that stream.
    ///
    /// `stream` is the output stream index. Only successors connected to
    /// `from` on this stream will receive the handle.
    pub fn deliver_stream_output(
        &mut self,
        from: NodeIndex,
        stream: u32,
        handle: T::Handle,
        lineage: BatchLineage,
        num_rows: usize,
    ) -> Result<()> {
        let is_sink = self.sink_indices.contains(&from);

        if is_sink {
            let task_name = String::new(); // Filled by caller via deliver_sink_output.
            self.pending_sink.push_back(SinkOutput {
                handle,
                lineage,
                task_name,
            });
            return Ok(());
        }

        let succs = self
            .stream_successors
            .get(&(from, stream))
            .cloned()
            .unwrap_or_default();

        debug_assert!(
            !succs.is_empty(),
            "deliver_stream_output called for node {from:?} stream {stream} with no successors; \
             graph validation should prevent this"
        );

        // Register additional consumers for fan-out.
        let additional = succs.len().saturating_sub(1);
        if additional > 0 {
            self.transport.add_consumers(&handle, additional)?;
        }

        for &succ in &succs {
            let tagged = TaggedHandle {
                handle: handle.clone(),
                lineage: lineage.clone(),
                num_rows,
            };
            self.enqueue_to_node(succ, Some(from), tagged);
        }

        Ok(())
    }

    /// Route a multi-stream task's outputs to their respective successors.
    ///
    /// `outputs` is indexed by stream number: `outputs[i]` is `(handle, num_rows)`
    /// for stream `i`. Each stream's output is delivered only to successors
    /// connected on that stream.
    pub fn deliver_stream_outputs(
        &mut self,
        from: NodeIndex,
        outputs: Vec<(T::Handle, usize)>,
        lineage: BatchLineage,
    ) -> Result<()> {
        for (stream, (handle, num_rows)) in outputs.into_iter().enumerate() {
            self.deliver_stream_output(from, stream as u32, handle, lineage.clone(), num_rows)?;
        }
        Ok(())
    }

    /// Route a task's output to a single specific successor.
    ///
    /// Does not handle fan-out or ref counting — the caller is responsible
    /// for managing transport consumers when delivering to multiple
    /// successors individually.
    pub fn deliver_to_successor(
        &mut self,
        from: NodeIndex,
        to: NodeIndex,
        handle: T::Handle,
        lineage: BatchLineage,
        num_rows: usize,
    ) {
        let tagged = TaggedHandle {
            handle,
            lineage,
            num_rows,
        };
        self.enqueue_to_node(to, Some(from), tagged);
    }

    /// Successor node indices for a given node and stream.
    pub fn stream_successors_of(&self, node: NodeIndex, stream: u32) -> &[NodeIndex] {
        self.stream_successors
            .get(&(node, stream))
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    /// Route a sink task's output to the pending output buffer.
    pub fn deliver_sink_output(
        &mut self,
        handle: T::Handle,
        lineage: BatchLineage,
        task_name: String,
    ) {
        self.pending_sink.push_back(SinkOutput {
            handle,
            lineage,
            task_name,
        });
    }

    /// Pop the next buffered sink output, if any.
    pub fn take_sink_output(&mut self) -> Option<SinkOutput<T::Handle>> {
        self.pending_sink.pop_front()
    }

    /// Enable flush mode: MaxRows nodes will drain partial accumulations.
    pub fn mark_flush(&mut self) {
        self.flushing = true;
    }

    /// Whether all queues are empty (scheduler is done).
    pub fn is_idle(&self) -> bool {
        let queues_empty = self.queues.values().all(|q| q.is_empty());
        let origin_queues_empty = self
            .origin_queues
            .values()
            .all(|pred_map| pred_map.values().all(|q| q.is_empty()));
        queues_empty && origin_queues_empty
    }

    /// Release input handles after execution.
    pub fn release_handles(&self, handles: &[TaggedHandle<T::Handle>]) -> Result<()> {
        for tagged in handles {
            self.transport.release(&tagged.handle)?;
        }
        Ok(())
    }

    /// Load and concatenate handles into a single RecordBatch.
    pub fn load_and_concat(&self, handles: &[TaggedHandle<T::Handle>]) -> Result<RecordBatch> {
        let batches: Vec<RecordBatch> = handles
            .iter()
            .map(|t| self.transport.load(&t.handle))
            .collect::<Result<Vec<_>>>()?;

        if batches.len() == 1 {
            return Ok(batches.into_iter().next().unwrap());
        }

        let schema = batches[0].schema();
        concat_batches(&schema, &batches).map_err(RplError::Arrow)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::DefaultGenerator;
    use crate::graph::PipelineGraph;
    use crate::task::TaskDef;
    use crate::transport::memory::InMemoryTransport;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn make_batch(id: i64) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![id]))]).unwrap()
    }

    /// Result of a single step in the TestExecutor.
    #[derive(Debug)]
    #[allow(dead_code)]
    struct StepResult {
        node: NodeIndex,
        lineage: BatchLineage,
        /// Number of input handles the task received.
        input_handles: usize,
        /// Number of output rows per stream.
        output_rows: Vec<usize>,
    }

    /// Minimal executor that drives a BatchScheduler for testing.
    ///
    /// Handles single-stream and multi-stream tasks, load/concat/store/deliver.
    /// Tests can use `step()` for fine-grained control or `run_to_idle()` for
    /// outcome-focused assertions.
    struct TestExecutor {
        sched: BatchScheduler<InMemoryTransport>,
        graph: PipelineGraph,
        /// History of all steps executed.
        history: Vec<StepResult>,
    }

    impl TestExecutor {
        fn new(graph: PipelineGraph) -> Self {
            let transport = InMemoryTransport::new();
            let sched = BatchScheduler::new(&graph, transport);
            TestExecutor {
                sched,
                graph,
                history: Vec::new(),
            }
        }

        fn enqueue(&mut self, batch: &RecordBatch) {
            self.sched.enqueue_source_batch(batch).unwrap();
        }

        /// Execute the next ready task. Returns None if no task is ready.
        fn step(&mut self) -> Option<&StepResult> {
            let ready = self.sched.next_ready_task()?;
            let node = ready.node;
            let input_handles = ready.handles.len();
            let lineage = ready.lineage.clone();

            let input_batch = self.sched.load_and_concat(&ready.handles).unwrap();
            self.sched.release_handles(&ready.handles).unwrap();

            let task = self.graph.task(node);
            let outputs = task.execute(input_batch).unwrap();

            let output_rows: Vec<usize> = outputs.iter().map(|b| b.num_rows()).collect();

            if outputs.len() == 1 {
                let handle = self.sched.transport().store(&outputs[0]).unwrap();
                self.sched
                    .deliver_stream_output(node, 0, handle, lineage.clone(), outputs[0].num_rows())
                    .unwrap();
            } else {
                let stored: Vec<(_, usize)> = outputs
                    .iter()
                    .map(|b| (self.sched.transport().store(b).unwrap(), b.num_rows()))
                    .collect();
                self.sched
                    .deliver_stream_outputs(node, stored, lineage.clone())
                    .unwrap();
            }

            self.history.push(StepResult {
                node,
                lineage,
                input_handles,
                output_rows,
            });
            self.history.last()
        }

        /// Drive the scheduler until no tasks are ready.
        fn run_to_idle(&mut self) {
            while self.step().is_some() {}
        }

        /// Drive until `target` fires, returning its StepResult index.
        /// Panics if the scheduler goes idle without `target` firing.
        fn run_until(&mut self, target: NodeIndex) -> &StepResult {
            for _ in 0..100 {
                match self.step() {
                    Some(result) if result.node == target => {
                        return self.history.last().unwrap();
                    }
                    Some(_) => continue,
                    None => panic!("scheduler went idle before node {target:?} fired"),
                }
            }
            panic!("exceeded iteration limit waiting for node {target:?}");
        }

        /// All firings of a given node.
        fn firings_of(&self, node: NodeIndex) -> Vec<&StepResult> {
            self.history.iter().filter(|r| r.node == node).collect()
        }

        fn mark_flush(&mut self) {
            self.sched.mark_flush();
        }

        #[allow(dead_code)]
        fn is_idle(&self) -> bool {
            self.sched.is_idle()
        }

        #[allow(dead_code)]
        fn scheduler(&self) -> &BatchScheduler<InMemoryTransport> {
            &self.sched
        }

        #[allow(dead_code)]
        fn scheduler_mut(&mut self) -> &mut BatchScheduler<InMemoryTransport> {
            &mut self.sched
        }
    }

    #[test]
    fn maxrows_threshold() {
        // Linear: source -> task(MaxRows(3)) -> sink
        let mut graph = PipelineGraph::new();
        graph
            .add_linear(vec![
                TaskDef::passthrough("source", |b| Ok(b)),
                TaskDef::passthrough("accumulate", |b| Ok(b)).with_batch_size(3),
                TaskDef::passthrough("sink", |b| Ok(b)),
            ])
            .unwrap();
        graph.validate(&DefaultGenerator::new()).unwrap();

        let mut exec = TestExecutor::new(graph);

        // Enqueue 2 batches — after processing sources, accumulate should not fire (need 3 rows).
        exec.enqueue(&make_batch(0));
        exec.enqueue(&make_batch(1));

        // Process both source firings.
        exec.step().unwrap();
        exec.step().unwrap();

        // Accumulate node should NOT be ready yet (2 rows < 3).
        assert!(exec.step().is_none());

        // Enqueue 3rd source batch and process it.
        exec.enqueue(&make_batch(2));
        exec.step().unwrap();

        // Now accumulate should be ready with 3 rows.
        let result = exec.step().unwrap();
        assert_eq!(result.input_handles, 3);
        assert_eq!(result.lineage.origins, BTreeSet::from([0, 1, 2]));
    }

    #[test]
    fn maxrows_flush() {
        let mut graph = PipelineGraph::new();
        graph
            .add_linear(vec![
                TaskDef::passthrough("source", |b| Ok(b)),
                TaskDef::passthrough("accumulate", |b| Ok(b)).with_batch_size(5),
                TaskDef::passthrough("sink", |b| Ok(b)),
            ])
            .unwrap();
        graph.validate(&DefaultGenerator::new()).unwrap();

        let mut exec = TestExecutor::new(graph);

        // Enqueue 2 batches and process sources.
        exec.enqueue(&make_batch(0));
        exec.enqueue(&make_batch(1));
        exec.step().unwrap();
        exec.step().unwrap();

        // Accumulate not ready (2 < 5).
        assert!(exec.step().is_none());

        // Flush mode — should drain partial.
        exec.mark_flush();
        let result = exec.step().unwrap();
        assert_eq!(result.input_handles, 2);
    }

    #[test]
    fn common_origin_symmetric_diamond() {
        // A -> B -> D(CommonOrigin)
        // A -> C -> D
        let mut graph = PipelineGraph::new();
        let a = graph.add_task(TaskDef::passthrough("A", |b| Ok(b)));
        let b = graph.add_task(TaskDef::passthrough("B", |b| Ok(b)));
        let c = graph.add_task(TaskDef::passthrough("C", |b| Ok(b)));
        let d = graph.add_task(
            TaskDef::passthrough("D", |b| Ok(b)).with_batch_mode(BatchMode::CommonOrigin),
        );
        graph.add_edge(a, b).unwrap();
        graph.add_edge(a, c).unwrap();
        graph.add_edge(b, d).unwrap();
        graph.add_edge(c, d).unwrap();
        graph.validate(&DefaultGenerator::new()).unwrap();

        let mut exec = TestExecutor::new(graph);
        exec.enqueue(&make_batch(0));

        // Process A.
        exec.step().unwrap();
        // Process B or C (one branch).
        exec.step().unwrap();
        // D should NOT be ready yet — process the other branch.
        let result = exec.step().unwrap();
        assert_ne!(result.node, d);

        // Now D should fire with contributions from both B and C.
        let result = exec.step().unwrap();
        assert_eq!(result.node, d);
        assert_eq!(result.input_handles, 2);
        assert_eq!(result.lineage.origins, BTreeSet::from([0]));
    }

    #[test]
    fn common_origin_asymmetric_with_maxrows() {
        // A -> B0(MaxRows(2)) -> B -> D(CommonOrigin)
        // A -> C                  -> D
        //
        // With 2 source batches (origins 0, 1):
        //   B0 merges origins {0,1} into one batch -> B processes -> D gets from pred B
        //   C processes each origin -> D gets from pred C
        //   D should fire matching all origins.
        let mut graph = PipelineGraph::new();
        let _a = graph.add_task(TaskDef::passthrough("A", |b| Ok(b)));
        let _b0 = graph.add_task(TaskDef::passthrough("B0", |b| Ok(b)).with_batch_size(2));
        let _b = graph.add_task(TaskDef::passthrough("B", |b| Ok(b)));
        let _c = graph.add_task(TaskDef::passthrough("C", |b| Ok(b)));
        let d = graph.add_task(
            TaskDef::passthrough("D", |b| Ok(b)).with_batch_mode(BatchMode::CommonOrigin),
        );
        graph.add_edge(_a, _b0).unwrap();
        graph.add_edge(_b0, _b).unwrap();
        graph.add_edge(_a, _c).unwrap();
        graph.add_edge(_b, d).unwrap();
        graph.add_edge(_c, d).unwrap();
        graph.validate(&DefaultGenerator::new()).unwrap();

        let mut exec = TestExecutor::new(graph);
        exec.enqueue(&make_batch(0));
        exec.enqueue(&make_batch(1));
        exec.mark_flush(); // allow B0 to fire with 2 rows.

        let result = exec.run_until(d);
        assert!(result.lineage.origins.contains(&0));
        assert!(result.lineage.origins.contains(&1));
    }

    #[test]
    fn triple_fanout_common_origin() {
        // A -> {B, C, D} -> E(CommonOrigin)
        let mut graph = PipelineGraph::new();
        let _a = graph.add_task(TaskDef::passthrough("A", |b| Ok(b)));
        let _b = graph.add_task(TaskDef::passthrough("B", |b| Ok(b)));
        let _c = graph.add_task(TaskDef::passthrough("C", |b| Ok(b)));
        let _d = graph.add_task(TaskDef::passthrough("D", |b| Ok(b)));
        let e = graph.add_task(
            TaskDef::passthrough("E", |b| Ok(b)).with_batch_mode(BatchMode::CommonOrigin),
        );
        graph.add_edge(_a, _b).unwrap();
        graph.add_edge(_a, _c).unwrap();
        graph.add_edge(_a, _d).unwrap();
        graph.add_edge(_b, e).unwrap();
        graph.add_edge(_c, e).unwrap();
        graph.add_edge(_d, e).unwrap();
        graph.validate(&DefaultGenerator::new()).unwrap();

        let mut exec = TestExecutor::new(graph);
        exec.enqueue(&make_batch(0));

        let result = exec.run_until(e);
        assert_eq!(result.input_handles, 3);
        assert_eq!(result.lineage.origins, BTreeSet::from([0]));
    }

    #[test]
    fn common_origin_out_of_order_completion() {
        // A -> B -> D(CommonOrigin)
        // A -> C -> D
        //
        // Simulate distributed scenario: origin 1 completes on both branches
        // before origin 0's B-branch delivers. D should fire origin 1 first,
        // not block on origin 0.
        let mut graph = PipelineGraph::new();
        let a = graph.add_task(TaskDef::passthrough("A", |b| Ok(b)));
        let b = graph.add_task(TaskDef::passthrough("B", |b| Ok(b)));
        let c = graph.add_task(TaskDef::passthrough("C", |b| Ok(b)));
        let d = graph.add_task(
            TaskDef::passthrough("D", |b| Ok(b)).with_batch_mode(BatchMode::CommonOrigin),
        );
        graph.add_edge(a, b).unwrap();
        graph.add_edge(a, c).unwrap();
        graph.add_edge(b, d).unwrap();
        graph.add_edge(c, d).unwrap();
        graph.validate(&DefaultGenerator::new()).unwrap();

        let transport = InMemoryTransport::new();
        let mut sched = BatchScheduler::new(&graph, transport);

        // Enqueue 2 source batches (origins 0 and 1).
        sched.enqueue_source_batch(&make_batch(0)).unwrap();
        sched.enqueue_source_batch(&make_batch(1)).unwrap();

        // Process all non-D tasks (A, B, C) and collect B/C outputs by origin.
        let mut b_outputs: Vec<(RecordBatch, BatchLineage)> = Vec::new();
        let mut c_outputs: Vec<(RecordBatch, BatchLineage)> = Vec::new();

        loop {
            match sched.next_ready_task() {
                Some(ready) if ready.node == d => {
                    // D shouldn't be ready yet — but if it is, something is wrong.
                    panic!("D fired before we expected it to");
                }
                Some(ready) => {
                    let batch = sched.load_and_concat(&ready.handles).unwrap();
                    let lineage = ready.lineage.clone();
                    sched.release_handles(&ready.handles).unwrap();

                    if ready.node == b {
                        b_outputs.push((batch, lineage));
                    } else if ready.node == c {
                        c_outputs.push((batch, lineage));
                    } else {
                        // A (source) — deliver normally so B/C get input.
                        let handle = sched.transport().store(&batch).unwrap();
                        sched
                            .deliver_stream_output(ready.node, 0, handle, lineage, batch.num_rows())
                            .unwrap();
                    }
                }
                None => break,
            }
        }

        assert_eq!(b_outputs.len(), 2);
        assert_eq!(c_outputs.len(), 2);

        // Deliver to D in this order: C(0), C(1), B(1) — withholding B(0).
        for (batch, lineage) in &c_outputs {
            let handle = sched.transport().store(batch).unwrap();
            sched
                .deliver_stream_output(c, 0, handle, lineage.clone(), batch.num_rows())
                .unwrap();
        }
        let b1_idx = b_outputs
            .iter()
            .position(|(_, l)| l.origins.contains(&1))
            .unwrap();
        let (ref batch, ref lineage) = b_outputs[b1_idx];
        let handle = sched.transport().store(batch).unwrap();
        sched
            .deliver_stream_output(b, 0, handle, lineage.clone(), batch.num_rows())
            .unwrap();

        // D should fire for origin 1 (both B(1) and C(1) delivered),
        // even though origin 0's B-branch hasn't delivered yet.
        let ready = sched.next_ready_task().unwrap();
        assert_eq!(ready.node, d);
        assert!(
            ready.lineage.origins.contains(&1),
            "D should fire for origin 1 first, got origins {:?}",
            ready.lineage.origins
        );
        assert!(
            !ready.lineage.origins.contains(&0),
            "D should NOT include origin 0 (B(0) not delivered yet), got {:?}",
            ready.lineage.origins
        );

        // Now deliver B(0).
        let b0_idx = 1 - b1_idx;
        let (ref batch, ref lineage) = b_outputs[b0_idx];
        let handle = sched.transport().store(batch).unwrap();
        sched
            .deliver_stream_output(b, 0, handle, lineage.clone(), batch.num_rows())
            .unwrap();

        // D should now fire for origin 0.
        let ready = sched.next_ready_task().unwrap();
        assert_eq!(ready.node, d);
        assert_eq!(ready.lineage.origins, BTreeSet::from([0]));
    }

    #[test]
    fn multi_stream_routes_to_different_successors() {
        // source -> splitter(2 streams) -> stream0 -> sink_a
        //                                -> stream1 -> sink_b
        let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);

        let mut graph = PipelineGraph::new();
        let source = graph.add_task(TaskDef::passthrough("source", |b| Ok(b)));
        let _splitter = graph.add_task(TaskDef::new_multi_stream(
            "splitter",
            schema.clone(),
            vec![schema.clone(), schema.clone()],
            |batch| Ok(vec![batch.clone(), batch]),
        ));
        let sink_a = graph.add_task(TaskDef::passthrough("sink_a", |b| Ok(b)));
        let sink_b = graph.add_task(TaskDef::passthrough("sink_b", |b| Ok(b)));

        graph.add_edge(source, _splitter).unwrap();
        graph.add_edge_with_streams(_splitter, sink_a, vec![0]).unwrap();
        graph.add_edge_with_streams(_splitter, sink_b, vec![1]).unwrap();
        graph.validate(&DefaultGenerator::new()).unwrap();

        let mut exec = TestExecutor::new(graph);
        exec.enqueue(&make_batch(42));
        exec.run_to_idle();

        // Both sinks should have fired.
        let mut fired: Vec<_> = exec.firings_of(sink_a).iter().chain(exec.firings_of(sink_b).iter()).map(|r| r.node).collect();
        fired.sort();
        let mut expected = vec![sink_a, sink_b];
        expected.sort();
        assert_eq!(fired, expected);
    }

    #[test]
    fn multi_stream_fan_out_single_stream() {
        // source -> splitter(2 streams)
        //   stream 0 -> consumer_a AND consumer_b
        //   stream 1 -> consumer_c
        let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);

        let mut graph = PipelineGraph::new();
        let source = graph.add_task(TaskDef::passthrough("source", |b| Ok(b)));
        let splitter = graph.add_task(TaskDef::new_multi_stream(
            "splitter",
            schema.clone(),
            vec![schema.clone(), schema.clone()],
            |batch| Ok(vec![batch.clone(), batch]),
        ));
        let consumer_a = graph.add_task(TaskDef::passthrough("consumer_a", |b| Ok(b)));
        let consumer_b = graph.add_task(TaskDef::passthrough("consumer_b", |b| Ok(b)));
        let consumer_c = graph.add_task(TaskDef::passthrough("consumer_c", |b| Ok(b)));

        graph.add_edge(source, splitter).unwrap();
        graph.add_edge_with_streams(splitter, consumer_a, vec![0]).unwrap();
        graph.add_edge_with_streams(splitter, consumer_b, vec![0]).unwrap();
        graph.add_edge_with_streams(splitter, consumer_c, vec![1]).unwrap();
        graph.validate(&DefaultGenerator::new()).unwrap();

        let mut exec = TestExecutor::new(graph);
        exec.enqueue(&make_batch(1));
        exec.run_to_idle();

        // All three consumers should have fired exactly once.
        assert_eq!(exec.firings_of(consumer_a).len(), 1);
        assert_eq!(exec.firings_of(consumer_b).len(), 1);
        assert_eq!(exec.firings_of(consumer_c).len(), 1);
    }

    #[test]
    fn multi_stream_one_branch_common_origin() {
        // Splitter produces 2 streams:
        //   stream 0 -> B -> D(CommonOrigin)
        //   stream 1 -> C -> D
        // D should fire when both B and C deliver with matching origin.
        let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);

        let mut graph = PipelineGraph::new();
        let _source = graph.add_task(TaskDef::passthrough("source", |b| Ok(b)));
        let splitter = graph.add_task(TaskDef::new_multi_stream(
            "splitter",
            schema.clone(),
            vec![Schema::empty(), Schema::empty()],
            |batch| Ok(vec![batch.clone(), batch]),
        ));
        let _b = graph.add_task(TaskDef::passthrough("B", |b| Ok(b)));
        let _c = graph.add_task(TaskDef::passthrough("C", |b| Ok(b)));
        let d = graph.add_task(
            TaskDef::passthrough("D", |b| Ok(b)).with_batch_mode(BatchMode::CommonOrigin),
        );

        graph.add_edge(_source, splitter).unwrap();
        graph.add_edge_with_streams(splitter, _b, vec![0]).unwrap();
        graph.add_edge_with_streams(splitter, _c, vec![1]).unwrap();
        graph.add_edge(_b, d).unwrap();
        graph.add_edge(_c, d).unwrap();
        graph.validate(&DefaultGenerator::new()).unwrap();

        let mut exec = TestExecutor::new(graph);
        exec.enqueue(&make_batch(0));

        let result = exec.run_until(d);
        assert_eq!(result.input_handles, 2);
        assert_eq!(result.lineage.origins, BTreeSet::from([0]));
    }

    #[test]
    fn multi_stream_both_branches_common_origin() {
        // Splitter produces 2 streams, each feeding a separate CommonOrigin join:
        //   stream 0 -> B0 -> join_0(CommonOrigin)
        //   stream 1 -> B1 -> join_1(CommonOrigin)
        //   source2       -> join_0
        //   source2       -> join_1
        //
        // Both join nodes should fire independently when their predecessors deliver.
        let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);

        let mut graph = PipelineGraph::new();
        let _source = graph.add_task(TaskDef::passthrough("source", |b| Ok(b)));
        let splitter = graph.add_task(TaskDef::new_multi_stream(
            "splitter",
            schema.clone(),
            vec![Schema::empty(), Schema::empty()],
            |batch| Ok(vec![batch.clone(), batch]),
        ));
        let _b0 = graph.add_task(TaskDef::passthrough("B0", |b| Ok(b)));
        let _b1 = graph.add_task(TaskDef::passthrough("B1", |b| Ok(b)));
        let _source2 = graph.add_task(TaskDef::passthrough("source2", |b| Ok(b)));
        let join_0 = graph.add_task(
            TaskDef::passthrough("join_0", |b| Ok(b)).with_batch_mode(BatchMode::CommonOrigin),
        );
        let join_1 = graph.add_task(
            TaskDef::passthrough("join_1", |b| Ok(b)).with_batch_mode(BatchMode::CommonOrigin),
        );

        graph.add_edge(_source, splitter).unwrap();
        graph.add_edge_with_streams(splitter, _b0, vec![0]).unwrap();
        graph.add_edge_with_streams(splitter, _b1, vec![1]).unwrap();
        graph.add_edge(_b0, join_0).unwrap();
        graph.add_edge(_b1, join_1).unwrap();
        graph.add_edge(_source2, join_0).unwrap();
        graph.add_edge(_source2, join_1).unwrap();
        graph.validate(&DefaultGenerator::new()).unwrap();

        let mut exec = TestExecutor::new(graph);
        exec.enqueue(&make_batch(0));
        exec.run_to_idle();

        // Both joins should have fired with 2 handles and matching origin.
        let j0 = exec.firings_of(join_0);
        assert_eq!(j0.len(), 1);
        assert_eq!(j0[0].input_handles, 2);
        assert_eq!(j0[0].lineage.origins, BTreeSet::from([0]));

        let j1 = exec.firings_of(join_1);
        assert_eq!(j1.len(), 1);
        assert_eq!(j1[0].input_handles, 2);
        assert_eq!(j1[0].lineage.origins, BTreeSet::from([0]));
    }
}
