use std::collections::{BTreeSet, HashMap, VecDeque};

use arrow::compute::concat_batches;
use arrow::record_batch::RecordBatch;
use petgraph::graph::NodeIndex;

use crate::error::{Result, RplError};
use crate::executor::BatchLineage;
use crate::graph::PipelineGraph;
use crate::task::BatchMode;
use crate::transport::DataTransport;

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
    /// Successor lists per node.
    successors: HashMap<NodeIndex, Vec<NodeIndex>>,
    /// Predecessor lists per node.
    #[allow(dead_code)]
    predecessors: HashMap<NodeIndex, Vec<NodeIndex>>,

    // --- Queues ---
    /// Input queues for non-CommonOrigin nodes.
    queues: HashMap<NodeIndex, VecDeque<TaggedHandle<T::Handle>>>,
    /// Per-predecessor input queues for CommonOrigin nodes.
    /// Key: (receiving node) -> (predecessor node) -> queue of tagged handles.
    origin_queues: HashMap<NodeIndex, HashMap<NodeIndex, VecDeque<TaggedHandle<T::Handle>>>>,

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
        let mut successors_map = HashMap::new();
        let mut predecessors_map = HashMap::new();
        let mut queues = HashMap::new();
        let mut origin_queues = HashMap::new();

        for node in graph.node_indices() {
            let task = graph.task(node);
            batch_modes.insert(node, task.batch_mode.clone());
            successors_map.insert(node, graph.successors(node));
            let preds = graph.predecessors(node);
            predecessors_map.insert(node, preds.clone());

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
            successors: successors_map,
            predecessors: predecessors_map,
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
            lineage.origins.extend(tagged.lineage.origins.iter().copied());
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
                    if tagged.lineage.origins.iter().any(|o| group_origins.contains(o)) {
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

    /// Route a task's output to its successors.
    ///
    /// Call this after executing a task and storing the result.
    /// Handles fan-out by adding consumers to the transport.
    pub fn deliver_output(
        &mut self,
        from: NodeIndex,
        handle: T::Handle,
        lineage: BatchLineage,
        num_rows: usize,
    ) -> Result<()> {
        let succs = self.successors[&from].clone();
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

        if succs.is_empty() {
            // No successors and not tracked as sink — release.
            self.transport.release(&handle)?;
            return Ok(());
        }

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

    /// Route a task's output to a single specific successor.
    ///
    /// Unlike [`deliver_output`](Self::deliver_output), this does not handle
    /// fan-out or ref counting — the caller is responsible for managing
    /// transport consumers when delivering to multiple successors individually.
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

    /// Successor node indices for a given node.
    pub fn successors_of(&self, node: NodeIndex) -> &[NodeIndex] {
        // Return slice from the stored Vec.
        self.successors.get(&node).map(|v| v.as_slice()).unwrap_or(&[])
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
        concat_batches(&schema, &batches).map_err(|e| RplError::Arrow(e))
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

        let transport = InMemoryTransport::new();
        let mut sched = BatchScheduler::new(&graph, transport);

        // Enqueue 2 batches — should not be ready (need 3 rows).
        sched.enqueue_source_batch(&make_batch(0)).unwrap();
        sched.enqueue_source_batch(&make_batch(1)).unwrap();

        // Source node (MaxRows(1)) should be ready.
        let ready = sched.next_ready_task().unwrap();
        // Process source — just pass through and deliver.
        let batch = sched.load_and_concat(&ready.handles).unwrap();
        sched.release_handles(&ready.handles).unwrap();
        let handle = sched.transport().store(&batch).unwrap();
        sched.deliver_output(ready.node, handle, ready.lineage, batch.num_rows()).unwrap();

        // Process second source batch.
        let ready = sched.next_ready_task().unwrap();
        let batch = sched.load_and_concat(&ready.handles).unwrap();
        sched.release_handles(&ready.handles).unwrap();
        let handle = sched.transport().store(&batch).unwrap();
        sched.deliver_output(ready.node, handle, ready.lineage, batch.num_rows()).unwrap();

        // Accumulate node should NOT be ready yet (2 rows < 3).
        // Next ready should be None (sink has nothing, accumulate not ready).
        assert!(sched.next_ready_task().is_none());

        // Enqueue 3rd source batch.
        sched.enqueue_source_batch(&make_batch(2)).unwrap();
        let ready = sched.next_ready_task().unwrap();
        let batch = sched.load_and_concat(&ready.handles).unwrap();
        sched.release_handles(&ready.handles).unwrap();
        let handle = sched.transport().store(&batch).unwrap();
        sched.deliver_output(ready.node, handle, ready.lineage, batch.num_rows()).unwrap();

        // Now accumulate should be ready with 3 rows.
        let ready = sched.next_ready_task().unwrap();
        assert_eq!(ready.handles.len(), 3);
        let batch = sched.load_and_concat(&ready.handles).unwrap();
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(ready.lineage.origins, BTreeSet::from([0, 1, 2]));
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

        let transport = InMemoryTransport::new();
        let mut sched = BatchScheduler::new(&graph, transport);

        // Enqueue 2 batches and process sources.
        for i in 0..2 {
            sched.enqueue_source_batch(&make_batch(i)).unwrap();
            let ready = sched.next_ready_task().unwrap();
            let batch = sched.load_and_concat(&ready.handles).unwrap();
            sched.release_handles(&ready.handles).unwrap();
            let handle = sched.transport().store(&batch).unwrap();
            sched.deliver_output(ready.node, handle, ready.lineage, batch.num_rows()).unwrap();
        }

        // Accumulate not ready (2 < 5).
        assert!(sched.next_ready_task().is_none());

        // Flush mode — should drain partial.
        sched.mark_flush();
        let ready = sched.next_ready_task().unwrap();
        assert_eq!(ready.handles.len(), 2);
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

        let transport = InMemoryTransport::new();
        let mut sched = BatchScheduler::new(&graph, transport);

        // Enqueue source batch.
        sched.enqueue_source_batch(&make_batch(0)).unwrap();

        // Process A (source).
        let ready = sched.next_ready_task().unwrap();
        let batch = sched.load_and_concat(&ready.handles).unwrap();
        sched.release_handles(&ready.handles).unwrap();
        let handle = sched.transport().store(&batch).unwrap();
        sched.deliver_output(ready.node, handle, ready.lineage, batch.num_rows()).unwrap();

        // D should NOT be ready yet (only one predecessor has delivered).
        // Should get B or C next.
        let ready = sched.next_ready_task().unwrap();
        let first_name = if ready.node == b { "B" } else { "C" };
        let batch = sched.load_and_concat(&ready.handles).unwrap();
        sched.release_handles(&ready.handles).unwrap();
        let handle = sched.transport().store(&batch).unwrap();
        sched.deliver_output(ready.node, handle, ready.lineage, batch.num_rows()).unwrap();

        // D still not ready (missing one predecessor).
        // Should get the other branch.
        let ready = sched.next_ready_task().unwrap();
        let second_name = if ready.node == b { "B" } else { "C" };
        assert_ne!(first_name, second_name);
        let batch = sched.load_and_concat(&ready.handles).unwrap();
        sched.release_handles(&ready.handles).unwrap();
        let handle = sched.transport().store(&batch).unwrap();
        sched.deliver_output(ready.node, handle, ready.lineage, batch.num_rows()).unwrap();

        // Now D should be ready with contributions from both B and C.
        let ready = sched.next_ready_task().unwrap();
        assert_eq!(ready.node, d);
        assert_eq!(ready.handles.len(), 2);
        assert_eq!(ready.lineage.origins, BTreeSet::from([0]));
    }

    #[test]
    fn common_origin_asymmetric_with_maxrows() {
        // A -> B0(MaxRows(2)) -> B -> D(CommonOrigin)
        // A -> C                  -> D
        //
        // With 2 source batches (origins 0, 1):
        //   B0 merges origins {0,1} into one batch -> B processes -> D gets b_{0+1} from pred B
        //   C processes origin 0 -> D gets c_0 from pred C
        //   C processes origin 1 -> D gets c_1 from pred C
        //   D should fire: b_{0+1} (origins={0,1}) matched with c_0+c_1 (each covering one origin).

        let mut graph = PipelineGraph::new();
        let a = graph.add_task(TaskDef::passthrough("A", |b| Ok(b)));
        let b0 = graph.add_task(
            TaskDef::passthrough("B0", |b| Ok(b)).with_batch_size(2),
        );
        let b = graph.add_task(TaskDef::passthrough("B", |b| Ok(b)));
        let c = graph.add_task(TaskDef::passthrough("C", |b| Ok(b)));
        let d = graph.add_task(
            TaskDef::passthrough("D", |b| Ok(b)).with_batch_mode(BatchMode::CommonOrigin),
        );
        graph.add_edge(a, b0).unwrap();
        graph.add_edge(b0, b).unwrap();
        graph.add_edge(a, c).unwrap();
        graph.add_edge(b, d).unwrap();
        graph.add_edge(c, d).unwrap();
        graph.validate(&DefaultGenerator::new()).unwrap();

        let transport = InMemoryTransport::new();
        let mut sched = BatchScheduler::new(&graph, transport);

        // Helper: process all ready non-D tasks until nothing else fires.
        fn process_non_target<T: DataTransport>(
            sched: &mut BatchScheduler<T>,
            target: NodeIndex,
        ) -> bool {
            let mut did_work = false;
            loop {
                let ready = match sched.next_ready_task() {
                    Some(r) if r.node != target => r,
                    Some(r) => {
                        // Put handles back — not a great API but for test purposes,
                        // we re-enqueue. Instead, let's just check after processing everything.
                        // Actually, we can't un-pop. Let's just process target too and break.
                        let batch = sched.load_and_concat(&r.handles).unwrap();
                        sched.release_handles(&r.handles).unwrap();
                        let handle = sched.transport().store(&batch).unwrap();
                        sched.deliver_output(r.node, handle, r.lineage, batch.num_rows()).unwrap();
                        return true;
                    }
                    None => break,
                };
                let batch = sched.load_and_concat(&ready.handles).unwrap();
                sched.release_handles(&ready.handles).unwrap();
                let handle = sched.transport().store(&batch).unwrap();
                sched
                    .deliver_output(ready.node, handle, ready.lineage, batch.num_rows())
                    .unwrap();
                did_work = true;
            }
            did_work
        }

        // Enqueue 2 source batches.
        sched.enqueue_source_batch(&make_batch(0)).unwrap();
        sched.enqueue_source_batch(&make_batch(1)).unwrap();

        // Process everything until D fires.
        // Keep stepping until scheduler is idle or D fires.
        sched.mark_flush(); // allow B0 to fire with 2 rows.

        let mut d_fired = false;
        for _ in 0..20 {
            if process_non_target(&mut sched, d) {
                // D may have been processed.
            }
            // Check if D has fired by seeing if sink output is available or queues are idle.
            if sched.is_idle() {
                d_fired = true;
                break;
            }
        }

        assert!(d_fired, "D should have fired after all branches delivered");
    }

    #[test]
    fn triple_fanout_common_origin() {
        // A -> {B, C, D} -> E(CommonOrigin)
        let mut graph = PipelineGraph::new();
        let a = graph.add_task(TaskDef::passthrough("A", |b| Ok(b)));
        let b = graph.add_task(TaskDef::passthrough("B", |b| Ok(b)));
        let c = graph.add_task(TaskDef::passthrough("C", |b| Ok(b)));
        let d = graph.add_task(TaskDef::passthrough("D", |b| Ok(b)));
        let e = graph.add_task(
            TaskDef::passthrough("E", |b| Ok(b)).with_batch_mode(BatchMode::CommonOrigin),
        );
        graph.add_edge(a, b).unwrap();
        graph.add_edge(a, c).unwrap();
        graph.add_edge(a, d).unwrap();
        graph.add_edge(b, e).unwrap();
        graph.add_edge(c, e).unwrap();
        graph.add_edge(d, e).unwrap();
        graph.validate(&DefaultGenerator::new()).unwrap();

        let transport = InMemoryTransport::new();
        let mut sched = BatchScheduler::new(&graph, transport);

        sched.enqueue_source_batch(&make_batch(0)).unwrap();

        // Process all non-E tasks.
        let mut processed_e = false;
        for _ in 0..20 {
            match sched.next_ready_task() {
                Some(ready) => {
                    if ready.node == e {
                        // E should have 3 handles (one from each predecessor).
                        assert_eq!(ready.handles.len(), 3);
                        assert_eq!(ready.lineage.origins, BTreeSet::from([0]));
                        processed_e = true;
                        break;
                    }
                    let batch = sched.load_and_concat(&ready.handles).unwrap();
                    sched.release_handles(&ready.handles).unwrap();
                    let handle = sched.transport().store(&batch).unwrap();
                    sched
                        .deliver_output(ready.node, handle, ready.lineage, batch.num_rows())
                        .unwrap();
                }
                None => break,
            }
        }
        assert!(processed_e, "E should have fired with 3 predecessor contributions");
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
                            .deliver_output(ready.node, handle, lineage, batch.num_rows())
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
            sched.deliver_output(c, handle, lineage.clone(), batch.num_rows()).unwrap();
        }
        let b1_idx = b_outputs.iter().position(|(_, l)| l.origins.contains(&1)).unwrap();
        let (ref batch, ref lineage) = b_outputs[b1_idx];
        let handle = sched.transport().store(batch).unwrap();
        sched.deliver_output(b, handle, lineage.clone(), batch.num_rows()).unwrap();

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
        sched.deliver_output(b, handle, lineage.clone(), batch.num_rows()).unwrap();

        // D should now fire for origin 0.
        let ready = sched.next_ready_task().unwrap();
        assert_eq!(ready.node, d);
        assert_eq!(ready.lineage.origins, BTreeSet::from([0]));
    }
}
