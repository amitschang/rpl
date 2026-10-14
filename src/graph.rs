//! Pipeline DAG construction and schema validation.
//!
//! [`PipelineGraph`] stores tasks as nodes in a directed acyclic graph and
//! the data-flow edges between them. Helper methods such as [`add_linear`]
//! and [`add_fan_out`] build common topologies, while [`validate`] walks
//! the graph in topological order to verify that every task's required
//! columns are provided by its upstream producers.
//!
//! [`add_linear`]: PipelineGraph::add_linear
//! [`add_fan_out`]: PipelineGraph::add_fan_out
//! [`validate`]: PipelineGraph::validate

use std::collections::HashMap;
use std::fmt;

use petgraph::Direction;
use petgraph::algo::{is_cyclic_directed, toposort};
use petgraph::graph::{DiGraph, NodeIndex};
use petgraph::visit::EdgeRef;

use crate::error::{Result, RplError};
use crate::executor::SourceGenerator;
use crate::schema::ColumnSet;
use crate::task::TaskDef;

/// Metadata on a data-flow edge.
///
/// Each edge carries the set of output stream indices from the source task
/// that should be delivered to the target task. For single-stream tasks
/// this is always `vec![0]`.
#[derive(Clone, Debug)]
pub struct EdgeDef {
    pub streams: Vec<u32>,
}

/// A reference to a task within a [`PipelineGraph`].
///
/// Derefs to [`TaskDef`], so task methods and fields (e.g. `node.name`,
/// `node.execute(batch)`) are directly accessible.
#[derive(Clone, Copy, Debug)]
pub struct Node<'a> {
    pub index: NodeIndex,
    pub is_source: bool,
    pub is_sink: bool,
    task: &'a TaskDef,
}

impl std::ops::Deref for Node<'_> {
    type Target = TaskDef;
    fn deref(&self) -> &TaskDef {
        self.task
    }
}

/// A directed acyclic graph of pipeline tasks.
///
/// Nodes are [`TaskDef`]s, edges represent data flow (output of upstream
/// becomes input of downstream). Schema compatibility is validated when
/// edges are added.
pub struct PipelineGraph {
    graph: DiGraph<TaskDef, EdgeDef>,
}

impl PipelineGraph {
    pub fn new() -> Self {
        PipelineGraph {
            graph: DiGraph::new(),
        }
    }

    /// Add a task to the graph, returning its node index.
    pub fn add_task(&mut self, task: TaskDef) -> NodeIndex {
        self.graph.add_node(task)
    }

    /// Connect two tasks with a data-flow edge on stream 0.
    ///
    /// Rejects edges that would create a cycle. Schema validation is
    /// deferred to [`validate()`](PipelineGraph::validate).
    pub fn add_edge(&mut self, from: NodeIndex, to: NodeIndex) -> Result<()> {
        self.add_edge_with_streams(from, to, vec![0])
    }

    /// Connect two tasks with a data-flow edge carrying specific stream indices.
    ///
    /// `streams` must be non-empty. Each value identifies an output stream
    /// of the `from` task whose batches will be delivered to `to`.
    pub fn add_edge_with_streams(
        &mut self,
        from: NodeIndex,
        to: NodeIndex,
        streams: Vec<u32>,
    ) -> Result<()> {
        assert!(!streams.is_empty(), "edge streams must be non-empty");
        self.graph.add_edge(from, to, EdgeDef { streams });

        // Check for cycles after adding the edge.
        if is_cyclic_directed(&self.graph) {
            let edge = self.graph.find_edge(from, to).unwrap();
            self.graph.remove_edge(edge);
            return Err(RplError::GraphError(
                "adding this edge would create a cycle".into(),
            ));
        }

        Ok(())
    }

    /// Chain a sequence of tasks into a linear pipeline, returning their node indices.
    pub fn add_linear(&mut self, tasks: Vec<TaskDef>) -> Result<Vec<NodeIndex>> {
        let indices: Vec<NodeIndex> = tasks.into_iter().map(|t| self.add_task(t)).collect();
        for window in indices.windows(2) {
            self.add_edge(window[0], window[1])?;
        }
        Ok(indices)
    }

    /// Tasks with no incoming edges (entry points of the pipeline).
    pub fn source_tasks(&self) -> Vec<NodeIndex> {
        self.graph
            .node_indices()
            .filter(|&n| self.is_source(n))
            .collect()
    }

    /// Tasks with no outgoing edges (exit points of the pipeline).
    pub fn sink_tasks(&self) -> Vec<NodeIndex> {
        self.graph
            .node_indices()
            .filter(|&n| self.is_sink(n))
            .collect()
    }

    /// Whether a node has no incoming edges.
    pub fn is_source(&self, index: NodeIndex) -> bool {
        self.graph
            .edges_directed(index, Direction::Incoming)
            .next()
            .is_none()
    }

    /// Whether a node has no outgoing edges.
    pub fn is_sink(&self, index: NodeIndex) -> bool {
        self.graph
            .edges_directed(index, Direction::Outgoing)
            .next()
            .is_none()
    }

    /// Get a [`Node`] reference by index.
    pub fn node(&self, index: NodeIndex) -> Node<'_> {
        Node {
            index,
            is_source: self.is_source(index),
            is_sink: self.is_sink(index),
            task: &self.graph[index],
        }
    }

    /// Get a reference to a task by node index.
    pub fn task(&self, index: NodeIndex) -> &TaskDef {
        &self.graph[index]
    }

    /// Get a mutable reference to a task by node index.
    pub fn task_mut(&mut self, index: NodeIndex) -> &mut TaskDef {
        &mut self.graph[index]
    }

    /// All node indices in the graph.
    pub fn node_indices(&self) -> impl Iterator<Item = NodeIndex> + '_ {
        self.graph.node_indices()
    }

    /// All nodes in the graph.
    pub fn nodes(&self) -> Vec<Node<'_>> {
        self.graph.node_indices().map(|i| self.node(i)).collect()
    }

    /// Source nodes (no incoming edges).
    pub fn source_nodes(&self) -> Vec<Node<'_>> {
        self.source_tasks()
            .into_iter()
            .map(|i| self.node(i))
            .collect()
    }

    /// Sink nodes (no outgoing edges).
    pub fn sink_nodes(&self) -> Vec<Node<'_>> {
        self.sink_tasks()
            .into_iter()
            .map(|i| self.node(i))
            .collect()
    }

    /// Predecessor node indices (upstream tasks).
    pub fn predecessors(&self, node: NodeIndex) -> Vec<NodeIndex> {
        self.graph
            .edges_directed(node, Direction::Incoming)
            .map(|e| e.source())
            .collect()
    }

    /// Successor node indices with their stream labels.
    ///
    /// Returns `(target_node, streams)` for each outgoing edge.
    pub fn successors_with_streams(&self, node: NodeIndex) -> Vec<(NodeIndex, Vec<u32>)> {
        self.graph
            .edges_directed(node, Direction::Outgoing)
            .map(|e| (e.target(), e.weight().streams.clone()))
            .collect()
    }

    /// Incoming edges with their stream labels.
    ///
    /// Returns `(source_node, streams)` for each incoming edge.
    pub fn predecessors_with_streams(&self, node: NodeIndex) -> Vec<(NodeIndex, Vec<u32>)> {
        self.graph
            .edges_directed(node, Direction::Incoming)
            .map(|e| (e.source(), e.weight().streams.clone()))
            .collect()
    }

    /// Number of successor edges (fan-out degree).
    pub fn out_degree(&self, node: NodeIndex) -> usize {
        self.graph.edges_directed(node, Direction::Outgoing).count()
    }

    /// Number of tasks in the graph.
    pub fn task_count(&self) -> usize {
        self.graph.node_count()
    }

    /// Validate the entire graph structure and schema contracts.
    ///
    /// Checks:
    /// - Graph is a DAG (no cycles).
    /// - At least one source and one sink task.
    /// - Walking the DAG in topological order, each task's `requires` columns
    ///   are available from upstream (seed schema + accumulated `produces` - `drops`).
    /// - Each task's `drops` columns are available to drop.
    pub fn validate(&self, source: &dyn SourceGenerator) -> Result<()> {
        if is_cyclic_directed(&self.graph) {
            return Err(RplError::GraphError("graph contains a cycle".into()));
        }

        let sources = self.source_tasks();
        if sources.is_empty() {
            return Err(RplError::GraphError("graph has no source tasks".into()));
        }

        if self.sink_tasks().is_empty() {
            return Err(RplError::GraphError("graph has no sink tasks".into()));
        }

        // Validate stream indices on all edges.
        for node_idx in self.graph.node_indices() {
            let task = &self.graph[node_idx];
            let num_streams = task.num_output_streams();

            if self.is_sink(node_idx) {
                // Sink tasks must produce exactly one output stream.
                if num_streams != 1 {
                    return Err(RplError::GraphError(format!(
                        "sink task '{}' has {num_streams} output streams, but sink tasks must have exactly 1",
                        task.name,
                    )));
                }
            } else {
                // Non-sink tasks: every output stream must appear on at least one outgoing edge.
                let wired: std::collections::HashSet<u32> = self
                    .successors_with_streams(node_idx)
                    .into_iter()
                    .flat_map(|(_, streams)| streams)
                    .collect();
                let all= std::collections::HashSet::from_iter(0..num_streams as u32);
                if all.symmetric_difference(&wired).next().is_some() {
                    return Err(RplError::GraphError(format!(
                        "task '{}' has {num_streams} output streams but streams {:?} are referenced in outgoing edges; \
                         all streams must be wired to a successor and must be valid",
                        task.name, wired,
                    )));
                }
            }
        }

        // Topological sort (guaranteed to succeed since we checked acyclicity).
        let topo = toposort(&self.graph, None)
            .map_err(|_| RplError::GraphError("graph contains a cycle".into()))?;

        let source_set: std::collections::HashSet<NodeIndex> = sources.into_iter().collect();
        let seed = ColumnSet::from_schema(source.produces());

        // Track per-stream available columns at each node.
        // For a node with N output streams, available_at stores the columns
        // available *after* applying each stream's produces/drops.
        let mut available_at: HashMap<NodeIndex, Vec<ColumnSet>> = HashMap::new();

        for node_idx in topo {
            let task = &self.graph[node_idx];
            let incoming = self.predecessors_with_streams(node_idx);

            // Compute available columns arriving at this node from all incoming edges.
            let available = if incoming.is_empty() {
                // Source node: seeded by the generator.
                assert!(source_set.contains(&node_idx));
                seed.clone()
            } else {
                // Merge: for each incoming edge, compute the union of columns
                // across all streams on that edge, then intersect across edges.
                let mut edge_column_sets: Vec<ColumnSet> = Vec::new();
                for (pred, streams) in &incoming {
                    let pred_outputs = &available_at[pred];
                    // Union columns across all streams on this edge.
                    let mut edge_cols: Option<ColumnSet> = None;
                    for &stream in streams {
                        let stream_cols = &pred_outputs[stream as usize];
                        edge_cols = Some(match edge_cols {
                            None => stream_cols.clone(),
                            Some(acc) => acc.intersect(stream_cols),
                        });
                    }
                    if let Some(cols) = edge_cols {
                        edge_column_sets.push(cols);
                    }
                }
                let mut iter = edge_column_sets.into_iter();
                let first = iter.next().unwrap();
                iter.fold(first, |acc, cs| acc.intersect(&cs))
            };

            // Validate requires.
            available.satisfies(task.requires(), &task.name)?;

            // Validate drops.
            available.contains_all(task.drops(), &task.name)?;

            // Build per-stream output column sets.
            let num_streams = task.num_output_streams();
            let mut stream_outputs = Vec::with_capacity(num_streams);
            for stream_schema in task.produces() {
                let mut stream_available = available.clone();
                stream_available.apply(stream_schema, task.drops());
                stream_outputs.push(stream_available);
            }

            available_at.insert(node_idx, stream_outputs);
        }

        Ok(())
    }

    /// Compute rank for each node: distance from sink (higher rank = closer to sink).
    ///
    /// Used by the streaming scheduler to prioritize tasks closer to completion.
    pub fn rank_from_sink(&self) -> std::collections::HashMap<NodeIndex, usize> {
        use petgraph::algo::dijkstra;

        let mut ranks = std::collections::HashMap::new();
        let reversed = petgraph::visit::Reversed(&self.graph);

        for &sink in &self.sink_tasks() {
            let distances = dijkstra(reversed, sink, None, |_| 1usize);
            for (node, dist) in distances {
                let entry = ranks.entry(node).or_insert(0);
                *entry = (*entry).max(dist);
            }
        }
        ranks
    }

    /// Nodes sorted by rank (closest to sink first).
    ///
    /// Useful for executors that want to prioritize completing work
    /// near the output end of the pipeline.
    pub fn ranked_nodes(&self) -> Vec<Node<'_>> {
        let ranks = self.rank_from_sink();
        let mut nodes = self.nodes();
        nodes.sort_by_key(|n| ranks.get(&n.index).copied().unwrap_or(usize::MAX));
        nodes
    }

    /// Reference to the underlying petgraph.
    pub fn inner(&self) -> &DiGraph<TaskDef, EdgeDef> {
        &self.graph
    }

    /// Print a detailed summary of the graph structure to stdout.
    ///
    /// Includes task list with roles, batch modes, resources, edges,
    /// and sink-rank priority ordering.
    pub fn print_summary(&self) {
        self.write_summary(&mut std::io::stdout().lock()).ok();
    }

    /// Write a detailed summary of the graph structure to the given writer.
    pub fn write_summary(&self, out: &mut impl std::io::Write) -> std::io::Result<()> {
        let sources = self.source_tasks().len();
        let sinks = self.sink_tasks().len();
        writeln!(
            out,
            "Pipeline: {} tasks ({sources} source, {sinks} sink), {} edges",
            self.task_count(),
            self.graph.edge_count(),
        )?;
        writeln!(out)?;

        for node in self.nodes() {
            let role = match (node.is_source, node.is_sink) {
                (true, _) => "SOURCE",
                (_, true) => "SINK  ",
                _ => "      ",
            };
            writeln!(
                out,
                "  [{role}] {name:<15} batch_mode={mode:?}  resources=(cpus={cpus}, gpus={gpus})",
                name = node.name,
                mode = node.batch_mode,
                cpus = node.resources.num_cpus,
                gpus = node.resources.num_gpus,
            )?;
        }

        writeln!(out)?;
        writeln!(out, "Edges:")?;
        for node in self.nodes() {
            for (succ, streams) in self.successors_with_streams(node.index) {
                writeln!(
                    out,
                    "  {} -> {} (streams={:?})",
                    node.name,
                    self.task(succ).name,
                    streams,
                )?;
            }
        }

        writeln!(out)?;
        writeln!(out, "Sink-rank (priority ordering):")?;
        let ranks = self.rank_from_sink();
        let mut ranked: Vec<_> = ranks.iter().collect();
        ranked.sort_by_key(|(_, r)| **r);
        for (idx, rank) in ranked {
            writeln!(out, "  {:<15} rank={}", self.task(*idx).name, rank)?;
        }
        Ok(())
    }
}

impl fmt::Display for PipelineGraph {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let sources = self.source_tasks().len();
        let sinks = self.sink_tasks().len();
        write!(
            f,
            "Pipeline({} tasks, {} edges, {sources} source, {sinks} sink)",
            self.task_count(),
            self.graph.edge_count(),
        )
    }
}

impl Default for PipelineGraph {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::DefaultGenerator;
    use crate::schema::schema_of;
    use arrow::datatypes::{DataType, Schema};

    fn passthrough_task(name: &str) -> TaskDef {
        TaskDef::passthrough(name, |batch| Ok(batch))
    }

    #[test]
    fn linear_pipeline() {
        let mut g = PipelineGraph::new();
        let ids = g
            .add_linear(vec![
                passthrough_task("a"),
                passthrough_task("b"),
                passthrough_task("c"),
            ])
            .unwrap();

        assert_eq!(ids.len(), 3);
        assert_eq!(g.source_tasks(), vec![ids[0]]);
        assert_eq!(g.sink_tasks(), vec![ids[2]]);
        assert!(g.validate(&DefaultGenerator::new()).is_ok());
    }

    #[test]
    fn schema_additive_validation() {
        // source produces {id}, A requires {id} and produces {a_col},
        // B requires {a_col} (which A provides).
        let mut g = PipelineGraph::new();
        let a = g.add_task(TaskDef::new(
            "A",
            schema_of(&[("id", DataType::Int64)]),
            schema_of(&[("a_col", DataType::Float64)]),
            |batch| Ok(batch),
        ));
        let b = g.add_task(TaskDef::new(
            "B",
            schema_of(&[("a_col", DataType::Float64)]),
            Schema::empty(),
            |batch| Ok(batch),
        ));
        g.add_edge(a, b).unwrap();
        assert!(g.validate(&DefaultGenerator::new()).is_ok());
    }

    #[test]
    fn schema_missing_requirement_rejected() {
        // B requires {x_col} which neither source nor A produce.
        let mut g = PipelineGraph::new();
        let a = g.add_task(TaskDef::passthrough("A", |batch| Ok(batch)));
        let b = g.add_task(TaskDef::new(
            "B",
            schema_of(&[("x_col", DataType::Float64)]),
            Schema::empty(),
            |batch| Ok(batch),
        ));
        g.add_edge(a, b).unwrap();
        assert!(g.validate(&DefaultGenerator::new()).is_err());
    }

    #[test]
    fn schema_drops_validated() {
        // A tries to drop "nonexistent" which isn't available.
        let mut g = PipelineGraph::new();
        g.add_task(
            TaskDef::passthrough("A", |batch| Ok(batch)).with_drops(vec!["nonexistent".into()]),
        );
        assert!(g.validate(&DefaultGenerator::new()).is_err());
    }

    #[test]
    fn schema_drop_removes_column() {
        // A drops "id", B requires "id" => should fail.
        let mut g = PipelineGraph::new();
        let a =
            g.add_task(TaskDef::passthrough("A", |batch| Ok(batch)).with_drops(vec!["id".into()]));
        let b = g.add_task(TaskDef::new(
            "B",
            schema_of(&[("id", DataType::Int64)]),
            Schema::empty(),
            |batch| Ok(batch),
        ));
        g.add_edge(a, b).unwrap();
        assert!(g.validate(&DefaultGenerator::new()).is_err());
    }

    #[test]
    fn cycle_rejected() {
        let mut g = PipelineGraph::new();
        let a = g.add_task(passthrough_task("a"));
        let b = g.add_task(passthrough_task("b"));
        g.add_edge(a, b).unwrap();
        assert!(g.add_edge(b, a).is_err());
    }

    #[test]
    fn diamond_graph() {
        let mut g = PipelineGraph::new();
        let a = g.add_task(passthrough_task("a"));
        let b = g.add_task(passthrough_task("b"));
        let c = g.add_task(passthrough_task("c"));
        let d = g.add_task(passthrough_task("d"));
        g.add_edge(a, b).unwrap();
        g.add_edge(a, c).unwrap();
        g.add_edge(b, d).unwrap();
        g.add_edge(c, d).unwrap();
        assert!(g.validate(&DefaultGenerator::new()).is_ok());

        assert_eq!(g.source_tasks(), vec![a]);
        assert_eq!(g.sink_tasks(), vec![d]);
    }

    #[test]
    fn diamond_intersection() {
        // B produces {b_col}, C produces {c_col}.
        // D requires {b_col} => should fail because c_col path doesn't have it.
        let mut g = PipelineGraph::new();
        let a = g.add_task(passthrough_task("A"));
        let b = g.add_task(TaskDef::new(
            "B",
            Schema::empty(),
            schema_of(&[("b_col", DataType::Int32)]),
            |batch| Ok(batch),
        ));
        let c = g.add_task(TaskDef::new(
            "C",
            Schema::empty(),
            schema_of(&[("c_col", DataType::Int32)]),
            |batch| Ok(batch),
        ));
        let d = g.add_task(TaskDef::new(
            "D",
            schema_of(&[("b_col", DataType::Int32)]),
            Schema::empty(),
            |batch| Ok(batch),
        ));
        g.add_edge(a, b).unwrap();
        g.add_edge(a, c).unwrap();
        g.add_edge(b, d).unwrap();
        g.add_edge(c, d).unwrap();
        assert!(g.validate(&DefaultGenerator::new()).is_err());
    }

    #[test]
    fn rank_from_sink_linear() {
        let mut g = PipelineGraph::new();
        let ids = g
            .add_linear(vec![
                passthrough_task("a"),
                passthrough_task("b"),
                passthrough_task("c"),
            ])
            .unwrap();
        let ranks = g.rank_from_sink();
        assert_eq!(ranks[&ids[2]], 0);
        assert_eq!(ranks[&ids[1]], 1);
        assert_eq!(ranks[&ids[0]], 2);
    }

    #[test]
    fn multi_stream_validation_accepts_valid_streams() {
        // A produces 2 streams, stream 0 -> B, stream 1 -> C.
        let mut g = PipelineGraph::new();
        let a = g.add_task(TaskDef::new_multi_stream(
            "A",
            schema_of(&[("id", DataType::Int64)]),
            vec![
                schema_of(&[("a_out", DataType::Float64)]),
                schema_of(&[("b_out", DataType::Utf8)]),
            ],
            |batch| Ok(vec![batch.clone(), batch]),
        ));
        let b = g.add_task(TaskDef::new(
            "B",
            schema_of(&[("a_out", DataType::Float64)]),
            Schema::empty(),
            |batch| Ok(batch),
        ));
        let c = g.add_task(TaskDef::new(
            "C",
            schema_of(&[("b_out", DataType::Utf8)]),
            Schema::empty(),
            |batch| Ok(batch),
        ));
        g.add_edge_with_streams(a, b, vec![0]).unwrap();
        g.add_edge_with_streams(a, c, vec![1]).unwrap();
        assert!(g.validate(&DefaultGenerator::new()).is_ok());
    }

    #[test]
    fn multi_stream_invalid_stream_index_rejected() {
        // A has 1 output stream but edge references stream 1.
        let mut g = PipelineGraph::new();
        let a = g.add_task(passthrough_task("A"));
        let b = g.add_task(passthrough_task("B"));
        g.add_edge_with_streams(a, b, vec![1]).unwrap();
        let err = g.validate(&DefaultGenerator::new()).unwrap_err();
        assert!(
            err.to_string().contains("has 1 output streams but streams {1} are referenced") &&
             err.to_string().contains("all streams must be wired to a successor and must be valid"),
            "expected stream index error, got: {err}"
        );
    }

    #[test]
    fn multi_stream_schema_mismatch_rejected() {
        // A produces 2 streams: stream 0 has {x_col}, stream 1 has {y_col}.
        // B requires {y_col} but receives stream 0 (which only has {x_col}).
        let mut g = PipelineGraph::new();
        let a = g.add_task(TaskDef::new_multi_stream(
            "A",
            schema_of(&[("id", DataType::Int64)]),
            vec![
                schema_of(&[("x_col", DataType::Int32)]),
                schema_of(&[("y_col", DataType::Int32)]),
            ],
            |batch| Ok(vec![batch.clone(), batch]),
        ));
        let b = g.add_task(TaskDef::new(
            "B",
            schema_of(&[("y_col", DataType::Int32)]),
            Schema::empty(),
            |batch| Ok(batch),
        ));
        // Wire stream 0 to B — B needs y_col which only stream 1 has.
        g.add_edge_with_streams(a, b, vec![0]).unwrap();
        assert!(g.validate(&DefaultGenerator::new()).is_err());
    }

    #[test]
    fn multi_stream_edge_with_multiple_streams() {
        // A produces 2 streams with different schemas.
        // B receives both streams via a single edge — available columns are
        // the intersection of the two stream schemas (plus seed columns).
        let mut g = PipelineGraph::new();
        let a = g.add_task(TaskDef::new_multi_stream(
            "A",
            schema_of(&[("id", DataType::Int64)]),
            vec![
                schema_of(&[("common", DataType::Int32), ("only_s0", DataType::Int32)]),
                schema_of(&[("common", DataType::Int32), ("only_s1", DataType::Int32)]),
            ],
            |batch| Ok(vec![batch.clone(), batch]),
        ));
        // B requires "common" which both streams produce — should pass.
        let b = g.add_task(TaskDef::new(
            "B",
            schema_of(&[("common", DataType::Int32)]),
            Schema::empty(),
            |batch| Ok(batch),
        ));
        g.add_edge_with_streams(a, b, vec![0, 1]).unwrap();
        assert!(g.validate(&DefaultGenerator::new()).is_ok());

        // C requires "only_s0" which only stream 0 has — fails with both streams.
        let mut g2 = PipelineGraph::new();
        let a2 = g2.add_task(TaskDef::new_multi_stream(
            "A",
            schema_of(&[("id", DataType::Int64)]),
            vec![
                schema_of(&[("common", DataType::Int32), ("only_s0", DataType::Int32)]),
                schema_of(&[("common", DataType::Int32), ("only_s1", DataType::Int32)]),
            ],
            |batch| Ok(vec![batch.clone(), batch]),
        ));
        let c = g2.add_task(TaskDef::new(
            "C",
            schema_of(&[("only_s0", DataType::Int32)]),
            Schema::empty(),
            |batch| Ok(batch),
        ));
        g2.add_edge_with_streams(a2, c, vec![0, 1]).unwrap();
        assert!(g2.validate(&DefaultGenerator::new()).is_err());
    }

    #[test]
    fn multi_stream_successors_with_streams() {
        let mut g = PipelineGraph::new();
        let a = g.add_task(TaskDef::new_multi_stream(
            "A",
            Schema::empty(),
            vec![Schema::empty(), Schema::empty(), Schema::empty()],
            |_| Ok(vec![]),
        ));
        let b = g.add_task(passthrough_task("B"));
        let c = g.add_task(passthrough_task("C"));

        g.add_edge_with_streams(a, b, vec![0, 2]).unwrap();
        g.add_edge_with_streams(a, c, vec![1]).unwrap();

        let succs = g.successors_with_streams(a);
        assert_eq!(succs.len(), 2);
        // Find the edge to B and C.
        let to_b = succs.iter().find(|(n, _)| *n == b).unwrap();
        let to_c = succs.iter().find(|(n, _)| *n == c).unwrap();
        assert_eq!(to_b.1, vec![0, 2]);
        assert_eq!(to_c.1, vec![1]);
    }

    #[test]
    fn multi_stream_sink_rejected() {
        // Sink task with 2 output streams should be rejected.
        let mut g = PipelineGraph::new();
        g.add_task(TaskDef::new_multi_stream(
            "multi_sink",
            schema_of(&[("id", DataType::Int64)]),
            vec![Schema::empty(), Schema::empty()],
            |batch| Ok(vec![batch.clone(), batch]),
        ));
        let err = g.validate(&DefaultGenerator::new()).unwrap_err();
        assert!(
            err.to_string().contains("sink task") && err.to_string().contains("exactly 1"),
            "expected sink stream count error, got: {err}"
        );
    }

    #[test]
    fn unwired_stream_rejected() {
        // A produces 2 streams but only stream 0 is wired.
        let mut g = PipelineGraph::new();
        let a = g.add_task(TaskDef::new_multi_stream(
            "A",
            schema_of(&[("id", DataType::Int64)]),
            vec![Schema::empty(), Schema::empty()],
            |batch| Ok(vec![batch.clone(), batch]),
        ));
        let b = g.add_task(passthrough_task("B"));
        g.add_edge_with_streams(a, b, vec![0]).unwrap();
        let err = g.validate(&DefaultGenerator::new()).unwrap_err();
        assert!(
            err.to_string().contains("all streams must be wired"),
            "expected unwired stream error, got: {err}"
        );
    }

    #[test]
    fn all_streams_wired_accepted() {
        // A produces 2 streams, both wired — should pass.
        let mut g = PipelineGraph::new();
        let a = g.add_task(TaskDef::new_multi_stream(
            "A",
            schema_of(&[("id", DataType::Int64)]),
            vec![Schema::empty(), Schema::empty()],
            |batch| Ok(vec![batch.clone(), batch]),
        ));
        let b = g.add_task(passthrough_task("B"));
        let c = g.add_task(passthrough_task("C"));
        g.add_edge_with_streams(a, b, vec![0]).unwrap();
        g.add_edge_with_streams(a, c, vec![1]).unwrap();
        assert!(g.validate(&DefaultGenerator::new()).is_ok());
    }
}
