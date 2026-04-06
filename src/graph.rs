use std::collections::HashMap;

use petgraph::algo::{is_cyclic_directed, toposort};
use petgraph::graph::{DiGraph, NodeIndex};
use petgraph::visit::EdgeRef;
use petgraph::Direction;

use crate::error::{Result, RplError};
use crate::executor::SourceGenerator;
use crate::schema::ColumnSet;
use crate::task::TaskDef;

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
    graph: DiGraph<TaskDef, ()>,
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

    /// Connect two tasks with a data-flow edge.
    ///
    /// Rejects edges that would create a cycle. Schema validation is
    /// deferred to [`validate()`](PipelineGraph::validate).
    pub fn add_edge(&mut self, from: NodeIndex, to: NodeIndex) -> Result<()> {
        self.graph.add_edge(from, to, ());

        // Check for cycles after adding the edge.
        if is_cyclic_directed(&self.graph) {
            let edge = self.graph.find_edge(from, to).unwrap();
            self.graph.remove_edge(edge);
            return Err(RplError::GraphError("adding this edge would create a cycle".into()));
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
        self.graph.edges_directed(index, Direction::Incoming).next().is_none()
    }

    /// Whether a node has no outgoing edges.
    pub fn is_sink(&self, index: NodeIndex) -> bool {
        self.graph.edges_directed(index, Direction::Outgoing).next().is_none()
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
        self.source_tasks().into_iter().map(|i| self.node(i)).collect()
    }

    /// Sink nodes (no outgoing edges).
    pub fn sink_nodes(&self) -> Vec<Node<'_>> {
        self.sink_tasks().into_iter().map(|i| self.node(i)).collect()
    }

    /// Predecessor node indices (upstream tasks).
    pub fn predecessors(&self, node: NodeIndex) -> Vec<NodeIndex> {
        self.graph
            .edges_directed(node, Direction::Incoming)
            .map(|e| e.source())
            .collect()
    }

    /// Successor node indices (downstream tasks).
    pub fn successors(&self, node: NodeIndex) -> Vec<NodeIndex> {
        self.graph
            .edges_directed(node, Direction::Outgoing)
            .map(|e| e.target())
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

        // Topological sort (guaranteed to succeed since we checked acyclicity).
        let topo = toposort(&self.graph, None)
            .map_err(|_| RplError::GraphError("graph contains a cycle".into()))?;

        let source_set: std::collections::HashSet<NodeIndex> = sources.into_iter().collect();
        let seed = ColumnSet::from_schema(source.produces());

        let mut available_at: HashMap<NodeIndex, ColumnSet> = HashMap::new();

        for node_idx in topo {
            let task = &self.graph[node_idx];
            let preds = self.predecessors(node_idx);

            // Compute available columns at this node.
            let mut available = if preds.is_empty() {
                // Source node: seeded by the generator.
                assert!(source_set.contains(&node_idx));
                seed.clone()
            } else {
                // Merge: intersection of all predecessor outputs.
                let mut iter = preds.iter().map(|p| available_at[p].clone());
                let first = iter.next().unwrap();
                iter.fold(first, |acc, cs| acc.intersect(&cs))
            };

            // Validate requires.
            available.satisfies(task.requires(), &task.name)?;

            // Validate drops.
            available.contains_all(task.drops(), &task.name)?;

            // Apply this task's effect.
            available.apply(task.produces(), task.drops());

            available_at.insert(node_idx, available);
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
    pub fn inner(&self) -> &DiGraph<TaskDef, ()> {
        &self.graph
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
    use arrow::datatypes::{DataType, Field, Schema};

    fn schema(fields: &[(&str, DataType)]) -> Schema {
        Schema::new(
            fields
                .iter()
                .map(|(name, dt)| Field::new(*name, dt.clone(), true))
                .collect::<Vec<_>>(),
        )
    }

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
            schema(&[("id", DataType::Int64)]),
            schema(&[("a_col", DataType::Float64)]),
            |batch| Ok(batch),
        ));
        let b = g.add_task(TaskDef::new(
            "B",
            schema(&[("a_col", DataType::Float64)]),
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
            schema(&[("x_col", DataType::Float64)]),
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
            TaskDef::passthrough("A", |batch| Ok(batch))
                .with_drops(vec!["nonexistent".into()]),
        );
        assert!(g.validate(&DefaultGenerator::new()).is_err());
    }

    #[test]
    fn schema_drop_removes_column() {
        // A drops "id", B requires "id" => should fail.
        let mut g = PipelineGraph::new();
        let a = g.add_task(
            TaskDef::passthrough("A", |batch| Ok(batch))
                .with_drops(vec!["id".into()]),
        );
        let b = g.add_task(TaskDef::new(
            "B",
            schema(&[("id", DataType::Int64)]),
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
            schema(&[("b_col", DataType::Int32)]),
            |batch| Ok(batch),
        ));
        let c = g.add_task(TaskDef::new(
            "C",
            Schema::empty(),
            schema(&[("c_col", DataType::Int32)]),
            |batch| Ok(batch),
        ));
        let d = g.add_task(TaskDef::new(
            "D",
            schema(&[("b_col", DataType::Int32)]),
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
}
