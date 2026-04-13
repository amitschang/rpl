//! Shared graph builders, sources, and assertion helpers for executor tests.

use crate::batch_ext::RecordBatchExt;
use crate::executor::{DefaultGenerator, Executor, SourceGenerator};
use crate::graph::PipelineGraph;
use crate::schema::schema_of;
use crate::task::{BatchMode, TaskDef};
use arrow::array::{Float64Array, Int64Array};
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

// ---- Graph fixtures -------------------------------------------------------

/// Linear: source → add_value (appends a "value" column).
pub fn linear_add_value_graph() -> PipelineGraph {
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
    graph
}

/// Diamond: branch_b and branch_c feed into merge.
pub fn diamond_graph() -> PipelineGraph {
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
    let d = graph.add_task(TaskDef::passthrough("merge", |batch| Ok(batch)));
    graph.add_edge(b, d).unwrap();
    graph.add_edge(c, d).unwrap();
    graph
}

/// Linear: source → accumulate(MaxRows(3)).
pub fn max_rows_accumulation_graph() -> PipelineGraph {
    let mut graph = PipelineGraph::new();
    graph
        .add_linear(vec![
            TaskDef::passthrough("source", |b| Ok(b)),
            TaskDef::passthrough("accumulate", |b| Ok(b)).with_batch_size(3),
        ])
        .unwrap();
    graph
}

/// Linear: process(MaxRows(3)) that records the largest batch it ever saw.
/// Returns `(graph, max_seen)`.
pub fn input_splitting_graph() -> (PipelineGraph, Arc<AtomicUsize>) {
    let max_seen = Arc::new(AtomicUsize::new(0));
    let max_seen_clone = max_seen.clone();

    let mut graph = PipelineGraph::new();
    graph
        .add_linear(vec![
            TaskDef::passthrough("process", move |b: RecordBatch| {
                max_seen_clone.fetch_max(b.num_rows(), Ordering::Relaxed);
                Ok(b)
            })
            .with_batch_mode(BatchMode::MaxRows(3)),
        ])
        .unwrap();
    (graph, max_seen)
}

// ---- Sources --------------------------------------------------------------

/// Source that emits a single batch of `n` rows (ids 0..n), then exhausts.
pub struct BigSource {
    pub schema: Schema,
    pub emitted: bool,
    pub n: usize,
}

impl BigSource {
    pub fn new(n: usize) -> Self {
        BigSource {
            schema: schema_of(&[("id", DataType::Int64)]),
            emitted: false,
            n,
        }
    }
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
        let ids: Vec<i64> = (0..self.n as i64).collect();
        Some(
            RecordBatch::try_new(
                Arc::new(self.schema.clone()),
                vec![Arc::new(Int64Array::from(ids))],
            )
            .unwrap(),
        )
    }
}

// ---- Assertion helpers ----------------------------------------------------

/// Run an executor with a linear_add_value graph, 3 batches, and verify output.
pub fn assert_linear_pipeline(executor: &mut dyn Executor) {
    let graph = linear_add_value_graph();
    let mut source = DefaultGenerator::new();
    let results: Vec<_> = executor.run(&graph, &mut source).unwrap().collect();

    assert_eq!(results.len(), 3);
    for result in &results {
        let batch = &result.as_ref().unwrap().data;
        assert_eq!(batch.num_columns(), 2); // id + value
        assert!(batch.schema().field_with_name("value").is_ok());
    }
}

/// Run an executor with a diamond graph, 1 source batch, and verify output.
pub fn assert_diamond_graph(executor: &mut dyn Executor) {
    let graph = diamond_graph();
    let mut source = DefaultGenerator::new();
    let results: Vec<_> = executor.run(&graph, &mut source).unwrap().collect();

    assert_eq!(results.len(), 2);
    let names: Vec<_> = results
        .iter()
        .map(|r| r.as_ref().unwrap().task.as_str())
        .collect();
    assert!(names.iter().all(|n| *n == "merge"));
}

/// Run an executor with a max_rows_accumulation graph, 5 batches, verify 2 outputs totalling 5 rows.
pub fn assert_max_rows_accumulation(executor: &mut dyn Executor) {
    let graph = max_rows_accumulation_graph();
    let mut source = DefaultGenerator::new();
    let results: Vec<_> = executor.run(&graph, &mut source).unwrap().collect();

    assert_eq!(results.len(), 2);
    let mut rows: Vec<usize> = results
        .iter()
        .map(|r| r.as_ref().unwrap().data.num_rows())
        .collect();
    rows.sort();
    assert!(rows.contains(&3));
    assert!(rows.contains(&2));
    assert_eq!(rows.iter().sum::<usize>(), 5);
}

/// Run an executor with input_splitting (BigSource of 10 rows, MaxRows(3)).
/// Verifies the task never sees >3 rows, and output is 4 batches totalling 10 rows.
pub fn assert_input_splitting(executor: &mut dyn Executor) {
    let (graph, max_seen) = input_splitting_graph();
    let mut source = BigSource::new(10);
    let results: Vec<_> = executor.run(&graph, &mut source).unwrap().collect();

    assert!(max_seen.load(Ordering::Relaxed) <= 3);
    assert_eq!(results.len(), 4);
    let mut rows: Vec<usize> = results
        .iter()
        .map(|r| r.as_ref().unwrap().data.num_rows())
        .collect();
    rows.sort();
    assert_eq!(rows, vec![1, 3, 3, 3]);
    assert_eq!(rows.iter().sum::<usize>(), 10);
}
