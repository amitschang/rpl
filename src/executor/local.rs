//! Serial single-threaded executor.
//!
//! [`LocalExecutor`] runs all tasks in the current thread, processing one
//! batch at a time. Useful for development, testing, and debugging since
//! execution order is fully deterministic.

use std::collections::VecDeque;
use std::time::Instant;

use crate::batch_ext::RecordBatchExt;
use crate::error::Result;
use crate::executor::scheduler::BatchScheduler;
use crate::executor::{Executor, OutputBatch, PathStep, SourceGenerator, next_exec_id};
use crate::graph::PipelineGraph;
use crate::task::BatchMode;
use crate::transport::DataTransport;
use crate::transport::memory::InMemoryTransport;

/// Serial, single-threaded executor for testing and development.
///
/// Processes batches one at a time from source to sink, prioritizing
/// tasks closer to the sink. Returns a lazy iterator — each call to
/// `next()` advances the pipeline until a sink task produces output.
pub struct LocalExecutor {
    max_batches: Option<usize>,
}

impl LocalExecutor {
    pub fn new() -> Self {
        LocalExecutor { max_batches: None }
    }

    /// Limit the number of source batches generated.
    pub fn with_max_batches(mut self, n: usize) -> Self {
        self.max_batches = Some(n);
        self
    }
}

impl Default for LocalExecutor {
    fn default() -> Self {
        Self::new()
    }
}

impl Executor for LocalExecutor {
    fn run<'a>(
        &'a mut self,
        graph: &'a PipelineGraph,
        source: &'a mut dyn SourceGenerator,
    ) -> Result<Box<dyn Iterator<Item = Result<OutputBatch>> + 'a>> {
        graph.validate(source)?;

        let transport = InMemoryTransport::new();
        let scheduler = BatchScheduler::new(graph, transport);

        Ok(Box::new(LocalIter {
            graph,
            scheduler,
            source,
            max_batches: self.max_batches,
            batch_count: 0,
            pending_outputs: VecDeque::new(),
            done: false,
        }))
    }
}

/// Lazy iterator that drives the local pipeline.
///
/// Each call to `next()` processes tasks until a sink produces output,
/// or the pipeline is exhausted.
struct LocalIter<'a> {
    graph: &'a PipelineGraph,
    scheduler: BatchScheduler<InMemoryTransport>,
    source: &'a mut dyn SourceGenerator,
    max_batches: Option<usize>,
    batch_count: usize,
    /// Buffer for sink outputs produced during a single scheduling round.
    /// We may produce multiple sink outputs in one round (e.g. diamond graph),
    /// so we buffer them and drain one per `next()` call.
    pending_outputs: VecDeque<Result<OutputBatch>>,
    done: bool,
}

impl<'a> LocalIter<'a> {
    /// Run one scheduling round: feed sources, then process tasks in
    /// sink-priority order until at least one sink output is produced
    /// or no more work can be done.
    fn step(&mut self) -> bool {
        if self.done {
            return false;
        }

        // Feed source tasks when under limit.
        match super::feed_source(
            &mut *self.source,
            &mut self.scheduler,
            self.max_batches,
            &mut self.batch_count,
        ) {
            super::FeedResult::Fed => {}
            super::FeedResult::Exhausted => self.scheduler.mark_flush(),
            super::FeedResult::Error(e) => {
                self.pending_outputs.push_back(Err(e));
                self.done = true;
                return true;
            }
        }

        // Process tasks via scheduler until no more are ready.
        let mut did_work = false;

        while let Some(ready) = self.scheduler.next_ready_task() {
            let node_idx = ready.node;
            let lineage = ready.lineage;

            // Load and concatenate input batches.
            let input = match self.scheduler.load_and_concat(&ready.handles) {
                Ok(batch) => batch,
                Err(e) => {
                    self.pending_outputs.push_back(Err(e));
                    self.done = true;
                    return true;
                }
            };

            // Release input handles.
            if let Err(e) = self.scheduler.release_handles(&ready.handles) {
                self.pending_outputs.push_back(Err(e));
                self.done = true;
                return true;
            }

            // Split oversized input for MaxRows tasks so the task never
            // receives more than `n` rows.
            let task = self.graph.task(node_idx);
            let input_chunks: Vec<_> = match &task.batch_mode {
                BatchMode::MaxRows(n) if input.num_rows() > *n => input.split(*n),
                _ => vec![input],
            };

            for input_chunk in input_chunks {
                // Execute the task on each chunk, measuring time.
                let started = Instant::now();
                let output = match task.execute(input_chunk) {
                    Ok(batch) => batch,
                    Err(e) => {
                        self.pending_outputs.push_back(Err(e));
                        self.done = true;
                        return true;
                    }
                };
                let exec_duration = started.elapsed();

                let step = PathStep {
                    exec_id: next_exec_id(),
                    task: task.name.clone(),
                    exec_duration,
                    wall_duration: exec_duration,
                };
                let mut new_lineage = lineage.clone();
                new_lineage.path.push(step);

                let num_rows = output.num_rows();
                let is_sink = self.graph.is_sink(node_idx);

                if is_sink {
                    self.pending_outputs.push_back(Ok(OutputBatch {
                        data: output,
                        task: task.name.clone(),
                        lineage: new_lineage,
                    }));
                } else {
                    let handle = match self.scheduler.transport().store(&output) {
                        Ok(h) => h,
                        Err(e) => {
                            self.pending_outputs.push_back(Err(e));
                            self.done = true;
                            return true;
                        }
                    };
                    if let Err(e) =
                        self.scheduler
                            .deliver_output(node_idx, handle, new_lineage, num_rows)
                    {
                        self.pending_outputs.push_back(Err(e));
                        self.done = true;
                        return true;
                    }
                }
            }

            did_work = true;
        }

        if !did_work {
            self.done = true;
            return false;
        }

        true
    }
}

impl Iterator for LocalIter<'_> {
    type Item = Result<OutputBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        crate::executor::drain_step_loop!(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::batch_ext::RecordBatchExt;
    use crate::executor::test_fixtures;
    use crate::executor::{DefaultGenerator, SourceGenerator};
    use crate::graph::PipelineGraph;
    use crate::schema::schema_of;
    use crate::task::TaskDef;
    use arrow::array::{Float64Array, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    #[test]
    fn simple_linear_pipeline() {
        let mut executor = LocalExecutor::new().with_max_batches(3);
        test_fixtures::assert_linear_pipeline(&mut executor);
    }

    #[test]
    fn custom_source() {
        let mut graph = PipelineGraph::new();
        graph
            .add_linear(vec![TaskDef::passthrough("passthrough", |batch| Ok(batch))])
            .unwrap();

        struct CountSource {
            schema: Schema,
            count: i64,
            max: i64,
        }
        impl SourceGenerator for CountSource {
            fn produces(&self) -> &Schema {
                &self.schema
            }
            fn next_batch(&mut self) -> Option<RecordBatch> {
                if self.count >= self.max {
                    return None;
                }
                self.count += 1;
                let batch = RecordBatch::try_new(
                    Arc::new(self.schema.clone()),
                    vec![Arc::new(Int64Array::from(vec![self.count]))],
                )
                .unwrap();
                Some(batch)
            }
        }

        let mut source = CountSource {
            schema: Schema::new(vec![Field::new("x", DataType::Int64, false)]),
            count: 0,
            max: 2,
        };

        let mut executor = LocalExecutor::new();
        let results: Vec<_> = executor.run(&graph, &mut source).unwrap().collect();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn diamond_graph_execution() {
        let mut executor = LocalExecutor::new().with_max_batches(1);
        test_fixtures::assert_diamond_graph(&mut executor);
    }

    #[test]
    fn with_config_task() {
        struct ScaleConfig {
            factor: f64,
        }

        fn scale(batch: RecordBatch, config: &ScaleConfig) -> crate::error::Result<RecordBatch> {
            let id_col = batch.column_as::<Int64Array>("id")?;
            let scaled: Vec<f64> = id_col
                .values()
                .iter()
                .map(|v| *v as f64 * config.factor)
                .collect();
            batch.append_column("scaled", Arc::new(Float64Array::from(scaled)))
        }

        let mut graph = PipelineGraph::new();
        graph
            .add_linear(vec![TaskDef::new_with_config(
                "scale",
                schema_of(&[("id", DataType::Int64)]),
                schema_of(&[("scaled", DataType::Float64)]),
                scale,
                ScaleConfig { factor: 10.0 },
            )])
            .unwrap();

        let mut executor = LocalExecutor::new().with_max_batches(1);
        let mut source = DefaultGenerator::new();
        let results: Vec<_> = executor.run(&graph, &mut source).unwrap().collect();
        assert_eq!(results.len(), 1);

        let batch = &results[0].as_ref().unwrap().data;
        let scaled = batch.column_as::<Float64Array>("scaled").unwrap();
        assert_eq!(scaled.value(0), 0.0); // id=0, factor=10.0 => 0.0
    }

    #[test]
    fn max_rows_accumulation() {
        let mut executor = LocalExecutor::new().with_max_batches(5);
        test_fixtures::assert_max_rows_accumulation(&mut executor);
    }

    #[test]
    fn common_origin_diamond() {
        use crate::task::BatchMode;

        // B -> D(CommonOrigin)
        // C -> D
        // D should receive one merged output per source batch (from both B and C).
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
        let d = graph.add_task(
            TaskDef::passthrough("merge", |b| Ok(b)).with_batch_mode(BatchMode::CommonOrigin),
        );
        graph.add_edge(b, d).unwrap();
        graph.add_edge(c, d).unwrap();

        let mut executor = LocalExecutor::new().with_max_batches(2);
        let mut source = DefaultGenerator::new();
        let results: Vec<_> = executor.run(&graph, &mut source).unwrap().collect();

        // With CommonOrigin, D fires once per origin, merging B and C outputs.
        // 2 source batches -> 2 merge outputs.
        assert_eq!(results.len(), 2);
        for result in &results {
            let batch = &result.as_ref().unwrap().data;
            // Each merged batch should have 2 rows (one from B, one from C),
            // and 2 columns (id is common, from_b + from_c would only appear
            // if schema intersection includes them — actually since B produces
            // {id, from_b} and C produces {id, from_c}, intersection is just {id}).
            // The merge concatenates the batches, so it has 2 rows with just id.
            assert_eq!(batch.num_rows(), 2);
            assert_eq!(result.as_ref().unwrap().task, "merge");
        }
    }

    #[test]
    fn input_splitting() {
        let mut executor = LocalExecutor::new();
        test_fixtures::assert_input_splitting(&mut executor);
    }

    #[test]
    fn passthrough_no_accumulation() {
        // 3 batches of 1 row each -> 3 outputs (no merging).
        let mut graph = PipelineGraph::new();
        graph
            .add_linear(vec![TaskDef::passthrough("middle", |b| Ok(b))])
            .unwrap();

        let mut executor = LocalExecutor::new().with_max_batches(3);
        let mut source = DefaultGenerator::new();
        let results: Vec<_> = executor.run(&graph, &mut source).unwrap().collect();

        assert_eq!(results.len(), 3);
        for result in &results {
            assert_eq!(result.as_ref().unwrap().data.num_rows(), 1);
        }
    }

    #[test]
    #[should_panic(expected = "MaxRows(0) is invalid")]
    fn max_rows_zero_rejected_batch_size() {
        TaskDef::passthrough("bad", |b| Ok(b)).with_batch_size(0);
    }

    #[test]
    #[should_panic(expected = "MaxRows(0) is invalid")]
    fn max_rows_zero_rejected_batch_mode() {
        use crate::task::BatchMode;
        TaskDef::passthrough("bad", |b| Ok(b)).with_batch_mode(BatchMode::MaxRows(0));
    }
}
