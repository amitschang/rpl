use std::collections::VecDeque;

use crate::error::Result;
use crate::executor::scheduler::BatchScheduler;
use crate::executor::{Executor, OutputBatch, SourceGenerator};
use crate::graph::PipelineGraph;
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
        LocalExecutor {
            max_batches: None,
        }
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
        let under_limit = match self.max_batches {
            Some(limit) => self.batch_count < limit,
            None => true,
        };
        if under_limit {
            if let Some(batch) = self.source.next_batch() {
                if let Err(e) = self.scheduler.enqueue_source_batch(&batch) {
                    self.pending_outputs.push_back(Err(e));
                    self.done = true;
                    return true;
                }
                self.batch_count += 1;
            } else {
                // Source exhausted — enable flush mode for partial MaxRows drain.
                self.scheduler.mark_flush();
            }
        } else {
            // Max batches reached — flush any partial accumulations.
            self.scheduler.mark_flush();
        }

        // Process tasks via scheduler until no more are ready.
        let mut did_work = false;

        while let Some(ready) = self.scheduler.next_ready_task() {
            let node_idx = ready.node;
            let origins = ready.origins;

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

            // Execute the task.
            let task = self.graph.task(node_idx);
            let output = match task.execute(input) {
                Ok(batch) => batch,
                Err(e) => {
                    self.pending_outputs.push_back(Err(e));
                    self.done = true;
                    return true;
                }
            };

            let num_rows = output.num_rows();
            let is_sink = self.graph.is_sink(node_idx);

            if is_sink {
                self.pending_outputs.push_back(Ok(OutputBatch {
                    data: output,
                    task: task.name.clone(),
                }));
            } else {
                // Store output and route to successors.
                let handle = match self.scheduler.transport().store(&output) {
                    Ok(h) => h,
                    Err(e) => {
                        self.pending_outputs.push_back(Err(e));
                        self.done = true;
                        return true;
                    }
                };
                if let Err(e) = self.scheduler.deliver_output(node_idx, handle, origins, num_rows) {
                    self.pending_outputs.push_back(Err(e));
                    self.done = true;
                    return true;
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
        // Drain any buffered outputs first.
        if let Some(output) = self.pending_outputs.pop_front() {
            return Some(output);
        }

        // Run scheduling rounds until we get output or exhaust the pipeline.
        while self.step() {
            if let Some(output) = self.pending_outputs.pop_front() {
                return Some(output);
            }
        }

        // Final drain after done.
        self.pending_outputs.pop_front()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::batch_ext::RecordBatchExt;
    use crate::executor::{DefaultGenerator, SourceGenerator};
    use crate::graph::PipelineGraph;
    use crate::task::TaskDef;
    use arrow::array::{Float64Array, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    fn schema(fields: &[(&str, DataType)]) -> Schema {
        Schema::new(
            fields
                .iter()
                .map(|(name, dt)| Field::new(*name, dt.clone(), true))
                .collect::<Vec<_>>(),
        )
    }

    #[test]
    fn simple_linear_pipeline() {
        // generate -> add_column -> sink
        let mut graph = PipelineGraph::new();
        graph
            .add_linear(vec![
                TaskDef::passthrough("generate", |batch| Ok(batch)),
                TaskDef::new(
                    "add_value",
                    Schema::empty(),
                    schema(&[("value", DataType::Float64)]),
                    |batch| {
                        let len = batch.num_rows();
                        let values: Vec<f64> = (0..len).map(|i| i as f64 * 1.5).collect();
                        batch.append_column("value", Arc::new(Float64Array::from(values)))
                    },
                ),
                TaskDef::passthrough("sink", |batch| Ok(batch)),
            ])
            .unwrap();

        let mut executor = LocalExecutor::new().with_max_batches(3);
        let mut source = DefaultGenerator::new();
        let results: Vec<_> = executor.run(&graph, &mut source).unwrap().collect();

        assert_eq!(results.len(), 3);
        for result in &results {
            let batch = &result.as_ref().unwrap().data;
            assert_eq!(batch.num_columns(), 2); // id, value
            assert!(batch.schema().field_with_name("value").is_ok());
        }
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
        // a -> b -> d
        // a -> c -> d
        let mut graph = PipelineGraph::new();
        let a = graph.add_task(TaskDef::passthrough("source", |batch| Ok(batch)));
        let b = graph.add_task(TaskDef::new(
            "branch_b",
            Schema::empty(),
            schema(&[("from_b", DataType::Float64)]),
            |batch| {
                let len = batch.num_rows();
                batch.append_column("from_b", Arc::new(Float64Array::from(vec![1.0; len])))
            },
        ));
        let c = graph.add_task(TaskDef::new(
            "branch_c",
            Schema::empty(),
            schema(&[("from_c", DataType::Float64)]),
            |batch| {
                let len = batch.num_rows();
                batch.append_column("from_c", Arc::new(Float64Array::from(vec![2.0; len])))
            },
        ));
        let d = graph.add_task(TaskDef::passthrough("merge", |batch| Ok(batch)));
        graph.add_edge(a, b).unwrap();
        graph.add_edge(a, c).unwrap();
        graph.add_edge(b, d).unwrap();
        graph.add_edge(c, d).unwrap();

        let mut executor = LocalExecutor::new().with_max_batches(1);
        let mut source = DefaultGenerator::new();
        let results: Vec<_> = executor.run(&graph, &mut source).unwrap().collect();

        // d should receive two batches (one from b, one from c)
        assert_eq!(results.len(), 2);
        let names: Vec<_> = results
            .iter()
            .map(|r| r.as_ref().unwrap().task.as_str())
            .collect();
        assert!(names.iter().all(|n| *n == "merge"));
    }

    #[test]
    fn with_config_task() {
        struct ScaleConfig {
            factor: f64,
        }

        fn scale(batch: RecordBatch, config: &ScaleConfig) -> crate::error::Result<RecordBatch> {
            let id_col = batch.column_as::<Int64Array>("id")?;
            let scaled: Vec<f64> = id_col.values().iter().map(|v| *v as f64 * config.factor).collect();
            batch.append_column("scaled", Arc::new(Float64Array::from(scaled)))
        }

        let mut graph = PipelineGraph::new();
        graph
            .add_linear(vec![
                TaskDef::passthrough("gen", |b| Ok(b)),
                TaskDef::new_with_config(
                    "scale",
                    schema(&[("id", DataType::Int64)]),
                    schema(&[("scaled", DataType::Float64)]),
                    scale,
                    ScaleConfig { factor: 10.0 },
                ),
            ])
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
        // source -> accumulate(MaxRows(3)) -> sink
        // 5 source batches of 1 row each -> expect 2 sink outputs: 3 rows, then 2 rows.
        let mut graph = PipelineGraph::new();
        graph
            .add_linear(vec![
                TaskDef::passthrough("source", |b| Ok(b)),
                TaskDef::passthrough("accumulate", |b| Ok(b)).with_batch_size(3),
                TaskDef::passthrough("sink", |b| Ok(b)),
            ])
            .unwrap();

        let mut executor = LocalExecutor::new().with_max_batches(5);
        let mut source = DefaultGenerator::new();
        let results: Vec<_> = executor
            .run(&graph, &mut source)
            .unwrap()
            .collect::<Vec<_>>();

        // Should get 2 outputs: one with 3 rows and one with 2 rows.
        assert_eq!(results.len(), 2);
        let rows: Vec<usize> = results
            .iter()
            .map(|r| r.as_ref().unwrap().data.num_rows())
            .collect();
        assert!(rows.contains(&3));
        assert!(rows.contains(&2));
        assert_eq!(rows.iter().sum::<usize>(), 5);
    }

    #[test]
    fn common_origin_diamond() {
        use crate::task::BatchMode;

        // A -> B -> D(CommonOrigin)
        // A -> C -> D
        // D should receive one merged output per source batch (from both B and C).
        let mut graph = PipelineGraph::new();
        let a = graph.add_task(TaskDef::passthrough("source", |b| Ok(b)));
        let b = graph.add_task(TaskDef::new(
            "branch_b",
            Schema::empty(),
            schema(&[("from_b", DataType::Float64)]),
            |batch| {
                let len = batch.num_rows();
                batch.append_column("from_b", Arc::new(Float64Array::from(vec![1.0; len])))
            },
        ));
        let c = graph.add_task(TaskDef::new(
            "branch_c",
            Schema::empty(),
            schema(&[("from_c", DataType::Float64)]),
            |batch| {
                let len = batch.num_rows();
                batch.append_column("from_c", Arc::new(Float64Array::from(vec![2.0; len])))
            },
        ));
        let d = graph.add_task(
            TaskDef::passthrough("merge", |b| Ok(b))
                .with_batch_mode(BatchMode::CommonOrigin),
        );
        graph.add_edge(a, b).unwrap();
        graph.add_edge(a, c).unwrap();
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
}
