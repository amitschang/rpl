use std::collections::{HashMap, VecDeque};

use arrow::record_batch::RecordBatch;

use crate::error::Result;
use crate::executor::{Executor, OutputBatch, SourceGenerator};
use crate::graph::{Node, PipelineGraph};

use petgraph::graph::NodeIndex;

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

        let ranked_order = graph.ranked_nodes();
        let source_indices = graph.source_tasks();

        let mut queues: HashMap<NodeIndex, VecDeque<RecordBatch>> = HashMap::new();
        for node in &ranked_order {
            queues.insert(node.index, VecDeque::new());
        }

        Ok(Box::new(LocalIter {
            graph,
            ranked_order,
            source_indices,
            queues,
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
    ranked_order: Vec<Node<'a>>,
    source_indices: Vec<NodeIndex>,
    queues: HashMap<NodeIndex, VecDeque<RecordBatch>>,
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

        // Feed source tasks only when their queues are empty.
        let sources_idle = self.source_indices
            .iter()
            .all(|idx| self.queues[idx].is_empty());
        let under_limit = match self.max_batches {
            Some(limit) => self.batch_count < limit,
            None => true,
        };
        if sources_idle && under_limit && let Some(batch) = self.source.next_batch()
        {
            for idx in &self.source_indices {
                self.queues.get_mut(idx).unwrap().push_back(batch.clone());
            }
            self.batch_count += 1;
        }

        // Process tasks in priority order (closest to sink first).
        let mut did_work = false;

        for node in &self.ranked_order {
            let queue = self.queues.get_mut(&node.index).unwrap();
            if queue.is_empty() {
                continue;
            }

            let input = queue.pop_front().unwrap();

            let output = match node.execute(input) {
                Ok(batch) => batch,
                Err(e) => {
                    self.pending_outputs.push_back(Err(e));
                    self.done = true;
                    return true;
                }
            };

            if node.is_sink {
                self.pending_outputs.push_back(Ok(OutputBatch {
                    data: output,
                    task: node.name.clone(),
                }));
            } else {
                for successor in &self.graph.successors(node.index) {
                    self.queues
                        .get_mut(successor)
                        .unwrap()
                        .push_back(output.clone());
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
}
