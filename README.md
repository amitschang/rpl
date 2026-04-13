# rpl

An embeddable Rust library for building data pipelines as validated directed
acyclic graphs (DAGs), with Apache Arrow RecordBatch as the native data format,
declarative batching controls, and a single-binary distributed execution model.

## Key Features

### Arrow-native data flow

Every task receives and produces an Arrow `RecordBatch`. Data flows between
tasks in a columnar, strongly-typed format — no serialization boundaries, no
opaque blobs. Intermediate results integrate directly with the Arrow ecosystem
(DataFusion, Polars, DuckDB, PyArrow) without conversion.

### Schema contracts validated before execution

Each task declares the columns it `requires`, `produces`, and `drops`. When the
pipeline graph is built, `rpl` walks the DAG in topological order and verifies
that every task's required columns are available from its upstream producers —
catching mismatches before any data is processed.

### Declarative batch accumulation

Three built-in `BatchMode` variants control how data is grouped before a task
runs:

- **`Passthrough`** — each incoming batch is forwarded as-is.
- **`MaxRows(n)`** — accumulate batches until the row count reaches *n*, then
  concatenate and execute. Oversized outputs are automatically split for
  downstream consumers.
- **`CommonOrigin`** — wait for contributions from all predecessor branches
  sharing the same origin set before executing, providing a built-in barrier for
  merge points in diamond-shaped graphs.

These are declared per-task and handled entirely by the scheduler — no manual
batching logic required.

### Pluggable executors, shared scheduling

The `BatchScheduler` — which manages queues, origin tracking, accumulation, and
flush semantics — is generic over the data transport and shared across all
executor backends. This means:

- **`LocalExecutor`**: serial, single-threaded, in-process. Ideal for
  development and testing.
- **`ThreadExecutor`**: multi-threaded execution within a single process, using
  a thread pool. Good for CPU-bound tasks on a single machine.
- **`HqExecutor`**: distributed execution via
  [HyperQueue](https://github.com/It4innovations/hyperqueue). Tasks run as jobs
  on a cluster.

Both use identical scheduling logic, so local test runs faithfully reproduce
distributed behavior. Adding a new backend (Slurm, Kubernetes, etc.) means
implementing a small trait — not rewriting the scheduler.

### Single-binary driver/worker model

The HQ executor invokes the current binary with `--rpl-worker` to run tasks on
remote nodes. A `TaskRegistry` ensures the same task definitions (functions +
config) are available on both the driver and worker sides. Ship one binary;
HyperQueue distributes it. No container images, no package deployment, no
environment drift.

### Pluggable data transport

The `DataTransport` trait abstracts how batches move between tasks, with
explicit lifecycle management (store, load, release, fan-out reference
counting). Built-in transports:

- **`InMemoryTransport`** — for local execution.
- **`FileTransport`** — Arrow IPC files on a shared filesystem for distributed runs.
- **`AnyTransport`** — runtime-dispatched variant for configuration-driven selection.

### Lightweight and embeddable

`rpl` is a library, not a platform. Its dependency footprint is minimal:
`arrow`, `petgraph`, `serde`, `serde_json`, `uuid`, `tempfile`, `thiserror`. No
async runtime, no RPC framework, no external services required for local use.

### Utilities for column operations

Common patterns like renaming, dropping, and passthrough transformations are
provided as utilities, reducing boilerplate for simple common tasks on arrow
record batches. Available via `RecordBatchExt` trait methods, including:

- `append_column(name, array)`
- `drop_column(name)`
- `rename_column(old_name, new_name)`
- `column_as::<T>(name) -> Result<&T>`

### Utilities for execution tracking and reporting

Per batch-task timing and lineage are available in `Output` metadata. The
`PipelineTracker` utility aggregates this information across batches,
deduplicating at fan-out points, to provide live progress reporting and a final
execution summary.

## Example

### Defining and running a pipeline locally

```rust
use std::sync::Arc;
use arrow::array::Float64Array;
use arrow::datatypes::{DataType, Schema};
use rpl::{
    BatchMode, DefaultGenerator, Executor, PipelineGraph, RecordBatchExt, TaskDef,
    executor::local::LocalExecutor,
    schema_of,
};

fn main() {
    // Build the pipeline graph.
    let mut graph = PipelineGraph::new();
    graph.add_linear(vec![
        // First task: read the source `id` column, produce `value`.
        TaskDef::new(
            "compute_value",
            Schema::empty(),
            schema_of(&[("value", DataType::Float64)]),
            |batch| {
                let len = batch.num_rows();
                let values: Vec<f64> = (0..len).map(|i| i as f64 * 1.5).collect();
                batch.append_column("value", Arc::new(Float64Array::from(values)))
            },
        ),
        // Second task: read `value`, produce `scaled`, accumulate in batches of 10.
        TaskDef::new(
            "scale",
            schema_of(&[("value", DataType::Float64)]),
            schema_of(&[("scaled", DataType::Float64)]),
            |batch| {
                let values = batch.column_as::<Float64Array>("value")?;
                let scaled: Float64Array = values.iter()
                    .map(|v| v.map(|x| x * 2.0))
                    .collect();
                batch.append_column("scaled", Arc::new(scaled))
            },
        ).with_batch_mode(BatchMode::MaxRows(10)),
    ]).unwrap();

    // Execute with the local (single-threaded) executor.
    let mut executor = LocalExecutor::new().with_max_batches(20);
    let mut source = DefaultGenerator::new();

    for result in executor.run(&graph, &mut source).unwrap() {
        let output = result.unwrap();
        println!("{}: {} rows", output.task, output.data.num_rows());
    }
}
```

### Distributed execution with HyperQueue

The same graph runs on a cluster by switching to `HqExecutor`. The binary
acts as both driver and worker — add the worker entry point at the top of
`main` and swap the executor:

```rust
use rpl::{
    HqExecutor, TaskRegistry, PipelineGraph,
    run_worker_if_invoked,
    Executor, DefaultGenerator,
};

fn build_graph() -> PipelineGraph {
    // ... same graph construction as above ...
    # let graph = PipelineGraph::new();
    # graph
}

fn main() {
    let graph = build_graph();

    // If invoked as a worker by HyperQueue, execute the task and exit.
    let registry = TaskRegistry::from(&graph);
    if run_worker_if_invoked(&registry) {
        return;
    }

    // Driver path: submit tasks to HyperQueue.
    let mut executor = HqExecutor::new("/shared/staging/my-run")
        .unwrap()
        .with_max_batches(100);

    let mut source = DefaultGenerator::new();
    for result in executor.run(&graph, &mut source).unwrap() {
        let output = result.unwrap();
        println!("{}: {} rows", output.task, output.data.num_rows());
    }
}
```

### Execution tracking

`PipelineTracker` aggregates per-task timing from batch lineage, deduplicating
across fan-out points. Wrap the output iterator to get live progress on stderr
and a final summary on stdout:

```rust
use rpl::PipelineTracker;

// Collect task names for display ordering.
let task_names: Vec<String> = graph.nodes().into_iter().map(|n| n.name.clone()).collect();
let mut tracker = PipelineTracker::new(task_names);

for result in executor.run(&graph, &mut source).unwrap() {
    let output = result.unwrap();
    tracker.update_and_report(&output); // live ANSI table on stderr
    // ... process output ...
}
tracker.finish();        // clear live display
tracker.print_summary(); // final table on stdout
```

Use `tracker.update(&output)` instead of `update_and_report` for silent
accumulation — useful when you only want the final summary or need to inspect
`tracker.summary()` programmatically.