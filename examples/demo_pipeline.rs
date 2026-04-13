//! Example: HQ-based pipeline exercising scheduling, batching, fan-out, and merge.
//!
//! This builds a non-trivial DAG with:
//!   - A default source generator (`id` column)
//!   - A "double" task that adds a `doubled` column
//!   - A fan-out into two branches:
//!       branch_a: passthrough
//!       branch_b: passthrough, MaxRows(3) batching
//!   - A merge sink: passthrough
//!
//! Each task simulates work with a configurable sleep; see the constants
//! at the top of the file (`DOUBLE_SLEEP_MS`, `BRANCH_A_SLEEP_MS`, etc.).
//!
//! Lineage tracking (BatchLineage) is built automatically by the executor,
//! so tasks don't need to stamp path columns. The live counter and final
//! summary use the lineage from OutputBatch.
//!
//! The graph looks like:
//!
//!   [source/double] ---+---> [branch_a] ---+---> [merge_sink]
//!                      +---> [branch_b] ---+
//!
//! Prerequisites:
//!   1. Install HyperQueue: https://it4innovations.github.io/hyperqueue/
//!   2. Start an HQ server:   hq server start &
//!   3. Start an HQ worker:   hq worker start &
//!
//! Run with:
//!   cargo run --example hq_pipeline [-- --hq-bin /path/to/hq]
//!   cargo run --example hq_pipeline -- --local

use std::sync::Arc;
use std::thread;
use std::time::Duration;

use arrow::array::{Float64Array, Int64Array};
use arrow::datatypes::DataType;

use rpl::executor::local::LocalExecutor;
use rpl::executor::threaded::ThreadExecutor;
use rpl::task::Resources;
use rpl::{
    BatchMode, DefaultGenerator, Executor, HqExecutor, PipelineGraph, PipelineTracker,
    RecordBatchExt, TaskDef, TaskRegistry, run_worker_if_invoked, schema_of,
};

// ---------------------------------------------------------------------------
// Tunable parameters
// ---------------------------------------------------------------------------

const NUM_SOURCE_BATCHES: usize = 100;
const SOURCE_BUFFER: usize = 12;

const DOUBLE_SLEEP_MS: u64 = 2000;
const BRANCH_A_SLEEP_MS: u64 = 1500;
const BRANCH_B_SLEEP_MS: u64 = 3000;
const MERGE_SINK_SLEEP_MS: u64 = 1000;

// ---------------------------------------------------------------------------
// Build the pipeline graph
// ---------------------------------------------------------------------------

fn build_graph() -> PipelineGraph {
    let mut graph = PipelineGraph::new();

    // Source task: reads `id`, produces `doubled` column. 200ms sleep.
    let double = graph.add_task(TaskDef::new(
        "double",
        schema_of(&[("id", DataType::Int64)]),
        schema_of(&[("doubled", DataType::Float64)]),
        |batch| {
            thread::sleep(Duration::from_millis(DOUBLE_SLEEP_MS));
            let ids = batch.column_as::<Int64Array>("id")?;
            let doubled: Vec<f64> = ids.iter().map(|v| v.unwrap() as f64 * 2.0).collect();
            batch.append_column("doubled", Arc::new(Float64Array::from(doubled)))
        },
    ));

    // Branch A: passthrough
    let branch_a = graph.add_task(
        TaskDef::passthrough("branch_a", |batch| {
            thread::sleep(Duration::from_millis(BRANCH_A_SLEEP_MS));
            Ok(batch)
        })
        .with_resources(Resources {
            num_cpus: 1,
            ..Default::default()
        }),
    );

    // Branch B: passthrough. MaxRows(3) batching.
    let branch_b = graph.add_task(
        TaskDef::passthrough("branch_b", |batch| {
            thread::sleep(Duration::from_millis(BRANCH_B_SLEEP_MS));
            Ok(batch)
        })
        .with_batch_mode(BatchMode::MaxRows(3))
        .with_resources(Resources {
            num_cpus: 2,
            ..Default::default()
        }),
    );

    // Merge sink: passthrough.
    let merge_sink = graph.add_task(TaskDef::passthrough("merge_sink", |batch| {
        thread::sleep(Duration::from_millis(MERGE_SINK_SLEEP_MS));
        Ok(batch)
    }));

    // Wire up the DAG
    graph.add_edge(double, branch_a).unwrap();
    graph.add_edge(double, branch_b).unwrap();
    graph.add_edge(branch_a, merge_sink).unwrap();
    graph.add_edge(branch_b, merge_sink).unwrap();

    graph
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

fn main() -> rpl::Result<()> {
    // -- Worker path: if HQ invoked us as a worker, handle it and exit. --
    let graph = build_graph();
    let registry = TaskRegistry::from(&graph);
    if run_worker_if_invoked(&registry) {
        return Ok(());
    }

    // -- Driver path -------------------------------------------------------

    // Parse CLI arguments.
    let args: Vec<String> = std::env::args().collect();
    let use_local = args.iter().any(|a| a == "--local");
    let use_local_threaded = args.iter().any(|a| a == "--local-threaded");
    let hq_bin = args
        .windows(2)
        .find_map(|w| {
            if w[0] == "--hq-bin" {
                Some(w[1].clone())
            } else {
                None
            }
        })
        .unwrap_or_else(|| "hq".to_string());

    println!("=== RPL - HyperQueue Pipeline Example ===");
    println!();

    // 1. Print graph diagnostics
    println!("=== Pipeline Graph ===");
    graph.print_summary();
    println!();

    // 2. Validate with the default generator
    let generator = DefaultGenerator::new();
    graph.validate(&generator)?;
    println!("Graph validation: OK");
    println!();

    // 3. Set up the executor
    let tmp = tempfile::tempdir()?;

    let mut local_executor;
    let mut threaded_executor;
    let mut hq_executor;
    let executor: &mut dyn Executor = if use_local {
        println!("Executor:          local (single-threaded)");
        local_executor = LocalExecutor::new().with_max_batches(NUM_SOURCE_BATCHES);
        &mut local_executor
    } else if use_local_threaded {
        println!("Executor:          local-threaded");
        threaded_executor = ThreadExecutor::new().with_max_batches(NUM_SOURCE_BATCHES);
        &mut threaded_executor
    } else {
        println!("Executor:          HQ (distributed)");
        println!("HQ binary:         {hq_bin}");
        println!("Staging directory:  {}", tmp.path().display());
        println!("Source buffer:      {SOURCE_BUFFER}");
        hq_executor = HqExecutor::new(tmp.path())?
            .with_hq_binary(&hq_bin)
            .with_max_batches(NUM_SOURCE_BATCHES)
            .with_source_buffer(SOURCE_BUFFER);
        &mut hq_executor
    };
    println!("Source batches:     {NUM_SOURCE_BATCHES}");
    println!();

    // 4. Run the pipeline with live progress
    println!("=== Execution ===");
    let mut source = DefaultGenerator::new();
    let iter = executor.run(&graph, &mut source)?;

    // Collect task names for the live tracker (in graph order).
    let task_names: Vec<String> = graph.nodes().into_iter().map(|n| n.name.clone()).collect();
    let mut tracker = PipelineTracker::new(task_names);

    for result in iter {
        match result {
            Ok(output) => {
                tracker.update_and_report(&output);
            }
            Err(e) => {
                eprintln!("  ERROR: {e}");
            }
        }
    }
    tracker.finish();

    Ok(())
}
