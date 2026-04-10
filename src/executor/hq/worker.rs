use std::collections::BTreeSet;
use std::time::Instant;

use serde::de::DeserializeOwned;

use crate::batch_ext::RecordBatchExt;
use crate::error::{Result, RplError};
use crate::transport::{DataTransport, OutputEntry};
use crate::transport::file::FileTransport;

use crate::registry::TaskRegistry;

/// If the current process was invoked as an HQ worker (i.e. `--rpl-worker`
/// is present in argv), execute the task and exit. Returns `false` if this
/// is not a worker invocation and the caller should continue as the driver.
///
/// This convenience version uses [`FileTransport`], deriving the staging
/// directory from the `--output-token` path. For other transports, use
/// [`run_worker_if_invoked_with`].
///
/// On success, publishes output via the transport and returns `true`.
/// On error, prints to stderr and exits with code 1.
pub fn run_worker_if_invoked(registry: &TaskRegistry) -> bool {
    let args: Vec<String> = std::env::args().collect();
    if !args.iter().any(|a| a == "--rpl-worker") {
        return false;
    }

    // Derive the staging directory from the output token, which is an
    // absolute path like "<staging_dir>/manifest-<uuid>.json".
    let output_dir = args.windows(2).find_map(|w| {
        if w[0] == "--output-token" {
            let token: std::result::Result<crate::transport::file::FileOutputToken, _> =
                serde_json::from_str(&w[1]);
            token.ok().and_then(|t| {
                t.path().parent().map(|p| p.to_string_lossy().to_string())
            })
        } else {
            None
        }
    });
    let output_dir = match output_dir {
        Some(d) => d,
        None => {
            eprintln!("rpl-worker error: cannot determine staging directory from --output-token");
            std::process::exit(1);
        }
    };

    let transport = match FileTransport::new(output_dir) {
        Ok(t) => t,
        Err(e) => {
            eprintln!("rpl-worker error: {e}");
            std::process::exit(1);
        }
    };

    match run_worker(&args, registry, &transport) {
        Ok(()) => true,
        Err(e) => {
            eprintln!("rpl-worker error: {e}");
            std::process::exit(1);
        }
    }
}

/// Generic version: invoke the worker with a caller-provided transport.
///
/// Returns `false` if `--rpl-worker` is not in argv.
pub fn run_worker_if_invoked_with<T: DataTransport>(
    registry: &TaskRegistry,
    transport: &T,
) -> bool {
    let args: Vec<String> = std::env::args().collect();
    if !args.iter().any(|a| a == "--rpl-worker") {
        return false;
    }

    match run_worker(&args, registry, transport) {
        Ok(()) => true,
        Err(e) => {
            eprintln!("rpl-worker error: {e}");
            std::process::exit(1);
        }
    }
}

pub(crate) fn run_worker<T: DataTransport>(
    args: &[String],
    registry: &TaskRegistry,
    transport: &T,
) -> Result<()> {
    let parsed = parse_worker_args::<T::Handle, T::OutputToken>(args)?;
    run_worker_parsed(parsed, registry, transport)
}

fn run_worker_parsed<T: DataTransport>(
    parsed: ParsedArgs<T::Handle, T::OutputToken>,
    registry: &TaskRegistry,
    transport: &T,
) -> Result<()> {
    match parsed.mode {
        WorkerMode::Execute { task_name } => {
            run_execute(parsed.common, &task_name, registry, transport)
        }
        WorkerMode::Split { max_rows } => run_split(parsed.common, max_rows, transport),
    }
}

/// Execute a task: load inputs, merge, run task, store output.
fn run_execute<T: DataTransport>(
    common: CommonArgs<T::Handle, T::OutputToken>,
    task_name: &str,
    registry: &TaskRegistry,
    transport: &T,
) -> Result<()> {
    // Load and concatenate all input batches.
    let input = load_and_concat(&common.input_handles, transport)?;

    // Look up and execute the task, measuring execution time.
    let task = registry.build(task_name)?;
    let exec_start = Instant::now();
    let output = task.execute(input)?;
    let exec_duration_ms = exec_start.elapsed().as_millis() as u64;

    // Publish output via the transport.
    let num_rows = output.num_rows();
    let handle = transport.store(&output)?;
    transport.publish_output(
        &common.output_token,
        &[OutputEntry {
            handle,
            num_rows,
            origins: common.origins,
            exec_duration_ms: Some(exec_duration_ms),
        }],
    )
}

/// Split mode: load input, split into chunks, store each chunk.
/// No task execution — this is a scheduling helper submitted by the driver
/// when a large batch needs to be broken up for a MaxRows successor.
fn run_split<T: DataTransport>(
    common: CommonArgs<T::Handle, T::OutputToken>,
    max_rows: usize,
    transport: &T,
) -> Result<()> {
    let input = load_and_concat(&common.input_handles, transport)?;
    let chunks = input.split(max_rows);

    let entries: Vec<OutputEntry<T::Handle>> = chunks
        .iter()
        .map(|chunk| {
            let handle = transport.store(chunk)?;
            Ok(OutputEntry {
                handle,
                num_rows: chunk.num_rows(),
                origins: common.origins.clone(),
                exec_duration_ms: None,
            })
        })
        .collect::<Result<_>>()?;

    transport.publish_output(&common.output_token, &entries)
}

fn load_and_concat<T: DataTransport>(
    handles: &[T::Handle],
    transport: &T,
) -> Result<arrow::record_batch::RecordBatch> {
    let mut batches = Vec::new();
    for handle in handles {
        batches.push(transport.load(handle)?);
    }

    if batches.len() == 1 {
        Ok(batches.into_iter().next().unwrap())
    } else {
        let schema = batches[0].schema();
        arrow::compute::concat_batches(&schema, &batches).map_err(RplError::Arrow)
    }
}

// --- Argument parsing ---

enum WorkerMode {
    Execute { task_name: String },
    Split { max_rows: usize },
}

struct CommonArgs<H, OT> {
    input_handles: Vec<H>,
    origins: BTreeSet<u64>,
    output_token: OT,
}

struct ParsedArgs<H, OT> {
    mode: WorkerMode,
    common: CommonArgs<H, OT>,
}

fn parse_worker_args<H: DeserializeOwned, OT: DeserializeOwned>(
    args: &[String],
) -> Result<ParsedArgs<H, OT>> {
    let mut task_name = None;
    let mut input_handles = None;
    let mut origins = None;
    let mut output_token = None;
    let mut split_max_rows = None;
    let mut is_split = false;

    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--split" => {
                is_split = true;
            }
            "--task-name" => {
                i += 1;
                task_name = Some(args.get(i).cloned().ok_or_else(|| {
                    RplError::Hq("--task-name requires a value".into())
                })?);
            }
            "--input-handles" => {
                i += 1;
                let json = args.get(i).ok_or_else(|| {
                    RplError::Hq("--input-handles requires a value".into())
                })?;
                input_handles = Some(serde_json::from_str(json).map_err(|e| {
                    RplError::Hq(format!("invalid --input-handles JSON: {e}"))
                })?);
            }
            "--max-rows" => {
                i += 1;
                let val = args.get(i).ok_or_else(|| {
                    RplError::Hq("--max-rows requires a value".into())
                })?;
                split_max_rows = Some(val.parse::<usize>().map_err(|e| {
                    RplError::Hq(format!("invalid --max-rows: {e}"))
                })?);
            }
            "--origins" => {
                i += 1;
                let json = args.get(i).ok_or_else(|| {
                    RplError::Hq("--origins requires a value".into())
                })?;
                origins = Some(serde_json::from_str(json).map_err(|e| {
                    RplError::Hq(format!("invalid --origins JSON: {e}"))
                })?);
            }
            "--output-token" => {
                i += 1;
                let json = args.get(i).ok_or_else(|| {
                    RplError::Hq("--output-token requires a value".into())
                })?;
                output_token = Some(serde_json::from_str(json).map_err(|e| {
                    RplError::Hq(format!("invalid --output-token JSON: {e}"))
                })?);
            }
            _ => {}
        }
        i += 1;
    }

    let common = CommonArgs {
        input_handles: input_handles
            .ok_or_else(|| RplError::Hq("missing --input-handles".into()))?,
        origins: origins
            .ok_or_else(|| RplError::Hq("missing --origins".into()))?,
        output_token: output_token
            .ok_or_else(|| RplError::Hq("missing --output-token".into()))?,
    };

    let mode = if is_split {
        let max_rows = split_max_rows
            .ok_or_else(|| RplError::Hq("--split requires --max-rows".into()))?;
        WorkerMode::Split { max_rows }
    } else {
        let name = task_name
            .ok_or_else(|| RplError::Hq("missing --task-name".into()))?;
        WorkerMode::Execute { task_name: name }
    };

    Ok(ParsedArgs { mode, common })
}