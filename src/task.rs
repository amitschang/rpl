use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;

use crate::error::Result;

/// Type-erased interface for task execution.
///
/// Implemented by [`TaskNodeImpl`] to hide the concrete config type `C`,
/// allowing heterogeneous tasks to be stored in a single graph.
pub(crate) trait TaskNode: Send + Sync {
    fn execute(&self, batch: RecordBatch) -> Result<RecordBatch>;
}

type TaskFn<C> = Box<dyn Fn(RecordBatch, &C) -> Result<RecordBatch> + Send + Sync>;

/// Generic task implementation holding a function and its config.
///
/// For tasks with no config, `C = ()`.
struct TaskNodeImpl<C: Send + Sync + 'static> {
    func: TaskFn<C>,
    config: C,
}

impl<C: Send + Sync + 'static> TaskNode for TaskNodeImpl<C> {
    fn execute(&self, batch: RecordBatch) -> Result<RecordBatch> {
        (self.func)(batch, &self.config)
    }
}

/// Resource requirements for a task.
#[derive(Debug, Clone, Default)]
pub struct Resources {
    pub num_cpus: u32,
    pub num_gpus: u32,
    pub custom: HashMap<String, u32>,
}

/// Controls how incoming batches are accumulated before a task executes,
/// and whether outputs are split to respect a row limit.
#[derive(Debug, Clone, Default)]
pub enum BatchMode {
    /// Pass each incoming batch through as-is — no accumulation, no
    /// splitting. This is the default.
    #[default]
    Passthrough,

    /// Accumulate incoming batches until total rows >= N, then concatenate
    /// and execute. After execution, if the output exceeds N rows it is
    /// split into chunks of at most N rows before delivery to successors.
    /// N must be >= 1.
    MaxRows(usize),

    /// Collect contributions from ALL predecessor branches covering the same
    /// origin set before executing. Used at merge points in the DAG where
    /// you need the full picture from every upstream path.
    CommonOrigin,
}

impl BatchMode {
    /// Return the max rows value if this is `MaxRows(n)`, otherwise `None`.
    pub fn max_rows(&self) -> Option<usize> {
        match self {
            BatchMode::MaxRows(n) => Some(*n),
            _ => None,
        }
    }
}

/// A task definition within a pipeline graph.
///
/// Each task declares its schema contract:
/// - **`requires`**: columns the task reads from the incoming batch.
/// - **`produces`**: new columns the task adds to the batch.
/// - **`drops`**: columns the task removes from the batch.
///
/// These are always required and used by [`PipelineGraph::validate`] to
/// verify that column dependencies are satisfied across the DAG.
#[derive(Clone)]
pub struct TaskDef {
    pub name: String,
    node: Arc<dyn TaskNode>,
    requires: Schema,
    produces: Schema,
    drops: Vec<String>,
    pub resources: Resources,

    pub batch_mode: BatchMode,
}

impl TaskDef {
    /// Create a task with no config.
    ///
    /// ```ignore
    /// use arrow::datatypes::Schema;
    /// let task = TaskDef::new("passthrough", Schema::empty(), Schema::empty(), |batch| Ok(batch));
    /// ```
    pub fn new<F>(name: impl Into<String>, requires: Schema, produces: Schema, func: F) -> Self
    where
        F: Fn(RecordBatch) -> Result<RecordBatch> + Send + Sync + 'static,
    {
        TaskDef {
            name: name.into(),
            node: Arc::new(TaskNodeImpl {
                func: Box::new(move |batch, _: &()| func(batch)),
                config: (),
            }),
            requires,
            produces,
            drops: Vec::new(),
            resources: Resources {
                num_cpus: 1,
                ..Default::default()
            },
            batch_mode: BatchMode::default(),
        }
    }

    /// Create a task with typed configuration.
    ///
    /// ```ignore
    /// let task = TaskDef::new_with_config(
    ///     "scale",
    ///     requires_schema,
    ///     produces_schema,
    ///     |batch, cfg: &MyConfig| Ok(batch),
    ///     MyConfig { factor: 2.0 },
    /// );
    /// ```
    pub fn new_with_config<C, F>(
        name: impl Into<String>,
        requires: Schema,
        produces: Schema,
        func: F,
        config: C,
    ) -> Self
    where
        C: Send + Sync + 'static,
        F: Fn(RecordBatch, &C) -> Result<RecordBatch> + Send + Sync + 'static,
    {
        TaskDef {
            name: name.into(),
            node: Arc::new(TaskNodeImpl {
                func: Box::new(func),
                config,
            }),
            requires,
            produces,
            drops: Vec::new(),
            resources: Resources {
                num_cpus: 1,
                ..Default::default()
            },
            batch_mode: BatchMode::default(),
        }
    }

    /// Convenience constructor for a passthrough task (requires/produces/drops nothing).
    pub fn passthrough(
        name: impl Into<String>,
        func: impl Fn(RecordBatch) -> Result<RecordBatch> + Send + Sync + 'static,
    ) -> Self {
        Self::new(name, Schema::empty(), Schema::empty(), func)
    }

    /// Set columns this task removes from the batch.
    pub fn with_drops(mut self, drops: Vec<String>) -> Self {
        self.drops = drops;
        self
    }

    /// Set resource requirements.
    pub fn with_resources(mut self, resources: Resources) -> Self {
        self.resources = resources;
        self
    }

    /// Set batch mode.
    ///
    /// # Panics
    /// Panics if `batch_mode` is `MaxRows(0)`.
    pub fn with_batch_mode(mut self, batch_mode: BatchMode) -> Self {
        if let BatchMode::MaxRows(0) = batch_mode {
            panic!("MaxRows(0) is invalid: batch size must be >= 1");
        }
        self.batch_mode = batch_mode;
        self
    }

    /// Set batch size (sugar for `BatchMode::MaxRows(n)`).
    ///
    /// # Panics
    /// Panics if `n` is 0.
    pub fn with_batch_size(mut self, n: usize) -> Self {
        assert!(n > 0, "MaxRows(0) is invalid: batch size must be >= 1");
        self.batch_mode = BatchMode::MaxRows(n);
        self
    }

    /// Execute this task on a RecordBatch.
    pub fn execute(&self, batch: RecordBatch) -> Result<RecordBatch> {
        self.node.execute(batch)
    }

    /// Columns this task reads from the incoming batch.
    pub fn requires(&self) -> &Schema {
        &self.requires
    }

    /// New columns this task adds to the batch.
    pub fn produces(&self) -> &Schema {
        &self.produces
    }

    /// Columns this task removes from the batch.
    pub fn drops(&self) -> &[String] {
        &self.drops
    }
}

impl std::fmt::Debug for TaskDef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TaskDef")
            .field("name", &self.name)
            .field("resources", &self.resources)
            .field("batch_mode", &self.batch_mode)
            .finish()
    }
}
