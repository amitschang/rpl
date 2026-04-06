use std::collections::HashMap;

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

/// A task definition within a pipeline graph.
///
/// Each task declares its schema contract:
/// - **`requires`**: columns the task reads from the incoming batch.
/// - **`produces`**: new columns the task adds to the batch.
/// - **`drops`**: columns the task removes from the batch.
///
/// These are always required and used by [`PipelineGraph::validate`] to
/// verify that column dependencies are satisfied across the DAG.
pub struct TaskDef {
    pub name: String,
    node: Box<dyn TaskNode>,
    requires: Schema,
    produces: Schema,
    drops: Vec<String>,
    pub resources: Resources,
    pub batch_size: Option<usize>,
}

impl TaskDef {
    /// Create a task with no config.
    ///
    /// ```ignore
    /// use arrow::datatypes::Schema;
    /// let task = TaskDef::new("passthrough", Schema::empty(), Schema::empty(), |batch| Ok(batch));
    /// ```
    pub fn new<F>(
        name: impl Into<String>,
        requires: Schema,
        produces: Schema,
        func: F,
    ) -> Self
    where
        F: Fn(RecordBatch) -> Result<RecordBatch> + Send + Sync + 'static,
    {
        TaskDef {
            name: name.into(),
            node: Box::new(TaskNodeImpl {
                func: Box::new(move |batch, _: &()| func(batch)),
                config: (),
            }),
            requires,
            produces,
            drops: Vec::new(),
            resources: Resources { num_cpus: 1, ..Default::default() },
            batch_size: None,
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
            node: Box::new(TaskNodeImpl {
                func: Box::new(func),
                config,
            }),
            requires,
            produces,
            drops: Vec::new(),
            resources: Resources { num_cpus: 1, ..Default::default() },
            batch_size: None,
        }
    }

    /// Convenience constructor for a passthrough task (requires/produces/drops nothing).
    pub fn passthrough(name: impl Into<String>, func: impl Fn(RecordBatch) -> Result<RecordBatch> + Send + Sync + 'static) -> Self {
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

    /// Set batch size.
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = Some(batch_size);
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
            .field("batch_size", &self.batch_size)
            .finish()
    }
}
