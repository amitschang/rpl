use std::collections::HashMap;

use crate::error::{Result, RplError};
use crate::graph::PipelineGraph;
use crate::task::TaskDef;

/// Maps task names to cloneable [`TaskDef`] instances.
///
/// Used by remote executors to ensure the same task definitions are available
/// in both the driver (for graph construction) and worker (for execution)
/// paths within a single binary.
pub struct TaskRegistry {
    tasks: HashMap<String, TaskDef>,
}

impl TaskRegistry {
    pub fn new() -> Self {
        TaskRegistry {
            tasks: HashMap::new(),
        }
    }

    /// Register a task definition by name.
    pub fn register(&mut self, task: TaskDef) {
        self.tasks.insert(task.name.clone(), task);
    }

    /// Register all tasks from a pipeline graph.
    pub fn register_graph(&mut self, graph: &PipelineGraph) {
        for node in graph.nodes() {
            let task: &TaskDef = &node;
            self.tasks.insert(task.name.clone(), task.clone());
        }
    }

    /// Build a fresh clone of the named task.
    pub fn build(&self, name: &str) -> Result<TaskDef> {
        self.tasks
            .get(name)
            .cloned()
            .ok_or_else(|| RplError::Hq(format!("task '{name}' not found in registry")))
    }

    /// All registered task names.
    pub fn task_names(&self) -> Vec<&str> {
        self.tasks.keys().map(|s| s.as_str()).collect()
    }
}

impl Default for TaskRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl From<&PipelineGraph> for TaskRegistry {
    fn from(graph: &PipelineGraph) -> Self {
        let mut registry = Self::new();
        registry.register_graph(graph);
        registry
    }
}
