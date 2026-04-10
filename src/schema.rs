use std::collections::HashMap;

use arrow::datatypes::{DataType, Field, Schema};

use crate::error::{Result, RplError};

/// A set of available columns at a point in the graph, keyed by field name.
///
/// Used internally by [`validate_schemas`] to track column propagation.
#[derive(Clone, Debug)]
pub struct ColumnSet {
    fields: HashMap<String, Field>,
}

impl ColumnSet {
    /// Create from a schema.
    pub fn from_schema(schema: &Schema) -> Self {
        let mut fields = HashMap::new();
        for field in schema.fields() {
            fields.insert(field.name().clone(), field.as_ref().clone());
        }
        ColumnSet { fields }
    }

    /// Check that all fields in `required` are present with compatible types.
    pub fn satisfies(&self, required: &Schema, task_name: &str) -> Result<()> {
        let mut missing = Vec::new();
        for req_field in required.fields() {
            match self.fields.get(req_field.name()) {
                Some(avail_field) => {
                    if !types_compatible(avail_field.data_type(), req_field.data_type()) {
                        missing.push(format!(
                            "{} (expected {}, got {})",
                            req_field.name(),
                            req_field.data_type(),
                            avail_field.data_type()
                        ));
                    }
                }
                None => {
                    missing.push(req_field.name().to_string());
                }
            }
        }
        if missing.is_empty() {
            Ok(())
        } else {
            Err(RplError::SchemaMismatch {
                provider_task: "upstream".to_string(),
                consumer_task: task_name.to_string(),
                missing_fields: missing,
            })
        }
    }

    /// Check that all named columns exist (for `drops` validation).
    pub fn contains_all(&self, names: &[String], task_name: &str) -> Result<()> {
        let missing: Vec<String> = names
            .iter()
            .filter(|n| !self.fields.contains_key(n.as_str()))
            .cloned()
            .collect();
        if missing.is_empty() {
            Ok(())
        } else {
            Err(RplError::SchemaMismatch {
                provider_task: "upstream".to_string(),
                consumer_task: task_name.to_string(),
                missing_fields: missing,
            })
        }
    }

    /// Apply a task's effect: remove dropped columns, add produced columns.
    pub fn apply(&mut self, produces: &Schema, drops: &[String]) {
        for name in drops {
            self.fields.remove(name);
        }
        for field in produces.fields() {
            self.fields.insert(field.name().clone(), field.as_ref().clone());
        }
    }

    /// Intersect with another column set, keeping only fields present in both
    /// with compatible types. Used at merge points (multiple predecessors).
    pub fn intersect(&self, other: &ColumnSet) -> ColumnSet {
        let mut fields = HashMap::new();
        for (name, field) in &self.fields {
            if let Some(other_field) = other.fields.get(name)
                && types_compatible(field.data_type(), other_field.data_type())
            {
                fields.insert(name.clone(), field.clone());
            }
        }
        ColumnSet { fields }
    }
}

/// Check if two Arrow data types are compatible.
///
/// For now, types must be equal. This could be relaxed to allow
/// safe coercions (e.g. Int32 -> Int64) in the future.
fn types_compatible(a: &DataType, b: &DataType) -> bool {
    a == b
}

/// Build an Arrow [`Schema`] from a slice of `(name, DataType)` pairs.
///
/// All fields are created as nullable. This is a convenience shorthand for
/// the common pattern of constructing schemas in task definitions.
///
/// ```
/// use arrow::datatypes::DataType;
/// use rpl::schema_of;
///
/// let s = schema_of(&[("id", DataType::Int64), ("value", DataType::Float64)]);
/// assert_eq!(s.fields().len(), 2);
/// ```
pub fn schema_of(fields: &[(&str, DataType)]) -> Schema {
    Schema::new(
        fields
            .iter()
            .map(|(name, dt)| Field::new(*name, dt.clone(), true))
            .collect::<Vec<_>>(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::DataType;

    fn schema(fields: &[(&str, DataType)]) -> Schema {
        Schema::new(
            fields
                .iter()
                .map(|(name, dt)| Field::new(*name, dt.clone(), true))
                .collect::<Vec<_>>(),
        )
    }

    #[test]
    fn column_set_satisfies_superset() {
        let available = ColumnSet::from_schema(&schema(&[
            ("a", DataType::Int32),
            ("b", DataType::Utf8),
            ("c", DataType::Float64),
        ]));
        let required = schema(&[("a", DataType::Int32), ("b", DataType::Utf8)]);
        assert!(available.satisfies(&required, "test").is_ok());
    }

    #[test]
    fn column_set_missing_field() {
        let available = ColumnSet::from_schema(&schema(&[("a", DataType::Int32)]));
        let required = schema(&[("a", DataType::Int32), ("b", DataType::Utf8)]);
        let err = available.satisfies(&required, "test").unwrap_err();
        match err {
            RplError::SchemaMismatch { missing_fields, .. } => {
                assert_eq!(missing_fields, vec!["b"]);
            }
            _ => panic!("expected SchemaMismatch"),
        }
    }

    #[test]
    fn column_set_type_mismatch() {
        let available = ColumnSet::from_schema(&schema(&[("a", DataType::Utf8)]));
        let required = schema(&[("a", DataType::Int32)]);
        assert!(available.satisfies(&required, "test").is_err());
    }

    #[test]
    fn column_set_apply() {
        let mut cs = ColumnSet::from_schema(&schema(&[
            ("a", DataType::Int32),
            ("b", DataType::Utf8),
        ]));
        let produces = schema(&[("c", DataType::Float64)]);
        cs.apply(&produces, &["b".to_string()]);
        assert!(cs.fields.contains_key("a"));
        assert!(!cs.fields.contains_key("b"));
        assert!(cs.fields.contains_key("c"));
    }

    #[test]
    fn column_set_intersect() {
        let a = ColumnSet::from_schema(&schema(&[
            ("x", DataType::Int32),
            ("y", DataType::Utf8),
        ]));
        let b = ColumnSet::from_schema(&schema(&[
            ("x", DataType::Int32),
            ("z", DataType::Float64),
        ]));
        let result = a.intersect(&b);
        assert!(result.fields.contains_key("x"));
        assert!(!result.fields.contains_key("y"));
        assert!(!result.fields.contains_key("z"));
    }

    #[test]
    fn empty_requires_always_satisfied() {
        let available = ColumnSet::from_schema(&schema(&[("a", DataType::Int32)]));
        assert!(available.satisfies(&Schema::empty(), "test").is_ok());
    }
}
