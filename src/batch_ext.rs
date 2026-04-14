//! Extension trait for Arrow [`RecordBatch`] column operations.
//!
//! The [`RecordBatchExt`] trait adds convenience methods for appending,
//! dropping, replacing, renaming, and splitting columns without manually
//! rebuilding the schema and column arrays each time.

use std::sync::Arc;

use arrow::array::Array;
use arrow::datatypes::{Field, Schema};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;

use crate::error::Result;

/// Convenience methods for common RecordBatch column operations.
pub trait RecordBatchExt {
    /// Append a new column to the batch.
    fn append_column(&self, name: &str, col: Arc<dyn Array>) -> Result<RecordBatch>;

    /// Remove a column by name.
    fn drop_column(&self, name: &str) -> Result<RecordBatch>;

    /// Replace a column by name with a new array (may change the type).
    fn replace_column(&self, name: &str, col: Arc<dyn Array>) -> Result<RecordBatch>;

    /// Split a batch into chunks of at most `max_rows` rows each.
    ///
    /// Uses `RecordBatch::slice` (zero-copy). Returns the batch as-is
    /// (in a single-element vec) when `num_rows <= max_rows`.
    fn split(&self, max_rows: usize) -> Vec<RecordBatch>;

    /// Get a typed reference to a column by name.
    ///
    /// # Example
    /// ```ignore
    /// let ids = batch.column_as::<Int64Array>("id")?;
    /// ```
    fn column_as<T: 'static>(&self, name: &str) -> Result<&T>;
}

impl RecordBatchExt for RecordBatch {
    fn append_column(&self, name: &str, col: Arc<dyn Array>) -> Result<RecordBatch> {
        let mut fields: Vec<Arc<Field>> = self.schema().fields().to_vec();
        fields.push(Arc::new(Field::new(name, col.data_type().clone(), true)));
        let schema = Arc::new(Schema::new(fields));

        let mut columns: Vec<Arc<dyn Array>> = self.columns().to_vec();
        columns.push(col);

        Ok(RecordBatch::try_new(schema, columns)?)
    }

    fn drop_column(&self, name: &str) -> Result<RecordBatch> {
        let idx = self.schema().index_of(name)?;
        let mut batch = self.clone();
        batch.remove_column(idx);
        Ok(batch)
    }

    fn column_as<T: 'static>(&self, name: &str) -> Result<&T> {
        let idx = self.schema().index_of(name)?;
        self.column(idx)
            .as_any()
            .downcast_ref::<T>()
            .ok_or_else(|| {
                ArrowError::CastError(format!("column '{name}' cannot be cast to requested type"))
                    .into()
            })
    }

    fn replace_column(&self, name: &str, col: Arc<dyn Array>) -> Result<RecordBatch> {
        let idx = self.schema().index_of(name)?;
        let fields: Vec<Arc<Field>> = self
            .schema()
            .fields()
            .iter()
            .enumerate()
            .map(|(i, f)| {
                if i == idx {
                    Arc::new(Field::new(name, col.data_type().clone(), true))
                } else {
                    f.clone()
                }
            })
            .collect();
        let columns: Vec<Arc<dyn Array>> = self
            .columns()
            .iter()
            .enumerate()
            .map(|(i, c)| if i == idx { col.clone() } else { c.clone() })
            .collect();
        let schema = Arc::new(Schema::new(fields));
        Ok(RecordBatch::try_new(schema, columns)?)
    }

    fn split(&self, max_rows: usize) -> Vec<RecordBatch> {
        assert!(max_rows > 0, "max_rows must be >= 1");
        let total = self.num_rows();
        if total <= max_rows {
            return vec![self.clone()];
        }
        let mut chunks = Vec::with_capacity(total.div_ceil(max_rows));
        let mut offset = 0;
        while offset < total {
            let len = (total - offset).min(max_rows);
            chunks.push(self.slice(offset, len));
            offset += len;
        }
        chunks
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Int32Array, StringArray};
    use arrow::datatypes::DataType;

    fn test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["x", "y"])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn append() {
        let batch = test_batch();
        let col = Arc::new(Float64Array::from(vec![1.0, 2.0]));
        let result = batch.append_column("c", col).unwrap();
        assert_eq!(result.num_columns(), 3);
        assert_eq!(result.schema().field(2).name(), "c");
    }

    #[test]
    fn drop() {
        let batch = test_batch();
        let result = batch.drop_column("b").unwrap();
        assert_eq!(result.num_columns(), 1);
        assert_eq!(result.schema().field(0).name(), "a");
    }

    #[test]
    fn replace() {
        let batch = test_batch();
        let col = Arc::new(Float64Array::from(vec![10.0, 20.0]));
        let result = batch.replace_column("a", col).unwrap();
        assert_eq!(result.num_columns(), 2);
        assert_eq!(result.schema().field(0).data_type(), &DataType::Float64);
    }

    #[test]
    fn split_exact() {
        // 6 rows, split into chunks of 3 -> two chunks of 3.
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6]))],
        )
        .unwrap();
        let chunks = batch.split(3);
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].num_rows(), 3);
        assert_eq!(chunks[1].num_rows(), 3);
    }

    #[test]
    fn split_remainder() {
        // 7 rows, split into chunks of 3 -> 3, 3, 1.
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7]))],
        )
        .unwrap();
        let chunks = batch.split(3);
        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[0].num_rows(), 3);
        assert_eq!(chunks[1].num_rows(), 3);
        assert_eq!(chunks[2].num_rows(), 1);
    }

    #[test]
    fn split_no_op() {
        // 2 rows, split into chunks of 5 -> single chunk as-is.
        let batch = test_batch();
        let chunks = batch.split(5);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].num_rows(), 2);
    }
}
