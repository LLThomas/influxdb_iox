//! Functions for computing a sort key based on cardinality of primary key columns.

use crate::data::{QueryableBatch, SnapshotBatch};
use arrow::{
    array::{Array, DictionaryArray, StringArray},
    datatypes::{DataType, Int32Type},
    record_batch::RecordBatch,
};
use observability_deps::tracing::trace;
use query::QueryChunkMeta;
use schema::{
    sort::{SortKey, SortKeyBuilder},
    TIME_COLUMN_NAME,
};
use std::{
    collections::{HashMap, HashSet},
    num::NonZeroU64,
    sync::Arc,
};

/// Given a `QueryableBatch`, compute a sort key based on:
///
/// - The columns that make up the primary key of the schema of this batch
/// - Order those columns from low cardinality to high cardinality based on the data
/// - Always have the time column last
pub fn compute_sort_key(queryable_batch: &QueryableBatch) -> SortKey {
    let schema = queryable_batch.schema();
    let primary_key = schema.primary_key();

    let cardinalities = distinct_counts(&queryable_batch.data, &primary_key);

    trace!(cardinalities=?cardinalities, "cardinalities of of columns to compute sort key");

    let mut cardinalities: Vec<_> = cardinalities.into_iter().collect();
    // Sort by (cardinality, column_name) to have deterministic order if same cardinality
    cardinalities.sort_by_cached_key(|x| (x.1, x.0.clone()));

    let mut builder = SortKeyBuilder::with_capacity(cardinalities.len() + 1);
    for (col, _) in cardinalities {
        builder = builder.with_col(col)
    }
    builder = builder.with_col(TIME_COLUMN_NAME);

    let key = builder.build();

    trace!(computed_sort_key=?key, "Value of sort key from compute_sort_key");

    key
}

/// Takes batches of data and the columns that make up the primary key. Computes the number of
/// distinct values for each primary key column across all batches, also known as "cardinality".
/// Used to determine sort order.
fn distinct_counts(
    batches: &[Arc<SnapshotBatch>],
    primary_key: &[&str],
) -> HashMap<String, NonZeroU64> {
    let mut distinct_values_across_batches = HashMap::with_capacity(primary_key.len());

    for batch in batches {
        for (column, distinct_values) in distinct_values(&batch.data, primary_key) {
            let set = distinct_values_across_batches
                .entry(column)
                .or_insert_with(HashSet::new);
            set.extend(distinct_values.into_iter());
        }
    }

    distinct_values_across_batches
        .into_iter()
        .filter_map(|(column, distinct_values)| {
            distinct_values
                .len()
                .try_into()
                .ok()
                .and_then(NonZeroU64::new)
                .map(|count| (column, count))
        })
        .collect()
}

/// Takes a `RecordBatch` and the column names that make up the primary key of the schema. Returns
/// a map of column names to the set of the distinct string values, for the specified columns. Used
/// to compute cardinality across multiple `RecordBatch`es.
fn distinct_values(batch: &RecordBatch, primary_key: &[&str]) -> HashMap<String, HashSet<String>> {
    let schema = batch.schema();
    batch
        .columns()
        .iter()
        .zip(schema.fields())
        .filter(|(_col, field)| primary_key.contains(&field.name().as_str()))
        .flat_map(|(col, field)| match field.data_type() {
            // Dictionaries of I32 => Utf8 are supported as tags in
            // `schema::InfluxColumnType::valid_arrow_type`
            DataType::Dictionary(key, value)
                if key.as_ref() == &DataType::Int32 && value.as_ref() == &DataType::Utf8 =>
            {
                let col = col
                    .as_any()
                    .downcast_ref::<DictionaryArray<Int32Type>>()
                    .expect("unexpected datatype");

                let values = col.values();
                let values = values
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("unexpected datatype");

                Some((
                    field.name().into(),
                    values.iter().flatten().map(ToString::to_string).collect(),
                ))
            }
            // Utf8 types are supported as tags
            DataType::Utf8 => {
                let values = col
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("unexpected datatype");

                Some((
                    field.name().into(),
                    values.iter().flatten().map(ToString::to_string).collect(),
                ))
            }
            // No other data types are supported as tags; don't compute distinct values for them
            _ => None,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use data_types2::SequenceNumber;
    use schema::selection::Selection;

    fn lp_to_queryable_batch(line_protocol_batches: &[&str]) -> QueryableBatch {
        let data = line_protocol_batches
            .iter()
            .map(|line_protocol| {
                let (_, mb) = mutable_batch_lp::test_helpers::lp_to_mutable_batch(line_protocol);
                let rb = mb.to_arrow(Selection::All).unwrap();

                Arc::new(SnapshotBatch {
                    min_sequencer_number: SequenceNumber::new(0),
                    max_sequencer_number: SequenceNumber::new(1),
                    data: Arc::new(rb),
                })
            })
            .collect();

        QueryableBatch {
            data,
            delete_predicates: Default::default(),
            table_name: Default::default(),
        }
    }

    #[test]
    fn test_distinct_values() {
        let lp = r#"
            cpu,host=a val=23 1
            cpu,host=b,env=prod val=2 1
            cpu,host=c,env=stage val=11 1
            cpu,host=a,env=prod val=14 2
        "#;
        let qb = lp_to_queryable_batch(&[lp]);
        let rb = &qb.data[0].data;

        // Pass the tag field names plus time as the primary key, this is what should happen
        let distinct = distinct_values(rb, &["host", "env", "time"]);

        // The hashmap should contain the distinct values for "host" and "env" only
        assert_eq!(distinct.len(), 2);

        // Return unique values
        assert_eq!(
            *distinct.get("host").unwrap(),
            HashSet::from(["a".into(), "b".into(), "c".into()]),
        );
        // TODO: do nulls count as a value?
        assert_eq!(
            *distinct.get("env").unwrap(),
            HashSet::from(["prod".into(), "stage".into()]),
        );

        // Requesting a column not present returns None
        assert_eq!(distinct.get("foo"), None);

        // Distinct count isn't computed for the time column or fields
        assert_eq!(distinct.get("time"), None);
        assert_eq!(distinct.get("val"), None);

        // Specify a column in the primary key that doesn't appear in the data
        let distinct = distinct_values(rb, &["host", "env", "foo", "time"]);
        // The hashmap should contain the distinct values for "host" and "env" only
        assert_eq!(distinct.len(), 2);

        // Don't specify one of the tag columns for the primary key
        let distinct = distinct_values(rb, &["host", "foo", "time"]);
        // The hashmap should contain the distinct values for the specified columns only
        assert_eq!(distinct.len(), 1);
    }

    #[test]
    fn test_sort_key() {
        // Across these three record batches:
        // - `host` has 2 distinct values: "a", "b"
        // - 'env' has 3 distinct values: "prod", "stage", "dev"
        // host's 2 values appear in each record batch, so the distinct counts could be incorrectly
        // aggregated together as 2 + 2 + 2 = 6. env's 3 values each occur in their own record
        // batch, so they should always be aggregated as 3.
        // host has the lower cardinality, so it should appear first in the sort key.
        let lp1 = r#"
            cpu,host=a,env=prod val=23 1
            cpu,host=b,env=prod val=2 2
        "#;
        let lp2 = r#"
            cpu,host=a,env=stage val=23 3
            cpu,host=b,env=stage val=2 4
        "#;
        let lp3 = r#"
            cpu,host=a,env=dev val=23 5
            cpu,host=b,env=dev val=2 6
        "#;
        let qb = lp_to_queryable_batch(&[lp1, lp2, lp3]);

        let sort_key = compute_sort_key(&qb);

        assert_eq!(sort_key, SortKey::from_columns(["host", "env", "time"]));
    }
}