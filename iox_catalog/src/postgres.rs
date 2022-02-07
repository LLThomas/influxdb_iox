//! A Postgres backed implementation of the Catalog

use crate::interface::{
    Catalog, Column, ColumnRepo, ColumnType, Error, KafkaPartition, KafkaTopic, KafkaTopicId,
    KafkaTopicRepo, Namespace, NamespaceId, NamespaceRepo, ParquetFile, ParquetFileId,
    ParquetFileRepo, Partition, PartitionId, PartitionRepo, ProcessedTombstone,
    ProcessedTombstoneRepo, QueryPool, QueryPoolId, QueryPoolRepo, Result, SequenceNumber,
    Sequencer, SequencerId, SequencerRepo, Table, TableId, TableRepo, Timestamp, Tombstone,
    TombstoneId, TombstoneRepo,
};
use async_trait::async_trait;
use observability_deps::tracing::{info, warn};
use sqlx::{migrate::Migrator, postgres::PgPoolOptions, Executor, Pool, Postgres, Transaction};
use std::time::Duration;
use uuid::Uuid;

const MAX_CONNECTIONS: u32 = 5;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
const IDLE_TIMEOUT: Duration = Duration::from_secs(500);
/// the default schema name to use in Postgres
pub const SCHEMA_NAME: &str = "iox_catalog";

static MIGRATOR: Migrator = sqlx::migrate!();

/// In-memory catalog that implements the `RepoCollection` and individual repo traits.
#[derive(Debug)]
pub struct PostgresCatalog {
    pool: Pool<Postgres>,
}

// struct to get return value from "select count(*) ..." wuery"
#[derive(sqlx::FromRow)]
struct Count {
    count: i64,
}

impl PostgresCatalog {
    /// Connect to the catalog store.
    pub async fn connect(
        app_name: &'static str,
        schema_name: &'static str,
        dsn: &str,
    ) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .min_connections(1)
            .max_connections(MAX_CONNECTIONS)
            .connect_timeout(CONNECT_TIMEOUT)
            .idle_timeout(IDLE_TIMEOUT)
            .test_before_acquire(true)
            .after_connect(move |c| {
                Box::pin(async move {
                    // Tag the connection with the provided application name.
                    c.execute(sqlx::query("SET application_name = '$1';").bind(app_name))
                        .await?;
                    let search_path_query = format!("SET search_path TO public,{}", schema_name);
                    c.execute(sqlx::query(&search_path_query)).await?;

                    Ok(())
                })
            })
            .connect(dsn)
            .await
            .map_err(|e| Error::SqlxError { source: e })?;

        // Log a connection was successfully established and include the application
        // name for cross-correlation between Conductor logs & database connections.
        info!(application_name=%app_name, "connected to catalog store");

        Ok(Self { pool })
    }
}

#[async_trait]
impl Catalog for PostgresCatalog {
    async fn setup(&self) -> Result<(), Error> {
        MIGRATOR
            .run(&self.pool)
            .await
            .map_err(|e| Error::SqlxError { source: e.into() })?;

        Ok(())
    }

    fn kafka_topics(&self) -> &dyn KafkaTopicRepo {
        self
    }

    fn query_pools(&self) -> &dyn QueryPoolRepo {
        self
    }

    fn namespaces(&self) -> &dyn NamespaceRepo {
        self
    }

    fn tables(&self) -> &dyn TableRepo {
        self
    }

    fn columns(&self) -> &dyn ColumnRepo {
        self
    }

    fn sequencers(&self) -> &dyn SequencerRepo {
        self
    }

    fn partitions(&self) -> &dyn PartitionRepo {
        self
    }

    fn tombstones(&self) -> &dyn TombstoneRepo {
        self
    }

    fn parquet_files(&self) -> &dyn ParquetFileRepo {
        self
    }

    fn processed_tombstones(&self) -> &dyn ProcessedTombstoneRepo {
        self
    }

    async fn add_parquet_file_with_tombstones(
        &self,
        parquet_file: &ParquetFile,
        tombstones: &[Tombstone],
    ) -> Result<(ParquetFile, Vec<ProcessedTombstone>), Error> {
        // Start a transaction
        let txt = self.pool.begin().await;
        if let Err(error) = txt {
            return Err(Error::StartTransaction { source: error });
        }
        let mut txt = txt.unwrap();

        // create a parquet file in the catalog first
        let parquet = self
            .parquet_files()
            .create(
                Some(&mut txt),
                parquet_file.sequencer_id,
                parquet_file.table_id,
                parquet_file.partition_id,
                parquet_file.object_store_id,
                parquet_file.min_sequence_number,
                parquet_file.max_sequence_number,
                parquet_file.min_time,
                parquet_file.max_time,
            )
            .await;

        if let Err(error) = parquet {
            // Error while adding parquet file into the catalog, stop the transaction
            warn!(object_store_id=?parquet_file.object_store_id.to_string(), "{}", error.to_string());
            let _rollback = txt.rollback().await;
            return Err(error);
        }
        let parquet = parquet.unwrap();

        // Now the parquet available, create its processed tombstones
        let processed_tombstones = self
            .processed_tombstones()
            .create_many(Some(&mut txt), parquet.id, tombstones)
            .await;

        let processed_tombstones = match processed_tombstones {
            Ok(processed_tombstones) => processed_tombstones,
            Err(e) => {
                // Error while adding processed tombstones
                warn!(
                    "Error while adding processed tombstone: {}. Transaction stops.",
                    e.to_string()
                );
                let _rollback = txt.rollback().await;
                return Err(e);
            }
        };

        // Commit the transaction
        let _commit = txt.commit().await;

        Ok((parquet, processed_tombstones))
    }
}

#[async_trait]
impl KafkaTopicRepo for PostgresCatalog {
    async fn create_or_get(&self, name: &str) -> Result<KafkaTopic> {
        let rec = sqlx::query_as::<_, KafkaTopic>(
            r#"
INSERT INTO kafka_topic ( name )
VALUES ( $1 )
ON CONFLICT ON CONSTRAINT kafka_topic_name_unique
DO UPDATE SET name = kafka_topic.name RETURNING *;
        "#,
        )
        .bind(&name) // $1
        .fetch_one(&self.pool)
        .await
        .map_err(|e| Error::SqlxError { source: e })?;

        Ok(rec)
    }

    async fn get_by_name(&self, name: &str) -> Result<Option<KafkaTopic>> {
        let rec = sqlx::query_as::<_, KafkaTopic>(
            r#"
SELECT * FROM kafka_topic WHERE name = $1;
        "#,
        )
        .bind(&name) // $1
        .fetch_one(&self.pool)
        .await;

        if let Err(sqlx::Error::RowNotFound) = rec {
            return Ok(None);
        }

        let kafka_topic = rec.map_err(|e| Error::SqlxError { source: e })?;

        Ok(Some(kafka_topic))
    }
}

#[async_trait]
impl QueryPoolRepo for PostgresCatalog {
    async fn create_or_get(&self, name: &str) -> Result<QueryPool> {
        let rec = sqlx::query_as::<_, QueryPool>(
            r#"
INSERT INTO query_pool ( name )
VALUES ( $1 )
ON CONFLICT ON CONSTRAINT query_pool_name_unique
DO UPDATE SET name = query_pool.name RETURNING *;
        "#,
        )
        .bind(&name) // $1
        .fetch_one(&self.pool)
        .await
        .map_err(|e| Error::SqlxError { source: e })?;

        Ok(rec)
    }
}

#[async_trait]
impl NamespaceRepo for PostgresCatalog {
    async fn create(
        &self,
        name: &str,
        retention_duration: &str,
        kafka_topic_id: KafkaTopicId,
        query_pool_id: QueryPoolId,
    ) -> Result<Namespace> {
        let rec = sqlx::query_as::<_, Namespace>(
            r#"
INSERT INTO namespace ( name, retention_duration, kafka_topic_id, query_pool_id )
VALUES ( $1, $2, $3, $4 )
RETURNING *
        "#,
        )
        .bind(&name) // $1
        .bind(&retention_duration) // $2
        .bind(kafka_topic_id) // $3
        .bind(query_pool_id) // $4
        .fetch_one(&self.pool)
        .await
        .map_err(|e| {
            if is_unique_violation(&e) {
                Error::NameExists {
                    name: name.to_string(),
                }
            } else if is_fk_violation(&e) {
                Error::ForeignKeyViolation { source: e }
            } else {
                Error::SqlxError { source: e }
            }
        })?;

        Ok(rec)
    }

    async fn get_by_name(&self, name: &str) -> Result<Option<Namespace>> {
        let rec = sqlx::query_as::<_, Namespace>(
            r#"
SELECT * FROM namespace WHERE name = $1;
        "#,
        )
        .bind(&name) // $1
        .fetch_one(&self.pool)
        .await;

        if let Err(sqlx::Error::RowNotFound) = rec {
            return Ok(None);
        }

        let namespace = rec.map_err(|e| Error::SqlxError { source: e })?;

        Ok(Some(namespace))
    }
}

#[async_trait]
impl TableRepo for PostgresCatalog {
    async fn create_or_get(&self, name: &str, namespace_id: NamespaceId) -> Result<Table> {
        let rec = sqlx::query_as::<_, Table>(
            r#"
INSERT INTO table_name ( name, namespace_id )
VALUES ( $1, $2 )
ON CONFLICT ON CONSTRAINT table_name_unique
DO UPDATE SET name = table_name.name RETURNING *;
        "#,
        )
        .bind(&name) // $1
        .bind(&namespace_id) // $2
        .fetch_one(&self.pool)
        .await
        .map_err(|e| {
            if is_fk_violation(&e) {
                Error::ForeignKeyViolation { source: e }
            } else {
                Error::SqlxError { source: e }
            }
        })?;

        Ok(rec)
    }

    async fn list_by_namespace_id(&self, namespace_id: NamespaceId) -> Result<Vec<Table>> {
        let rec = sqlx::query_as::<_, Table>(
            r#"
SELECT * FROM table_name
WHERE namespace_id = $1;
            "#,
        )
        .bind(&namespace_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| Error::SqlxError { source: e })?;

        Ok(rec)
    }
}

#[async_trait]
impl ColumnRepo for PostgresCatalog {
    async fn create_or_get(
        &self,
        name: &str,
        table_id: TableId,
        column_type: ColumnType,
    ) -> Result<Column> {
        let ct = column_type as i16;

        let rec = sqlx::query_as::<_, Column>(
            r#"
INSERT INTO column_name ( name, table_id, column_type )
VALUES ( $1, $2, $3 )
ON CONFLICT ON CONSTRAINT column_name_unique
DO UPDATE SET name = column_name.name RETURNING *;
        "#,
        )
        .bind(&name) // $1
        .bind(&table_id) // $2
        .bind(&ct) // $3
        .fetch_one(&self.pool)
        .await
        .map_err(|e| {
            if is_fk_violation(&e) {
                Error::ForeignKeyViolation { source: e }
            } else {
                Error::SqlxError { source: e }
            }
        })?;

        if rec.column_type != ct {
            return Err(Error::ColumnTypeMismatch {
                name: name.to_string(),
                existing: rec.name,
                new: column_type.to_string(),
            });
        }

        Ok(rec)
    }

    async fn list_by_namespace_id(&self, namespace_id: NamespaceId) -> Result<Vec<Column>> {
        let rec = sqlx::query_as::<_, Column>(
            r#"
SELECT column_name.* FROM table_name
INNER JOIN column_name on column_name.table_id = table_name.id
WHERE table_name.namespace_id = $1;
            "#,
        )
        .bind(&namespace_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| Error::SqlxError { source: e })?;

        Ok(rec)
    }
}

#[async_trait]
impl SequencerRepo for PostgresCatalog {
    async fn create_or_get(
        &self,
        topic: &KafkaTopic,
        partition: KafkaPartition,
    ) -> Result<Sequencer> {
        sqlx::query_as::<_, Sequencer>(
            r#"
        INSERT INTO sequencer
            ( kafka_topic_id, kafka_partition, min_unpersisted_sequence_number )
        VALUES
            ( $1, $2, 0 )
        ON CONFLICT ON CONSTRAINT sequencer_unique
        DO UPDATE SET kafka_topic_id = sequencer.kafka_topic_id RETURNING *;
        "#,
        )
        .bind(&topic.id) // $1
        .bind(&partition) // $2
        .fetch_one(&self.pool)
        .await
        .map_err(|e| {
            if is_fk_violation(&e) {
                Error::ForeignKeyViolation { source: e }
            } else {
                Error::SqlxError { source: e }
            }
        })
    }

    async fn get_by_topic_id_and_partition(
        &self,
        topic_id: KafkaTopicId,
        partition: KafkaPartition,
    ) -> Result<Option<Sequencer>> {
        let rec = sqlx::query_as::<_, Sequencer>(
            r#"
SELECT * FROM sequencer WHERE kafka_topic_id = $1 AND kafka_partition = $2;
        "#,
        )
        .bind(topic_id) // $1
        .bind(partition) // $2
        .fetch_one(&self.pool)
        .await;

        if let Err(sqlx::Error::RowNotFound) = rec {
            return Ok(None);
        }

        let sequencer = rec.map_err(|e| Error::SqlxError { source: e })?;

        Ok(Some(sequencer))
    }

    async fn list(&self) -> Result<Vec<Sequencer>> {
        sqlx::query_as::<_, Sequencer>(r#"SELECT * FROM sequencer;"#)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| Error::SqlxError { source: e })
    }

    async fn list_by_kafka_topic(&self, topic: &KafkaTopic) -> Result<Vec<Sequencer>> {
        sqlx::query_as::<_, Sequencer>(r#"SELECT * FROM sequencer WHERE kafka_topic_id = $1;"#)
            .bind(&topic.id) // $1
            .fetch_all(&self.pool)
            .await
            .map_err(|e| Error::SqlxError { source: e })
    }
}

#[async_trait]
impl PartitionRepo for PostgresCatalog {
    async fn create_or_get(
        &self,
        key: &str,
        sequencer_id: SequencerId,
        table_id: TableId,
    ) -> Result<Partition> {
        sqlx::query_as::<_, Partition>(
            r#"
        INSERT INTO partition
            ( partition_key, sequencer_id, table_id )
        VALUES
            ( $1, $2, $3 )
        ON CONFLICT ON CONSTRAINT partition_key_unique
        DO UPDATE SET partition_key = partition.partition_key RETURNING *;
        "#,
        )
        .bind(key) // $1
        .bind(&sequencer_id) // $2
        .bind(&table_id) // $3
        .fetch_one(&self.pool)
        .await
        .map_err(|e| {
            if is_fk_violation(&e) {
                Error::ForeignKeyViolation { source: e }
            } else {
                Error::SqlxError { source: e }
            }
        })
    }

    async fn list_by_sequencer(&self, sequencer_id: SequencerId) -> Result<Vec<Partition>> {
        sqlx::query_as::<_, Partition>(r#"SELECT * FROM partition WHERE sequencer_id = $1;"#)
            .bind(&sequencer_id) // $1
            .fetch_all(&self.pool)
            .await
            .map_err(|e| Error::SqlxError { source: e })
    }
}

#[async_trait]
impl TombstoneRepo for PostgresCatalog {
    async fn create_or_get(
        &self,
        table_id: TableId,
        sequencer_id: SequencerId,
        sequence_number: SequenceNumber,
        min_time: Timestamp,
        max_time: Timestamp,
        predicate: &str,
    ) -> Result<Tombstone> {
        sqlx::query_as::<_, Tombstone>(
            r#"
        INSERT INTO tombstone
            ( table_id, sequencer_id, sequence_number, min_time, max_time, serialized_predicate )
        VALUES
            ( $1, $2, $3, $4, $5, $6 )
        ON CONFLICT ON CONSTRAINT tombstone_unique
        DO UPDATE SET table_id = tombstone.table_id RETURNING *;
        "#,
        )
        .bind(&table_id) // $1
        .bind(&sequencer_id) // $2
        .bind(&sequence_number) // $3
        .bind(&min_time) // $4
        .bind(&max_time) // $5
        .bind(predicate) // $6
        .fetch_one(&self.pool)
        .await
        .map_err(|e| {
            if is_fk_violation(&e) {
                Error::ForeignKeyViolation { source: e }
            } else {
                Error::SqlxError { source: e }
            }
        })
    }

    async fn list_tombstones_by_sequencer_greater_than(
        &self,
        sequencer_id: SequencerId,
        sequence_number: SequenceNumber,
    ) -> Result<Vec<Tombstone>> {
        sqlx::query_as::<_, Tombstone>(r#"SELECT * FROM tombstone WHERE sequencer_id = $1 AND sequence_number > $2 ORDER BY id;"#)
            .bind(&sequencer_id) // $1
            .bind(&sequence_number) // $2
            .fetch_all(&self.pool)
            .await
            .map_err(|e| Error::SqlxError { source: e })
    }
}

#[async_trait]
impl ParquetFileRepo for PostgresCatalog {
    async fn create(
        &self,
        txt: Option<&mut Transaction<'_, Postgres>>,
        sequencer_id: SequencerId,
        table_id: TableId,
        partition_id: PartitionId,
        object_store_id: Uuid,
        min_sequence_number: SequenceNumber,
        max_sequence_number: SequenceNumber,
        min_time: Timestamp,
        max_time: Timestamp,
    ) -> Result<ParquetFile> {
        let rec = sqlx::query_as::<_, ParquetFile>(
            r#"
INSERT INTO parquet_file ( sequencer_id, table_id, partition_id, object_store_id, min_sequence_number, max_sequence_number, min_time, max_time, to_delete )
VALUES ( $1, $2, $3, $4, $5, $6, $7, $8, false )
RETURNING *
        "#,
        )
            .bind(sequencer_id) // $1
            .bind(table_id) // $2
            .bind(partition_id) // $3
            .bind(object_store_id) // $4
            .bind(min_sequence_number) // $5
            .bind(max_sequence_number) // $6
            .bind(min_time) // $7
            .bind(max_time); // $8

        let rec = match txt {
            Some(txt) => rec.fetch_one(txt).await,
            None => rec.fetch_one(&self.pool).await,
        };

        let rec = rec.map_err(|e| {
            if is_unique_violation(&e) {
                Error::FileExists { object_store_id }
            } else if is_fk_violation(&e) {
                Error::ForeignKeyViolation { source: e }
            } else {
                Error::SqlxError { source: e }
            }
        })?;

        Ok(rec)
    }

    async fn flag_for_delete(&self, id: ParquetFileId) -> Result<()> {
        let _ = sqlx::query(r#"UPDATE parquet_file SET to_delete = true WHERE id = $1;"#)
            .bind(&id) // $1
            .execute(&self.pool)
            .await
            .map_err(|e| Error::SqlxError { source: e })?;

        Ok(())
    }

    async fn list_by_sequencer_greater_than(
        &self,
        sequencer_id: SequencerId,
        sequence_number: SequenceNumber,
    ) -> Result<Vec<ParquetFile>> {
        sqlx::query_as::<_, ParquetFile>(r#"SELECT * FROM parquet_file WHERE sequencer_id = $1 AND max_sequence_number > $2 ORDER BY id;"#)
            .bind(&sequencer_id) // $1
            .bind(&sequence_number) // $2
            .fetch_all(&self.pool)
            .await
            .map_err(|e| Error::SqlxError { source: e })
    }

    async fn exist(&self, id: ParquetFileId) -> Result<bool> {
        let read_result = sqlx::query_as::<_, Count>(
            r#"SELECT count(*) as count FROM parquet_file WHERE id = $1;"#,
        )
        .bind(&id) // $1
        .fetch_one(&self.pool)
        .await
        .map_err(|e| Error::SqlxError { source: e })?;

        Ok(read_result.count > 0)
    }

    async fn count(&self) -> Result<i64> {
        let read_result =
            sqlx::query_as::<_, Count>(r#"SELECT count(*) as count  FROM parquet_file;"#)
                .fetch_one(&self.pool)
                .await
                .map_err(|e| Error::SqlxError { source: e })?;

        Ok(read_result.count)
    }
}

#[async_trait]
impl ProcessedTombstoneRepo for PostgresCatalog {
    async fn create_many(
        &self,
        txt: Option<&mut Transaction<'_, Postgres>>,
        parquet_file_id: ParquetFileId,
        tombstones: &[Tombstone],
    ) -> Result<Vec<ProcessedTombstone>> {
        if txt.is_none() {
            return Err(Error::NoTransaction);
        }
        let txt = txt.unwrap();

        // no transaction provided
        // todo: we should never needs this but since right now we implement 2 catalogs,
        // postgres (for production)  and mem (for testing only) that does not need to provide txt
        // this will be refactor when Marco has his new abstraction done
        let mut processed_tombstones = vec![];
        for tombstone in tombstones {
            let processed_tombstone = sqlx::query_as::<_, ProcessedTombstone>(
                r#"
                INSERT INTO processed_tombstone ( tombstone_id, parquet_file_id )
                VALUES ( $1, $2 )
                RETURNING *
                "#,
            )
            .bind(tombstone.id) // $1
            .bind(parquet_file_id) // $2
            .fetch_one(&mut *txt)
            .await
            .map_err(|e| {
                if is_unique_violation(&e) {
                    Error::ProcessTombstoneExists {
                        tombstone_id: tombstone.id.get(),
                        parquet_file_id: parquet_file_id.get(),
                    }
                } else if is_fk_violation(&e) {
                    Error::ForeignKeyViolation { source: e }
                } else {
                    Error::SqlxError { source: e }
                }
            })?;

            processed_tombstones.push(processed_tombstone);
        }

        Ok(processed_tombstones)
    }

    async fn exist(
        &self,
        parquet_file_id: ParquetFileId,
        tombstone_id: TombstoneId,
    ) -> Result<bool> {
        let read_result = sqlx::query_as::<_, Count>(
            r#"SELECT count(*) as count FROM processed_tombstone WHERE parquet_file_id = $1 AND tombstone_id = $2;"#)
            .bind(&parquet_file_id) // $1
            .bind(&tombstone_id) // $2
            .fetch_one(&self.pool)
            .await
            .map_err(|e| Error::SqlxError { source: e })?;

        Ok(read_result.count > 0)
    }

    async fn count(&self) -> Result<i64> {
        let read_result =
            sqlx::query_as::<_, Count>(r#"SELECT count(*) as count FROM processed_tombstone;"#)
                .fetch_one(&self.pool)
                .await
                .map_err(|e| Error::SqlxError { source: e })?;

        Ok(read_result.count)
    }
}

/// The error code returned by Postgres for a unique constraint violation.
///
/// See <https://www.postgresql.org/docs/9.2/errcodes-appendix.html>
const PG_UNIQUE_VIOLATION: &str = "23505";

/// Returns true if `e` is a unique constraint violation error.
fn is_unique_violation(e: &sqlx::Error) -> bool {
    if let sqlx::Error::Database(inner) = e {
        if let Some(code) = inner.code() {
            if code == PG_UNIQUE_VIOLATION {
                return true;
            }
        }
    }

    false
}

/// Error code returned by Postgres for a foreign key constraint violation.
const PG_FK_VIOLATION: &str = "23503";

fn is_fk_violation(e: &sqlx::Error) -> bool {
    if let sqlx::Error::Database(inner) = e {
        if let Some(code) = inner.code() {
            if code == PG_FK_VIOLATION {
                return true;
            }
        }
    }

    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;
    use std::sync::Arc;

    // Helper macro to skip tests if TEST_INTEGRATION and the AWS environment variables are not set.
    macro_rules! maybe_skip_integration {
        () => {{
            dotenv::dotenv().ok();

            let required_vars = ["DATABASE_URL"];
            let unset_vars: Vec<_> = required_vars
                .iter()
                .filter_map(|&name| match env::var(name) {
                    Ok(_) => None,
                    Err(_) => Some(name),
                })
                .collect();
            let unset_var_names = unset_vars.join(", ");

            let force = env::var("TEST_INTEGRATION");

            if force.is_ok() && !unset_var_names.is_empty() {
                panic!(
                    "TEST_INTEGRATION is set, \
                            but variable(s) {} need to be set",
                    unset_var_names
                );
            } else if force.is_err() {
                eprintln!(
                    "skipping Postgres integration test - set {}TEST_INTEGRATION to run",
                    if unset_var_names.is_empty() {
                        String::new()
                    } else {
                        format!("{} and ", unset_var_names)
                    }
                );
                return;
            }
        }};
    }

    async fn setup_db() -> PostgresCatalog {
        let dsn = std::env::var("DATABASE_URL").unwrap();
        PostgresCatalog::connect("test", SCHEMA_NAME, &dsn)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn test_catalog() {
        // If running an integration test on your laptop, this requires that you have Postgres
        // running and that you've done the sqlx migrations. See the README in this crate for
        // info to set it up.
        maybe_skip_integration!();

        let postgres = setup_db().await;
        postgres.setup().await.unwrap();
        clear_schema(&postgres.pool).await;
        let postgres: Arc<dyn Catalog> = Arc::new(postgres);

        crate::interface::test_helpers::test_catalog(postgres).await;
    }

    async fn clear_schema(pool: &Pool<Postgres>) {
        sqlx::query("delete from processed_tombstone;")
            .execute(pool)
            .await
            .unwrap();
        sqlx::query("delete from tombstone;")
            .execute(pool)
            .await
            .unwrap();
        sqlx::query("delete from parquet_file;")
            .execute(pool)
            .await
            .unwrap();
        sqlx::query("delete from column_name;")
            .execute(pool)
            .await
            .unwrap();
        sqlx::query("delete from partition;")
            .execute(pool)
            .await
            .unwrap();
        sqlx::query("delete from table_name;")
            .execute(pool)
            .await
            .unwrap();
        sqlx::query("delete from namespace;")
            .execute(pool)
            .await
            .unwrap();
        sqlx::query("delete from sequencer;")
            .execute(pool)
            .await
            .unwrap();
    }
}
