//! Implementation of command line option for running ingester

use crate::influxdb_ioxd::{
    self,
    server_type::{
        common_state::{CommonServerState, CommonServerStateError},
        ingester::IngesterServerType,
    },
};
use clap_blocks::{
    catalog_dsn::CatalogDsnConfig, run_config::RunConfig, write_buffer::WriteBufferConfig,
};
use ingester::{
    handler::IngestHandlerImpl,
    server::{grpc::GrpcDelegate, http::HttpDelegate, IngesterServer},
};
use iox_catalog::interface::KafkaPartition;
use object_store::ObjectStore;
use observability_deps::tracing::*;
use std::collections::BTreeMap;
use std::convert::TryFrom;
use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Run: {0}")]
    Run(#[from] influxdb_ioxd::Error),

    #[error("Invalid config: {0}")]
    InvalidConfig(#[from] CommonServerStateError),

    #[error("Catalog error: {0}")]
    Catalog(#[from] iox_catalog::interface::Error),

    #[error("Kafka topic {0} not found in the catalog")]
    KafkaTopicNotFound(String),

    #[error("Cannot parse object store config: {0}")]
    ObjectStoreParsing(#[from] clap_blocks::object_store::ParseError),

    #[error("kafka_partition_range_start must be <= kafka_partition_range_end")]
    KafkaRange,

    #[error("sequencer record not found for partition {0}")]
    SequencerNotFound(KafkaPartition),

    #[error("error initializing ingester: {0}")]
    Ingester(#[from] ingester::handler::Error),

    #[error("error initializing write buffer {0}")]
    WriteBuffer(#[from] write_buffer::core::WriteBufferError),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, clap::Parser)]
#[clap(
    name = "run",
    about = "Runs in ingester mode",
    long_about = "Run the IOx ingester server.\n\nThe configuration options below can be \
    set either with the command line flags or with the specified environment \
    variable. If there is a file named '.env' in the current working directory, \
    it is sourced before loading the configuration.
Configuration is loaded from the following sources (highest precedence first):
        - command line arguments
        - user set environment variables
        - .env file contents
        - pre-configured default values"
)]
pub struct Config {
    #[clap(flatten)]
    pub(crate) run_config: RunConfig,

    #[clap(flatten)]
    pub(crate) catalog_dsn: CatalogDsnConfig,

    #[clap(flatten)]
    pub(crate) write_buffer_config: WriteBufferConfig,

    /// Write buffer partition number to start (inclusive) range with
    #[clap(
        long = "--write-buffer-partition-range-start",
        env = "INFLUXDB_IOX_WRITE_BUFFER_PARTITION_RANGE_START"
    )]
    pub write_buffer_partition_range_start: i32,

    /// Write buffer partition number to end (inclusive) range with
    #[clap(
        long = "--write-buffer-partition-range-end",
        env = "INFLUXDB_IOX_WRITE_BUFFER_PARTITION_RANGE_END"
    )]
    pub write_buffer_partition_range_end: i32,
}

pub async fn command(config: Config) -> Result<()> {
    let common_state = CommonServerState::from_config(config.run_config.clone())?;

    let catalog = config.catalog_dsn.get_catalog("ingester").await?;

    let kafka_topic = catalog
        .kafka_topics()
        .get_by_name(&config.write_buffer_config.topic)
        .await?
        .ok_or_else(|| Error::KafkaTopicNotFound(config.write_buffer_config.topic.clone()))?;

    if config.write_buffer_partition_range_start > config.write_buffer_partition_range_end {
        return Err(Error::KafkaRange);
    }

    let kafka_partitions: Vec<_> = (config.write_buffer_partition_range_start
        ..=config.write_buffer_partition_range_end)
        .map(KafkaPartition::new)
        .collect();

    let object_store = Arc::new(
        ObjectStore::try_from(&config.run_config.object_store_config)
            .map_err(Error::ObjectStoreParsing)?,
    );

    let mut sequencers = BTreeMap::new();
    for k in kafka_partitions {
        let s = catalog
            .sequencers()
            .get_by_topic_id_and_partition(kafka_topic.id, k)
            .await?
            .ok_or(Error::SequencerNotFound(k))?;
        sequencers.insert(k, s);
    }

    let metric_registry: Arc<metric::Registry> = Default::default();
    let trace_collector = common_state.trace_collector();

    let write_buffer = config
        .write_buffer_config
        .reading(Arc::clone(&metric_registry), trace_collector.clone())
        .await?;

    let ingest_handler = Arc::new(
        IngestHandlerImpl::new(
            kafka_topic,
            sequencers,
            catalog,
            object_store,
            write_buffer,
            &metric_registry,
        )
        .await?,
    );
    let http = HttpDelegate::new(Arc::clone(&ingest_handler));
    let grpc = GrpcDelegate::new(ingest_handler);

    let ingester = IngesterServer::new(metric_registry, http, grpc);
    let server_type = Arc::new(IngesterServerType::new(ingester, &common_state));

    info!("starting ingester");

    Ok(influxdb_ioxd::main(common_state, server_type).await?)
}
