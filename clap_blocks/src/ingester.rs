use crate::kafka_partitions::KafkaPartitionConfig;

/// CLI config for catalog ingest lifecycle
#[derive(Debug, Clone, clap::Parser)]
pub struct IngesterConfig {
    /// Kafka Partition options
    #[clap(flatten)]
    pub kafka_partition_config: KafkaPartitionConfig,

    /// The ingester will continue to pull data and buffer it from Kafka
    /// as long as it is below this size. If it hits this size it will pause
    /// ingest from Kafka until persistence goes below this threshold.
    #[clap(
        long = "--pause-ingest-size-bytes",
        env = "INFLUXDB_IOX_PAUSE_INGEST_SIZE_BYTES"
    )]
    pub pause_ingest_size_bytes: usize,

    /// Once the ingester crosses this threshold of data buffered across
    /// all sequencers, it will pick the largest partitions and persist
    /// them until it falls below this threshold. An ingester running in
    /// a steady state is expected to take up this much memory.
    #[clap(
        long = "--persist-memory-threshold-bytes",
        env = "INFLUXDB_IOX_PERSIST_MEMORY_THRESHOLD_BYTES"
    )]
    pub persist_memory_threshold_bytes: usize,

    /// If an individual partition crosses this size threshold, it will be persisted.
    /// The default value is 300MB (in bytes).
    #[clap(
        long = "--persist-partition-size-threshold-bytes",
        env = "INFLUXDB_IOX_PERSIST_PARTITION_SIZE_THRESHOLD_BYTES",
        default_value = "314572800"
    )]
    pub persist_partition_size_threshold_bytes: usize,

    /// If a partition has had data buffered for longer than this period of time
    /// it will be persisted. This puts an upper bound on how far back the
    /// ingester may need to read in Kafka on restart or recovery. The default value
    /// is 30 minutes (in seconds).
    #[clap(
        long = "--persist-partition-age-threshold-seconds",
        env = "INFLUXDB_IOX_PERSIST_PARTITION_AGE_THRESHOLD_SECONDS",
        default_value = "1800"
    )]
    pub persist_partition_age_threshold_seconds: u64,

    /// If a partition has had data buffered and hasn't received a write for this
    /// period of time, it will be persisted. The default value is 300 seconds (5 minutes).
    #[clap(
        long = "--persist-partition-cold-threshold-seconds",
        env = "INFLUXDB_IOX_PERSIST_PARTITION_COLD_THRESHOLD_SECONDS",
        default_value = "300"
    )]
    pub persist_partition_cold_threshold_seconds: u64,
}
