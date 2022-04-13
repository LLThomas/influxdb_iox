use crate::kafka_partitions::KafkaPartitionConfig;

/// CLI config for compactor
#[derive(Debug, Clone, clap::Parser)]
pub struct CompactorConfig {
    /// Write buffer topic/database that the compactor will be compacting files for. It won't
    /// connect to Kafka, but uses this to get the sequencers out of the catalog.
    #[clap(
        long = "--write-buffer-topic",
        env = "INFLUXDB_IOX_WRITE_BUFFER_TOPIC",
        default_value = "iox-shared"
    )]
    pub topic: String,

    /// Kafka Partition options
    #[clap(flatten)]
    pub kafka_partition_config: KafkaPartitionConfig,

    /// Percentage of least recent data we want to split to reduce compacting non-overlapped data
    /// Must be between 0 and 100. Default is 100, which won't split the resulting file.
    #[clap(
        long = "--compaction-split-percentage",
        env = "INFLUXDB_IOX_COMPACTION_SPLIT_PERCENTAGE",
        default_value = "100"
    )]
    pub split_percentage: i64,

    /// The compactor will limit the number of simultaneous compaction jobs based on the
    /// size of the input files to be compacted. Currently this only takes into account the
    /// level 0 files, but should later also consider the level 1 files to be compacted. This
    /// number should be less than 1/10th of the available memory to ensure compactions have
    /// enough space to run. Default is 100,000,000 (100MB).
    #[clap(
        long = "--compaction-concurrent-size-bytes",
        env = "INFLUXDB_IOX_COMPACTION_CONCURRENT_SIZE_BYTES",
        default_value = "100000000"
    )]
    pub max_concurrent_compaction_size_bytes: i64,

    /// The compactor will compact overlapped files no matter how much large they are.
    /// For non-overlapped and contiguous files, compactor will also compact them into
    /// a larger file of max size defined by the config value.
    /// Default is 100,000,000 (100MB).
    #[clap(
        long = "--compaction-max-size-bytes",
        env = "INFLUXDB_IOX_COMPACTION_MAX_SIZE_BYTES",
        default_value = "100000000"
    )]
    pub compaction_max_size_bytes: i64,
}
