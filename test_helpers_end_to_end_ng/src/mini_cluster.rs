use std::sync::{Arc, Weak};

use http::Response;
use hyper::Body;
use tokio::sync::Mutex;

use crate::{
    rand_id,
    server_fixture::{create_test_server, restart_test_server},
    write_to_router, TestConfig, TestServer,
};

/// Structure that holds NG services and helpful accessors
#[derive(Debug, Default)]
pub struct MiniCluster {
    /// Standard optional router2
    router2: Option<Arc<TestServer>>,

    /// Standard optional ingster2
    ingester: Option<Arc<TestServer>>,

    /// Standard optional querier
    querier: Option<Arc<TestServer>>,

    /// Standard optional compactor
    compactor: Option<Arc<TestServer>>,

    /// Optional additional `Arc<TestServer>`s that can be used for specific tests
    other_servers: Vec<Arc<TestServer>>,

    // Potentially helpful data
    org_id: String,
    bucket_id: String,
    namespace: String,
}

impl MiniCluster {
    /// Create a new, unshared MiniCluster.
    pub fn new() -> Self {
        let org_id = rand_id();
        let bucket_id = rand_id();
        let namespace = format!("{}_{}", org_id, bucket_id);

        Self {
            org_id,
            bucket_id,
            namespace,
            ..Self::default()
        }
    }

    /// Create a new mini cluser that shares the same underlying
    /// servers as `template` but has a different namespace
    fn with_new_namespace(&self) -> Self {
        let org_id = rand_id();
        let bucket_id = rand_id();
        let namespace = format!("{}_{}", org_id, bucket_id);

        Self {
            router2: self.router2.clone(),
            ingester: self.ingester.clone(),
            querier: self.querier.clone(),
            compactor: self.compactor.clone(),
            other_servers: self.other_servers.clone(),

            org_id,
            bucket_id,
            namespace,
        }
    }

    /// Create a "standard" shared MiniCluster that has a router, ingester,
    /// querier
    ///
    /// Note: Since the underlying server processes are shared across multiple
    /// tests so all users of this MiniCluster should only modify
    /// their namespace
    pub async fn create_shared(database_url: String) -> MiniCluster {
        let mut global_shared_cluster = GLOBAL_SHARED_CLUSTER.lock().await;

        // see if there are any concurrently used cluster
        let global_cluster = global_shared_cluster.take().and_then(|w| w.upgrade());

        let global_cluster = match global_cluster {
            Some(cluster) => cluster,
            None => {
                // First time through, need to create one
                let router2_config = TestConfig::new_router2(&database_url);
                // fast parquet
                let ingester_config =
                    TestConfig::new_ingester(&router2_config).with_fast_parquet_generation();
                let querier_config = TestConfig::new_querier(&ingester_config);

                // Set up the cluster  ====================================
                Arc::new(
                    Self::new()
                        .with_router2(router2_config)
                        .await
                        .with_ingester(ingester_config)
                        .await
                        .with_querier(querier_config)
                        .await,
                )
            }
        };

        let cluster = global_cluster.with_new_namespace();
        // Put the shared cluster back
        *global_shared_cluster = Some(Arc::downgrade(&global_cluster));
        cluster
    }

    /// return a "standard" shared MiniCluster that has a router, ingester,
    /// querier and quickly persists files to parquet
    ///
    /// Note: The underlying server processes are shared across multiple
    /// tests so all users of this MiniCluster should only modify
    /// their namespace
    pub async fn create_shared_quickly_peristing(database_url: String) -> MiniCluster {
        let router2_config = TestConfig::new_router2(&database_url);
        // fast parquet
        let ingester_config =
            TestConfig::new_ingester(&router2_config).with_fast_parquet_generation();
        let querier_config = TestConfig::new_querier(&ingester_config);

        // Set up the cluster  ====================================
        Self::new()
            .with_router2(router2_config)
            .await
            .with_ingester(ingester_config)
            .await
            .with_querier(querier_config)
            .await
    }

    /// create a router2 with the specified configuration
    pub async fn with_router2(mut self, router2_config: TestConfig) -> Self {
        self.router2 = Some(create_test_server(router2_config).await);
        self
    }

    /// create an ingester with the specified configuration;
    pub async fn with_ingester(mut self, ingester_config: TestConfig) -> Self {
        self.ingester = Some(create_test_server(ingester_config).await);
        self
    }

    /// create an querier with the specified configuration;
    pub async fn with_querier(mut self, querier_config: TestConfig) -> Self {
        self.querier = Some(create_test_server(querier_config).await);
        self
    }

    /// create a compactor with the specified configuration;
    pub async fn with_compactor(mut self, compactor_config: TestConfig) -> Self {
        self.compactor = Some(create_test_server(compactor_config).await);
        self
    }

    /// create another server compactor with the specified configuration;
    pub async fn with_other(mut self, config: TestConfig) -> Self {
        self.other_servers.push(create_test_server(config).await);
        self
    }

    /// Retrieve the underlying router2 server, if set
    pub fn router2(&self) -> &TestServer {
        self.router2.as_ref().expect("router2 not initialized")
    }

    /// Retrieve the underlying ingester server, if set
    pub fn ingester(&self) -> &TestServer {
        self.ingester.as_ref().expect("ingester not initialized")
    }

    /// Restart ingester.
    ///
    /// This will break all currently connected clients!
    pub async fn restart_ingester(&mut self) {
        self.ingester =
            Some(restart_test_server(self.ingester.take().expect("ingester not initialized")).await)
    }

    /// Retrieve the underlying querier server, if set
    pub fn querier(&self) -> &TestServer {
        self.querier.as_ref().expect("querier not initialized")
    }

    /// Retrieve the underlying compactor server, if set
    pub fn compactor(&self) -> &TestServer {
        self.compactor.as_ref().expect("compactor not initialized")
    }

    /// Get a reference to the mini cluster's org.
    pub fn org_id(&self) -> &str {
        self.org_id.as_ref()
    }

    /// Get a reference to the mini cluster's bucket.
    pub fn bucket_id(&self) -> &str {
        self.bucket_id.as_ref()
    }

    /// Get a reference to the mini cluster's namespace.
    pub fn namespace(&self) -> &str {
        self.namespace.as_ref()
    }

    /// Writes the line protocol to the write_base/api/v2/write endpoint on the router into the org/bucket
    pub async fn write_to_router(&self, line_protocol: impl Into<String>) -> Response<Body> {
        write_to_router(
            line_protocol,
            &self.org_id,
            &self.bucket_id,
            self.router2().router_http_base(),
        )
        .await
    }

    /// Get a reference to the mini cluster's other servers.
    pub fn other_servers(&self) -> &[Arc<TestServer>] {
        self.other_servers.as_ref()
    }
}

lazy_static::lazy_static! {
    static ref GLOBAL_SHARED_CLUSTER: Mutex<Option<Weak<MiniCluster>>> = Mutex::new(None);
}
