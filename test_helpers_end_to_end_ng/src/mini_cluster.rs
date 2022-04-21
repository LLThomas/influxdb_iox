use std::{sync::{Arc, Weak}, collections::HashMap};

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

    /// Create a new MiniCluster that shares the same underlying
    /// servers and a new unique namespace. Internal implementation --
    /// see [create_shared], [create_shared_quickly_peristing] and [new]
    /// to create new MiniClusters.
    fn new_from_servers(
        router2: Option<Arc<TestServer>>,
        ingester: Option<Arc<TestServer>>,
        querier: Option<Arc<TestServer>>,
        compactor: Option<Arc<TestServer>>,
    ) -> Self {
        let org_id = rand_id();
        let bucket_id = rand_id();
        let namespace = format!("{}_{}", org_id, bucket_id);

        Self {
            router2,
            ingester,
            querier,
            compactor,
            other_servers: vec![],

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
        let mut shared_servers = GLOBAL_SHARED_SERVERS.lock().await;

        let hash_key = "SHARED".to_string();

        let maybe_cluster = shared_servers.get(&hash_key)
            .and_then(|shared| shared.try_new_cluster());


        let new_cluster = match maybe_cluster {
            Some(cluster) => {
                println!("AAL reusing existing cluster");
                cluster
            },
            None => {
                println!("AAL need to create a new server");

                // First time through, need to create one
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
        };

        // Update the shared servers to point at the newly created server proesses
        shared_servers.insert(hash_key, SharedServers::new(&new_cluster));
        new_cluster
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

/// holds shared server processes
struct SharedServers {
    router2: Option<Weak<TestServer>>,
    ingester: Option<Weak<TestServer>>,
    querier: Option<Weak<TestServer>>,
    compactor: Option<Weak<TestServer>>,
}

impl SharedServers {
    /// Save the server processes in this shared servers as weak references
    pub fn new(cluster: &MiniCluster) -> Self {
        assert!(cluster.other_servers.is_empty(), "other servers not yet handled in shared mini clusters");
        let tag_name = cluster.router2.as_ref().map(|f| f.to_string()).unwrap_or_default();
        println!("AAL Saving exisitng servers: {}", tag_name);
        Self {
            router2: cluster.router2.as_ref().map(Arc::downgrade),
            ingester: cluster.ingester.as_ref().map(Arc::downgrade),
            querier: cluster.querier.as_ref().map(Arc::downgrade),
            compactor: cluster.compactor.as_ref().map(Arc::downgrade),
        }


    }

    /// tries to create a new MiniCluster reusing the existing
    /// [TestServer]s. Return None if they are no longer active
    fn try_new_cluster(&self) -> Option<MiniCluster> {
        // The goal of the following code is to bail out (return None
        // from the function) if any of the optional weak references aren't present
        let router2 = if let Some(router2) = self.router2.as_ref() {
            Some(router2.upgrade()?) // return None if can't upgrade
        } else {
            None
        };

        let ingester = if let Some(ingester) = self.ingester.as_ref() {
            Some(ingester.upgrade()?) // return None if can't upgrade
        } else {
            None
        };

        let querier = if let Some(querier) = self.querier.as_ref() {
            Some(querier.upgrade()?) // return None if can't upgrade
        } else {
            None
        };

        let compactor = if let Some(compactor) = self.compactor.as_ref() {
            Some(compactor.upgrade()?) // return None if can't upgrade
        } else {
            None
        };

        // If haven't returned above, means we were able to upgrade
        // all available servers
        let tag_name = router2.as_ref().map(|f| f.to_string()).unwrap_or_default();
        println!("AAL Reusing new servers: {}", tag_name);
        Some(MiniCluster::new_from_servers(router2, ingester, querier, compactor))
    }
}




lazy_static::lazy_static! {
    static ref GLOBAL_SHARED_SERVERS: Mutex<HashMap<String, SharedServers>> = Mutex::new(HashMap::new());
}
