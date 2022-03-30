use clap_blocks::run_config::RunConfig;
use ioxd_common::{
    grpc_listener, http_listener, serve,
    server_type::{CommonServerState, ServerType},
};
use observability_deps::tracing::{error, info};
use panic_logging::SendPanicsToTracing;
use snafu::{ResultExt, Snafu};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

#[cfg(all(not(feature = "heappy"), feature = "jemalloc_replacing_malloc"))]
mod jemalloc;

pub mod server_type;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("{}", source))]
    Wrapper { source: ioxd_common::Error },

    #[snafu(display("Error joining server task: {}", source))]
    Joining { source: tokio::task::JoinError },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

impl From<ioxd_common::Error> for Error {
    fn from(source: ioxd_common::Error) -> Self {
        Self::Wrapper { source }
    }
}

#[cfg(all(not(feature = "heappy"), not(feature = "jemalloc_replacing_malloc")))]
fn build_malloc_conf() -> String {
    "system".to_string()
}

#[cfg(all(feature = "heappy", not(feature = "jemalloc_replacing_malloc")))]
fn build_malloc_conf() -> String {
    "heappy".to_string()
}

#[cfg(all(not(feature = "heappy"), feature = "jemalloc_replacing_malloc"))]
fn build_malloc_conf() -> String {
    tikv_jemalloc_ctl::config::malloc_conf::mib()
        .unwrap()
        .read()
        .unwrap()
        .to_string()
}

#[cfg(all(
    feature = "heappy",
    feature = "jemalloc_replacing_malloc",
    not(feature = "clippy")
))]
fn build_malloc_conf() -> String {
    compile_error!("must use exactly one memory allocator")
}

#[cfg(feature = "clippy")]
fn build_malloc_conf() -> String {
    "clippy".to_string()
}

/// A service that will start on the specified addresses
pub struct Service {
    http_bind_address: Option<clap_blocks::socket_addr::SocketAddr>,
    grpc_bind_address: clap_blocks::socket_addr::SocketAddr,
    server_type: Arc<dyn ServerType>,
}

impl Service {
    pub fn create(server_type: Arc<dyn ServerType>, run_config: &RunConfig) -> Self {
        Self {
            http_bind_address: Some(run_config.http_bind_address),
            grpc_bind_address: run_config.grpc_bind_address,
            server_type,
        }
    }

    pub fn create_grpc_only(server_type: Arc<dyn ServerType>, run_config: &RunConfig) -> Self {
        Self {
            http_bind_address: None,
            grpc_bind_address: run_config.grpc_bind_address,
            server_type,
        }
    }
}

/// This is the entry point for the IOx server.
///
/// This entry point ensures that the given set of Services are
/// started using best practice, e.g. that we print the GIT-hash and
/// malloc-configs, that a panic handler is installed, etc.
///
/// Due to its invasive nature (install global panic handling,
/// logging, etc) this function should not be used during unit tests.
pub async fn main(common_state: CommonServerState, services: Vec<Service>) -> Result<()> {
    let git_hash = option_env!("GIT_HASH").unwrap_or("UNKNOWN");
    let num_cpus = num_cpus::get();
    let build_malloc_conf = build_malloc_conf();
    info!(
        git_hash,
        num_cpus,
        %build_malloc_conf,
        "InfluxDB IOx server starting",
    );

    for service in &services {
        if let Some(http_bind_address) = &service.http_bind_address {
            if (&service.grpc_bind_address == http_bind_address)
                && (service.grpc_bind_address.port() != 0)
            {
                error!(
                    grpc_bind_address=%service.grpc_bind_address,
                    http_bind_address=%http_bind_address,
                    "grpc and http bind addresses must differ",
                );
                std::process::exit(1);
            }
        }
    }

    // Install custom panic handler and forget about it.
    //
    // This leaks the handler and prevents it from ever being dropped during the
    // lifetime of the program - this is actually a good thing, as it prevents
    // the panic handler from being removed while unwinding a panic (which in
    // turn, causes a panic - see #548)
    let f = SendPanicsToTracing::new();
    std::mem::forget(f);

    // Register jemalloc metrics
    #[cfg(all(not(feature = "heappy"), feature = "jemalloc_replacing_malloc"))]
    for service in &services {
        service
            .server_type
            .metric_registry()
            .register_instrument("jemalloc_metrics", jemalloc::JemallocMetrics::new);
    }

    // Construct a token to trigger clean shutdown
    let frontend_shutdown = CancellationToken::new();

    let mut serving_futures = Vec::new();
    for service in services {
        let common_state = common_state.clone();
        // start them all in their own tasks so the servers run at the same time
        let frontend_shutdown = frontend_shutdown.clone();
        serving_futures.push(tokio::spawn(async move {
            let trace_exporter = common_state.trace_exporter();
            let Service {
                http_bind_address,
                grpc_bind_address,
                server_type,
            } = service;

            info!(?grpc_bind_address, "Binding gRPC services");
            let grpc_listener = grpc_listener(grpc_bind_address.into()).await?;

            let http_listener = match http_bind_address {
                Some(http_bind_address) => {
                    info!(?http_bind_address, "Completed bind of gRPC, binding http");
                    Some(http_listener(http_bind_address.into()).await?)
                }
                None => {
                    info!("No http server specified");
                    None
                }
            };

            let r = serve(
                common_state,
                frontend_shutdown,
                grpc_listener,
                http_listener,
                server_type,
            )
            .await;

            info!(
                ?grpc_bind_address,
                ?http_bind_address,
                "done serving, draining futures"
            );
            if let Some(trace_exporter) = trace_exporter {
                if let Err(e) = trace_exporter.drain().await {
                    error!(%e, "error draining trace exporter");
                }
            }
            r
        }));
    }

    for f in serving_futures {
        // Use ?? to unwrap Result<Result<..>>
        // "I heard you like errors, so I put an error in your error...."
        f.await.context(JoiningSnafu)??;
    }

    Ok(())
}