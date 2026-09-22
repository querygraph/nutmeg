//! `nutmeg-server [--ip 127.0.0.1] [--port 50051]` — Sail's Spark Connect
//! server, unchanged, with Nutmeg installed in every session.
//!
//! Everything here is Sail's public API plus one hook Sail does not have
//! yet: a session factory chosen by the embedder
//! (`serve_with_session_factory`, `SparkSessionMutator::new`). That hook is
//! the isolated commit on the `session-factory-hook` branch of the Sail
//! checkout; see `docs/sail-prs.md`.
use std::net::IpAddr;
use std::sync::Arc;

use nutmeg_sail::NutmegSessionMutator;
use sail_common::config::AppConfig;
use sail_common::runtime::{RuntimeHandle, RuntimeManager};
use sail_session::session_factory::{ServerSessionFactory, ServerSessionInfo, SessionFactory};
use sail_spark_connect::entrypoint::serve_with_session_factory;
use sail_spark_connect::session_manager::SparkSessionMutator;
use sail_telemetry::telemetry::{init_telemetry, shutdown_telemetry};
use sail_telemetry::{ResourceKind, ResourceOptions};
use tokio::net::TcpListener;

fn nutmeg_session_factory(
    config: Arc<AppConfig>,
    runtime: RuntimeHandle,
) -> Box<dyn SessionFactory<ServerSessionInfo>> {
    let spark = Box::new(SparkSessionMutator::new(config.clone()));
    // `main` has checked `NUTMEG_READS` already, so this cannot fail.
    let nutmeg = NutmegSessionMutator::wrap(spark, &config.mode).expect("NUTMEG_READS is valid");
    Box::new(ServerSessionFactory::new(config, runtime, nutmeg))
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut ip: IpAddr = "127.0.0.1".parse()?;
    let mut port: u16 = 50051;
    let mut args = std::env::args().skip(1);
    while let Some(flag) = args.next() {
        let mut value = || args.next().ok_or_else(|| format!("{flag} needs a value"));
        match flag.as_str() {
            "--ip" => ip = value()?.parse()?,
            "--port" => port = value()?.parse()?,
            other => return Err(format!("unknown argument `{other}`").into()),
        }
    }
    pyo3::Python::initialize();
    let config = Arc::new(AppConfig::load()?);
    let reads = nutmeg_sail::read_execution(&config.mode)?;
    let runtime = RuntimeManager::try_new(&config.runtime)?;
    let handle = runtime.handle();
    runtime.handle().primary().block_on(async move {
        // As Sail's own server does: sessions need the telemetry system store.
        let resource = ResourceOptions {
            kind: ResourceKind::Server,
        };
        init_telemetry(&config.telemetry, &config.catalog.system, resource)?;
        let listener = TcpListener::bind((ip, port)).await?;
        eprintln!(
            "nutmeg-server: Spark Connect on sc://{} ({:?} mode, reads {:?})",
            listener.local_addr()?,
            config.mode,
            reads
        );
        let signal = async {
            let _ = tokio::signal::ctrl_c().await;
        };
        let result =
            serve_with_session_factory(listener, signal, config, handle, nutmeg_session_factory)
                .await;
        shutdown_telemetry();
        result
    })
}
