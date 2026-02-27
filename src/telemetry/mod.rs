use tracing_subscriber::{fmt, EnvFilter};

pub fn init(service_name: &str) {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    let _ = fmt()
        .compact()
        .with_target(false)
        .with_thread_ids(true)
        .with_env_filter(filter)
        .try_init();

    tracing::info!(service = service_name, "telemetry initialized");
}
