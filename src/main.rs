//! ArcRun HTTP Server
//!
//! A service for orchestrating task execution with DAG dependencies,
//! concurrency control, and webhook-based actions.

use mimalloc::MiMalloc;
use std::sync::Arc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

use actix_web::{App, HttpServer, middleware::from_fn, web};
use actix_web_prom::PrometheusMetricsBuilder;
use arcrun::{
    DbPool,
    action::{ActionContext, ActionExecutor},
    auth,
    circuit_breaker::{CircuitBreaker, CircuitBreakerConfig},
    config::Config,
    handlers::{self, AppState},
    initialize_db_pool, metrics,
    tracing::{TracingConfig, init_tracing, shutdown_tracing},
    validation,
    workers::{UpdateEvent, WorkerNudges},
};
use diesel::{Connection, PgConnection};
use tokio::sync::{mpsc, watch};

use diesel_migrations::MigrationHarness;
use diesel_migrations::{EmbeddedMigrations, embed_migrations};
pub const MIGRATIONS: EmbeddedMigrations = embed_migrations!("migrations");

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    dotenvy::dotenv().ok();

    // Initialize distributed tracing (before env_logger if enabled)
    let tracing_config = TracingConfig::from_env();
    let tracer_provider = if tracing_config.enabled {
        init_tracing(&tracing_config)
    } else {
        env_logger::init_from_env(env_logger::Env::new().default_filter_or("info"));
        None
    };

    let config = Arc::new(load_config());

    log::info!("Starting HTTP server at http://0.0.0.0:{}", config.port);
    log::info!("Using public url {}", &config.host_url);

    init_security(&config);
    init_auth(&config);
    init_retention(&config);

    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("Failed to install rustls crypto provider");

    let pool = initialize_db_pool(&config.pool).await;
    log::info!("Database pool initialized");

    run_migrations(&config.database_url);

    let (sender, receiver) = mpsc::channel::<UpdateEvent>(config.worker.batch_channel_capacity);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let circuit_breaker = create_circuit_breaker(&config);
    // Instantiate a single ActionExecutor and share it between the workers and
    // AppState. ActionExecutor is Clone and wraps a reqwest::Client (internally an
    // Arc), so clones share the same HTTP connection pool.
    let action_executor = new_action_executor(&config);
    let action_context = Arc::new(action_executor.clone());

    // In-process worker nudges (B4): shared between the handlers (producers) and the
    // start/delivery loops (consumers) so a committed transition wakes the relevant
    // loop immediately instead of waiting a full poll tick.
    let nudges = WorkerNudges::new();

    let app_data = AppState {
        pool: pool.clone(),
        sender,
        action_executor,
        config: config.clone(),
        circuit_breaker,
        nudges: nudges.clone(),
    };

    let workers = spawn_workers(
        &pool,
        &action_context,
        &config,
        receiver,
        &shutdown_rx,
        &nudges,
    );

    metrics::init_metrics();
    let prometheus = PrometheusMetricsBuilder::new("api")
        .endpoint("/metrics")
        .registry(metrics::REGISTRY.clone())
        .build()
        .unwrap();

    let port = config.port;
    let auth_token = config.security.auth_token.clone();
    let payload_max_bytes = config.payload_max_bytes;

    let server_result = HttpServer::new(move || {
        // Auth middleware built per worker with the configured token. `None` ⇒
        // total pass-through (see `auth::authorize`).
        let auth_token = auth_token.clone();
        App::new()
            .app_data(web::Data::new(app_data.clone()))
            // A10: bound the JSON request body explicitly (default 2 MiB, the
            // historical implicit actix cap). Larger payloads are rejected with 413
            // before deserialization, backstopping the per-batch structural limits.
            .app_data(web::JsonConfig::default().limit(payload_max_bytes))
            // Middlewares execute in REVERSE registration order on the request
            // path: the LAST `.wrap()` is the outermost and runs first. `auth`
            // is therefore registered AFTER `prometheus` so it runs BEFORE it —
            // this is deliberate: the Prometheus middleware serves the
            // `/metrics` endpoint itself, so auth must sit outside it to gate
            // `/metrics` and to reject unauthorized requests before they are
            // recorded. (Regression test:
            // `test_audit2_a6_metrics_endpoint_gated_by_auth`.)
            .wrap(prometheus.clone())
            .wrap(from_fn(move |req, next| {
                auth::authorize(auth_token.clone(), req, next)
            }))
            .configure(handlers::configure_routes)
    })
    .bind(("0.0.0.0", port))?
    .shutdown_timeout(30)
    .run()
    .await;

    // Graceful shutdown
    log::info!("HTTP server stopped, signaling workers to shut down");
    let _ = shutdown_tx.send(true);
    workers.join(std::time::Duration::from_secs(10)).await;
    log::info!("All workers shut down");

    shutdown_tracing(tracer_provider);

    server_result
}

// =============================================================================
// Initialization helpers
// =============================================================================

fn load_config() -> Config {
    let config = Config::from_env().unwrap_or_else(|e| {
        log::error!("Configuration error: {}", e);
        std::process::exit(1);
    });
    log::info!("Configuration loaded successfully");
    config
}

fn init_security(config: &Config) {
    validation::init_security_config(config.security.clone());
    validation::init_limits_config(config.limits);
    if config.security.skip_ssrf_validation {
        log::warn!("SSRF validation is disabled - this should only be used in development!");
    } else if !config.security.allowed_hostnames.is_empty()
        || !config.security.allowed_cidrs.is_empty()
    {
        log::info!(
            "SSRF allowlist active: hostnames {:?}, networks {:?}",
            config.security.allowed_hostnames,
            config.security.allowed_cidrs
        );
    }
}

/// Log the effective auth posture, warning loudly in release builds when the API
/// is left open (Audit 2, A6). SSRF protection alone does not secure an open API.
fn init_auth(config: &Config) {
    match &config.security.auth_token {
        Some(_) => log::info!(
            "Bearer-token authentication ENABLED (AUTH_TOKEN set); /health and /ready remain open"
        ),
        None => {
            let msg = "AUTH_TOKEN is not set: the API is UNAUTHENTICATED — every endpoint \
                       (task create with outbound webhooks, batch cancel, metadata read, \
                       /metrics, Swagger) is open. Set AUTH_TOKEN or restrict access at the \
                       network layer.";
            if cfg!(debug_assertions) {
                log::info!("{msg}");
            } else {
                log::warn!("{msg}");
            }
        }
    }
}

/// Warn when automatic retention cleanup is disabled (B7 hygiene). Without it,
/// webhook_execution rows and terminal tasks accumulate without bound.
fn init_retention(config: &Config) {
    if !config.retention.enabled {
        let msg = "RETENTION_ENABLED is off (default): completed webhook_execution rows and \
                   terminal tasks accumulate without bound. Set RETENTION_ENABLED=1 or run \
                   external cleanup to prevent unbounded growth.";
        if cfg!(debug_assertions) {
            log::info!("{msg}");
        } else {
            log::warn!("{msg}");
        }
    } else {
        log::info!(
            "Retention cleanup ENABLED (RETENTION_DAYS={} → move to task_archive, \
             RETENTION_ARCHIVE_DAYS={} [0=keep forever], interval={}s)",
            config.retention.retention_days,
            config.retention.archive_retention_days,
            config.retention.cleanup_interval_secs
        );
    }
}

fn run_migrations(database_url: &str) {
    let mut conn = PgConnection::establish(database_url).unwrap_or_else(|e| {
        log::error!("Failed to connect to database for migrations: {}", e);
        std::process::exit(1);
    });
    conn.run_pending_migrations(MIGRATIONS).unwrap_or_else(|e| {
        log::error!("Failed to run migrations: {}", e);
        std::process::exit(1);
    });
    log::info!("Database migrations completed");
}

fn new_action_executor(config: &Config) -> ActionExecutor {
    // Pass the security config explicitly so the delivery-time SSRF resolver
    // (Audit 2, A5) is wired independently of global-init ordering.
    ActionExecutor::with_security_config(
        ActionContext {
            host_address: config.host_url.clone(),
            webhook_idempotency_timeout: config.worker.claim_timeout,
        },
        &config.security,
    )
}

fn create_circuit_breaker(config: &Config) -> Arc<CircuitBreaker> {
    if config.circuit_breaker.enabled {
        log::info!(
            "Circuit breaker enabled with failure_threshold={}, recovery_timeout={}s",
            config.circuit_breaker.failure_threshold,
            config.circuit_breaker.recovery_timeout_secs
        );
        Arc::new(CircuitBreaker::new(CircuitBreakerConfig {
            failure_threshold: config.circuit_breaker.failure_threshold,
            failure_window: std::time::Duration::from_secs(
                config.circuit_breaker.failure_window_secs,
            ),
            recovery_timeout: std::time::Duration::from_secs(
                config.circuit_breaker.recovery_timeout_secs,
            ),
            success_threshold: config.circuit_breaker.success_threshold,
        }))
    } else {
        log::info!("Circuit breaker disabled");
        Arc::new(CircuitBreaker::new(CircuitBreakerConfig {
            failure_threshold: u32::MAX,
            failure_window: std::time::Duration::from_secs(1),
            recovery_timeout: std::time::Duration::from_secs(1),
            success_threshold: 1,
        }))
    }
}

// =============================================================================
// Worker management
// =============================================================================

struct WorkerHandles {
    start: tokio::task::JoinHandle<()>,
    timeout: tokio::task::JoinHandle<()>,
    batch: tokio::task::JoinHandle<()>,
    retention: tokio::task::JoinHandle<()>,
    delivery: tokio::task::JoinHandle<()>,
    metrics_sampler: tokio::task::JoinHandle<()>,
}

impl WorkerHandles {
    async fn join(self, timeout: std::time::Duration) {
        let _ = tokio::time::timeout(timeout, async {
            let _ = self.start.await;
            let _ = self.timeout.await;
            let _ = self.batch.await;
            let _ = self.retention.await;
            let _ = self.delivery.await;
            let _ = self.metrics_sampler.await;
        })
        .await;
    }
}

fn spawn_workers(
    pool: &DbPool,
    action_executor: &Arc<ActionExecutor>,
    config: &Config,
    receiver: mpsc::Receiver<UpdateEvent>,
    shutdown_rx: &watch::Receiver<bool>,
    nudges: &WorkerNudges,
) -> WorkerHandles {
    let start = {
        let pool = pool.clone();
        let executor = action_executor.clone();
        let interval = config.worker.loop_interval;
        let dead_end_enabled = config.worker.dead_end_cancel_enabled;
        let start_batch_size = config.worker.start_batch_size;
        let webhook_concurrency = config.worker.webhook_concurrency;
        let claim_timeout = config.worker.claim_timeout;
        // D7 leader lease: the scheduler runs only on the elected leader (single-replica
        // always wins). The dedicated lease connection is opened from DATABASE_URL.
        let database_url = config.database_url.clone();
        let shutdown = shutdown_rx.clone();
        let nudges = nudges.clone();
        actix_web::rt::spawn(async move {
            arcrun::workers::start_loop_leased(
                database_url,
                executor.as_ref(),
                pool,
                interval,
                dead_end_enabled,
                start_batch_size,
                webhook_concurrency,
                shutdown,
                nudges,
                claim_timeout,
            )
            .await;
        })
    };

    let timeout = {
        let pool = pool.clone();
        let interval = config.worker.timeout_check_interval;
        let claim_timeout = config.worker.claim_timeout;
        let dead_end_enabled = config.worker.dead_end_cancel_enabled;
        let timeout_batch_size = config.worker.timeout_batch_size;
        let shutdown = shutdown_rx.clone();
        let nudges = nudges.clone();
        actix_web::rt::spawn(async move {
            arcrun::workers::timeout_loop(
                pool,
                interval,
                claim_timeout,
                dead_end_enabled,
                timeout_batch_size,
                shutdown,
                nudges,
            )
            .await;
        })
    };

    let batch = {
        let pool = pool.clone();
        let interval = config.worker.batch_flush_interval;
        let shutdown = shutdown_rx.clone();
        actix_web::rt::spawn(async move {
            arcrun::workers::batch_updater(pool, receiver, interval, shutdown).await;
        })
    };

    let retention = {
        let pool = pool.clone();
        let retention_config = config.retention.clone();
        let shutdown = shutdown_rx.clone();
        actix_web::rt::spawn(async move {
            arcrun::workers::retention_cleanup_loop(pool, retention_config, shutdown).await;
        })
    };

    let delivery = {
        let pool = pool.clone();
        let executor = action_executor.clone();
        let interval = config.worker.webhook_delivery_interval;
        let delivery_cfg = arcrun::workers::DeliveryConfig {
            batch_size: config.worker.webhook_delivery_batch_size,
            max_attempts: config.worker.webhook_max_attempts,
            backoff_base_secs: config.worker.webhook_retry_backoff_base_secs,
            backoff_cap_secs: config.worker.webhook_retry_backoff_cap_secs,
            lease_secs: config.worker.webhook_delivery_lease_secs,
            concurrency: config.worker.webhook_delivery_concurrency,
            // A2: the start-before-end gate relaxes once a pending `start` row is
            // older than the claim timeout (mirror of webhook_idempotency_timeout).
            start_stale_secs: config.worker.claim_timeout.as_secs() as i64,
        };
        let shutdown = shutdown_rx.clone();
        let nudges = nudges.clone();
        actix_web::rt::spawn(async move {
            arcrun::workers::delivery_loop(
                executor,
                pool,
                interval,
                delivery_cfg,
                shutdown,
                nudges,
            )
            .await;
        })
    };

    let metrics_sampler = {
        let pool = pool.clone();
        let interval = config.worker.metrics_sampler_interval;
        let shutdown = shutdown_rx.clone();
        actix_web::rt::spawn(async move {
            arcrun::workers::metrics_sampler_loop(pool, interval, shutdown).await;
        })
    };

    WorkerHandles {
        start,
        timeout,
        batch,
        retention,
        delivery,
        metrics_sampler,
    }
}
