//! Application configuration management.
//!
//! Provides typed configuration loaded from environment variables with validation.

use std::time::Duration;

/// Per-request HTTP timeout applied to every webhook call (on_start, end/cancel
/// delivery). Defined here (rather than only in `action.rs`) so `Config::validate`
/// can compare it against `WORKER_CLAIM_TIMEOUT_SECS` for the D7 `stale_after`
/// guardrail. `ActionExecutor` reads the same constant when building its client.
pub const WEBHOOK_HTTP_TIMEOUT_SECS: u64 = 10;

/// Application configuration loaded from environment variables.
#[derive(Debug, Clone)]
pub struct Config {
    /// PostgreSQL database connection URL
    pub database_url: String,

    /// Public host URL for webhook callbacks
    pub host_url: String,

    /// Server port to bind to
    pub port: u16,

    /// Database connection pool settings
    pub pool: PoolConfig,

    /// Pagination settings
    pub pagination: PaginationConfig,

    /// Worker settings
    pub worker: WorkerConfig,

    /// Circuit breaker settings for connection pool resilience
    pub circuit_breaker: CircuitBreakerConfig,

    /// Observability settings
    pub observability: ObservabilityConfig,

    /// Security settings
    pub security: SecurityConfig,

    /// Data retention settings
    pub retention: RetentionConfig,

    /// Structural limits on request payloads (Audit 2, A10)
    pub limits: LimitsConfig,

    /// Maximum accepted JSON request body size in bytes (`web::JsonConfig` limit).
    /// Defaults to 2 MiB — the historical implicit actix default, so this is
    /// non-breaking. A payload larger than this is rejected with 413.
    pub payload_max_bytes: usize,
}

/// Structural limits on `POST /task` payloads (Audit 2, A10).
///
/// Without these, a single unauthenticated request could submit an unbounded number
/// of tasks / dependencies / actions, blowing past PostgreSQL's 65535 bind-parameter
/// ceiling (→ 500) or exhausting server resources. Defaults are generous (well above
/// any realistic DAG) so they are non-breaking. All three are `Copy`-cheap.
#[derive(Debug, Clone, Copy)]
pub struct LimitsConfig {
    /// Maximum number of tasks accepted in one `POST /task` batch.
    pub max_tasks_per_batch: usize,

    /// Maximum number of dependencies a single task may declare.
    pub max_deps_per_task: usize,

    /// Maximum number of actions (on_start + on_failure + on_success) per task.
    pub max_actions_per_task: usize,
}

impl Default for LimitsConfig {
    fn default() -> Self {
        Self {
            max_tasks_per_batch: 1000,
            max_deps_per_task: 100,
            max_actions_per_task: 20,
        }
    }
}

/// Database connection pool configuration.
#[derive(Debug, Clone)]
pub struct PoolConfig {
    /// Maximum number of connections in the pool
    pub max_size: u32,

    /// Minimum number of idle connections to maintain
    pub min_idle: u32,

    /// Maximum lifetime of a connection
    pub max_lifetime: Duration,

    /// Idle timeout for connections
    pub idle_timeout: Duration,

    /// Connection acquisition timeout
    pub connection_timeout: Duration,

    /// Number of retries when acquiring a connection
    pub acquire_retries: u32,

    /// Delay between retry attempts
    pub retry_delay: Duration,
}

/// Pagination configuration.
#[derive(Debug, Clone)]
pub struct PaginationConfig {
    /// Default number of items per page
    pub default_per_page: i64,

    /// Maximum allowed items per page
    pub max_per_page: i64,
}

/// Worker loop configuration.
#[derive(Debug, Clone)]
pub struct WorkerConfig {
    /// Interval between worker loop iterations
    pub loop_interval: Duration,

    /// Interval for timeout check loop
    pub timeout_check_interval: Duration,

    /// Maximum time a task can stay Claimed before being requeued
    pub claim_timeout: Duration,

    /// Interval for batch updater flush
    pub batch_flush_interval: Duration,

    /// Batch update channel capacity
    pub batch_channel_capacity: usize,

    /// Whether dead-end ancestor cancellation is enabled (safety valve)
    pub dead_end_cancel_enabled: bool,

    /// Maximum number of Pending tasks to fetch per start_loop iteration
    pub start_batch_size: i64,

    /// Maximum number of timed-out Running tasks processed per timeout_loop pass.
    /// The loop drains in bounded passes so a mass-timeout never pins the loop and
    /// delays the stale-Claimed requeue that shares it.
    pub timeout_batch_size: i64,

    /// Maximum number of concurrent webhook executions in start_loop
    pub webhook_concurrency: usize,

    /// Interval between webhook delivery-loop iterations (outbox drain)
    pub webhook_delivery_interval: Duration,

    /// Max outbox rows claimed per delivery-loop iteration
    pub webhook_delivery_batch_size: i64,

    /// Max delivery attempts before an outbox row is marked `exhausted`
    pub webhook_max_attempts: i32,

    /// Base of the exponential retry backoff, in seconds (delay = base^attempts)
    pub webhook_retry_backoff_base_secs: i64,

    /// Cap on the retry backoff delay, in seconds
    pub webhook_retry_backoff_cap_secs: i64,

    /// Lease applied to an outbox row at claim time (seconds): the row is not
    /// re-claimable until the lease expires. Must exceed the worst-case single-row
    /// delivery time so an in-flight delivery is never double-claimed.
    pub webhook_delivery_lease_secs: i64,

    /// Max concurrent HTTP deliveries within one delivery-loop batch.
    pub webhook_delivery_concurrency: usize,

    /// Interval between metrics-sampler iterations (gauges: tasks_by_status,
    /// running_tasks_by_kind, db_pool_connections).
    pub metrics_sampler_interval: Duration,
}

/// Circuit breaker configuration for connection pool resilience.
#[derive(Debug, Clone)]
pub struct CircuitBreakerConfig {
    /// Whether the circuit breaker is enabled
    pub enabled: bool,

    /// Number of failures before circuit opens
    pub failure_threshold: u32,

    /// Time window for counting failures (in seconds)
    pub failure_window_secs: u64,

    /// How long to wait before trying half-open (in seconds)
    pub recovery_timeout_secs: u64,

    /// Number of successes in half-open state to close circuit
    pub success_threshold: u32,
}

/// Observability configuration for logging and metrics.
#[derive(Debug, Clone)]
pub struct ObservabilityConfig {
    /// Threshold in milliseconds for slow query warnings
    pub slow_query_threshold_ms: u64,

    /// Whether distributed tracing is enabled
    pub tracing_enabled: bool,

    /// OTLP endpoint for trace export
    pub otlp_endpoint: Option<String>,

    /// Service name for traces
    pub service_name: String,

    /// Sampling ratio (0.0 to 1.0)
    pub sampling_ratio: f64,
}

/// Data retention configuration for automatic cleanup of old terminal tasks.
#[derive(Debug, Clone)]
pub struct RetentionConfig {
    /// Whether automatic cleanup is enabled
    pub enabled: bool,

    /// Number of days a terminal task lives in the hot `task` table before the retention
    /// loop MOVES it to the cold `task_archive` (Audit 2, D6). The archived record still
    /// serves `GET /task/{id}`; only its actions/links/webhook rows are dropped.
    pub retention_days: u32,

    /// Number of days an archived task lives in `task_archive` before it is purged
    /// (Audit 2, D6). `0` (the default) = keep forever: growth just shifts to the cold
    /// table (tight hot indexes, healthy vacuum) without being bounded. Set > 0 to bound
    /// the archive. Purging is gated by `enabled` like the task move.
    pub archive_retention_days: u32,

    /// Interval between cleanup runs in seconds
    pub cleanup_interval_secs: u64,

    /// Maximum number of tasks to move/purge per cleanup cycle
    pub batch_size: i64,
}

/// Security configuration for SSRF protection and validation.
#[derive(Debug, Clone)]
pub struct SecurityConfig {
    /// Skip SSRF validation (useful for development/testing)
    /// In debug builds, this defaults to true
    pub skip_ssrf_validation: bool,

    /// List of blocked hostnames for webhook URLs
    pub blocked_hostnames: Vec<String>,

    /// List of blocked hostname suffixes (e.g., ".local", ".internal")
    pub blocked_hostname_suffixes: Vec<String>,

    /// Webhook hostnames explicitly trusted despite SSRF protection (env
    /// `ALLOWED_HOSTNAMES`). An entry is either an exact hostname (`backend`,
    /// `api.svc.cluster.local`) or a leading-dot suffix (`.svc.cluster.local`).
    /// Matching is case-insensitive. An allowlisted hostname skips the hostname
    /// blocklists at creation time AND the resolved-IP check in the delivery-time
    /// resolver: the operator is vouching for whatever the name resolves to
    /// (typically an internal docker/k8s service). Empty by default.
    pub allowed_hostnames: Vec<String>,

    /// IP networks explicitly trusted despite being private/reserved (env
    /// `ALLOWED_CIDRS`, e.g. `10.42.0.0/16,192.168.1.10`). A bare IP means a
    /// single-host network. Applies to IP-literal URLs at creation time and to
    /// every IP a hostname resolves to at delivery time. Empty by default.
    pub allowed_cidrs: Vec<ipnet::IpNet>,

    /// Optional static bearer token for API authentication (Audit 2, A6).
    ///
    /// `None`/empty (env `AUTH_TOKEN` unset or blank) ⇒ authentication is
    /// **disabled** and every endpoint is open (the historical behavior); the
    /// server logs a loud warning at startup in release builds because SSRF
    /// protection alone does not secure an otherwise-open API.
    ///
    /// When set, every request must carry `Authorization: Bearer <token>` except
    /// the `/health` and `/ready` probes. This also gates `/metrics`, the Swagger
    /// UI, and the static DAG UI at `/view`. The token is only ever read from the
    /// header — never a query string — so the browser-facing `/view` UI is only
    /// reachable behind a reverse proxy that injects the header.
    pub auth_token: Option<String>,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            max_size: 10,
            min_idle: 5,
            max_lifetime: Duration::from_secs(60 * 60 * 24), // 24 hours
            idle_timeout: Duration::from_secs(60 * 2),       // 2 minutes
            connection_timeout: Duration::from_secs(30),
            acquire_retries: 3,
            retry_delay: Duration::from_millis(100),
        }
    }
}

impl Default for PaginationConfig {
    fn default() -> Self {
        Self {
            default_per_page: 50,
            max_per_page: 100,
        }
    }
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            loop_interval: Duration::from_secs(1),
            timeout_check_interval: Duration::from_secs(1),
            claim_timeout: Duration::from_secs(30),
            batch_flush_interval: Duration::from_millis(100),
            batch_channel_capacity: 100,
            dead_end_cancel_enabled: true,
            start_batch_size: 50,
            timeout_batch_size: 100,
            webhook_concurrency: 10,
            webhook_delivery_interval: Duration::from_millis(1000),
            webhook_delivery_batch_size: 50,
            webhook_max_attempts: 10,
            webhook_retry_backoff_base_secs: 2,
            webhook_retry_backoff_cap_secs: 300,
            webhook_delivery_lease_secs: 210,
            webhook_delivery_concurrency: 10,
            metrics_sampler_interval: Duration::from_secs(15),
        }
    }
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            failure_threshold: 5,
            failure_window_secs: 10,
            recovery_timeout_secs: 30,
            success_threshold: 2,
        }
    }
}

impl Default for ObservabilityConfig {
    fn default() -> Self {
        Self {
            slow_query_threshold_ms: 100, // 100ms default threshold
            tracing_enabled: false,
            otlp_endpoint: None,
            service_name: "arcrun".to_string(),
            sampling_ratio: 1.0,
        }
    }
}

impl Default for RetentionConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            retention_days: 30,
            archive_retention_days: 0,
            cleanup_interval_secs: 3600,
            batch_size: 1000,
        }
    }
}

impl Default for SecurityConfig {
    fn default() -> Self {
        Self {
            // In debug builds, skip SSRF validation by default for easier local development
            skip_ssrf_validation: cfg!(debug_assertions),
            blocked_hostnames: vec![
                "localhost".to_string(),
                "127.0.0.1".to_string(),
                "::1".to_string(),
                "0.0.0.0".to_string(),
                "local".to_string(),
                "internal".to_string(),
            ],
            blocked_hostname_suffixes: vec![
                ".local".to_string(),
                ".internal".to_string(),
                ".localdomain".to_string(),
                ".localhost".to_string(),
            ],
            allowed_hostnames: Vec::new(),
            allowed_cidrs: Vec::new(),
            auth_token: None,
        }
    }
}

/// Configuration loading error.
#[derive(Debug)]
pub struct ConfigError {
    pub field: String,
    pub message: String,
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Configuration error for '{}': {}",
            self.field, self.message
        )
    }
}

impl std::error::Error for ConfigError {}

impl Config {
    /// Load configuration from environment variables.
    ///
    /// Required environment variables:
    /// - `DATABASE_URL`: PostgreSQL connection string
    /// - `HOST_URL`: Public URL for webhook callbacks
    ///
    /// Optional environment variables:
    /// - `PORT`: Server port (default: 8085)
    /// - `POOL_MAX_SIZE`: Max pool connections (default: 10)
    /// - `POOL_MIN_IDLE`: Min idle connections (default: 5)
    /// - `POOL_ACQUIRE_RETRIES`: Connection acquire retries (default: 3)
    /// - `PAGINATION_DEFAULT`: Default items per page (default: 50)
    /// - `PAGINATION_MAX`: Max items per page (default: 100)
    /// - `WORKER_LOOP_INTERVAL_MS`: Worker loop interval in ms (default: 1000)
    /// - `WORKER_CLAIM_TIMEOUT_SECS`: Max time a task can stay Claimed before requeue (default: 30)
    /// - `BATCH_CHANNEL_CAPACITY`: Batch update channel size (default: 100)
    /// - `CIRCUIT_BREAKER_ENABLED`: Enable circuit breaker (default: true)
    /// - `CIRCUIT_BREAKER_FAILURE_THRESHOLD`: Failures before opening (default: 5)
    /// - `CIRCUIT_BREAKER_FAILURE_WINDOW_SECS`: Failure counting window (default: 10)
    /// - `CIRCUIT_BREAKER_RECOVERY_TIMEOUT_SECS`: Time before half-open (default: 30)
    /// - `CIRCUIT_BREAKER_SUCCESS_THRESHOLD`: Successes to close (default: 2)
    /// - `WEBHOOK_DELIVERY_LEASE_SECS`: Lease applied to an outbox row at claim time, in seconds; must be >= 1 and exceed worst-case single-row delivery time (default: 210)
    /// - `WEBHOOK_DELIVERY_CONCURRENCY`: Max concurrent HTTP deliveries per delivery-loop batch; must be >= 1 (default: 10)
    /// - `SLOW_QUERY_THRESHOLD_MS`: Threshold for slow query warnings in ms (default: 100)
    /// - `TRACING_ENABLED`: Enable distributed tracing (default: false)
    /// - `OTEL_EXPORTER_OTLP_ENDPOINT`: OTLP endpoint URL for trace export
    /// - `OTEL_SERVICE_NAME`: Service name for traces (default: arcrun)
    /// - `OTEL_SAMPLING_RATIO`: Sampling ratio 0.0-1.0 (default: 1.0)
    /// - `SKIP_SSRF_VALIDATION`: Skip SSRF validation (default: 1 in debug, 0 in release)
    /// - `BLOCKED_HOSTNAMES`: Comma-separated list of blocked hostnames (default: localhost,127.0.0.1,::1,0.0.0.0,local,internal)
    /// - `BLOCKED_HOSTNAME_SUFFIXES`: Comma-separated list of blocked hostname suffixes (default: .local,.internal,.localdomain,.localhost)
    /// - `ALLOWED_HOSTNAMES`: Comma-separated webhook hostnames (exact, or `.suffix`) trusted despite SSRF protection — skips blocklists and the delivery-time resolved-IP check (default: empty)
    /// - `ALLOWED_CIDRS`: Comma-separated IP networks (CIDR or bare IP) trusted despite being private/reserved, at creation time and in the delivery-time resolver (default: empty)
    /// - `AUTH_TOKEN`: Optional static bearer token; when set, all endpoints except `/health` and `/ready` require `Authorization: Bearer <token>` (default: unset ⇒ auth disabled)
    /// - `MAX_TASKS_PER_BATCH`: Max tasks accepted in one POST /task batch (default: 1000)
    /// - `MAX_DEPS_PER_TASK`: Max dependencies a single task may declare (default: 100)
    /// - `MAX_ACTIONS_PER_TASK`: Max actions (on_start + on_failure + on_success) per task (default: 20)
    /// - `PAYLOAD_MAX_BYTES`: Max accepted JSON request body size in bytes; larger bodies get 413 (default: 2 MiB)
    /// - `RETENTION_ENABLED`: Enable the retention loop's task move + archive purge (default: 0/off)
    /// - `RETENTION_DAYS`: Days a terminal task stays in `task` before being moved to `task_archive` (default: 30)
    /// - `RETENTION_ARCHIVE_DAYS`: Days an archived task stays in `task_archive` before purge; 0 = keep forever (default: 0)
    /// - `RETENTION_CLEANUP_INTERVAL_SECS`: Interval between retention loop runs (default: 3600)
    /// - `RETENTION_BATCH_SIZE`: Max tasks moved/purged per retention cycle (default: 1000)
    pub fn from_env() -> Result<Self, ConfigError> {
        let database_url = std::env::var("DATABASE_URL").map_err(|_| ConfigError {
            field: "DATABASE_URL".to_string(),
            message: "Required environment variable not set".to_string(),
        })?;

        let host_url = std::env::var("HOST_URL").map_err(|_| ConfigError {
            field: "HOST_URL".to_string(),
            message: "Required environment variable not set".to_string(),
        })?;

        let port = parse_env_or("PORT", 8085)?;

        let pool = PoolConfig {
            max_size: parse_env_or("POOL_MAX_SIZE", 10)?,
            min_idle: parse_env_or("POOL_MIN_IDLE", 5)?,
            acquire_retries: parse_env_or("POOL_ACQUIRE_RETRIES", 3)?,
            connection_timeout: Duration::from_secs(parse_env_or("POOL_TIMEOUT_SECS", 30)?),
            ..Default::default()
        };

        let pagination = PaginationConfig {
            default_per_page: parse_env_or("PAGINATION_DEFAULT", 50)?,
            max_per_page: parse_env_or("PAGINATION_MAX", 100)?,
        };

        let worker = WorkerConfig {
            loop_interval: Duration::from_millis(parse_env_or("WORKER_LOOP_INTERVAL_MS", 1000)?),
            claim_timeout: Duration::from_secs(parse_env_or("WORKER_CLAIM_TIMEOUT_SECS", 30)?),
            batch_channel_capacity: parse_env_or("BATCH_CHANNEL_CAPACITY", 100)?,
            dead_end_cancel_enabled: parse_env_or("DEAD_END_CANCEL_ENABLED", 1)? != 0,
            start_batch_size: parse_env_or("WORKER_START_BATCH_SIZE", 50)?,
            timeout_batch_size: parse_env_or("WORKER_TIMEOUT_BATCH_SIZE", 100)?,
            webhook_concurrency: parse_env_or("WORKER_WEBHOOK_CONCURRENCY", 10)?,
            webhook_delivery_interval: Duration::from_millis(parse_env_or(
                "WEBHOOK_DELIVERY_INTERVAL_MS",
                1000,
            )?),
            webhook_delivery_batch_size: parse_env_or("WEBHOOK_DELIVERY_BATCH_SIZE", 50)?,
            webhook_max_attempts: parse_env_or("WEBHOOK_MAX_ATTEMPTS", 10)?,
            webhook_retry_backoff_base_secs: parse_env_or("WEBHOOK_RETRY_BACKOFF_BASE_SECS", 2)?,
            webhook_retry_backoff_cap_secs: parse_env_or("WEBHOOK_RETRY_BACKOFF_CAP_SECS", 300)?,
            webhook_delivery_lease_secs: parse_env_or("WEBHOOK_DELIVERY_LEASE_SECS", 210)?,
            webhook_delivery_concurrency: parse_env_or("WEBHOOK_DELIVERY_CONCURRENCY", 10)?,
            metrics_sampler_interval: Duration::from_secs(parse_env_or(
                "METRICS_SAMPLER_INTERVAL_SECS",
                15,
            )?),
            ..Default::default()
        };

        let circuit_breaker = CircuitBreakerConfig {
            enabled: parse_env_or("CIRCUIT_BREAKER_ENABLED", 1)? != 0,
            failure_threshold: parse_env_or("CIRCUIT_BREAKER_FAILURE_THRESHOLD", 5)?,
            failure_window_secs: parse_env_or("CIRCUIT_BREAKER_FAILURE_WINDOW_SECS", 10)?,
            recovery_timeout_secs: parse_env_or("CIRCUIT_BREAKER_RECOVERY_TIMEOUT_SECS", 30)?,
            success_threshold: parse_env_or("CIRCUIT_BREAKER_SUCCESS_THRESHOLD", 2)?,
        };

        let observability = ObservabilityConfig {
            slow_query_threshold_ms: parse_env_or("SLOW_QUERY_THRESHOLD_MS", 100)?,
            tracing_enabled: parse_env_or("TRACING_ENABLED", 0)? != 0,
            otlp_endpoint: std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").ok(),
            service_name: std::env::var("OTEL_SERVICE_NAME")
                .unwrap_or_else(|_| "arcrun".to_string()),
            sampling_ratio: parse_env_or_f64("OTEL_SAMPLING_RATIO", 1.0)?,
        };

        let security = SecurityConfig {
            // In debug builds default to true, in release default to false
            // Can be overridden via SKIP_SSRF_VALIDATION=1
            skip_ssrf_validation: parse_env_or(
                "SKIP_SSRF_VALIDATION",
                if cfg!(debug_assertions) { 1 } else { 0 },
            )? != 0,
            blocked_hostnames: {
                let mut hostnames = SecurityConfig::default().blocked_hostnames;
                if let Ok(extra) = std::env::var("BLOCKED_HOSTNAMES") {
                    hostnames.extend(extra.split(',').map(|h| h.trim().to_string()));
                }
                hostnames
            },
            blocked_hostname_suffixes: {
                let mut suffixes = SecurityConfig::default().blocked_hostname_suffixes;
                if let Ok(extra) = std::env::var("BLOCKED_HOSTNAME_SUFFIXES") {
                    suffixes.extend(extra.split(',').map(|h| h.trim().to_string()));
                }
                suffixes
            },
            allowed_hostnames: parse_csv_env("ALLOWED_HOSTNAMES"),
            allowed_cidrs: parse_cidr_list(
                "ALLOWED_CIDRS",
                &std::env::var("ALLOWED_CIDRS").unwrap_or_default(),
            )?,
            // Trim and treat a blank value as "unset" so `AUTH_TOKEN=` disables
            // auth rather than requiring an empty-string token.
            auth_token: std::env::var("AUTH_TOKEN").ok().and_then(|t| {
                let trimmed = t.trim();
                if trimmed.is_empty() {
                    None
                } else {
                    Some(trimmed.to_string())
                }
            }),
        };

        let retention = RetentionConfig {
            enabled: parse_env_or("RETENTION_ENABLED", 0)? != 0,
            retention_days: parse_env_or("RETENTION_DAYS", 30)?,
            archive_retention_days: parse_env_or("RETENTION_ARCHIVE_DAYS", 0)?,
            cleanup_interval_secs: parse_env_or("RETENTION_CLEANUP_INTERVAL_SECS", 3600)?,
            batch_size: parse_env_or("RETENTION_BATCH_SIZE", 1000)?,
        };

        let limits = LimitsConfig {
            max_tasks_per_batch: parse_env_or("MAX_TASKS_PER_BATCH", 1000)?,
            max_deps_per_task: parse_env_or("MAX_DEPS_PER_TASK", 100)?,
            max_actions_per_task: parse_env_or("MAX_ACTIONS_PER_TASK", 20)?,
        };

        let payload_max_bytes = parse_env_or("PAYLOAD_MAX_BYTES", 2 * 1024 * 1024)?;

        let config = Self {
            database_url,
            host_url,
            port,
            pool,
            pagination,
            worker,
            circuit_breaker,
            observability,
            security,
            retention,
            limits,
            payload_max_bytes,
        };

        config.validate()?;
        Ok(config)
    }

    /// Validate configuration values.
    fn validate(&self) -> Result<(), ConfigError> {
        if self.database_url.is_empty() {
            return Err(ConfigError {
                field: "DATABASE_URL".to_string(),
                message: "Cannot be empty".to_string(),
            });
        }

        if self.host_url.is_empty() {
            return Err(ConfigError {
                field: "HOST_URL".to_string(),
                message: "Cannot be empty".to_string(),
            });
        }

        if !self.host_url.starts_with("http://") && !self.host_url.starts_with("https://") {
            return Err(ConfigError {
                field: "HOST_URL".to_string(),
                message: "Must start with http:// or https://".to_string(),
            });
        }

        if self.pool.max_size == 0 {
            return Err(ConfigError {
                field: "POOL_MAX_SIZE".to_string(),
                message: "Must be greater than 0".to_string(),
            });
        }

        if self.pool.min_idle > self.pool.max_size {
            return Err(ConfigError {
                field: "POOL_MIN_IDLE".to_string(),
                message: "Cannot be greater than POOL_MAX_SIZE".to_string(),
            });
        }

        if self.pagination.max_per_page == 0 {
            return Err(ConfigError {
                field: "PAGINATION_MAX".to_string(),
                message: "Must be greater than 0".to_string(),
            });
        }

        if self.pagination.default_per_page > self.pagination.max_per_page {
            return Err(ConfigError {
                field: "PAGINATION_DEFAULT".to_string(),
                message: "Cannot be greater than PAGINATION_MAX".to_string(),
            });
        }

        if self.worker.claim_timeout.as_secs() == 0 {
            return Err(ConfigError {
                field: "WORKER_CLAIM_TIMEOUT_SECS".to_string(),
                message: "Must be greater than 0".to_string(),
            });
        }

        // Negative values would silently mean "unlimited claims" (the start_loop
        // guards are all `claim_cap > 0`); reject them along with 0.
        if self.worker.start_batch_size <= 0 {
            return Err(ConfigError {
                field: "WORKER_START_BATCH_SIZE".to_string(),
                message: "Must be greater than 0".to_string(),
            });
        }

        // A non-positive limit would make the timeout drain fetch 0 rows and never
        // time anything out; reject 0 and negatives.
        if self.worker.timeout_batch_size <= 0 {
            return Err(ConfigError {
                field: "WORKER_TIMEOUT_BATCH_SIZE".to_string(),
                message: "Must be greater than 0".to_string(),
            });
        }

        if self.worker.webhook_concurrency == 0 {
            return Err(ConfigError {
                field: "WORKER_WEBHOOK_CONCURRENCY".to_string(),
                message: "Must be greater than 0".to_string(),
            });
        }

        if self.worker.webhook_delivery_batch_size <= 0 {
            return Err(ConfigError {
                field: "WEBHOOK_DELIVERY_BATCH_SIZE".to_string(),
                message: "Must be greater than 0".to_string(),
            });
        }

        if self.worker.webhook_max_attempts <= 0 {
            return Err(ConfigError {
                field: "WEBHOOK_MAX_ATTEMPTS".to_string(),
                message: "Must be greater than 0".to_string(),
            });
        }

        if self.worker.webhook_retry_backoff_base_secs < 1 {
            return Err(ConfigError {
                field: "WEBHOOK_RETRY_BACKOFF_BASE_SECS".to_string(),
                message: "Must be greater than or equal to 1".to_string(),
            });
        }

        if self.worker.webhook_retry_backoff_cap_secs < 1 {
            return Err(ConfigError {
                field: "WEBHOOK_RETRY_BACKOFF_CAP_SECS".to_string(),
                message: "Must be greater than or equal to 1".to_string(),
            });
        }

        if self.worker.webhook_delivery_lease_secs < 1 {
            return Err(ConfigError {
                field: "WEBHOOK_DELIVERY_LEASE_SECS".to_string(),
                message: "Must be greater than or equal to 1".to_string(),
            });
        }

        if self.worker.webhook_delivery_concurrency == 0 {
            return Err(ConfigError {
                field: "WEBHOOK_DELIVERY_CONCURRENCY".to_string(),
                message: "Must be greater than 0".to_string(),
            });
        }

        // A10 structural limits: a zero limit would reject every request (or, for
        // payload_max_bytes, every non-empty body), which is never intended.
        if self.limits.max_tasks_per_batch == 0 {
            return Err(ConfigError {
                field: "MAX_TASKS_PER_BATCH".to_string(),
                message: "Must be greater than 0".to_string(),
            });
        }

        if self.limits.max_deps_per_task == 0 {
            return Err(ConfigError {
                field: "MAX_DEPS_PER_TASK".to_string(),
                message: "Must be greater than 0".to_string(),
            });
        }

        if self.limits.max_actions_per_task == 0 {
            return Err(ConfigError {
                field: "MAX_ACTIONS_PER_TASK".to_string(),
                message: "Must be greater than 0".to_string(),
            });
        }

        let worst_case_delivery_secs = (self.limits.max_actions_per_task as u64)
            .saturating_mul(WEBHOOK_HTTP_TIMEOUT_SECS)
            .saturating_add(WEBHOOK_HTTP_TIMEOUT_SECS);
        if (self.worker.webhook_delivery_lease_secs as u64) < worst_case_delivery_secs {
            log::warn!(
                "WEBHOOK_DELIVERY_LEASE_SECS ({}) is below the configured worst-case \
                 sequential delivery duration ({}s for {} actions). Lease fencing keeps \
                 stale workers from corrupting state, but duplicate HTTP delivery remains \
                 possible; raise the lease or lower MAX_ACTIONS_PER_TASK",
                self.worker.webhook_delivery_lease_secs,
                worst_case_delivery_secs,
                self.limits.max_actions_per_task
            );
        }

        if self.payload_max_bytes == 0 {
            return Err(ConfigError {
                field: "PAYLOAD_MAX_BYTES".to_string(),
                message: "Must be greater than 0".to_string(),
            });
        }

        // D7 `stale_after` guardrail (multi-replica): a `Claimed` task waiting on the
        // start_loop's on_start sequence must not be reclaimed by requeue-stale before
        // that sequence can finish. A single on_start webhook can take up to
        // `WEBHOOK_HTTP_TIMEOUT_SECS`; if the claim timeout is shorter, a slow (but
        // healthy) on_start would be requeued mid-flight and re-run — and its slot,
        // released on requeue, re-incremented. Warn (don't reject: legitimate for
        // fast/no on_start) so the operator raises WORKER_CLAIM_TIMEOUT_SECS.
        if self.worker.claim_timeout.as_secs() < WEBHOOK_HTTP_TIMEOUT_SECS {
            log::warn!(
                "WORKER_CLAIM_TIMEOUT_SECS ({}) is below the per-call webhook HTTP timeout \
                 ({}s): a slow-but-healthy on_start sequence can be requeued mid-flight by \
                 requeue-stale (and its concurrency slot released then re-acquired). Set \
                 WORKER_CLAIM_TIMEOUT_SECS at or above the max on_start sequence duration \
                 (>= WEBHOOK_HTTP_TIMEOUT_SECS, times the number of on_start actions)",
                self.worker.claim_timeout.as_secs(),
                WEBHOOK_HTTP_TIMEOUT_SECS
            );
        }

        if self.worker.webhook_concurrency >= self.pool.max_size as usize {
            log::warn!(
                "WORKER_WEBHOOK_CONCURRENCY ({}) is >= POOL_MAX_SIZE ({}); since B1, \
                 on_start webhooks no longer hold a pool connection during the HTTP \
                 call itself, but each still briefly borrows one in the phase-A claim \
                 and phase-C running-transition windows. At `==` a full burst of \
                 on_start work leaves zero connection headroom for HTTP handlers and \
                 the other worker loops during those windows; keep \
                 WORKER_WEBHOOK_CONCURRENCY strictly below POOL_MAX_SIZE",
                self.worker.webhook_concurrency,
                self.pool.max_size
            );
        }

        Ok(())
    }
}

/// Parse an environment variable or return a default value.
fn parse_env_or<T: std::str::FromStr>(name: &str, default: T) -> Result<T, ConfigError> {
    match std::env::var(name) {
        Ok(val) => val.parse().map_err(|_| ConfigError {
            field: name.to_string(),
            message: format!("Invalid value '{}', expected a valid number", val),
        }),
        Err(_) => Ok(default),
    }
}

/// Parse a floating-point environment variable or return a default value.
fn parse_env_or_f64(name: &str, default: f64) -> Result<f64, ConfigError> {
    match std::env::var(name) {
        Ok(val) => val.parse().map_err(|_| ConfigError {
            field: name.to_string(),
            message: format!("Invalid value '{}', expected a valid decimal number", val),
        }),
        Err(_) => Ok(default),
    }
}

/// Comma-separated env list: trimmed, empty entries dropped, unset ⇒ empty.
fn parse_csv_env(name: &str) -> Vec<String> {
    std::env::var(name)
        .ok()
        .map(|raw| {
            raw.split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default()
}

/// Parse a comma-separated list of IP networks. Each entry is a CIDR
/// (`10.0.0.0/8`, `fd00::/8`) or a bare IP, which becomes a single-host network
/// (`/32` or `/128`). `field` names the env var in the error.
pub fn parse_cidr_list(field: &str, raw: &str) -> Result<Vec<ipnet::IpNet>, ConfigError> {
    raw.split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|entry| {
            entry
                .parse::<ipnet::IpNet>()
                .or_else(|_| entry.parse::<std::net::IpAddr>().map(ipnet::IpNet::from))
                .map_err(|_| ConfigError {
                    field: field.to_string(),
                    message: format!("'{entry}' is not a valid IP address or CIDR network"),
                })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_cidr_list_accepts_cidrs_and_bare_ips() {
        let nets =
            parse_cidr_list("X", " 10.42.0.0/16, 192.168.1.10 ,fd00::/8,, 2001:db8::1 ").unwrap();
        assert_eq!(nets.len(), 4);
        assert!(nets[0].contains(&"10.42.7.7".parse::<std::net::IpAddr>().unwrap()));
        // A bare IP becomes a single-host network.
        assert_eq!(nets[1].prefix_len(), 32);
        assert!(nets[1].contains(&"192.168.1.10".parse::<std::net::IpAddr>().unwrap()));
        assert!(!nets[1].contains(&"192.168.1.11".parse::<std::net::IpAddr>().unwrap()));
        assert_eq!(nets[3].prefix_len(), 128);
    }

    #[test]
    fn parse_cidr_list_empty_and_invalid() {
        assert!(parse_cidr_list("X", "").unwrap().is_empty());
        assert!(parse_cidr_list("X", " , ").unwrap().is_empty());
        let err = parse_cidr_list("ALLOWED_CIDRS", "10.0.0.0/8,backend").unwrap_err();
        assert_eq!(err.field, "ALLOWED_CIDRS");
        assert!(err.message.contains("backend"));
        assert!(parse_cidr_list("X", "10.0.0.0/33").is_err());
    }
}
