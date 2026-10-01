# Configuration

Configuration is loaded from environment variables in `src/config.rs`; the server also loads a local `.env` file at startup. Boolean switches use numeric values (`0` to disable, `1` to enable), not `true`/`false`.

The server applies embedded database migrations before starting HTTP and workers. The database must already exist.

## Required

| Variable | Description |
|----------|-------------|
| `DATABASE_URL` | PostgreSQL connection string |
| `HOST_URL` | Public URL for webhook callbacks (must start with `http://` or `https://`) |

## Server

| Variable | Default | Description |
|----------|---------|-------------|
| `PORT` | `8085` | Server port |
| `RUST_LOG` | `info` | Log level |

## Connection Pool

| Variable | Default | Description |
|----------|---------|-------------|
| `POOL_MAX_SIZE` | `10` | Maximum connections |
| `POOL_MIN_IDLE` | `5` | Minimum idle connections |
| `POOL_ACQUIRE_RETRIES` | `3` | Maximum connection acquisition attempts; at least one is attempted |
| `POOL_TIMEOUT_SECS` | `30` | Connection timeout in seconds |

## Pagination

| Variable | Default | Description |
|----------|---------|-------------|
| `PAGINATION_DEFAULT` | `50` | Default items per page |
| `PAGINATION_MAX` | `100` | Maximum items per page |

## Workers

| Variable | Default | Description |
|----------|---------|-------------|
| `WORKER_LOOP_INTERVAL_MS` | `1000` | Worker loop interval in ms |
| `WORKER_CLAIM_TIMEOUT_SECS` | `30` | Inactivity limit for a Claimed task before requeue; waiting for a webhook permit refreshes its timestamp |
| `WORKER_START_BATCH_SIZE` | `50` | Max successful claims per scheduler iteration; blocked candidates do not limit how far the backlog is scanned. |
| `WORKER_TIMEOUT_BATCH_SIZE` | `100` | Max timed-out Running tasks per pass. Each iteration processes at most 50 full passes. |
| `WORKER_WEBHOOK_CONCURRENCY` | `10` | Max concurrent on_start webhook executions (startup warns when greater than or equal to `POOL_MAX_SIZE`; keep headroom for handlers and other workers) |
| `DEAD_END_CANCEL_ENABLED` | `1` | Cancel active ancestors when none of their children remain viable |
| `BATCH_CHANNEL_CAPACITY` | `100` | Batch update channel size |

## Webhook Delivery (outbox)

| Variable | Default | Description |
|----------|---------|-------------|
| `WEBHOOK_DELIVERY_INTERVAL_MS` | `1000` | Interval between webhook delivery-loop iterations (outbox drain) |
| `WEBHOOK_DELIVERY_BATCH_SIZE` | `50` | Max outbox rows claimed per delivery-loop iteration |
| `WEBHOOK_DELIVERY_LEASE_SECS` | `210` | Lease duration in seconds. Cover queueing within a claimed batch and sequential HTTP execution; expiry can permit duplicate delivery. |
| `WEBHOOK_DELIVERY_CONCURRENCY` | `10` | Maximum events delivered concurrently; actions within an event run sequentially. |
| `WEBHOOK_MAX_ATTEMPTS` | `10` | Delivery attempts before an outbox row is marked `exhausted` |
| `WEBHOOK_RETRY_BACKOFF_BASE_SECS` | `2` | Base of the exponential retry backoff (delay = base^attempt, capped) |
| `WEBHOOK_RETRY_BACKOFF_CAP_SECS` | `300` | Cap on the retry backoff delay |

## Request limits

| Variable | Default | Description |
|----------|---------|-------------|
| `MAX_TASKS_PER_BATCH` | `1000` | Max tasks accepted in one `POST /task` batch. Over the limit ⇒ 400. |
| `MAX_DEPS_PER_TASK` | `100` | Max dependencies a single task may declare. Over ⇒ 400. |
| `MAX_ACTIONS_PER_TASK` | `20` | Max actions per task (on_start + on_failure + on_success), and max `on_batch_complete` actions. Over ⇒ 400. |
| `PAYLOAD_MAX_BYTES` | 2 MiB | Maximum JSON request size in bytes. Larger bodies return `413`. |

## Circuit Breaker

| Variable | Default | Description |
|----------|---------|-------------|
| `CIRCUIT_BREAKER_ENABLED` | `1` | Enable circuit breaker (0 to disable) |
| `CIRCUIT_BREAKER_FAILURE_THRESHOLD` | `5` | Failures before circuit opens |
| `CIRCUIT_BREAKER_FAILURE_WINDOW_SECS` | `10` | Time window for counting failures |
| `CIRCUIT_BREAKER_RECOVERY_TIMEOUT_SECS` | `30` | Time before trying half-open |
| `CIRCUIT_BREAKER_SUCCESS_THRESHOLD` | `2` | Successes in half-open to close |

## Observability

| Variable | Default | Description |
|----------|---------|-------------|
| `SLOW_QUERY_THRESHOLD_MS` | `100` | Slow query warning threshold in ms |
| `METRICS_SAMPLER_INTERVAL_SECS` | `15` | Interval for the metrics sampler (tasks_by_status, running_tasks_by_kind, db_pool_connections gauges) |
| `TRACING_ENABLED` | `0` | Enable OpenTelemetry distributed tracing |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | - | OTLP endpoint URL (e.g., `http://localhost:4317`) |
| `OTEL_SERVICE_NAME` | `arcrun` | Service name for traces |
| `OTEL_SAMPLING_RATIO` | `1.0` | Sampling ratio (0.0 to 1.0) |

## Retention

| Variable | Default | Description |
|----------|---------|-------------|
| `RETENTION_ENABLED` | `0` | Enable task archiving and archive purging. Unused rule-slot cleanup always runs. |
| `RETENTION_DAYS` | `30` | Minimum age since terminal completion before archiving. Archived records remain available by task ID. |
| `RETENTION_ARCHIVE_DAYS` | `0` | Days since archiving before permanent deletion. `0` keeps archived records indefinitely. |
| `RETENTION_CLEANUP_INTERVAL_SECS` | `3600` | Interval between retention loop runs in seconds |
| `RETENTION_BATCH_SIZE` | `1000` | Max tasks moved (and archive rows purged) per retention cycle |

## Security

| Variable | Default | Description |
|----------|---------|-------------|
| `SKIP_SSRF_VALIDATION` | `1` (debug) / `0` (release) | Skip SSRF validation on webhook URLs |
| `BLOCKED_HOSTNAMES` | `localhost,127.0.0.1,::1,0.0.0.0,local,internal` | Comma-separated additions to the built-in blocked hostnames |
| `BLOCKED_HOSTNAME_SUFFIXES` | `.local,.internal,.localdomain,.localhost` | Comma-separated additions to the built-in blocked hostname suffixes |
| `ALLOWED_HOSTNAMES` | empty | Comma-separated trusted hostnames or leading-dot suffixes, case-insensitive. Bypasses hostname and resolved-IP checks for matching names. |
| `ALLOWED_CIDRS` | empty | Comma-separated trusted CIDRs or individual IP addresses. Invalid entries prevent startup. |
| `AUTH_TOKEN` | unset ⇒ auth disabled | Static bearer token. Unset or blank disables authentication. See authentication behavior below. |

### Authentication behavior

With `AUTH_TOKEN` configured, every endpoint except `/health` and `/ready` requires `Authorization: Bearer <token>`. This includes metrics, Swagger, the DAG viewer, and callbacks to the URL passed in `handle`. Tokens are accepted only through the header. Browser pages need a proxy that authenticates users and supplies that header.

### Internal webhook receivers

For an internal service, allow only its hostname or network rather than disabling all URL checks:

```bash
ALLOWED_HOSTNAMES=worker,.svc.cluster.local
ALLOWED_CIDRS=10.42.0.0/16,192.168.1.10
```

A hostname allowlist entry trusts every address to which that name resolves. A CIDR entry permits only matching IPs; other addresses in the same DNS answer are still checked. Blocklist environment variables add entries to the built-in lists rather than replacing them. See [SSRF protection](webhooks.md#ssrf-protection).

## Fixed timing and startup warnings

The timeout worker checks every second, counter updates flush every 100 ms, and each webhook HTTP request has a 10-second timeout. These values are not exposed as environment variables.

Startup warns when `WORKER_CLAIM_TIMEOUT_SECS` is below 10 seconds, or when `WEBHOOK_DELIVERY_LEASE_SECS` is below `(MAX_ACTIONS_PER_TASK + 1) * 10` seconds (210 seconds with the defaults). These warnings do not reject startup. Lease fencing protects database marks from stale workers; it does not prevent duplicate HTTP requests after a lease expires.
