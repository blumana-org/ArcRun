# Configuration

All configuration is via environment variables (loaded in `src/config.rs`).

## Required

| Variable | Description |
|----------|-------------|
| `DATABASE_URL` | PostgreSQL connection string |
| `HOST_URL` | Public URL for webhook callbacks (must start with `http://` or `https://`) |

## Optional

| Variable | Default | Description |
|----------|---------|-------------|
| `PORT` | `8085` | Server port |
| `RUST_LOG` | `info` | Log level |

## Connection Pool

| Variable | Default | Description |
|----------|---------|-------------|
| `POOL_MAX_SIZE` | `10` | Maximum connections |
| `POOL_MIN_IDLE` | `5` | Minimum idle connections |
| `POOL_ACQUIRE_RETRIES` | `3` | Connection acquire retries |
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
| `WORKER_CLAIM_TIMEOUT_SECS` | `30` | Max time a task can stay Claimed before requeue |
| `WORKER_START_BATCH_SIZE` | `50` | Max claims per start_loop iteration (claim cap). The Pending backlog is scanned page-by-page via keyset pagination (internal page size ~500) so the full backlog stays visible; only the number of claims per iteration is capped, never visibility. Early stop only fires once this cap is reached. |
| `WORKER_TIMEOUT_BATCH_SIZE` | `100` | Must be > 0. Max timed-out `Running` tasks processed per timeout_loop pass (Audit 2, B7). The loop drains in bounded passes (up to `MAX_TIMEOUT_DRAIN_PASSES` = 50 per iteration) so a mass-timeout never pins the loop and starves the stale-`Claimed` requeue that shares it. |
| `WORKER_WEBHOOK_CONCURRENCY` | `10` | Max concurrent on_start webhook executions (should not exceed `POOL_MAX_SIZE`) |
| `BATCH_CHANNEL_CAPACITY` | `100` | Batch update channel size |

## Webhook Delivery (outbox)

| Variable | Default | Description |
|----------|---------|-------------|
| `WEBHOOK_DELIVERY_INTERVAL_MS` | `1000` | Interval between webhook delivery-loop iterations (outbox drain) |
| `WEBHOOK_DELIVERY_BATCH_SIZE` | `50` | Max outbox rows claimed per delivery-loop iteration |
| `WEBHOOK_DELIVERY_LEASE_SECS` | `210` | Must be >= 1. Lease applied to an outbox row at claim time. Each claim is fenced by a unique token; keep this above the worst-case sequential action duration to avoid duplicate HTTP delivery. |
| `WEBHOOK_DELIVERY_CONCURRENCY` | `10` | Must be >= 1. Max concurrent HTTP deliveries within one delivery-loop batch (`buffer_unordered` bound) |
| `WEBHOOK_MAX_ATTEMPTS` | `10` | Delivery attempts before an outbox row is marked `exhausted` |
| `WEBHOOK_RETRY_BACKOFF_BASE_SECS` | `2` | Base of the exponential retry backoff (delay = base^attempt, capped) |
| `WEBHOOK_RETRY_BACKOFF_CAP_SECS` | `300` | Cap on the retry backoff delay |

## Structural Limits (Audit 2, A10)

| Variable | Default | Description |
|----------|---------|-------------|
| `MAX_TASKS_PER_BATCH` | `1000` | Max tasks accepted in one `POST /task` batch. Over the limit ⇒ 400. |
| `MAX_DEPS_PER_TASK` | `100` | Max dependencies a single task may declare. Over ⇒ 400. |
| `MAX_ACTIONS_PER_TASK` | `20` | Max actions per task (on_start + on_failure + on_success), and max `on_batch_complete` actions. Over ⇒ 400. |
| `PAYLOAD_MAX_BYTES` | 2 MiB | Explicit `web::JsonConfig` body-size cap; larger request bodies ⇒ 413. Matches the historical implicit actix default, so non-breaking. |

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
| `RETENTION_ENABLED` | `0` | Enable the retention loop's task move + archive purge (the `rule_slot` GC in the same loop always runs, regardless) |
| `RETENTION_DAYS` | `30` | Days a terminal task stays in the hot `task` table before being **moved** to the cold `task_archive` (Audit 2, D6 — the record is preserved and still served by `GET /task/{id}`; its actions/links/webhook rows are dropped). Not a delete |
| `RETENTION_ARCHIVE_DAYS` | `0` | Days an archived task stays in `task_archive` before being purged. `0` = **keep forever**: growth just shifts to the cold table (tight hot indexes, healthy vacuum) without being bounded. Set > 0 to bound the archive |
| `RETENTION_CLEANUP_INTERVAL_SECS` | `3600` | Interval between retention loop runs in seconds |
| `RETENTION_BATCH_SIZE` | `1000` | Max tasks moved (and archive rows purged) per retention cycle |

## Security

| Variable | Default | Description |
|----------|---------|-------------|
| `SKIP_SSRF_VALIDATION` | `1` (debug) / `0` (release) | Skip SSRF validation on webhook URLs |
| `BLOCKED_HOSTNAMES` | `localhost,127.0.0.1,::1,0.0.0.0,local,internal` | Comma-separated blocked hostnames |
| `BLOCKED_HOSTNAME_SUFFIXES` | `.local,.internal,.localdomain,.localhost` | Comma-separated blocked hostname suffixes |
| `ALLOWED_HOSTNAMES` | empty | Comma-separated webhook hostnames trusted despite SSRF protection: exact names (`backend`) or leading-dot suffixes (`.svc.cluster.local`), case-insensitive. An allowlisted name skips the hostname blocklists at creation time **and** the resolved-IP check in the delivery-time resolver — the operator vouches for whatever it resolves to (typical use: internal docker/k8s service names). |
| `ALLOWED_CIDRS` | empty | Comma-separated IP networks (CIDR, or a bare IP for a single host) trusted despite being private/reserved, e.g. `10.42.0.0/16,192.168.1.10`. Applies to IP-literal URLs at creation time and to every IP a hostname resolves to at delivery time. Invalid entries fail startup. Prefer this over `SKIP_SSRF_VALIDATION=1`, which drops every check. |
| `AUTH_TOKEN` | unset ⇒ auth disabled | Optional static bearer token (Audit 2, A6). When set, an actix `from_fn` middleware (`src/auth.rs`) requires `Authorization: Bearer <token>` on **every** endpoint (including `/metrics`, Swagger UI, `/view`) **except** `/health` and `/ready` (k8s probes). Comparison is constant-time (manual byte XOR — `subtle` is only a transitive dep). Unset/blank ⇒ total pass-through (historical open behavior), with a loud release-build warning at startup. Token is header-only (never a query string), so `/view` needs a reverse proxy injecting the header. The `?handle=` capability URL is NOT gated here (deferred to a later breaking lot). |
