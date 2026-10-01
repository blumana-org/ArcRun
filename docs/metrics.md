# Prometheus Metrics

All metrics are exposed at `GET /metrics` in Prometheus format. When `AUTH_TOKEN` is configured, scraping requires the bearer token.

## Monitoring signals

- Track `webhook_delivery_exhausted_total` and inspect exhausted deliveries when it increases.
- Compare `webhook_outbox_pending` with `webhook_outbox_oldest_pending_age_seconds` to detect a growing or stalled queue. The `leased` label includes both in-flight leases and retries scheduled for the future.
- Watch `db_pool_acquire_wait_seconds` and `db_pool_acquire_failures_total` for pool pressure.
- Use `worker_loop_last_iteration_timestamp_seconds` to detect stale worker activity. Allow for each worker's configured interval and execution duration.
- Watch `batch_updater_pending_tasks` and flush failures when PUT progress appears delayed.

Task and pool gauges refresh on `METRICS_SAMPLER_INTERVAL_SECS`. Outbox gauges are sampled separately by the delivery worker at most once every 15 seconds. These gauges are snapshots, so short-lived changes may occur between samples.

## Task counters

| Metric | Labels | Description |
|--------|--------|-------------|
| `tasks_created_total` | - | Total tasks created |
| `tasks_completed_total` | `outcome`, `kind` | Tasks completed by outcome and kind |
| `tasks_cancelled_total` | - | Tasks cancelled |
| `tasks_canceled_dead_end_total` | - | Ancestors canceled because no viable child remains |
| `tasks_timed_out_total` | - | Tasks timed out |
| `task_status_transitions_total` | `from_status`, `to_status` | Status transitions |

## Task Gauges

| Metric | Labels | Description |
|--------|--------|-------------|
| `tasks_by_status` | `status` | Current tasks by status (sampled every `METRICS_SAMPLER_INTERVAL_SECS`; `status` is the lowercase DB enum label) |
| `running_tasks_by_kind` | `kind` | Running tasks by kind (sampled) |

## Dependencies

| Metric | Labels | Description |
|--------|--------|-------------|
| `tasks_with_dependencies_total` | - | Tasks created with dependencies |
| `dependency_propagations_total` | `parent_outcome` | Dependency propagations |
| `tasks_unblocked_total` | - | Tasks unblocked after dependencies completed |
| `tasks_failed_by_dependency_total` | - | Tasks failed due to parent failure |

## Webhooks

| Metric | Labels | Description |
|--------|--------|-------------|
| `webhook_executions_total` | `trigger`, `outcome` | Webhook calls |
| `webhook_attempts_total` | `trigger`, `outcome` | Webhook attempts (includes failures) |
| `webhook_duration_seconds` | `trigger` | Webhook duration histogram |
| `webhook_idempotent_skips_total` | `trigger` | Webhook executions skipped due to idempotency |
| `webhook_idempotent_conflicts_total` | - | Idempotency conflicts when claiming executions |
| `webhook_delivery_retries_total` | `trigger` | Outbox deliveries that failed and were rescheduled |
| `webhook_delivery_exhausted_total` | `trigger` | Outbox deliveries that exhausted all retries |
| `webhook_delivery_success_total` | `trigger` | Outbox deliveries that succeeded |
| `webhook_delivery_lag_seconds` | - | Lag from outbox row creation to successful delivery |
| `webhook_outbox_pending` | `state` | Outbox backlog depth (`ready`: mature; `leased`: not yet due) |
| `webhook_outbox_oldest_pending_age_seconds` | - | Age of the oldest mature pending outbox row (stuck-row signal) |
| `webhook_mark_failures_total` | `mark` | Outbox mark writes that failed (`success`/`retry`/`exhausted`) |
| `webhooks_in_flight` | `phase` | Webhook executions in progress (`start`, `delivery`) |

## Concurrency

| Metric | Labels | Description |
|--------|--------|-------------|
| `tasks_blocked_by_concurrency_total` | - | Tasks blocked by rules |
| `concurrency_ko_cache_hits_total` | - | Claim-loop blocked-rule cache hits (skipped DB checks) |

## Duration

| Metric | Labels | Description |
|--------|--------|-------------|
| `task_duration_seconds` | `kind`, `outcome` | Task execution duration |
| `task_wait_seconds` | `kind` | Time from task creation to Running (scheduler latency) |

## Workers

| Metric | Labels | Description |
|--------|--------|-------------|
| `worker_loop_iterations_total` | `loop` | Worker loop iterations (per loop: `start`, `timeout`, `batch_updater`, `retention`, `delivery`, `metrics_sampler`) |
| `worker_loop_duration_seconds` | `loop` | Worker loop duration (per loop) |
| `worker_loop_last_iteration_timestamp_seconds` | `loop` | Unix timestamp of each loop's last iteration (liveness heartbeat) |
| `start_loop_is_leader` | - | 1 when this process holds the scheduler leader lease, otherwise 0 |
| `tasks_processed_per_loop` | - | Tasks processed per start-loop iteration |

## Database

| Metric | Labels | Description |
|--------|--------|-------------|
| `db_query_duration_seconds` | `query` | Query duration |
| `slow_queries_total` | `query` | Queries exceeding threshold |
| `db_pool_acquire_failures_total` | - | Failures to acquire a pool connection after all retries |
| `db_pool_acquire_wait_seconds` | - | Time to acquire a pool connection (HTTP path) |
| `db_pool_connections` | `state` | Pool connections by state (`in_use`, `idle`); sampled |
| `tasks_db_save_failures_total` | - | DB save failures after retries |
| `batch_update_failures_total` | - | Counter recovery failures; failed database writes are queued again, while terminal or missing tasks are skipped |

## Progress updates

| Metric | Labels | Description |
|--------|--------|-------------|
| `batch_update_events_total` | - | Counter-update events accepted by the PUT handler |
| `batch_channel_send_wait_seconds` | - | Time awaiting the channel `send()` (backpressure signal) |
| `batch_channel_capacity_available` | - | Available channel permits, sampled at send time |
| `batch_updater_flush_rows` | - | Task rows persisted per flush |
| `batch_updater_flush_duration_seconds` | - | Duration of a DB flush |
| `batch_updater_pending_tasks` | - | Distinct tasks with un-persisted counters (crash-loss window) |

## Batches and scheduling

| Metric | Labels | Description |
|--------|--------|-------------|
| `tasks_deduped_total` | - | Tasks skipped by a `dedupe_strategy` match on insert |
| `batch_insert_tasks` | - | Tasks per `POST /task` batch |
| `batches_completed_total` | - | `batch_complete` signals enqueued (last task terminal) |
| `claim_pages_scanned` | - | Keyset pages scanned per start-loop claim iteration |

## Circuit Breaker

| Metric | Labels | Description |
|--------|--------|-------------|
| `circuit_breaker_state_transitions_total` | `to_state` | State transitions |
| `circuit_breaker_rejections_total` | - | Requests rejected |

## Retention

| Metric | Labels | Description |
|--------|--------|-------------|
| `retention_tasks_cleaned_total` | - | Terminal tasks moved out of the hot table |
| `retention_cleanup_runs_total` | `outcome` | Retention cleanup runs (`success` or `error`) |
| `retention_cleanup_duration_seconds` | - | Retention cleanup duration |
