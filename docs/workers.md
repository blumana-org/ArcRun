# Background Workers

The server starts six background workers. Each receives a shared shutdown signal. Timing and batch limits are described in [Configuration](configuration.md).

| Worker | Purpose | Default cadence |
|--------|---------|-----------------|
| Start | Admit Pending tasks and call `on_start` | 1 second, plus wakeups |
| Timeout | Requeue stale claims and fail inactive Running tasks | 1 second |
| Counter updater | Persist queued progress increments | 100 ms |
| Delivery | Send queued end, cancel, and batch notifications | 1 second, plus wakeups |
| Retention | Clean unused rule slots and archive/purge eligible tasks | 1 hour |
| Metrics sampler | Refresh task and connection-pool gauges | 15 seconds |

Cadences are waits between iterations, not execution deadlines. Database and HTTP work can lengthen an iteration.

## Start worker

The scheduler first acquires leadership through a PostgreSQL advisory lock held on a dedicated, non-pooled connection. Only the leader schedules tasks; standby replicas retry on subsequent iterations.

Pending tasks are scanned in descending priority, then ascending creation time and UUID. The scan continues past blocked tasks so they cannot hide eligible work later in the backlog. `WORKER_START_BATCH_SIZE` limits successful claims per iteration, not the number of visible candidates.

For each eligible task, the scheduler:

1. Reserves all required rule slots and changes Pending to Claimed atomically.
2. Waits for a webhook-concurrency permit, refreshing the claim timestamp while waiting.
3. Creates or claims the start-execution record and rechecks that the task is still Claimed.
4. Calls `on_start` without holding a pool connection.
5. Records the result. A successful start persists any valid cancel action and changes the task to Running if it is still Claimed. A failed start fails the task and propagates the result.

The Claimed state includes the HTTP call. A receiver may report completion during this state; the later start-result transaction does not overwrite that terminal state.

### Concurrency and capacity reservations

Rules map to canonical keys based on rule type, matcher, and metadata values. Admission locks those keys in a stable order and reserves all slots in one transaction. If any rule blocks, the whole claim rolls back.

Concurrency rules reserve one unit. Capacity rules reserve the task's remaining work, `max(expected_count - success - failures, 0)`. Admission checks the existing capacity charge before adding the candidate, so a newly admitted task can take the total above the threshold.

Only tasks admitted through a rule key occupy that key's reservations. Matching tasks without the rule do not count. The consumed keys and capacity charge are stored on each task and released when it leaves Claimed/Running, including requeue after a stale claim.

Counter flushes reduce capacity reservations as work completes. Direct PATCH updates do not adjust capacity reservations mid-run. A capacity charge only decreases until it is released, even if `expected_count` is later increased.

## Timeout worker

Each iteration first requeues stale Claimed tasks whose activity timestamp is older than `WORKER_CLAIM_TIMEOUT_SECS`. Requeue releases their reservations and makes them Pending again.

The worker then finds Running tasks whose `last_updated` exceeds their task-specific timeout. Each timeout commits Failure with reason `Timeout`, dependency propagation, reservation release, and failure-notification enqueue.

Timed-out tasks are processed oldest first, in passes of at most `WORKER_TIMEOUT_BATCH_SIZE`. An iteration drains at most 50 full passes before yielding. This bounds the work between stale-claim checks.

Timeout measures inactivity. Persisted counter updates and PATCH updates refresh `last_updated`; an update merely accepted into the in-memory PUT queue has not yet refreshed it.

## Counter updater

`PUT /task/{id}` sends increments into a bounded channel. A receiver aggregates them by task in a `DashMap` with per-shard locking and atomic counters. The updater periodically snapshots accumulated increments and writes them to PostgreSQL.

A flush updates counters and `last_updated`, and reduces any capacity reservations in the same transaction. Persisted counter sums are computed with wider arithmetic and clamped to `i32::MAX`.

Failure handling distinguishes several cases:

- Updates targeting tasks that are now terminal, absent, or archived do not change them.
- A failed batch write falls back to per-task writes. Every per-task database error is logged and its increments are queued again, independently of other rows succeeding. A task that no longer matches because it became terminal or disappeared is consumed without retry.

A `202` response acknowledges in-memory acceptance. Abrupt process termination can lose unflushed increments. During graceful shutdown the receiver drains buffered events before the final flush is attempted; persistence still depends on a successful database write.

## Delivery loop

The delivery worker claims due outbox rows, loads the necessary actions, sends HTTP requests, and records their outcomes. Requests for different events run concurrently; actions belonging to one event run sequentially.

The lease, retries, ordering, and receiver contract are documented in [Webhooks](webhooks.md#delivery-loop). `run_delivery_once` exposes one iteration for integration tests.

## Retention worker

The retention worker always removes unused rule-slot rows. Task archiving and archive purging run only when `RETENTION_ENABLED=1`.

A task is eligible for archiving when it is terminal and its `ended_at` is older than `RETENTION_DAYS`. It is retained while its own notification is queued, its batch-complete notification is queued, or its batch has completion actions and is still incomplete.

Eligible tasks move to `task_archive` in a transaction that also removes their actions, dependency links, and webhook records. Empty batch records are cleaned up once no pending batch notification requires them.

When `RETENTION_ARCHIVE_DAYS` is positive, archive records older than that many days since `archived_at` are deleted. Zero keeps them indefinitely. Both moves and purges are bounded by `RETENTION_BATCH_SIZE`.

## Metrics sampler

The sampler reads task counts by status, Running tasks by kind, and pool occupancy every `METRICS_SAMPLER_INTERVAL_SECS`. Failures are logged and the next iteration retries.

The delivery loop samples outbox backlog separately, at most once every 15 seconds. Worker heartbeat and duration metrics are listed in [Metrics](metrics.md).

## Wakeups and shutdown

Handlers and workers notify the start or delivery loop after commits that make work available. These wakeups are local to a process. Polling provides the fallback for other replicas, retry deadlines, and lost wakeups.

Workers observe the shutdown signal at their loop boundaries. A process that exits during notification delivery leaves durable outbox rows available for retry after their leases expire.
