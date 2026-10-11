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

Start and delivery refill available execution slots continuously. Their cadences are fallback polling intervals when no work can be admitted. Other cadences are waits between iterations; none are execution deadlines.

## Start worker

The scheduler first acquires leadership through a PostgreSQL advisory lock held on a dedicated, non-pooled connection. Only the leader schedules tasks; standby replicas retry on subsequent iterations.

Pending tasks are scanned in descending priority, then ascending creation time and UUID. The scan continues past blocked tasks so they cannot hide eligible work later in the backlog. Each refill claims at most `WORKER_START_BATCH_SIZE` tasks and never more than the available execution slots; the candidate scan is not limited by this cap. Leadership is checked before each refill.

For each eligible task, the scheduler:

1. Reserves all required rule slots and changes Pending to Claimed atomically.
2. Immediately dispatches it into an available execution slot. Excess work remains Pending, without reserved rule slots or claim-timeout heartbeats.
3. Creates or claims the start-execution record and rechecks that the task is still Claimed.
4. Calls `on_start` without holding a pool connection.
5. Records the result. A successful start persists any valid cancel action and changes the task to Running if it is still Claimed. A failed start fails the task and propagates the result.

The Claimed state includes the HTTP call. A receiver may report completion during this state; the later start-result transaction does not overwrite that terminal state.

An execution slot stays occupied through the result transaction, bounding the number of tasks waiting for the database. A finished start frees that local slot for another task without waiting for slower requests. It does not release the Running task's concurrency/capacity reservations. Long HTTP calls or database stalls can still exceed the claim inactivity timeout.

### Concurrency and capacity reservations

Rules map to canonical keys based on rule type, matcher, and metadata values. Admission first locks the candidate task, then reserves its slots in key order, matching terminal transitions and progress flushes. The task lock and first slot upsert share a statement through a materialized CTE dependency. This preserves the number of SQL round-trips and avoids a speculative task UPDATE when a rule blocks. Row locking still has a WAL/CPU cost. If any rule blocks, the whole claim rolls back.

Slot-lock queries explicitly use `COLLATE "C"` to agree with Rust's byte ordering on UTF-8 databases, regardless of the database's default locale. This avoids opposite acquisition orders for keys containing accented characters or different letter cases.

Concurrency rules reserve one unit. Capacity rules reserve the task's remaining work, `max(expected_count - success - failures, 0)`. Admission checks the existing capacity charge before adding the candidate, so a newly admitted task can take the total above the threshold.

Claims verify that the rules still match the scheduler's snapshot. If a batch-rules edit wins the race, the claim rolls back all reservations and a later scan evaluates the new rules. The batch fast path likewise checks that each task still has no rules.

Only tasks admitted through a rule key occupy that key's reservations. Matching tasks without the rule do not count. The consumed keys and capacity charge are stored on each task and released when it leaves Claimed/Running, including requeue after a stale claim.

Counter flushes reduce capacity reservations as work completes. Direct PATCH updates do not adjust capacity reservations mid-run. A capacity charge only decreases until it is released, even if `expected_count` is later increased.

### Scan-local blocked-rule cache

After SQL rejects a reservation, the scheduler remembers only that exact canonical
slot key and the greatest threshold observed blocked during this scan. This applies
to both Concurrency and Capacity. Candidates for the same key with a threshold at
or below the cached threshold skip the claim transaction; larger thresholds still
reach SQL. A mixed-rule refusal never marks its other rules blocked. Invalid rule
inputs and database errors do not populate the cache.

The cache spans pages but is discarded at the end of every `run_claim_loop` call.
It stores no tasks, admits nothing, and never changes the transactional checks or
reservations. Other transactions can release capacity or concurrency during a scan:
a cached refusal can therefore defer an eligible task until a later scan and affect
the effective admission order. There is no fixed wall-clock admission deadline.
Each new scan starts with fresh observations; polling remains the fallback for
progress even without a wakeup.

Concurrency and Capacity share a budget of **4,096 entries and 1 MiB of stored key
text**. Keys use boxed strings so unused String capacity is not retained. Hash-table
storage and allocation overhead are additional, bounded by the entry count. When
either budget is reached, new keys fall back to SQL; existing entries can still
record a higher blocked threshold. There is no cross-scan cache or per-task wait
queue. The pending-task scan remains paginated and still visits candidates beyond
blocked prefixes: this optimization reduces claim transactions, not the number of
candidate rows read. Cache hits have separate Concurrency and Capacity counters.

## Timeout worker

Each iteration first requeues stale Claimed tasks whose activity timestamp is older than `WORKER_CLAIM_TIMEOUT_SECS`. Requeue releases their reservations and makes them Pending again.

The worker then finds Running tasks whose `last_updated` exceeds their task-specific timeout. It locks each candidate's complete possible downward write set in UUID order with `SKIP LOCKED`, and verifies both that every required row was acquired and that the locked root is still inactive. A busy descendant defers the entire timeout without state changes; a later pass recomputes the set and rechecks inactivity. A committed timeout includes Failure with reason `Timeout`, dependency propagation, reservation release, and failure-notification enqueue. Batch/slot writes can still wait after task acquisition.

Timed-out tasks are processed oldest first, in passes of at most `WORKER_TIMEOUT_BATCH_SIZE`. An iteration drains at most 50 full passes before yielding, and stops early when a pass commits no timeout. This avoids repeatedly traversing the same busy closures within one iteration. A busy oldest prefix can defer later candidates; there is no fixed timeout-processing deadline under contention.

Timeout measures inactivity. Persisted counter updates and PATCH updates refresh `last_updated`; an update merely accepted into the in-memory PUT queue has not yet refreshed it.

## Counter updater

`PUT /task/{id}` sends increments into a bounded channel. A receiver aggregates them by task in a `DashMap` with per-shard locking and atomic counters. The updater periodically snapshots accumulated increments and writes them to PostgreSQL.

A flush updates counters and `last_updated`, and reduces any capacity reservations in the same transaction. Persisted counter sums are computed with wider arithmetic and clamped to `i32::MAX`.

The flush locks its task rows in UUID order before reading and locking their stored capacity keys. Terminal transitions lock their complete possible downward write set in that same order before changing the root. Batch stop, batch-rules edits and rule-free batch claims also lock their eligible task sets in UUID order. Propagation no longer acquires blocking locks one frontier at a time; this closes the cross-frontier and parent/child cycles as well as the task/slot inversion.

Failure handling distinguishes several cases:

- Updates targeting tasks that are now terminal, absent, or archived do not change them.
- A failed batch write falls back to per-task writes. Every per-task database error is logged and its increments are queued again, independently of other rows succeeding. A task that no longer matches because it became terminal or disappeared is consumed without retry.

A `202` response acknowledges in-memory acceptance. Abrupt process termination can lose unflushed increments. During graceful shutdown the receiver drains buffered events before the final flush is attempted; persistence still depends on a successful database write.

## Delivery loop

The delivery worker claims due outbox rows, loads the necessary actions, sends HTTP requests, and records their outcomes as they finish. Ready marks take priority over new claims; one pool checkout coalesces available results and refills free slots. Active deliveries plus results awaiting persistence are bounded by `WEBHOOK_DELIVERY_CONCURRENCY`. A transient database error retains those results and pauses admission until persistence succeeds.

The lease, retries, ordering, and receiver contract are documented in [Webhooks](webhooks.md#delivery-loop). `run_delivery_once` exposes the same pipeline with a finite total claim budget for integration tests. It returns database errors to its caller; in that case unrecorded events recover through their durable leases.

## Retention worker

The retention worker always attempts to remove unused rule-slot rows. It locks empty slots in key order with `SKIP LOCKED`; busy rows are reconsidered on a later pass, so cleanup does not wait on admission or release. Task archiving and archive purging run only when `RETENTION_ENABLED=1`.

A task is eligible for archiving when it is terminal and its `ended_at` is older than `RETENTION_DAYS`. It is retained while its own notification is queued, its batch-complete notification is queued, or its batch has completion actions and is still incomplete.

Before removing actions, webhook records or links, retention locks the selected tasks in UUID order with `SKIP LOCKED`. Only the acquired subset is archived and counted. Busy tasks and their associated records remain for a later pass.

Eligible tasks move to `task_archive` in a transaction that also removes their actions, dependency links, and webhook records. Empty batch records are cleaned up once no pending batch notification requires them.

When `RETENTION_ARCHIVE_DAYS` is positive, archive records older than that many days since `archived_at` are deleted. Zero keeps them indefinitely. Both moves and purges are bounded by `RETENTION_BATCH_SIZE`.

## Metrics sampler

The sampler reads task counts by status, Running tasks by kind, and pool occupancy every `METRICS_SAMPLER_INTERVAL_SECS`. Failures are logged and the next iteration retries.

The delivery loop samples outbox backlog separately, at most once every 15 seconds. Worker heartbeat and duration metrics are listed in [Metrics](metrics.md).

## Wakeups and shutdown

Handlers and workers notify the start or delivery loop after commits that make work available. These wakeups are local to a process. Polling provides the fallback for other replicas, retry deadlines, and lost wakeups.

Start and delivery stop new admissions when they observe shutdown, then drain already-admitted work. If delivery cannot persist a result during shutdown, it leaves the durable event for recovery after lease expiry. Abrupt process termination has the same recovery mechanism. Other workers observe shutdown at their loop boundaries.
