# Architecture

ArcRun coordinates work performed by external HTTP services. PostgreSQL stores task state, dependencies, scheduling reservations, and pending notifications. An Actix Web server exposes the API; background workers schedule tasks and deliver webhooks.

For API contracts, see the [API reference](api.md). For implementation details of scheduling and delivery, see [Workers](workers.md) and [Webhooks](webhooks.md).

## Request and execution flow

```text
Client ── POST /task ──> API ── transaction ──> PostgreSQL
                                                 │
                                            Start worker
                                                 │
                                            on_start HTTP
                                                 ▼
                                         External service
                                                 │
                                        PATCH /task/{id}
                                                 ▼
                           Status + propagation + notification queue
                                                 │
                                          Delivery worker
                                                 │
                                      End / cancel / batch webhook
```

Task creation validates the complete request before inserting tasks, dependency links, and actions in a transaction. Tasks receive server-generated UUIDs; local IDs resolve dependencies within the request. Dependencies must reference earlier tasks, so input order prevents cycles.

Tasks without deduplication checks are inserted in groups. Inserts are split to stay within PostgreSQL's bind-parameter limit while sharing the same transaction. Deduplication checks can see tasks inserted earlier in that transaction.

## Transaction boundaries

| Operation | Committed together |
|-----------|--------------------|
| Task creation | Tasks, actions, dependency links, optional batch identity and completion actions |
| Scheduling claim | Pending → Claimed transition and all rule reservations |
| Successful start | Claimed → Running transition, start execution result, and a valid cancel action returned by the receiver |
| Terminal transition | Final status, dependency propagation, rule release, batch progress, and queued notifications |
| Counter flush | Counter increments, activity timestamp, and reduced capacity reservations |
| Notification completion | Removal from the queue and insertion into delivery history |
| Archiving | Task move to the archive and removal of associated actions, links, and webhook records |

A successful completion API response means the state change and its notifications are durable. It does not mean webhook receivers have acknowledged those notifications.

Counter updates accepted by `PUT /task/{id}` are an exception: they enter an in-memory queue and become durable at flush time. See [counter updates](api.md#update-progress-counters).

## Dependency propagation

Each task stores counts of outstanding dependencies. A parent reaching a terminal state satisfies completion-only dependencies. A failed or canceled parent causes children requiring success to fail; that failure can propagate through further required-success edges.

Failure propagation processes the graph one level at a time. Guarded updates ensure that a child reached through several paths is terminalized once. All resulting state changes and notifications belong to the originating transaction.

Paused tasks still receive dependency-counter updates and can fail because a required parent failed. A paused task whose dependencies are satisfied stays Paused until explicitly resumed.

When enabled, dead-end cancellation also walks upward: a non-terminal ancestor whose children are all terminal can be canceled. `dead_end_barrier` stops further traversal above that task. See [task lifecycle](concepts.md#task-lifecycle).

## Domain constraints

`TerminalStatus` wraps a task status only after verifying that it is Success, Failure, or Canceled. Its private field prevents callers from constructing an end notification for Pending, Waiting, Claimed, Running, or Paused tasks.

The outbox accepts a `TaskNotification` (End or Cancel) and derives its trigger, condition, and idempotency key. Callers cannot supply inconsistent combinations or route Start/BatchComplete through this task-notification interface. Batch completion has a separate enqueue function that derives the batch key.

These types constrain in-process operations. Database transactions and guarded updates still enforce concurrent state changes; the types do not prove that a database row has remained in the same state since it was read.

## Persistent data

The authoritative column definitions are in `src/schema.rs`; schema changes are under `migrations/`.

| Table | Responsibility |
|-------|----------------|
| `task` | Active and recent terminal tasks, metadata, progress counters, dependency counters, and scheduling reservations |
| `link` | Parent/child edges with their `requires_success` flag |
| `action` | Task webhook definitions and their lifecycle triggers |
| `batch` | Optional batch identity, completion actions, and the number of non-terminal tasks |
| `rule_slot` | Shared concurrency counts and capacity charges used during admission |
| `webhook_outbox` | Pending end, cancel, and batch-complete deliveries, including retry time and lease token |
| `webhook_execution` | Start-execution idempotency records and completed delivery history |
| `task_archive` | Archived task records plus `archived_at` |

Every task has a `batch_id`. A separate `batch` row is created only when completion actions, scope, or metadata are supplied. Batches represented solely by task rows have no persisted `remaining` counter.

The outbox contains only pending deliveries and has no status column. A terminal delivery is removed from that table and recorded in `webhook_execution` as `success` or `exhausted`. A unique event key and a history check prevent a completed event from being enqueued again.

## Scheduling and multiple replicas

Each server runs its own workers. A PostgreSQL advisory lock on a dedicated connection elects one active scheduler. Other replicas retry leadership acquisition; a dropped connection releases the lock.

Rule admission uses database rows, so concurrency and capacity reservations are atomic. Delivery workers claim notifications with `FOR UPDATE SKIP LOCKED`, a timed lease, and a fencing token. A worker with an expired token cannot overwrite the result of a newer claim, though duplicate HTTP delivery remains possible.

In-process notifications wake the scheduler and delivery worker after relevant commits. Polling remains necessary for missed wakeups, retry deadlines, and work committed by other replicas.

## Retention and reads

Retention moves eligible terminal tasks into `task_archive`. `GET /task/{id}` falls back to this archive and returns the usual task shape with an empty `actions` array. Listings, DAGs, batch statistics, and lifecycle writes operate on the main task table.

Archiving waits for pending task notifications and batch-completion requirements. This preserves the records needed to compute the final batch payload. An optional archive-retention period eventually deletes archived records; its default of zero keeps them indefinitely.

## Connections and failure handling

API handlers acquire connections through retry and circuit-breaker logic. The breaker rejects acquisition while open, probes recovery after a delay, and closes after enough successful probes. Health and readiness endpoints use a separate two-second acquisition bound.

Start webhooks release the pool connection during HTTP execution. The delivery worker runs HTTP outside database transactions and row locks, but retains one borrowed connection for its entire delivery iteration. Size the pool with handlers and other workers in mind.

## Code map

| Location | Responsibility |
|----------|----------------|
| `src/main.rs` | Configuration, migrations, middleware, HTTP server, and worker startup |
| `src/handlers/` | Routes, HTTP handlers, and OpenAPI annotations |
| `src/dtos/` | Request, response, and query types |
| `src/models.rs`, `src/schema.rs` | Persistent models and Diesel schema |
| `src/db/` | Queries, transactions, lifecycle changes, reservations, and outbox operations |
| `src/workers/` | Scheduling, propagation, timeout, delivery, counter flushing, retention, and sampling |
| `src/action.rs` | Outbound HTTP, event headers, callback URLs, and payload enrichment |
| `src/notification.rs` | Terminal-state validation and typed task notifications |
| `src/rule.rs` | Rule types and reservation keys |
| `src/validation/` | Payload validation and webhook URL checks |
| `src/auth.rs` | Optional bearer authentication |
| `src/config.rs` | Environment configuration and defaults |
| `src/metrics.rs`, `src/tracing.rs` | Prometheus and OpenTelemetry instrumentation |
| `sdk/` | Rust client library |
| `tests/integration/` | PostgreSQL-backed integration tests |
| `static/dag.html` | Built-in DAG viewer |
| `website/` | Documentation site built from `docs/` |
