# API Reference

ArcRun accepts JSON request bodies and returns JSON for task and batch data. UUID path parameters identify server-created tasks and batches. The generated schema is available at `/api-docs/openapi.json`, with an interactive explorer at `/swagger-ui/`.

For a complete local example, see [Getting started](getting-started.md). Webhook receiver behavior is documented in [Webhooks](webhooks.md).

## Authentication

When `AUTH_TOKEN` is set, every request except `/health` and `/ready` requires:

```http
Authorization: Bearer <token>
```

Missing or invalid credentials return `401`. Authentication also applies to completion callbacks, metrics, Swagger, and `/view`. The callback URL provided to a worker contains no token. An unset or blank `AUTH_TOKEN` disables authentication.

The built-in browser pages do not supply this header themselves. With authentication enabled, serve them through a proxy that authenticates the user and injects the header on their requests.

## Endpoint summary

| Method | Path | Purpose |
|--------|------|---------|
| GET | `/health` | Liveness and pool status |
| GET | `/ready` | Readiness |
| POST | `/task` | Create a batch of tasks |
| GET | `/task` | List and filter tasks |
| GET | `/task/{task_id}` | Read a task, including archived records |
| PATCH | `/task/{task_id}` | Update progress or report completion |
| PUT | `/task/{task_id}` | Queue progress increments |
| DELETE | `/task/{task_id}` | Cancel a task |
| PATCH | `/task/pause/{task_id}` | Pause a Pending/Waiting task |
| PATCH | `/task/resume/{task_id}` | Resume a Paused task |
| GET | `/batches` | Discover batches |
| GET | `/batch/{batch_id}` | Read aggregate progress |
| DELETE | `/batch/{batch_id}` | Stop a batch |
| PATCH | `/batch/{batch_id}/rules` | Change rules for tasks not yet active |
| GET | `/dag/{batch_id}` | Read graph data |
| GET | `/view?batch={batch_id}` | Open the graph viewer |
| GET | `/webhook-deliveries` | Inspect pending deliveries and execution history |
| GET | `/metrics` | Prometheus metrics |

## Pagination

Task, batch, and delivery listings return arrays and accept these parameters:

| Parameter | Default | Behavior |
|-----------|---------|----------|
| `page` | `0` | Zero-based page; negative values become zero |
| `page_size` | `50` | Values above the configured maximum are clamped; non-positive values use the default |

The default and maximum come from `PAGINATION_DEFAULT` and `PAGINATION_MAX`; the maximum defaults to 100.

## Create tasks

```http
POST /task
Content-Type: application/json
```

The simplest body is an array:

```json
[
  {
    "id": "import",
    "name": "Import records",
    "kind": "import",
    "timeout": 300,
    "metadata": {"tenant_id": "acme"},
    "expected_count": 100,
    "on_start": {
      "kind": "Webhook",
      "params": {
        "url": "https://worker.example.com/import",
        "verb": "Post",
        "body": {"source": "daily"}
      }
    }
  }
]
```

Example domains must be replaced with your receivers. The [local walkthrough](getting-started.md) uses the included worker instead.

### Task fields

| Field | Required | Meaning |
|-------|----------|---------|
| `id` | Yes | Non-empty local identifier, unique within this request |
| `name` | Yes | Non-empty display name; maximum 255 bytes |
| `kind` | Yes | Non-empty task category; maximum 100 bytes |
| `on_start` | Yes | One webhook action |
| `timeout` | No | Inactivity timeout while Running, 1–86400 seconds; default 60 |
| `metadata` | No | JSON metadata, maximum 64 KiB; used by rule and deduplication matchers |
| `dependencies` | No | Array of `{ "id": "earlier-local-id", "requires_success": true }` |
| `on_success` | No | Array of webhook actions for Success |
| `on_failure` | No | Array of webhook actions for Failure |
| `expected_count` | No | Non-negative expected item count; required by Capacity rules |
| `rules` | No | Array of Concurency or Capacity strategies |
| `dedupe_strategy` | No | Array of matchers that can skip task creation |
| `priority` | No | Integer from -1000 to 1000; default 0 |
| `dead_end_barrier` | No | Stop upward dead-end cancellation after this task; default false |

Dependencies must reference earlier tasks in the same request. Reaching `expected_count` does not change task status. See [Core concepts](concepts.md) for rules, dependency behavior, and deduplication.

### Batch fields

Use an object body to attach batch identity or completion actions:

```json
{
  "scope": "daily-import",
  "metadata": {"tenant_id": "acme"},
  "tasks": [{
    "id": "import",
    "name": "Import records",
    "kind": "import",
    "on_start": {
      "kind": "Webhook",
      "params": {"url": "https://worker.example.com/import", "verb": "Post"}
    }
  }],
  "on_batch_complete": [{
    "kind": "Webhook",
    "params": {"url": "https://worker.example.com/batch-done", "verb": "Post"}
  }]
}
```

`scope`, batch `metadata`, and `on_batch_complete` are optional. Scope must be non-empty and no more than 255 bytes. Batch metadata has a 64 KiB limit and is independent of task metadata.

Completion actions are queued when every inserted task is terminal, or immediately if all tasks were skipped by deduplication. Delivery may repeat; see [batch completion](webhooks.md#batch-completion).

### Responses

| Status | Meaning |
|--------|---------|
| `201 Created` | Array of created tasks (`BasicTaskDto`), with `X-Batch-ID` |
| `204 No Content` | All tasks were skipped by deduplication; still includes `X-Batch-ID` |
| `400 Bad Request` | Invalid task, dependency, rule, action, or batch metadata |
| `413 Payload Too Large` | Request body exceeds `PAYLOAD_MAX_BYTES` |

Creation is atomic. Configurable task, dependency, and action limits are listed in [Configuration](configuration.md#request-limits). A task-validation error has this shape:

```json
{
  "error": "Validation failed",
  "batch_id": "019a0000-0000-7000-8000-000000000001",
  "details": ["validation error description"]
}
```

Malformed JSON and other validation paths may use a different error body; branch on the HTTP status before parsing details.

## Read a task

```http
GET /task/{task_id}
```

Returns `200` with `TaskDto`: identity, status, rules, metadata, registered actions, progress counters, timeout, priority, batch ID, and lifecycle timestamps. Unknown or purged IDs return `404`.

An archived task has the same response shape with `actions: []`. Only this endpoint reads the archive. Task listings, DAGs, batch statistics, and lifecycle writes use the main task table.

## List tasks

```http
GET /task?page=0&page_size=50&status=Running&kind=import
```

Returns `200` with an array of lightweight `BasicTaskDto` objects, without actions, rules, or metadata.

| Filter | Matching |
|--------|----------|
| `name` | Substring |
| `kind` | Substring |
| `status` | Task state such as `Running` or `Success` |
| `timeout` | Exact integer timeout |
| `batch_id` | Exact batch UUID |
| `metadata` | JSON containment |

Filters combine with AND. URL-encode JSON values. For example, with authentication enabled:

```bash
curl --get "$ARCRUN_URL/task" \
  -H "Authorization: Bearer $ARCRUN_TOKEN" \
  --data-urlencode 'metadata={"tenant_id":"acme"}'
```

Set `ARCRUN_URL` to the server base URL and `ARCRUN_TOKEN` to its bearer token. Invalid JSON in the metadata filter returns `400`.

## Report completion or update a task

```http
PATCH /task/{task_id}
Content-Type: application/json

{"status":"Success","new_success":10}
```

To report failure:

```json
{"status":"Failure","failure_reason":"Source service unavailable"}
```

PATCH applies to Running or Claimed tasks. Supported fields are `status`, `new_success`, `new_failures`, `metadata`, `expected_count`, `priority`, and `failure_reason`. Counters are non-negative increments. `metadata` replaces the entire stored value. The target status can only be Success or Failure; Failure requires a reason.

The response follows the commit of the update, dependency propagation, and notification enqueue. End webhooks are delivered asynchronously.

| Situation | Response |
|-----------|----------|
| Running/Claimed task updated | `200` |
| Same terminal status requested again | `200`, no-op; additional fields are not reapplied |
| Different terminal status, or another state cannot accept completion | `409`, with `current_status` |
| Unknown or archived task | `404` |
| Invalid values | `400` |

Without `status`, PATCH can synchronously update progress or metadata, but only for Running/Claimed tasks. If no row matches that condition, it returns `404` even if a task in another state exists. Priority updates have the same restriction.

## Update progress counters

```http
PUT /task/{task_id}
Content-Type: application/json

{"new_success":5,"new_failures":2}
```

At least one counter must be positive; negative increments are rejected with `400`. Only counter fields are applied. Status, metadata, expected count, and priority are ignored.

`202 Accepted` means the increments entered the in-memory queue. It does not verify task existence or confirm persistence. The updater flushes periodically; abrupt process termination can lose accepted increments. Terminal, absent, and archived tasks are not updated, including a task that becomes terminal between acceptance and flush.

Persisted increments refresh the inactivity timestamp. Updates are not idempotent: resending an increment can count it again. Avoid relying on a PUT immediately before completion for an exact final count; include the final increment in the completion PATCH when it must commit with the result.

## Cancel, pause, and resume

| Endpoint | Allowed source states | Result |
|----------|-----------------------|--------|
| `DELETE /task/{task_id}` | Waiting, Pending, Paused, Claimed, Running | Canceled |
| `PATCH /task/pause/{task_id}` | Pending, Waiting | Paused |
| `PATCH /task/resume/{task_id}` | Paused | Waiting if dependencies remain, otherwise Pending |

These return `200` on success, `400` for an existing task in a disallowed state, and `404` for an unknown or archived task.

Cancellation fails dependent children that require success. Cancel actions for Claimed/Running tasks are queued for asynchronous delivery. Paused tasks still receive dependency updates and can fail when a required parent fails.

## List batches

```http
GET /batches?page=0&page_size=50
```

Returns `200` with batch summaries containing `batch_id`, `total_tasks`, creation/update timestamps, `status_counts`, distinct `kinds`, `scope`, `metadata`, and `remaining`.

| Filter | Matching |
|--------|----------|
| `name`, `kind` | Task substring filters |
| `status` | Task state |
| `created_after`, `created_before` | Task creation timestamps, inclusive |
| `scope` | Exact batch scope |
| `metadata` | Batch metadata JSON containment |
| `search` | Substring across batch scope and metadata text |

Filters combine with AND. Task filters select batches containing a matching task. Counts and timestamps describe all current tasks in each selected batch; `kinds` is collected from the tasks matching the filters. URL-encode JSON and timestamp query values.

`remaining` counts non-terminal tasks for batches with a persisted batch record. It is null when no completion actions, scope, or metadata were supplied at creation.

## Read batch progress

```http
GET /batch/{batch_id}
```

Returns `200` with `batch_id`, `total_tasks`, `total_success`, `total_failures`, `total_expected`, `status_counts`, `scope`, and `metadata`. `total_expected` is null if any task lacks an expected count. Batches with no current tasks return `404`, including fully archived or fully deduplicated batches.

The success/failure totals count processed items. The per-status counts count tasks.

## Stop a batch

```http
DELETE /batch/{batch_id}
```

Cancels every non-terminal task with reason `Batch stopped`. Already terminal tasks remain unchanged. Cancel notifications for formerly Claimed/Running tasks are queued transactionally.

Returns `200` with counts in `canceled_waiting`, `canceled_pending`, `canceled_claimed`, `canceled_running`, `canceled_paused`, and `already_terminal`, plus `batch_id`. Batches with no current tasks return `404`, including fully archived or fully deduplicated batches.

## Update batch rules

```http
PATCH /batch/{batch_id}/rules
Content-Type: application/json

{
  "kind":"import",
  "rules":[{
    "type":"Concurency",
    "matcher":{"kind":"import","status":"Running","fields":[]},
    "max_concurency":10
  }]
}
```

Replaces rules on Waiting, Pending, and Paused tasks of the specified kind. Claimed/Running tasks retain their rules and reservations. An empty rules array removes rules from eligible tasks.

Returns `200` with `batch_id`, `kind`, and `updated_count`, including zero when no task of that kind is eligible. Invalid rules or an empty kind return `400`; a batch with no current tasks returns `404`.

## Graph data and viewer

`GET /dag/{batch_id}` returns an object with `tasks` (`BasicTaskDto[]`) and `links`. Each link contains `parent_id`, `child_id`, and `requires_success`.

`GET /view?batch={batch_id}` opens the built-in graph viewer with automatic layout, status colors, task details, and optional periodic refresh. The viewer reads current task data, excluding archived tasks.

## Inspect webhook deliveries

```http
GET /webhook-deliveries?status=exhausted&page=0&page_size=50
```

Returns `200` with delivery records ordered by `updated_at DESC, id DESC`. The optional case-insensitive `status` accepts `pending`, `success`, `failure`, or `exhausted`; invalid values return `400`.

Each record contains event and subject IDs, trigger, condition, idempotency key, status, attempts, next-attempt time, timestamps, and last error. See [delivery inspection](webhooks.md#inspecting-deliveries) for status semantics. There is no replay endpoint.

## Health and metrics

| Endpoint | Behavior |
|----------|----------|
| `GET /health` | Always `200`; body reports `status: "ok"` or `"degraded"`, database health, `pool_size`, and `pool_idle` |
| `GET /ready` | `200` with `{"status":"ready"}` when a connection can be acquired; `503` when the pool is exhausted or acquisition fails |
| `GET /metrics` | Prometheus-format metrics; requires the bearer token when configured |

Probe connection acquisition has a two-second bound. Use `/health` for liveness and `/ready` for traffic readiness. See [Metrics](metrics.md) for the catalog.
