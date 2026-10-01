# Webhooks

ArcRun calls external services to start work and report lifecycle events. Receivers should acknowledge requests promptly and make their side effects idempotent.

## Events

| Configuration | When called | Execution |
|---------------|-------------|-----------|
| `on_start` | After the scheduler claims a task | The start worker waits for the HTTP result |
| `on_success` | A task reaches Success | Durable queue with retries |
| `on_failure` | A task reaches Failure, including timeout or dependency failure | Durable queue with retries |
| Cancel action | A Claimed/Running task is canceled | Durable queue with retries |
| `on_batch_complete` | All inserted tasks in a batch are terminal | Durable queue with retries |

The required `on_start` field contains one action. Success, failure, and batch-complete fields contain arrays of actions. A cancel action is registered through the start receiver's response.

## Action format

```json
{
  "kind": "Webhook",
  "params": {
    "url": "https://worker.example.com/jobs",
    "verb": "Post",
    "headers": {"Authorization": "Bearer receiver-token"},
    "body": {"job": "import"}
  }
}
```

Supported verbs are `Get`, `Post`, `Put`, `Patch`, and `Delete`. Headers and body are optional. A successful response is any 2xx status. Each request has a 10-second timeout; redirects are rejected. Response bodies are bounded to 64 KiB. An oversized start response fails the start; an oversized successful notification response is truncated.

The configured headers authenticate ArcRun to the receiver. They do not configure authentication for the receiver's callback to ArcRun.

## Starting work and reporting completion

Task-level requests include a URL-encoded `handle` query parameter containing `${HOST_URL}/task/{task_id}`. The receiver should accept the work, respond promptly, and later report completion to this URL:

```http
PATCH /task/{task_id}
Content-Type: application/json
Authorization: Bearer <arcrun-token>

{"status":"Success"}
```

Include Authorization when ArcRun has `AUTH_TOKEN` configured. The callback URL itself contains no credential. Failure reports use `{"status":"Failure","failure_reason":"description"}`.

A successful start response means the service accepted the work; it does not complete the task. A start request that fails marks the task Failure. Start execution is tracked separately from the notification queue and does not use its retry schedule.

### Registering cancellation

The start receiver can return an action as its JSON response body:

```json
{
  "kind": "Webhook",
  "params": {
    "url": "https://worker.example.com/jobs/import-42/cancel",
    "verb": "Post"
  }
}
```

ArcRun validates and stores this action with the start result. Invalid cancel actions are logged and skipped. Cancellation can occur while the task is Claimed and the start call is still in flight, so cancel notifications cover both Claimed and Running tasks.

## Event identity

| Header | Value |
|--------|-------|
| `Idempotency-Key` | Stable event identifier, shown below |
| `X-Task-Id` | Task UUID; absent for batch-complete events |
| `X-Task-Trigger` | `start`, `end`, `cancel`, or `batch_complete` |

| Event | Idempotency key |
|-------|-----------------|
| Start | `<task_id>:start` |
| Success | `<task_id>:end:success` |
| Failure | `<task_id>:end:failure` |
| Cancel | `<task_id>:cancel` |
| Batch complete | `batch:<batch_id>:complete` |

Keys identify events, not individual actions. Several actions attached to the same event receive the same key. If a receiver handles more than one such action, combine the event key with its own action or endpoint identity when deduplicating.

Persist deduplication decisions with the receiver's side effects where possible. A network failure or an expired lease can cause a request to repeat even after the receiver performed the work.

## Notification payloads

End and cancel requests add task information under the reserved `arcrun` key:

```json
{
  "job": "import",
  "arcrun": {
    "status": "Success",
    "ended_at": "2026-10-01T10:00:00Z",
    "trigger": "end"
  }
}
```

Custom object fields are preserved, except a custom `arcrun` field is replaced. A non-object custom body is placed under `body`; an absent body produces only the `arcrun` object. Start requests use the configured body without this enrichment.

### Batch completion

Register `on_batch_complete` in the object form of `POST /task`. ArcRun enqueues one completion event when the last inserted task becomes terminal. If all tasks are skipped by deduplication, the event is enqueued immediately. An event can be delivered more than once.

Batch requests have no `handle` parameter and no `X-Task-Id`. Their body contains:

```json
{
  "arcrun": {
    "batch_id": "019a0000-0000-7000-8000-000000000001",
    "counts": {"success": 8, "failure": 1, "canceled": 1},
    "completed_at": "2026-10-01T10:00:00Z",
    "trigger": "batch_complete"
  }
}
```

Counts refer to tasks in each terminal state, not their item-progress counters. The payload is computed at delivery time. Completion time is the latest task `ended_at`, with an event-timestamp fallback for a batch with no inserted tasks.

## Durability, ordering, and retries

End, cancel, and batch-complete events are enqueued in the same transaction as their associated state changes. The API response therefore confirms durable state and queueing, not successful delivery.

Failed notification attempts retry with `min(base^attempts, cap)` seconds of delay. With the default configuration, retry delays begin at 2, 4, and 8 seconds and stop growing at 300 seconds. After 10 failed attempts the event becomes `exhausted`. There is no guarantee of successful receipt when the receiver remains unavailable.

A retry repeats the event's action sequence, so actions that succeeded before a later action failed may receive duplicates. Inspect exhausted events through `GET /webhook-deliveries?status=exhausted`; there is no HTTP endpoint for replaying them.

There is no delivery order across tasks. A child's start may arrive before its parent's success notification. For one task, end/cancel delivery waits while a fresh start execution is pending. This gate expires after `WORKER_CLAIM_TIMEOUT_SECS`, so a crashed start cannot block notifications indefinitely. Start-before-end ordering is therefore bounded by that freshness window.

## Delivery loop

The delivery worker processes a batch in four phases:

1. **Claim:** select due queue rows with `FOR UPDATE SKIP LOCKED`, assign a fresh lease token, and defer their eligibility by `WEBHOOK_DELIVERY_LEASE_SECS` in a short transaction.
2. **Prepare:** load task or batch data and actions. Events with no actions, or whose subject no longer exists, are marked successful without HTTP. Malformed batch action data is exhausted.
3. **Send:** deliver up to `WEBHOOK_DELIVERY_CONCURRENCY` events concurrently, with sequential actions within each event.
4. **Record:** move successful or exhausted events into history; update failed events with another attempt and retry time.

Every result write checks the lease token. A worker finishing after another worker reclaimed the event cannot overwrite its state. Failed result writes leave the event recoverable after lease expiry.

HTTP runs outside transactions and row locks. The current implementation retains one pool connection for the full iteration, including HTTP execution. The lease must cover time waiting within a delivery batch as well as the sequential actions for an event.

## Inspecting deliveries

`GET /webhook-deliveries` supports `page`, `page_size`, and an optional case-insensitive `status` filter:

| Status | Meaning |
|--------|---------|
| `pending` | Queued notification, leased notification, scheduled retry, or pending start execution |
| `success` | Start execution succeeded or notification finished successfully; may include events with no actions |
| `failure` | Failed start-execution record |
| `exhausted` | Notification stopped retrying or could not be prepared |

Results include subject IDs, trigger, condition, idempotency key, attempt count, next-attempt time, timestamps, and last error. They are ordered by most recent update, then ID. Notification retries remain `pending`; they do not appear under `failure`.

## SSRF protection

With validation enabled, ArcRun checks webhook URLs at creation and checks resolved IP addresses again when delivering. Private and reserved addresses are blocked, including IPv4-mapped IPv6 addresses. IP-literal URLs are checked at creation; hostname resolution is checked during HTTP delivery. Redirects are disabled.

For trusted internal services, configure `ALLOWED_HOSTNAMES` or `ALLOWED_CIDRS`:

- Hostnames support exact names and leading-dot suffixes, matched without case sensitivity. A match bypasses hostname blocklists and resolved-IP checks.
- CIDRs allow specific networks or individual IPs. Other addresses returned by the same DNS lookup must still pass validation.

Release builds enable these checks by default; debug builds skip them unless `SKIP_SSRF_VALIDATION=0`. See [Configuration](configuration.md#security) for the settings.
