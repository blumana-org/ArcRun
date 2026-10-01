# ArcRun API Overview

ArcRun orchestrates tasks executed by external HTTP services. It stores dependencies, schedules eligible work, enforces concurrency and capacity rules, and delivers lifecycle notifications.

## Execution flow

1. Create tasks with `POST /task`. Send an array of task definitions, or an object containing `tasks` and optional batch identity and completion actions.
2. Reference earlier tasks by their local IDs to declare dependencies. The response contains server-assigned task UUIDs and an `X-Batch-ID` header.
3. ArcRun claims eligible tasks and calls their required `on_start` webhook. The `handle` query parameter contains the task's completion URL.
4. The receiver accepts the request and performs the work. It reports `Success` or `Failure` through `PATCH /task/{task_id}`; failure requires a reason.
5. ArcRun commits the result, dependency propagation, and end-notification queueing in one transaction. Notification HTTP calls happen asynchronously.

## Authentication

When `AUTH_TOKEN` is configured, send `Authorization: Bearer <token>` on every request except `/health` and `/ready`. This includes callback requests, metrics, Swagger, and the DAG viewer. The callback URL contains no credential. Authentication is disabled when the token is unset or blank.

## Updates

| Endpoint | Contract |
|----------|----------|
| `PATCH /task/{task_id}` | Updates a Running/Claimed task synchronously. Can commit progress with a final Success/Failure result. Repeating the same final status returns `200` without applying changes again; conflicting states return `409`. |
| `PUT /task/{task_id}` | Queues non-negative progress increments in memory. At least one counter must be positive. `202` acknowledges queueing, not persistence or task existence. Other fields are ignored. |

Item counters do not automatically complete tasks. A task's timeout measures inactivity since its last persisted update while Running.

## States and scheduling

Tasks move through Waiting, Pending, Claimed, and Running before reaching Success, Failure, or Canceled. Pending/Waiting tasks can be paused and explicitly resumed. Paused tasks still receive dependency updates.

Rules reserve shared database slots at claim time. Concurrency limits count claims through the same rule key. Capacity limits check existing reserved work before adding a candidate, so the total can exceed the admission threshold.

## Webhook delivery

Start webhooks are synchronous within the start worker. End, cancel, and batch-complete notifications use a durable queue with retries. Duplicate HTTP delivery is possible; receivers should deduplicate using the `Idempotency-Key` header and their action identity.

Persistent delivery failures become `exhausted` after the configured retry limit. Inspect them with `GET /webhook-deliveries?status=exhausted`. A batch-complete event is enqueued once when all inserted tasks are terminal; this does not imply exactly-once HTTP delivery.

The documentation site provides a getting-started walkthrough, full API and configuration references, webhook receiver guidance, and architecture details. This overview is also embedded in the generated OpenAPI description.
