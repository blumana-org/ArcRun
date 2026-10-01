# Core Concepts

## Tasks and batches

A task describes work performed by an external service. It contains a required `on_start` webhook, optional dependencies, metadata, and scheduling rules. ArcRun schedules the work; the receiving service executes it and reports the result.

Every `POST /task` request creates a batch ID. The local `id` supplied on each task is used only to reference dependencies in that request. Responses contain server-generated task UUIDs and the `X-Batch-ID` header.

The object form of task creation can attach a batch `scope`, `metadata`, and `on_batch_complete` actions. Batch metadata is separate from task metadata: it supports batch discovery and is not inherited by tasks.

## Task lifecycle

| State | Meaning | How it changes |
|-------|---------|----------------|
| Waiting | Dependencies remain outstanding | Becomes Pending when satisfied, or Failure when a required parent fails |
| Pending | Eligible for scheduling | Becomes Claimed when rules allow admission |
| Claimed | Reserved; start request may be waiting or in flight | Becomes Running after a successful start; stale claims return to Pending |
| Running | External work was accepted | Receiver reports Success/Failure, or inactivity causes Failure |
| Paused | Scheduling explicitly suspended | Resume returns to Waiting or Pending |
| Success | Work completed successfully | Terminal |
| Failure | Work failed, timed out, or could not satisfy a required dependency | Terminal |
| Canceled | Execution was canceled | Terminal |

Only Pending and Waiting tasks can be paused. Paused tasks still receive dependency updates and can fail when a required parent fails. Any non-terminal task can be canceled.

Completion reports are accepted while Running or Claimed, allowing a fast receiver to finish before the start request returns. Repeating the same terminal result is an idempotent no-op. There is no automatic retry of a failed task; webhook notification retries do not rerun the task.

Dead-end cancellation is enabled by default. When a non-terminal ancestor has no non-terminal children left, it can be canceled and the check continues upward. A task with `dead_end_barrier: true` can itself be canceled but stops that upward traversal.

## Dependencies

Declare parents before their children and reference their local IDs:

```json
[
  {
    "id": "build",
    "name": "Build",
    "kind": "ci",
    "on_start": {
      "kind": "Webhook",
      "params": {"url": "https://worker.example.com/build", "verb": "Post"}
    }
  },
  {
    "id": "deploy",
    "name": "Deploy",
    "kind": "ci",
    "dependencies": [{"id": "build", "requires_success": true}],
    "on_start": {
      "kind": "Webhook",
      "params": {"url": "https://worker.example.com/deploy", "verb": "Post"}
    }
  }
]
```

`requires_success: true` means the parent must succeed. Failure or cancellation of that parent fails the child and propagates through further required-success dependencies.

`requires_success: false` means the parent only needs to reach a terminal state: Success, Failure, or Canceled. This is useful for cleanup or reporting tasks that should run regardless of an earlier result.

A child starts only when all its dependencies are satisfied. Dependencies are limited to earlier tasks in the same request; task UUIDs from another batch cannot be used here.

## Progress and timeouts

`success` and `failures` count processed items. `expected_count` describes the expected item total. These counters are separate from task status: reaching the expected count does not complete a task. The receiver must report its final status with PATCH.

Task timeout measures inactivity while Running, based on `last_updated`. It defaults to 60 seconds. Persisted progress updates refresh that timestamp. Work that runs for a long time should report progress often enough to stay within its timeout.

PUT counter updates are accumulated in memory and flushed asynchronously. They are useful for throughput, but `202` does not mean persistence. Use PATCH when a counter increment must commit with the completion status. See [progress updates](api.md#update-progress-counters) for the limits of asynchronous updates.

## Concurrency rules

Add this fragment to a task to limit simultaneous claims for one tenant:

```json
{
  "metadata": {"tenant_id": "acme"},
  "rules": [{
    "type": "Concurency",
    "matcher": {"kind": "import", "status": "Running", "fields": ["tenant_id"]},
    "max_concurency": 3
  }]
}
```

`Concurency` and `max_concurency` are the exact API spellings. Use the same rule on tasks that should share a limit. The reservation key includes matcher kind, status, and the selected metadata values; an empty `fields` array shares a limit across that kind rather than separating tenants.

Reservations are acquired at Claimed and held while Running. Only tasks that claimed through the same rule key contribute. Every field named in the matcher must exist in the task metadata; otherwise admission is blocked. Matching tasks without that rule do not consume the reservation. Rules are shared across batches, so include a batch-specific value in task metadata if a limit should be isolated to one batch.

All rules on a task must allow admission. Batch rule updates apply only to Waiting, Pending, and Paused tasks; existing claims keep their reservations.

## Capacity rules

A capacity rule admits work based on the remaining work already reserved under the same rule key:

```json
{
  "metadata": {"tenant_id": "acme"},
  "expected_count": 200,
  "rules": [{
    "type": "Capacity",
    "matcher": {"kind": "import", "status": "Running", "fields": ["tenant_id"]},
    "max_capacity": 500
  }]
}
```

A task using Capacity must provide `expected_count`, and the matcher status must be Running. Its initial charge is `max(expected_count - success - failures, 0)`.

Admission checks whether the existing shared charge is below `max_capacity` before adding the candidate. For example, a charge of 400 allows a new task with 200 remaining items, raising the total to 600. Capacity is an admission threshold, not a hard upper bound on the total after admission.

Progress persisted by the PUT counter updater reduces the reservation. Direct PATCH progress updates do not reduce it mid-run. Charges are released when tasks leave Claimed/Running.

## Deduplication

Add this fragment to skip creation when an existing task matches:

```json
{
  "metadata": {"project_id": "project-42"},
  "dedupe_strategy": [{
    "kind": "import",
    "status": "Pending",
    "fields": ["project_id"]
  }]
}
```

A match uses the specified kind, status, and selected metadata values. If any matcher matches, that task is skipped. The matcher sees tasks inserted earlier in the same request as well as existing tasks. Deduplication is a creation-time check, distinct from scheduling limits and webhook event deduplication.

A request with some inserted tasks returns `201`; a fully skipped request returns `204`. Both include `X-Batch-ID`.

## Priority

`priority` ranges from -1000 to 1000 and defaults to zero. Higher-priority Pending tasks are considered first. Ties use creation time and then UUID for stable ordering.

Priority does not bypass dependencies or rules and does not preempt Running tasks. A blocked high-priority task does not prevent eligible lower-priority work from being considered.
