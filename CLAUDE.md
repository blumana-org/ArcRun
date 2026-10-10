# Claude Code Context for ArcRun

## Important Rules

- **Never read or write `static/dag.html`** — it is a build artifact generated from the SolidJS UI in `ui/`. Always edit source files in `ui/src/` instead.

## Project Overview

ArcRun is a Rust-based task orchestration service: DAG dependencies between tasks, DB-enforced concurrency/capacity rules, webhook-based actions with a transactional outbox, PostgreSQL persistence (Diesel), circuit breaker, OpenTelemetry tracing, SSRF protection.

## Documentation Map

Read the relevant doc **before** modifying a subsystem — they carry the audit-hardened invariants and design decisions:

- [`docs/architecture.md`](docs/architecture.md) — Task states (incl. Paused semantics), action model, `POST /task` body shapes & grouped insertion, dependency propagation (frontier BFS cascade), code map (handlers, models, DTOs, key functions), **full DB schema + indexes**
- [`docs/workers.md`](docs/workers.md) — The 5 worker loops (start/timeout/batch_updater/retention/delivery), in-process nudges, **rule_slot Concurrency & Capacity enforcement (D1/7.3)**, batch updater architecture
- [`docs/webhooks.md`](docs/webhooks.md) — Outbox delivery contract (at-least-once, ordering, lease), delivery loop phases, **batch-complete detection (`batch.remaining`, D2)**, SSRF protection
- [`docs/configuration.md`](docs/configuration.md) — All environment variables
- [`docs/concepts.md`](docs/concepts.md) — User-facing concepts (dependencies, rules, dedupe, priority)
- [`docs/api.md`](docs/api.md) — HTTP API reference
- [`docs/metrics.md`](docs/metrics.md) — Prometheus metrics (defined in `src/metrics.rs`)

## Project Structure

```
src/
+-- main.rs            # HTTP server, migration, worker spawning
+-- test_server.rs     # Test server binary (integration tests)
+-- handlers.rs        # HTTP handlers + configure_routes (centralized)
+-- models.rs          # DB models (Task, Action, Link, enums)
+-- dtos.rs            # API DTOs
+-- schema.rs          # Diesel schema (auto-generated)
+-- db_operation.rs    # DB operations (facade over src/db/)
+-- db/                # task_crud, task_lifecycle, task_query, propagation, webhook_execution, cleanup
+-- workers.rs         # Worker facade + batch updater
+-- workers/           # start_loop, timeout_loop, delivery_loop, batch_updater, propagation
+-- action.rs          # Webhook execution (ActionExecutor, SSRF resolver)
+-- rule.rs            # Concurrency/Capacity rules, matchers, slot keys
+-- validation.rs      # Input validation + creation-time SSRF
+-- auth.rs            # Optional bearer-token middleware (AUTH_TOKEN)
+-- config.rs / metrics.rs / error.rs / circuit_breaker.rs / tracing.rs / helper.rs
migrations/            # Diesel migrations
ui/                    # SolidJS DAG UI (source of static/dag.html)
test/test.ts           # Manual API testing script (Bun)
tests/integration/     # Integration tests (testcontainers PostgreSQL)
```

## Critical Invariants & Gotchas

1. **Failure propagation cascades**: a child failed because of its parent propagates to its own children (frontier BFS, `cascade_failure_frontier`).
2. **Canceled == Failed for propagation**: children with `requires_success=true` cascade-fail either way.
3. **wait_finished vs wait_success**: `wait_finished` counts ALL dependencies; `wait_success` only the `requires_success=true` ones. Child becomes `Pending` when both reach 0 (from `Waiting` only — a `Paused` child stays `Paused` until explicit resume).
4. **Timeout uses `last_updated`, NOT `started_at`**: counter updates via `PUT /task/{id}` reset the timeout clock. The timeout transition intentionally does NOT bump `last_updated`.
5. **Dependencies are intra-batch only**: `Dependency.id` references local IDs within the same `POST /task` batch — no cross-batch links, which is why `stop_batch` needs no per-task propagation.
6. **PATCH `metadata` is a full replace, not a merge** — omitted keys are dropped (including keys used by dedupe/rule matchers).
7. **Terminal counters are frozen**: a batch-updater flush landing on a terminal task is dropped, never re-queued (A7).
8. **Slot release never recomputes**: `claimed_slot_keys`/`capacity_charge` are read back as stored (metadata/expected_count are mutable while Running); released in the same tx as EVERY exit from Claimed/Running.
9. **A9 lock ordering**: task rows before batch/slot rows; blocking acquisitions of known task sets are sorted. Slot keys use Rust byte order and SQL `COLLATE "C"`. Claim folds its task lock into the first slot upsert. Propagation orders each frontier, not the entire DAG, so residual task-to-task deadlocks remain possible.
10. **Routes are centralized** in `handlers::configure_routes`, shared by main server and test server.
11. **Actions are per-task** (own records, never shared).

## Testing

- Unit tests live in source-file test modules; integration tests in `tests/integration/` (declared in `tests/integration/main.rs`), using testcontainers for PostgreSQL.
- Integration tests default to PostgreSQL 18 in Docker. `ARCRUN_TEST_PG_URL` can point to a dedicated disposable PostgreSQL test server with `CREATE DATABASE` permission. Each test run creates a unique template/database namespace; the caller tears down an external test server after use.
- Shared helpers in `tests/integration/common/`: `setup.rs` (`TestApp`, `setup_test_db`, `test_service!`), `state.rs` (test config/state), `builders.rs` (task JSON builders), `assertions.rs` (`setup_test_app`, `create_tasks_ok`, `succeed_task`, `fail_task`, `assert_task_status`, …).
- Test files are one-per-feature (`test_crud.rs`, `test_propagation.rs`, `test_outbox.rs`, `test_batch_complete.rs`, …) plus bug regressions in `test_bug_audit1.rs` / `test_bug_audit2.rs`.
- Manual testing: `bun test/test.ts dag|single|list|update <id> <status>|view <batch_id>`.

### Fixing a Bug

Always add an integration test in the appropriate `tests/integration/test_bug_audit*.rs` file that reproduces the bug and verifies the fix:
1. Name it `test_bug<N>_<short_description>`
2. Doc comment explaining the original bug, the fix, and what the test asserts
3. Assert the **correct** behavior (fails if the fix is reverted)
4. Use the shared helpers from `tests/integration/common/`

## Common Tasks

- **New endpoint**: handler in `src/handlers.rs` → register in `configure_routes` → DTOs in `src/dtos.rs` → DB ops in `src/db_operation.rs`.
- **New task status**: `StatusKind` in `src/models.rs` → migration for the enum value → update `propagate_to_children` and `cancel_task` if affected.
- **Propagation changes**: `propagate_to_children` (`src/workers.rs`) and `cascade_failure_frontier` (`src/workers/propagation.rs`) — read the propagation section of `docs/architecture.md` first.
- **Rule/slot changes**: read the rules section of `docs/workers.md` first (claim/release symmetry is load-bearing).
