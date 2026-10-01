# Integration Tests

The integration suite exercises HTTP handlers, PostgreSQL transactions, task lifecycle and dependency propagation, rule reservations, webhook delivery, retention, validation, and regression cases. The Cargo target is `integration`, rooted at `tests/integration/main.rs`.

## Prerequisites

- A Rust toolchain supporting edition 2024 and the workspace dependencies.
- A running Docker daemon, reachable by testcontainers and the Docker CLI.
- The `postgres:18-alpine` image (pulled automatically on first use).

Check Docker with `docker info`.

## Commands

Run these from the repository root:

```bash
# Server unit and integration tests
cargo test

# Include the Rust SDK workspace member
cargo test --workspace

# Integration suite only
cargo test --test integration

# One module or test, with captured output displayed
cargo test --test integration test_dag -- --nocapture
cargo test --test integration test_create_single_task

# Run serially for debugging or lower database load
cargo test --test integration -- --test-threads=1

# Inspect available test names
cargo test --test integration -- --list
```

## Database Isolation

`common/setup.rs` starts one shared PostgreSQL 18 Alpine container per integration test binary. Initialization runs migrations once in `test_template`. Each test gets its own database cloned with `CREATE DATABASE ... TEMPLATE test_template` and its own async connection pool (five connections by default).

Tests therefore share a container, but not task data. The container remains alive for the test process; an `atexit` callback removes it via `docker rm -f`. Databases are discarded with that container.

The migration helper handles migrations containing `ALTER TYPE ... ADD VALUE` outside the normal migration transaction, adds `IF NOT EXISTS` where needed, and records the migration version. Other migrations use Diesel's migration runner.

## Helpers and Worker Execution

| Component | Purpose |
|-----------|---------|
| `setup_test_db()` | Isolated database and pool |
| `setup_test_db_with_pool_size(n)` | Isolated database with a chosen pool limit |
| `TestApp` | Holds the test pool and connection URL |
| `setup_test_app()` | Database guard and default handler state |
| `create_test_state(pool)` | Handler state without a running batch updater |
| `test_service!(state)` | Actix service using the shared production routes |
| `task_json(id, name, kind)` | Valid task JSON including a start action |
| `create_tasks_ok(&app, &tasks)` | Submit tasks and decode a successful response |
| `common/mock_server.rs` | Mock HTTP endpoints for webhook scenarios |

Creating the Actix test service does not start the production worker loops or install all middleware from `main.rs`. Tests that need counter flushing, scheduling, delivery, or authentication must set those up explicitly. Existing modules show worker helpers such as `run_delivery_once` and `run_counter_flush_once`, which allow deterministic checks without waiting for polling ticks.

## Adding a Test

Add a module under `tests/integration/` and register it in `tests/integration/main.rs`. For example:

```rust
use crate::common::*;

#[tokio::test]
async fn test_my_feature() {
    let (_guard, state) = setup_test_app().await;
    let app = test_service!(state);
    let tasks = vec![task_json("my-task", "My Task", "my-kind")];

    let created = create_tasks_ok(&app, &tasks).await;

    assert_eq!(created.len(), 1);
    assert_eq!(created[0].name, "My Task");
}
```

Use lifecycle and outbox helpers when testing persisted state, and mock webhook receivers when the contract under test includes HTTP delivery. Keep the database guard alive for the duration of the test.

## Troubleshooting

- **Container startup failure:** check `docker info`, image availability, and Docker resources. `docker pull postgres:18-alpine` can isolate image download failures.
- **Database connection failures:** check Docker port access and resource limits; reduce parallelism with `--test-threads=1`.
- **Slow first run:** compilation, the first image pull, and template migrations happen before tests can use the shared container. Later tests clone the template instead of starting another container.

## Related Files

- `tests/integration/main.rs`: registered test modules.
- `tests/integration/common/`: setup, state, builders, assertions, and mock servers.
- `test/test.ts`: manual testing against a running service using Bun.
