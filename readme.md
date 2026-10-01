<p align="center">
  <img src="static/icon.png" alt="ArcRun" width="128" />
</p>

<h1 align="center">ArcRun</h1>

<p align="center">A Rust service for orchestrating tasks through HTTP webhooks, with DAG dependencies and shared concurrency controls.</p>

ArcRun decides when work can start; your services execute it and report the result. PostgreSQL stores tasks, dependency state, scheduling reservations, and pending lifecycle notifications.

## Capabilities

- Task dependencies with success requirements and failure propagation.
- Shared concurrency limits, capacity admission rules, and priority scheduling.
- Batch discovery, progress statistics, cancellation, and completion notifications.
- Durable end/cancel notifications with retries and event idempotency keys.
- Task metadata matching and creation-time deduplication.
- Optional bearer authentication and webhook SSRF protection.
- Prometheus metrics, OpenTelemetry tracing, health probes, and a DAG viewer.
- Optional task archiving with lookup by ID and configurable archive deletion.

## Get started

The [local walkthrough](docs/getting-started.md) runs PostgreSQL, ArcRun, and the example worker, then submits a task through to completion.

To run the server against an existing database:

```bash
DATABASE_URL=postgres://user:password@localhost/arcrun \
HOST_URL=http://localhost:8085 \
cargo run --bin server
```

ArcRun applies pending migrations automatically. `HOST_URL` must be reachable by webhook receivers because it forms their completion callback URL.

A Docker image is also available:

```bash
docker run --rm \
  -e DATABASE_URL=postgres://user:password@db-host/arcrun \
  -e HOST_URL=https://arcrun.example.com \
  -e AUTH_TOKEN=replace-with-your-token \
  -p 8085:8085 \
  plawn/arcrun:latest
```

Replace the database address, public callback address, and token with your deployment values. See [Configuration](docs/configuration.md) for internal receivers, authentication, and worker settings.

## Execution model

1. Submit tasks with `POST /task`, declaring dependencies through local IDs.
2. ArcRun claims eligible tasks and calls their `on_start` webhook.
3. The receiver accepts the work and reports Success or Failure to the URL in `handle`.
4. ArcRun commits the result and propagates it through the graph. End/cancel and batch-complete notifications are delivered asynchronously.

Notification delivery can repeat. Receivers should deduplicate events using their idempotency keys. Notification retries do not retry failed tasks.

Progress increments sent through PUT are buffered in memory before persistence. Use PATCH when progress must commit with a final task result. See the [API contract](docs/api.md).

## Documentation

| Guide | Covers |
|-------|--------|
| [Getting started](docs/getting-started.md) | Local end-to-end walkthrough and Docker deployment |
| [Core concepts](docs/concepts.md) | Lifecycle, dependencies, rules, progress, and deduplication |
| [API reference](docs/api.md) | Endpoints, fields, filters, and response behavior |
| [Webhooks](docs/webhooks.md) | Receiver implementation, payloads, cancellation, retries, and ordering |
| [Configuration](docs/configuration.md) | Environment variables and operational settings |
| [Metrics](docs/metrics.md) | Prometheus catalog and monitoring signals |
| [Architecture](docs/architecture.md) | Transactions, persistence, replicas, and code map |
| [Workers](docs/workers.md) | Scheduling and background processing |
| [Integration tests](tests/README.md) | Test commands, isolation, and helpers |

The running server exposes Swagger at `/swagger-ui/`, its schema at `/api-docs/openapi.json`, and the graph viewer at `/view?batch=<batch-id>`. With `AUTH_TOKEN` set, every endpoint except `/health` and `/ready` requires the bearer header, including worker callbacks.

## Development

The workspace contains the server and Rust client SDK in `sdk/`. Integration tests use one shared PostgreSQL container with isolated databases per test; Docker must be running.

```bash
cargo test --workspace
cargo test --test integration
cargo fmt --all -- --check
```

Build the embedded DAG viewer before compiling the server with `(cd ui && bun install --frozen-lockfile && bun run build)`. Without this build, a fresh checkout serves a placeholder at `/view`.

The manual example worker runs with `cargo run --bin test-server`. Additional Bun scripts live in `test/`.

The documentation site reads `docs/` directly:

```bash
cd website
bun install --frozen-lockfile
bun run typecheck
bun run build
```

Public guides describe the current implementation. Planning notes, audits, and handoff documents under `docs/` are internal records and are excluded from the published site.

## Releases

Pushing a version tag such as `v1.2.1` triggers CI builds for Linux amd64 and arm64. Images are published to `plawn/arcrun` with full version, major/minor, major (except v0), and commit-SHA tags. Main/master branch builds update `latest`.

Choose an unused version and keep the release tag consistent with the package version before pushing it.
