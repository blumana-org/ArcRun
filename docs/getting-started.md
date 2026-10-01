# Getting Started

ArcRun schedules tasks and calls your HTTP services to execute them. Your service accepts each start request, performs the work, and reports Success or Failure back to ArcRun. PostgreSQL stores task state and pending notifications.

This walkthrough runs a local database, ArcRun, and the example worker included in the repository.

## 1. Start PostgreSQL

You need Docker, a Rust toolchain supporting the workspace, and PostgreSQL client libraries for building Diesel. Run from the repository root:

```bash
docker run --name arcrun-postgres --rm \
  -e POSTGRES_USER=arcrun \
  -e POSTGRES_PASSWORD=arcrun \
  -e POSTGRES_DB=arcrun \
  -p 127.0.0.1:5432:5432 \
  postgres:18-alpine
```

Wait for PostgreSQL to report that it is ready to accept connections. If port 5432 is already occupied, choose another host port and update `DATABASE_URL` below.

## 2. Start ArcRun

In another terminal, build the DAG viewer once (requires Bun), then start the server:

```bash
(cd ui && bun install --frozen-lockfile && bun run build)

DATABASE_URL=postgres://arcrun:arcrun@localhost:5432/arcrun \
HOST_URL=http://localhost:8085 \
AUTH_TOKEN= \
SKIP_SSRF_VALIDATION=1 \
cargo run --bin server
```

The database must exist; ArcRun applies pending migrations automatically. This local example disables authentication and allows loopback webhook addresses. The example worker does not send a bearer token on callbacks.

Check readiness:

```bash
curl --fail http://localhost:8085/ready
```

Expected body: `{"status":"ready"}`.

## 3. Start the example worker

In another terminal at the repository root:

```bash
cargo run --bin test-server
```

The example worker listens on `127.0.0.1:9090`. It accepts a `wait_for` value, returns immediately, reports progress once per second, and then marks the task successful.

## 4. Submit a task

```bash
curl --fail-with-body -i http://localhost:8085/task \
  -H 'Content-Type: application/json' \
  --data '{
    "scope": "local-demo",
    "tasks": [{
      "id": "demo",
      "name": "Local demo",
      "kind": "example",
      "timeout": 60,
      "on_start": {
        "kind": "Webhook",
        "params": {
          "url": "http://127.0.0.1:9090/task",
          "verb": "Post",
          "body": {"wait_for": 1}
        }
      }
    }]
  }'
```

The response is `201 Created` with the created task and an `X-Batch-ID` header. ArcRun calls the example worker with a callback URL in the `handle` query parameter. The worker uses that URL to report progress and completion.

After about 15 seconds, list the example tasks:

```bash
curl --fail 'http://localhost:8085/task?kind=example'
```

The task should be Success. Open `http://localhost:8085/view?batch=<batch-id>` using the UUID from `X-Batch-ID` to inspect the graph. Swagger is available at `http://localhost:8085/swagger-ui/`.

## Running the Docker image

For deployment against an existing database:

```bash
docker run --rm \
  -e DATABASE_URL=postgres://user:password@db-host/arcrun \
  -e HOST_URL=https://arcrun.example.com \
  -e AUTH_TOKEN=replace-with-your-token \
  -p 8085:8085 \
  plawn/arcrun:latest
```

Replace the database address, callback address, and token with your deployment values. The database must be reachable from the container. `HOST_URL` must be reachable from webhook receivers; `localhost` inside a container refers to that container.

If receivers run on a private network, allow their hostnames or CIDRs in the [security configuration](configuration.md#security). Receivers must include the configured bearer token when calling ArcRun.

## Next steps

- [Core concepts](concepts.md): task states, dependencies, rules, and deduplication.
- [API reference](api.md): request fields, endpoints, and response behavior.
- [Webhooks](webhooks.md): receiver implementation, payloads, cancellation, and retries.
- [Configuration](configuration.md): environment variables and deployment settings.
- [Metrics](metrics.md): operational signals.
