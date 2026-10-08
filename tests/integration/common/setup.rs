use arcrun::DbPool;
use diesel::{Connection, PgConnection, RunQueryDsl};
use diesel_async::AsyncPgConnection;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::pooled_connection::bb8::Pool;
use diesel_migrations::{EmbeddedMigrations, MigrationHarness, embed_migrations};
use std::sync::{
    LazyLock, Mutex,
    atomic::{AtomicU64, Ordering},
};
use testcontainers::{ImageExt, runners::AsyncRunner};
use testcontainers_modules::postgres::Postgres;

/// Container ID for atexit cleanup (statics are never dropped in Rust).
static CONTAINER_ID: Mutex<Option<String>> = Mutex::new(None);

extern "C" fn cleanup_container() {
    if let Ok(guard) = CONTAINER_ID.lock()
        && let Some(id) = guard.as_ref()
    {
        let _ = std::process::Command::new("docker")
            .args(["rm", "-f", id])
            .output();
    }
}

pub const MIGRATIONS: EmbeddedMigrations = embed_migrations!("migrations");

/// Counter for unique database names.
static DB_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Shared PostgreSQL container + base URL.
/// Initialized once for the entire test binary.
/// A template database with all migrations applied is created during init.
///
/// Init runs on a dedicated std::thread to avoid "runtime within runtime" errors
/// when called from within a #[tokio::test] context.
static SHARED_PG: LazyLock<SharedPg> = LazyLock::new(|| {
    std::thread::spawn(|| {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("Failed to build runtime for shared PG setup");

        rt.block_on(async {
            let container = Postgres::default()
                .with_tag("18-alpine")
                .start()
                .await
                .expect("Failed to start shared PostgreSQL container");

            // Register atexit cleanup: Rust never drops statics, so without
            // this the container leaks after every test run.
            let cid = container.id().to_string();
            *CONTAINER_ID.lock().unwrap() = Some(cid);
            unsafe { libc::atexit(cleanup_container) };

            let host_port = container.get_host_port_ipv4(5432).await.unwrap();
            let base_url = format!(
                "postgres://postgres:postgres@127.0.0.1:{}/postgres",
                host_port
            );

            // Create a dedicated template database and run migrations on it.
            // This avoids running migrations per-test.
            {
                let mut conn = PgConnection::establish(&base_url)
                    .expect("Failed to connect to postgres for template setup");
                diesel::sql_query("CREATE DATABASE test_template")
                    .execute(&mut conn)
                    .expect("Failed to create test_template database");
            }

            let template_url = replace_db_name(&base_url, "test_template");
            run_migrations(&template_url);

            SharedPg {
                _container: container,
                base_url,
            }
        })
    })
    .join()
    .expect("Shared PG init thread panicked")
});

struct SharedPg {
    _container: testcontainers::ContainerAsync<Postgres>,
    base_url: String,
}

/// Replace only the database name (last path segment) in a PostgreSQL URL.
fn replace_db_name(url: &str, new_db: &str) -> String {
    match url.rfind('/') {
        Some(pos) => format!("{}/{}", &url[..pos], new_db),
        None => url.to_string(),
    }
}

// SAFETY: SharedPg is only mutated during LazyLock init (single-threaded).
// After init, only `base_url` (String, which is Sync) is read.
// `_container` is held solely to prevent drop; it is never accessed after init.
unsafe impl Sync for SharedPg {}

/// Test application state.
/// No longer owns the container — the shared LazyLock keeps it alive.
pub struct TestApp {
    pub pool: DbPool,
    /// Connection URL of this test's isolated database. Useful for opening an
    /// independent observer connection/pool (e.g. `test_bug_audit3` needs to
    /// confirm a write is durably committed from *outside* the pool under test).
    pub url: String,
}

/// Setup a test database and return the pool.
///
/// Each test gets its own database created instantly via
/// `CREATE DATABASE ... TEMPLATE test_template`, skipping per-test migrations.
pub async fn setup_test_db() -> TestApp {
    setup_test_db_with_pool_size(5).await
}

/// Like [`setup_test_db`] but with a caller-chosen pool `max_size`.
///
/// A pool of size 1 is useful for cancel-safety / connection-reuse tests
/// (e.g. `test_bug_audit3`): the single physical connection is guaranteed to be
/// handed back to the next borrower, so a leaked open transaction (if any)
/// would deterministically corrupt the following statements.
pub async fn setup_test_db_with_pool_size(max_size: u32) -> TestApp {
    let shared = &*SHARED_PG;

    // Generate a unique database name
    let seq = DB_COUNTER.fetch_add(1, Ordering::Relaxed);
    let db_name = format!("test_{}", seq);

    // Create the test database from the pre-migrated template (near-instant)
    {
        let mut conn = PgConnection::establish(&shared.base_url)
            .expect("Failed to connect to postgres for DB creation");
        diesel::sql_query(format!(
            "CREATE DATABASE {} TEMPLATE test_template",
            db_name
        ))
        .execute(&mut conn)
        .unwrap_or_else(|e| panic!("Failed to create database {}: {}", db_name, e));
    }

    let test_url = replace_db_name(&shared.base_url, &db_name);

    // Create async pool for this test's isolated database
    let config = AsyncDieselConnectionManager::<AsyncPgConnection>::new(&test_url);
    let pool = Pool::builder()
        .max_size(max_size)
        .build(config)
        .await
        .expect("Failed to create pool");

    TestApp {
        pool,
        url: test_url,
    }
}

/// Convert ALTER TYPE ... ADD VALUE to use IF NOT EXISTS for idempotency
fn make_alter_type_idempotent(statement: &str) -> String {
    let upper = statement.to_uppercase();
    if upper.contains("IF NOT EXISTS") {
        statement.to_string()
    } else {
        let add_value_pos = upper.find("ADD VALUE").unwrap_or(0);
        if add_value_pos > 0 {
            let before = &statement[..add_value_pos + 9]; // "ADD VALUE"
            let after = &statement[add_value_pos + 9..];
            format!("{} IF NOT EXISTS{}", before, after)
        } else {
            statement.to_string()
        }
    }
}

/// Run migrations, handling ALTER TYPE ... ADD VALUE specially.
///
/// PostgreSQL's ALTER TYPE ... ADD VALUE cannot run inside a transaction block.
/// This function detects such migrations by reading their SQL content and runs
/// them outside of transactions.
fn run_migrations(database_url: &str) {
    use std::fs;
    use std::path::Path;

    let mut conn = {
        let max_retries: u64 = 10;
        let mut result = PgConnection::establish(database_url);
        for attempt in 1..max_retries {
            if result.is_ok() {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(200 * attempt));
            result = PgConnection::establish(database_url);
        }
        result.expect("Failed to connect for migrations after retries")
    };

    let pending = conn
        .pending_migrations(MIGRATIONS)
        .expect("Failed to get pending migrations");

    let migrations_to_run: Vec<_> = pending
        .iter()
        .map(|m| (m.name().to_string(), m.name().version().to_string()))
        .collect();

    let migrations_dir = Path::new("migrations");
    let alter_type_migrations: std::collections::HashMap<String, Vec<String>> =
        if migrations_dir.exists() {
            fs::read_dir(migrations_dir)
                .ok()
                .map(|entries| {
                    entries
                        .flatten()
                        .filter_map(|entry| {
                            let dir_name = entry.file_name().to_string_lossy().to_string();
                            let up_sql_path = entry.path().join("up.sql");

                            if up_sql_path.exists() {
                                let content = fs::read_to_string(&up_sql_path).ok()?;

                                let raw_statements: Vec<String> = content
                                    .split(';')
                                    .map(|s| s.trim())
                                    .filter(|s| !s.is_empty())
                                    .map(|s| s.to_string())
                                    .collect();

                                let mut has_alter_type = false;
                                let mut statements = Vec::with_capacity(raw_statements.len());
                                for stmt in raw_statements {
                                    let upper = stmt.to_uppercase();
                                    if upper.contains("ALTER TYPE") && upper.contains("ADD VALUE") {
                                        has_alter_type = true;
                                        statements.push(make_alter_type_idempotent(&stmt));
                                    } else {
                                        statements.push(stmt);
                                    }
                                }

                                if has_alter_type {
                                    return Some((dir_name, statements));
                                }
                            }
                            None
                        })
                        .collect()
                })
                .unwrap_or_default()
        } else {
            std::collections::HashMap::new()
        };

    for (migration_name, version) in &migrations_to_run {
        let alter_statements: Option<&Vec<String>> = alter_type_migrations
            .iter()
            .find(|(dir_name, _)| migration_name.contains(dir_name.as_str()))
            .map(|(_, stmts)| stmts);

        if let Some(statements) = alter_statements {
            drop(conn);
            conn = PgConnection::establish(database_url)
                .expect("Failed to reconnect for ALTER TYPE migration");

            for stmt in statements {
                diesel::sql_query(stmt.clone())
                    .execute(&mut conn)
                    .unwrap_or_else(|e| {
                        panic!("Failed to run migration statement: {}\n{}", e, stmt);
                    });
            }

            diesel::sql_query(format!(
                "INSERT INTO __diesel_schema_migrations (version, run_on) VALUES ('{}', NOW()) ON CONFLICT DO NOTHING",
                version
            ))
            .execute(&mut conn)
            .expect("Failed to record migration");
        } else {
            let pending_now = conn
                .pending_migrations(MIGRATIONS)
                .expect("Failed to get pending migrations");

            for migration in pending_now {
                if migration.name().to_string() == *migration_name {
                    let _ = diesel::sql_query("COMMIT").execute(&mut conn);
                    conn.run_migration(&migration).unwrap_or_else(|error| {
                        panic!("Failed to run migration: {}: {:?}", migration_name, error)
                    });
                    break;
                }
            }
        }
    }
}
