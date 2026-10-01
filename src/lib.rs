pub mod action;
pub mod auth;
pub mod circuit_breaker;
pub mod config;
pub mod db;
pub use db as db_operation;
pub mod dtos;
pub mod error;
pub mod handlers;
pub mod metrics;
pub mod models;
pub mod notification;
pub mod rule;
pub(crate) mod schema;
pub mod tracing;
pub mod validation;
pub mod workers;

use diesel::{ConnectionError, ConnectionResult};
use diesel_async::AsyncPgConnection;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::pooled_connection::ManagerConfig;
use diesel_async::pooled_connection::bb8::PooledConnection;
use diesel_async::pooled_connection::bb8::{self, Pool};
use futures_util::FutureExt;
use futures_util::future::BoxFuture;
use rustls::ClientConfig;
use rustls_platform_verifier::ConfigVerifierExt;

pub type DbPool = bb8::Pool<AsyncPgConnection>;

pub type Conn<'a> = PooledConnection<'a, AsyncPgConnection>;

/// Initialize database connection pool based on `DATABASE_URL` environment variable.
///
/// See more: <https://docs.rs/diesel/latest/diesel/r2d2/index.html>.
pub async fn initialize_db_pool(pool_config: &config::PoolConfig) -> DbPool {
    let db_url = std::env::var("DATABASE_URL").expect("Env var `DATABASE_URL` not set");
    let mut config = ManagerConfig::default();
    config.custom_setup = Box::new(establish_connection);
    let mgr = AsyncDieselConnectionManager::<AsyncPgConnection>::new_with_config(db_url, config);

    Pool::builder()
        .max_size(pool_config.max_size)
        .min_idle(Some(pool_config.min_idle))
        .max_lifetime(Some(pool_config.max_lifetime))
        .idle_timeout(Some(pool_config.idle_timeout))
        .connection_timeout(pool_config.connection_timeout)
        .build(mgr)
        .await
        .expect("failed to get pool")
}

/// Establish a **dedicated** (non-pooled) async connection to `database_url`.
///
/// Used by the start_loop leader-lease (Audit 2, D7): a session-scoped
/// `pg_try_advisory_lock` must live on a connection the loop owns for its whole
/// lifetime — a pooled connection would leak the lock when returned to the pool.
pub async fn establish_direct_connection(
    database_url: &str,
) -> ConnectionResult<AsyncPgConnection> {
    // `establish_connection` builds a rustls TLS config, which needs a process-level
    // CryptoProvider. `main` installs `ring` at startup, but a direct caller (e.g. an
    // integration test) may not have — install it idempotently (the second install
    // returns Err, which we ignore).
    let _ = rustls::crypto::ring::default_provider().install_default();
    establish_connection(database_url).await
}

fn establish_connection(config: &'_ str) -> BoxFuture<'_, ConnectionResult<AsyncPgConnection>> {
    let fut = async {
        // We first set up the way we want rustls to work.
        let rustls_config = ClientConfig::with_platform_verifier()
            .map_err(|e| ConnectionError::BadConnection(e.to_string()))?;
        let tls = tokio_postgres_rustls::MakeRustlsConnect::new(rustls_config);
        let (client, conn) = tokio_postgres::connect(config, tls)
            .await
            .map_err(|e| ConnectionError::BadConnection(e.to_string()))?;

        AsyncPgConnection::try_from_client_and_connection(client, conn).await
    };
    fut.boxed()
}
