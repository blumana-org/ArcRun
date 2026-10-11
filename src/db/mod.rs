mod batch_listing;
mod cleanup;
mod task_crud;
mod task_lifecycle;
mod task_locking;
mod task_query;
mod webhook_execution;
mod webhook_outbox;

use crate::Conn;
use diesel_async::AsyncConnection;
use std::future::Future;
use std::pin::Pin;

pub(crate) type DbError = crate::error::ArcRunError;
pub(crate) use task_locking::{Transition, prepare_transition};

// Re-exports from task_crud
pub use crate::rule::concurrency_lock_key;
pub(crate) use crate::rule::{capacity_slot_key, concurrency_slot_key};
pub use task_crud::{
    ClaimResult, batch_claim_tasks, claim_task, claim_task_with_rules, mark_task_running,
    release_slots_for_tasks,
};
pub(crate) use task_crud::{
    claim_task_with_rules_detailed, find_detailed_task_by_id, insert_task_batch,
};

// Re-exports from task_lifecycle
pub use task_lifecycle::{UpdateTaskResult, update_running_task};
pub(crate) use task_lifecycle::{
    fail_task_and_propagate, pause_task, resume_task, save_cancel_actions, stop_batch,
};

// Re-exports from task_query
pub(crate) use task_query::{
    PendingCursor, count_running_tasks_by_kind, count_tasks_by_status, find_timed_out_tasks,
    get_dag_for_batch, list_pending_page, list_task_filtered_paged, requeue_stale_claimed_tasks,
};
// Exposed so integration tests can exercise one real timeout attempt.
pub use task_query::timeout_task_and_propagate;

// Re-exports from batch_listing
pub(crate) use batch_listing::{get_batch_stats, list_batches, update_batch_rules};

// Re-exports from cleanup
// `pub` (not pub(crate)) so integration tests can drive one cleanup pass
// deterministically, like `run_delivery_once` / `run_claim_loop`.
pub use cleanup::{cleanup_old_terminal_tasks, gc_empty_rule_slots, purge_old_archived_tasks};

// Re-exports from webhook_execution (ledger + batch-complete detection)
pub use webhook_execution::{
    BatchCompletionStats, batch_completion_stats, complete_webhook_execution,
    decrement_batch_remaining_for_task, decrement_batch_remaining_for_tasks, init_batch_remaining,
    insert_batch, load_batch_on_complete, try_claim_webhook_execution,
    zero_batch_remaining_and_complete,
};

// Re-exports from webhook_outbox (dedicated delivery queue, Audit 2 D3)
pub use webhook_outbox::{
    claim_due_outbox, claim_due_outbox_leased, enqueue_batch_complete_outbox, enqueue_outbox,
    list_webhook_deliveries, mark_outbox_exhausted, mark_outbox_retry, mark_outbox_success,
    outbox_backlog_stats,
};

/// Execute a closure within a database transaction.
/// Automatically rolls back on error. Commits on success.
/// Callers must wrap their async block with `Box::pin(async move { ... })`.
///
/// # Cancel-safety (Audit 2, A1)
///
/// This delegates to diesel-async's [`AsyncConnection::transaction`], which
/// drives the connection's `AnsiTransactionManager`, instead of emitting raw
/// `BEGIN` / `COMMIT` / `ROLLBACK` via `sql_query`. That distinction is what
/// makes the transaction safe against cancellation: if the surrounding future
/// is dropped mid-transaction (client disconnect, actix request timeout, a
/// `tokio::time::timeout`), the transaction manager records that the connection
/// is left inside an open transaction. bb8 inspects that manager state in
/// `is_broken` and discards the connection on return to the pool, instead of
/// handing it to the next borrower with a live transaction still holding row
/// locks (e.g. the batch `FOR UPDATE`). With the previous raw-SQL version the
/// manager was never updated, so a leaked open transaction could silently wrap
/// the next borrower's "autocommit" statements and a later `COMMIT` could make
/// a half-propagated transition durable.
///
/// Observable behavior for callers is unchanged: commit on `Ok`, rollback on
/// `Err`, and the same error is propagated. `DbError` implements
/// `From<diesel::result::Error>`, which `transaction` requires to surface
/// manager-level failures.
pub async fn run_in_transaction<'a, T: Send>(
    conn: &mut Conn<'a>,
    f: impl for<'c> FnOnce(
        &'c mut Conn<'a>,
    ) -> Pin<Box<dyn Future<Output = Result<T, DbError>> + Send + 'c>>
    + Send,
) -> Result<T, DbError> {
    conn.transaction(async move |c: &mut Conn<'a>| f(c).await)
        .await
}
