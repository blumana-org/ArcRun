//! Enqueue lifecycle notifications inside the transaction that changes task state.

use crate::{
    Conn,
    db_operation::{self, DbError},
    notification::{TaskNotification, TerminalStatus},
};

use super::propagation::CanceledAncestor;

/// Enqueue an `end` outbox row for a task that reached a terminal state.
pub(crate) async fn enqueue_end_outbox<'a>(
    task_id: &uuid::Uuid,
    result_status: TerminalStatus,
    conn: &mut Conn<'a>,
) -> Result<(), DbError> {
    db_operation::enqueue_outbox(conn, *task_id, TaskNotification::End(result_status)).await
}

/// Enqueue a `cancel` outbox row for a task that was canceled while Running.
pub(crate) async fn enqueue_cancel_outbox<'a>(
    task_id: &uuid::Uuid,
    conn: &mut Conn<'a>,
) -> Result<(), DbError> {
    db_operation::enqueue_outbox(conn, *task_id, TaskNotification::Cancel).await
}

/// Enqueue the `end` outbox row for a task plus on_failure rows for every
/// cascade-failed child. Runs inside the transition transaction.
pub(crate) async fn enqueue_end_outbox_with_cascade<'a>(
    task_id: &uuid::Uuid,
    result_status: TerminalStatus,
    cascade_failed_ids: &[uuid::Uuid],
    conn: &mut Conn<'a>,
) -> Result<(), DbError> {
    enqueue_end_outbox(task_id, result_status, conn).await?;
    for child_id in cascade_failed_ids {
        enqueue_end_outbox(child_id, TerminalStatus::FAILURE, conn).await?;
    }
    Ok(())
}

/// Enqueue outbox rows for ancestors canceled by dead-end detection:
/// - a `cancel` row if the ancestor's on_start may have run (Running OR Claimed — A4),
/// - plus an on_failure `end` row in all cases.
pub(crate) async fn enqueue_outbox_for_canceled_ancestors<'a>(
    ancestors: &[CanceledAncestor],
    conn: &mut Conn<'a>,
) -> Result<(), DbError> {
    for ancestor in ancestors {
        if ancestor.was_active {
            enqueue_cancel_outbox(&ancestor.id, conn).await?;
        }
        enqueue_end_outbox(&ancestor.id, TerminalStatus::FAILURE, conn).await?;
    }
    Ok(())
}
