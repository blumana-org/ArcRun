#![allow(clippy::redundant_field_names)]

//! Global task-row order for terminal transitions. Call only inside the transition
//! transaction, before taking any task, batch, notification, or reservation lock.

use crate::{Conn, models::StatusKind};
use diesel::{dsl::now, pg::data_types::PgInterval, prelude::*, sql_types};
use diesel_async::RunQueryDsl;
use std::sync::LazyLock;

use super::DbError;

#[derive(Clone, Copy)]
pub(crate) enum Transition {
    Complete { failure: bool },
    Cancel,
    Timeout,
}

pub(crate) struct PreparedTransition {
    pub(crate) previous_status: StatusKind,
    pub(crate) children: Vec<(uuid::Uuid, bool)>,
}

const LOCK_SQL: &str = r#"
WITH RECURSIVE root AS MATERIALIZED (
    SELECT id FROM task
    WHERE id = $1 AND status = ANY($2::status_kind[])
      AND (NOT $4 OR (started_at IS NOT NULL
           AND last_updated < now() - timeout * interval '1 second'))
), failed(id) AS (
    SELECT id FROM root
    UNION
    SELECT l.child_id FROM failed f
    JOIN link l ON l.parent_id = f.id
    JOIN task c ON c.id = l.child_id
    WHERE $3 AND l.requires_success
      AND c.status NOT IN ('success', 'failure', 'canceled')
), touched AS MATERIALIZED (
    SELECT id FROM failed
    UNION
    SELECT l.child_id FROM failed f JOIN link l ON l.parent_id = f.id
), locked AS MATERIALIZED (
    SELECT t.id, t.status, t.started_at, t.last_updated, t.timeout
    FROM task t WHERE t.id IN (SELECT id FROM touched)
    ORDER BY t.id FOR UPDATE OF t
)
SELECT CASE WHEN $4 AND count(*) <> (SELECT count(*) FROM touched) THEN NULL
       ELSE (max(status::text) FILTER (
           WHERE id = $1 AND status = ANY($2::status_kind[])
             AND (NOT $4 OR (started_at IS NOT NULL
                  AND last_updated < now() - timeout * interval '1 second'))
       ))::status_kind END AS locked_status
FROM locked
"#;

static TRY_LOCK_SQL: LazyLock<String> =
    LazyLock::new(|| LOCK_SQL.replace("FOR UPDATE OF t", "FOR UPDATE OF t SKIP LOCKED"));

/// Prepare a transition and acquire its possible downward write set in UUID order.
/// Leaf completions use their guarded UPDATE as the only blocking task lock.
///
/// Edges cannot be added to a committed graph. Terminal states cannot become
/// nonterminal, so pruning terminal branches cannot omit a later cascade write.
/// The one-hop fringe includes completion-only children and already-terminal
/// children. This is a conservative superset, not a prediction of actual updates.
/// Dead-end ancestors may subsequently be acquired ONLY WITHOUT WAITING.
///
/// The final aggregate consumes the whole locking CTE before returning the root's
/// status. An EXISTS/LIMIT here could stop its evaluation after locking just the
/// root! Returning one scalar also avoids sending the closure over the wire.
/// Root eligibility is checked again on the version returned by FOR UPDATE, not
/// only on the earlier CTE snapshot. Completion also retains its guarded root
/// UPDATE, which is authoritative on the leaf fast path.
///
/// Timeout attempts skip busy rows and require the COMPLETE set. A missing lock
/// means no writes; the caller ends this read-only attempt and retries in a later
/// worker pass. It must not propagate through a partially acquired set.
pub(crate) async fn prepare_transition(
    conn: &mut Conn<'_>,
    task_id: uuid::Uuid,
    transition: Transition,
) -> Result<Option<PreparedTransition>, DbError> {
    #[derive(diesel::QueryableByName)]
    struct Locked {
        #[diesel(sql_type = diesel::sql_types::Nullable<crate::schema::sql_types::StatusKind>)]
        locked_status: Option<StatusKind>,
    }
    let (statuses, failure, timeout): (&[StatusKind], _, _) = match transition {
        Transition::Complete { failure } => {
            (&[StatusKind::Running, StatusKind::Claimed], failure, false)
        }
        Transition::Cancel => (
            &[
                StatusKind::Pending,
                StatusKind::Waiting,
                StatusKind::Paused,
                StatusKind::Claimed,
                StatusKind::Running,
            ],
            true,
            false,
        ),
        Transition::Timeout => (&[StatusKind::Running], true, true),
    };
    // Replaces propagation's initial link read. No edge can be added later, so
    // an empty child set proves a completion only needs its guarded root UPDATE.
    // Return no rows for ineligible roots, avoiding a large idempotent traversal.
    // Eligibility is still rechecked by the pre-lock / root UPDATE before writes.
    use crate::schema::{link::dsl as l, task::dsl as t};
    let rows = t::task
        .left_join(l::link.on(l::parent_id.eq(t::id)))
        .filter(t::id.eq(task_id).and(t::status.eq_any(statuses)))
        .filter(diesel::dsl::not(timeout.into_sql::<sql_types::Bool>()).or(
            t::started_at.is_not_null().and(t::last_updated.lt(
                now.into_sql::<sql_types::Timestamptz>()
                    - (PgInterval::from_microseconds(1_000_000).into_sql::<sql_types::Interval>()
                        * t::timeout),
            )),
        ))
        .select((
            t::status,
            l::child_id.nullable(),
            l::requires_success.nullable(),
        ))
        .load::<(StatusKind, Option<uuid::Uuid>, Option<bool>)>(conn)
        .await?;
    let [first, ..] = rows.as_slice() else {
        return Ok(None);
    };
    let previous_status = first.0;
    let children: Vec<_> = rows
        .into_iter()
        .filter_map(|(_, id, required)| id.zip(required))
        .collect();
    if children.is_empty() && matches!(transition, Transition::Complete { .. }) {
        // previous_status is only a snapshot here and is not used by completion;
        // the guarded UPDATE decides whether the transition actually takes place.
        return Ok(Some(PreparedTransition {
            previous_status,
            children,
        }));
    }
    let sql = if timeout {
        TRY_LOCK_SQL.as_str()
    } else {
        LOCK_SQL
    };
    let row = diesel::sql_query(sql)
        .bind::<diesel::sql_types::Uuid, _>(task_id)
        .bind::<diesel::sql_types::Array<crate::schema::sql_types::StatusKind>, _>(statuses)
        .bind::<diesel::sql_types::Bool, _>(failure)
        .bind::<diesel::sql_types::Bool, _>(timeout)
        .get_result::<Locked>(conn)
        .await?;
    Ok(row.locked_status.map(|previous_status| PreparedTransition {
        previous_status,
        children,
    }))
}
