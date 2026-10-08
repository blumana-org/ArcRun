// Diesel QueryableByName emits separate impls with explicit field initializers;
// an allow on the struct does not cover those generated impls.
#![allow(clippy::redundant_field_names)]

use crate::{
    Conn,
    dtos::{self, BasicTaskDto, TaskDto},
    metrics,
    models::{self, Action, Link, NewAction, StatusKind, Task},
    rule::{self, Matcher, Strategy},
};
use diesel::prelude::*;
use diesel_async::{AsyncConnection, RunQueryDsl};
use std::collections::HashMap;
use uuid::Uuid;

use super::DbError;

/// PostgreSQL caps a single statement at 65535 bind parameters — the wire protocol
/// encodes the parameter count as an unsigned 16-bit integer. A grouped multi-row
/// INSERT binds `columns_per_row * rows` parameters, so a large (or link/action-heavy)
/// `POST /task` batch can blow past that ceiling and fail with a 500 (Audit 2, A10).
/// We chunk each multi-row INSERT so `rows * binds_per_row` stays under this
/// conservative budget, leaving a margin below the hard 65535 limit. All chunks run
/// on the SAME transaction connection, so atomicity is unchanged.
const BIND_BUDGET: usize = 60_000;

/// Bind parameters per row for each grouped INSERT. These MUST match the number of
/// columns in the corresponding `Insertable` struct — bump them if a column is added:
///   - `models::NewTask`: 13 columns.
///   - `models::Link`: 3 columns.
///   - `models::NewAction`: 5 columns.
const TASK_BINDS_PER_ROW: usize = 13;
const LINK_BINDS_PER_ROW: usize = 3;
const ACTION_BINDS_PER_ROW: usize = 5;

/// Max rows per chunk so `rows * binds_per_row <= BIND_BUDGET` (at least 1).
const fn chunk_rows(binds_per_row: usize) -> usize {
    let n = BIND_BUDGET / binds_per_row;
    if n == 0 { 1 } else { n }
}

/// Ensure we avoid creating duplicate tasks.
///
/// # Concurrency (Audit 2 A8)
/// This is a check-then-act sequence: `COUNT(*)` on the committed snapshot, then
/// (back in the caller) an INSERT later in the SAME transaction. Without a lock,
/// two concurrent `POST /task` requests carrying the same dedupe key both see
/// `count == 0` and both insert — producing duplicates despite `dedupe_strategy`.
///
/// To close that window, before running the counts we take a
/// `pg_advisory_xact_lock` on a stable hash of each applicable matcher
/// (`rule::dedupe_lock_key` = hash of kind + status + the matcher's metadata field
/// values). The lock is **transaction-scoped**: `handle_dedupe` is invoked inside
/// the `insert_task_batch` transaction (see `handlers::task::add_task`), so the lock
/// is held until that transaction COMMITs/ROLLBACKs. The advisory lock is blocking:
/// a second request with the same key parks on the lock until the first commits,
/// then its `COUNT(*)` observes the just-inserted task (`count > 0`) and correctly
/// dedupes. On collision (two *different* keys hashing equal) the only effect is
/// spurious serialization — never a false dedupe, since `COUNT(*)` stays the truth.
///
/// # Deadlock avoidance
/// All of this task's applicable lock keys are collected, **sorted and deduped**,
/// then acquired in that order in a single round-trip. A consistent global key
/// order is what prevents two requests that lock several keys from deadlocking.
/// The realistic A8 scenario — the *same* batch submitted concurrently — locks the
/// identical sorted key set, so it is deadlock-free. Limitation: `handle_dedupe` is
/// called once per dedupe task in a batch and the locks accumulate on the
/// transaction, so two *different* batches that each carry several dedupe tasks in a
/// different relative order could, in theory, acquire cross-task keys in opposite
/// orders and deadlock; PostgreSQL's deadlock detector then aborts one side (the
/// request surfaces an error and can be retried). This is rare and strictly safer
/// than the pre-fix silent duplicates.
async fn handle_dedupe<'a>(
    conn: &mut Conn<'a>,
    rules: Vec<Matcher>,
    _metadata: &Option<serde_json::Value>,
) -> Result<bool, DbError> {
    use crate::schema::task::dsl::*;
    use diesel::PgJsonbExpressionMethods;

    let empty_metadata = serde_json::Value::Null;
    let meta_ref = _metadata.as_ref().unwrap_or(&empty_metadata);

    // Pass 1: resolve the matchers we can actually evaluate (applying the two
    // "skip this rule" guards) and compute their advisory lock keys. A guard-skipped
    // matcher contributes neither a lock nor a count.
    let mut applicable: Vec<(&Matcher, serde_json::Value)> = Vec::with_capacity(rules.len());
    let mut lock_keys: Vec<i64> = Vec::with_capacity(rules.len());
    for matcher in rules.iter() {
        if !matcher.fields.is_empty() && _metadata.is_none() {
            // Metadata is None but the matcher requires field comparisons.
            // We can't evaluate this rule without metadata, so skip it
            // (allow creation). Without this guard, m stays as {} and
            // metadata.contains({}) would match ALL existing tasks with
            // non-null metadata, causing over-aggressive deduplication.
            log::warn!(
                "Metadata is None but dedupe matcher requires fields {:?}, skipping rule",
                matcher.fields
            );
            continue;
        }

        let m = match matcher.extract_metadata_fields(meta_ref) {
            Ok(m) => m,
            Err(field) => {
                log::warn!(
                    "Metadata missing field '{}' required by dedupe matcher, skipping rule",
                    field
                );
                continue;
            }
        };

        lock_keys.push(rule::dedupe_lock_key(matcher, meta_ref));
        applicable.push((matcher, m));
    }

    // Nothing evaluable -> allow creation (no lock, no count).
    if applicable.is_empty() {
        return Ok(true);
    }

    // Acquire all advisory locks up front, in a stable (sorted, deduped) order and a
    // single round-trip, BEFORE any COUNT. This serializes the check-then-act window
    // against concurrent requests carrying the same dedupe key(s).
    lock_keys.sort_unstable();
    lock_keys.dedup();
    diesel::sql_query("SELECT pg_advisory_xact_lock(k) FROM unnest($1::bigint[]) AS k")
        .bind::<diesel::sql_types::Array<diesel::sql_types::BigInt>, _>(&lock_keys)
        .execute(conn)
        .await?;

    // Pass 2: run the counts. Now that we hold the locks, a concurrent inserter of a
    // matching task has either already committed (its row is visible here -> we dedupe)
    // or is parked on the lock (it will see OUR row after we commit -> it dedupes).
    for (matcher, m) in applicable {
        let count = task
            .filter(
                kind.eq(&matcher.kind)
                    .and(status.eq(&matcher.status))
                    .and(metadata.contains(m)),
            )
            .count()
            .get_result::<i64>(conn)
            .await?;
        if count > 0 {
            return Ok(false);
        }
    }
    Ok(true)
}

pub(crate) async fn insert_actions<'a>(
    task_id: Uuid,
    actions: &[dtos::NewActionDto],
    trigger: &models::TriggerKind,
    condition: &models::TriggerCondition,
    conn: &mut Conn<'a>,
) -> Result<Vec<Action>, DbError> {
    use crate::schema::action::dsl::action;
    if actions.is_empty() {
        return Ok(vec![]);
    }
    let items = actions
        .iter()
        .map(|a| NewAction {
            task_id,
            kind: &a.kind,
            params: a.params.clone(),
            trigger,
            condition,
        })
        .collect::<Vec<_>>();

    let r = diesel::insert_into(action)
        .values(items)
        .returning(Action::as_returning())
        .get_results(conn)
        .await?;
    Ok(r)
}

/// A task DTO that has been resolved against the current `id_mapping` into the
/// concrete rows to insert (task row + links + action specs), with its
/// app-generated UUID already assigned. Built outside the DB so a contiguous run
/// of these can be flushed in a few multi-row INSERTs (Lot 3a).
struct PreparedTask {
    id: Uuid,
    new_task: models::NewTask,
    /// Resolved parent links; `child_id` is already set to `id`.
    links: Vec<Link>,
    on_start: dtos::NewActionDto,
    on_failure: Vec<dtos::NewActionDto>,
    on_success: Vec<dtos::NewActionDto>,
    wait_finished: i32,
}

/// Insert a whole batch of tasks, grouping contiguous runs of tasks WITHOUT a
/// `dedupe_strategy` into multi-row INSERTs (one per `task` / `link` / `action`),
/// while still evaluating dedupe tasks one-at-a-time so they can match tasks
/// inserted earlier in the same batch.
///
/// UUIDs are generated app-side (`Uuid::now_v7()`) so the full `id_mapping` is
/// known before any insert, letting links be built without intermediate RETURNINGs.
///
/// Semantics preserved exactly from the old per-task `insert_new_task`:
/// - dedupe-skipped tasks are absent from `id_mapping` (so a child depending on a
///   skipped parent has that dependency ignored with a warn),
/// - `wait_*` counters, initial Waiting/Pending status, and per-task metrics are
///   identical,
/// - the returned `BasicTaskDto`s preserve input order.
pub(crate) async fn insert_task_batch<'a>(
    conn: &mut Conn<'a>,
    dtos_in: Vec<dtos::NewTaskDto>,
    batch_id: Option<Uuid>,
) -> Result<Vec<BasicTaskDto>, DbError> {
    crate::metrics::record_batch_insert_tasks(dtos_in.len());
    let mut id_mapping: HashMap<String, Uuid> = HashMap::new();
    let mut results: Vec<BasicTaskDto> = Vec::with_capacity(dtos_in.len());

    // Buffer of prepared, dedupe-free tasks in the current contiguous run.
    let mut run: Vec<PreparedTask> = Vec::new();

    for dto in dtos_in.into_iter() {
        let has_dedupe = dto.dedupe_strategy.is_some();

        if has_dedupe {
            // Flush the pending run FIRST so its tasks are visible to the dedupe
            // check (a dedupe task may match a task inserted earlier in this batch).
            flush_run(conn, &mut run, &mut results).await?;

            let local_id = dto.id.clone();
            let dedupe_rules = dto.dedupe_strategy.clone().unwrap();
            let should_write = handle_dedupe(conn, dedupe_rules, &dto.metadata).await?;
            if !should_write {
                crate::metrics::record_task_deduped();
                continue;
            }

            let task_id = Uuid::now_v7();
            let prepared = resolve_task(dto, task_id, batch_id, &id_mapping);
            id_mapping.insert(local_id, task_id);
            // A dedupe task is its own one-element run (it may not be batched with
            // others past it, but flushing it alone is fine and keeps the code simple).
            run.push(prepared);
            flush_run(conn, &mut run, &mut results).await?;
        } else {
            let local_id = dto.id.clone();
            let task_id = Uuid::now_v7();
            let prepared = resolve_task(dto, task_id, batch_id, &id_mapping);
            id_mapping.insert(local_id, task_id);
            run.push(prepared);
        }
    }

    // Flush any trailing run.
    flush_run(conn, &mut run, &mut results).await?;

    Ok(results)
}

/// Resolve a DTO into a [`PreparedTask`] against the provided `id_mapping`.
/// (Standalone version that does not use the thread-local indirection.)
fn resolve_task(
    dto: dtos::NewTaskDto,
    task_id: Uuid,
    batch_id: Option<Uuid>,
    id_mapping: &HashMap<String, Uuid>,
) -> PreparedTask {
    let (wait_success, wait_finished, links) = if let Some(ref deps) = dto.dependencies {
        let mut ws = 0i32;
        let mut wf = 0i32;
        let mut resolved_links = Vec::new();
        for dep in deps {
            if let Some(&parent_id) = id_mapping.get(&dep.id) {
                wf += 1;
                if dep.requires_success {
                    ws += 1;
                }
                resolved_links.push(Link {
                    parent_id,
                    child_id: task_id,
                    requires_success: dep.requires_success,
                });
            } else {
                log::warn!("Dependency with local id '{}' not found in mapping", dep.id);
            }
        }
        (ws, wf, resolved_links)
    } else {
        (0, 0, Vec::new())
    };

    let initial_status = if wait_finished > 0 {
        models::StatusKind::Waiting
    } else {
        models::StatusKind::Pending
    };

    let new_task = models::NewTask {
        id: task_id,
        name: dto.name,
        kind: dto.kind,
        status: initial_status,
        timeout: dto.timeout.unwrap_or(60),
        metadata: dto.metadata.unwrap_or(serde_json::Value::Null),
        start_condition: dto.rules.unwrap_or_default(),
        wait_success,
        wait_finished,
        batch_id,
        expected_count: dto.expected_count,
        dead_end_barrier: dto.dead_end_barrier.unwrap_or(false),
        priority: dto.priority.unwrap_or(0),
    };

    PreparedTask {
        id: task_id,
        new_task,
        links,
        on_start: dto.on_start,
        on_failure: dto.on_failure.unwrap_or_default(),
        on_success: dto.on_success.unwrap_or_default(),
        wait_finished,
    }
}

/// Flush a run of prepared tasks into the DB with up to three multi-row INSERTs
/// (task, link, action), then append their `BasicTaskDto`s to `results`
/// (preserving run order) and record per-task metrics. Clears `run`.
///
/// Actions are inserted but NOT returned — `BasicTaskDto` doesn't carry them,
/// so we skip the action RETURNING round-trip (B6 conformity).
async fn flush_run<'a>(
    conn: &mut Conn<'a>,
    run: &mut Vec<PreparedTask>,
    results: &mut Vec<BasicTaskDto>,
) -> Result<(), DbError> {
    use crate::schema::action::dsl::action as action_tbl;
    use crate::schema::link::dsl::link as link_tbl;
    use crate::schema::task::dsl::task as task_tbl;

    if run.is_empty() {
        return Ok(());
    }

    let prepared: Vec<PreparedTask> = std::mem::take(run);

    // 1. Multi-row INSERT of task rows, chunked to stay under the bind-param ceiling.
    let new_tasks: Vec<&models::NewTask> = prepared.iter().map(|p| &p.new_task).collect();
    let mut task_by_id: HashMap<Uuid, Task> = HashMap::with_capacity(new_tasks.len());
    for chunk in new_tasks.chunks(chunk_rows(TASK_BINDS_PER_ROW)) {
        let inserted_tasks: Vec<Task> = diesel::insert_into(task_tbl)
            .values(chunk.to_vec())
            .returning(Task::as_returning())
            .get_results(conn)
            .await?;
        for t in inserted_tasks {
            task_by_id.insert(t.id, t);
        }
    }

    // 2. Multi-row INSERT of all links across the run, chunked.
    let all_links: Vec<Link> = prepared.iter().flat_map(|p| p.links.clone()).collect();
    for chunk in all_links.chunks(chunk_rows(LINK_BINDS_PER_ROW)) {
        diesel::insert_into(link_tbl)
            .values(chunk)
            .execute(conn)
            .await?;
    }

    // 3. Multi-row INSERT of all actions across the run (start + failure + success).
    //    Actions are fire-and-forget here — BasicTaskDto doesn't include them, so we
    //    use `.execute()` instead of `.returning().get_results()` to skip the round-trip.
    let mut all_new_actions: Vec<NewAction> = Vec::new();
    for p in &prepared {
        all_new_actions.push(NewAction {
            task_id: p.id,
            kind: &p.on_start.kind,
            params: p.on_start.params.clone(),
            trigger: &models::TriggerKind::Start,
            condition: &models::TriggerCondition::Success,
        });
        for a in &p.on_failure {
            all_new_actions.push(NewAction {
                task_id: p.id,
                kind: &a.kind,
                params: a.params.clone(),
                trigger: &models::TriggerKind::End,
                condition: &models::TriggerCondition::Failure,
            });
        }
        for a in &p.on_success {
            all_new_actions.push(NewAction {
                task_id: p.id,
                kind: &a.kind,
                params: a.params.clone(),
                trigger: &models::TriggerKind::End,
                condition: &models::TriggerCondition::Success,
            });
        }
    }
    for chunk in all_new_actions.chunks(chunk_rows(ACTION_BINDS_PER_ROW)) {
        diesel::insert_into(action_tbl)
            .values(chunk)
            .execute(conn)
            .await?;
    }

    // Assemble results in input (run) order + record metrics.
    for p in &prepared {
        let task = task_by_id
            .remove(&p.id)
            .expect("inserted task row missing for prepared id");

        metrics::record_task_created();
        if p.wait_finished > 0 {
            metrics::record_task_with_dependencies();
        }

        results.push(BasicTaskDto::from(task));
    }

    Ok(())
}

/// Find a task by ID with all its actions using a single LEFT JOIN query.
/// Returns None if the task doesn't exist.
pub(crate) async fn find_detailed_task_by_id<'a>(
    conn: &mut Conn<'a>,
    task_id: Uuid,
) -> Result<Option<dtos::TaskDto>, DbError> {
    use crate::schema::action::dsl as action_dsl;
    use crate::schema::task::dsl::*;

    // Use LEFT JOIN to fetch task and actions in a single query
    let results: Vec<(models::Task, Option<Action>)> = task
        .left_join(action_dsl::action)
        .filter(id.eq(task_id))
        .load::<(models::Task, Option<Action>)>(conn)
        .await?;

    if results.is_empty() {
        // Archive fallback (Audit 2, D6): the task is not in the hot `task` table — it
        // may have been moved to `task_archive` by retention. Serve its history with the
        // SAME DTO shape. The archive keeps no actions (they were deleted on archive), so
        // the `actions` array is empty. Only GET reads the archive; writes (PATCH/PUT/
        // cancel/resume) still target `task` and so stay 404 for an archived task.
        return find_archived_task_by_id(conn, task_id).await;
    }

    // All rows have the same task; take the first one by value, collect actions from the rest
    let mut iter = results.into_iter();
    let (base_task, first_action) = iter.next().unwrap();
    let actions: Vec<Action> = first_action
        .into_iter()
        .chain(iter.filter_map(|(_, a)| a))
        .collect();

    Ok(Some(TaskDto::new(base_task, actions)))
}

/// Look up a task in the cold `task_archive` table (Audit 2, D6) and render it as the
/// same `TaskDto` a live task would produce. Returns `None` if absent from the archive
/// too. Archived tasks carry no actions, so the DTO's `actions` array is empty.
async fn find_archived_task_by_id<'a>(
    conn: &mut Conn<'a>,
    task_id: Uuid,
) -> Result<Option<dtos::TaskDto>, DbError> {
    use crate::schema::task_archive::dsl as archive_dsl;

    let archived: Option<models::TaskArchive> = archive_dsl::task_archive
        .filter(archive_dsl::id.eq(task_id))
        .select(models::TaskArchive::as_select())
        .first::<models::TaskArchive>(conn)
        .await
        .optional()?;

    Ok(archived.map(|a| TaskDto::new(models::Task::from(a), Vec::new())))
}

/// Atomically claim a Pending task by transitioning it to Claimed.
/// Returns true if this caller successfully claimed the task, false if another worker got it first.
pub async fn claim_task<'a>(conn: &mut Conn<'a>, task_id: &uuid::Uuid) -> Result<bool, DbError> {
    use crate::schema::task::dsl::*;
    use diesel::dsl::now;

    let updated_count =
        diesel::update(task.filter(id.eq(task_id).and(status.eq(StatusKind::Pending))))
            .set((status.eq(StatusKind::Claimed), last_updated.eq(now)))
            .execute(conn)
            .await?;

    Ok(updated_count == 1)
}

/// Atomically claim a contiguous run of rule-free Pending tasks in a single
/// `UPDATE ... WHERE id = ANY($ids) AND status='pending' RETURNING id`.
///
/// Only valid for tasks with an empty `start_condition` (no concurrency/capacity
/// rules), because a single UPDATE cannot evaluate per-task rules. Returns the set
/// of IDs that were actually transitioned Pending -> Claimed (some may already have
/// been claimed by another worker, in which case they are absent from the result).
pub async fn batch_claim_tasks<'a>(
    conn: &mut Conn<'a>,
    ids: &[uuid::Uuid],
) -> Result<Vec<uuid::Uuid>, DbError> {
    use crate::schema::task::dsl::*;
    use diesel::dsl::now;

    if ids.is_empty() {
        return Ok(vec![]);
    }

    let claimed_ids =
        diesel::update(task.filter(id.eq_any(ids).and(status.eq(StatusKind::Pending))))
            .set((status.eq(StatusKind::Claimed), last_updated.eq(now)))
            .returning(id)
            .get_results::<uuid::Uuid>(conn)
            .await?;

    Ok(claimed_ids)
}

/// Transition a Claimed task to Running and set started_at.
/// Returns true if the task was updated, false if it was no longer Claimed.
pub async fn mark_task_running<'a>(
    conn: &mut Conn<'a>,
    task_id: &uuid::Uuid,
) -> Result<bool, DbError> {
    use crate::schema::task::dsl::*;
    use diesel::dsl::now;

    let updated_count =
        diesel::update(task.filter(id.eq(task_id).and(status.eq(StatusKind::Claimed))))
            .set((
                status.eq(StatusKind::Running),
                started_at.eq(now),
                last_updated.eq(now),
            ))
            .execute(conn)
            .await?;

    Ok(updated_count == 1)
}

/// Result of attempting to atomically check concurrency rules and claim a task.
#[derive(Debug, PartialEq)]
pub enum ClaimResult {
    /// Task was successfully claimed (Pending -> Claimed).
    Claimed,
    /// A concurrency rule blocked this task from being claimed.
    RuleBlocked,
    /// Task was already claimed by another worker (UPDATE touched 0 rows).
    AlreadyClaimed,
}

/// Exact concurrency slot that rejected a rule-bearing claim.  This is deliberately
/// separate from [`ClaimResult`], whose public shape is kept stable for callers that only
/// care about claimed/blocked/already-claimed.  The start-loop uses this detail to maintain
/// a sound negative cache: a Capacity failure, or a different Concurrency rule on the same
/// task, must never poison an unrelated concurrency key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BlockedConcurrency {
    pub key: String,
    pub threshold: i32,
}

pub(crate) struct RuleClaimOutcome {
    pub result: ClaimResult,
    pub blocked_concurrency: Option<BlockedConcurrency>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SlotKind {
    Concurrency,
    Capacity,
}

/// Re-export slot key functions from rule module for use by other crates/modules.
pub(crate) use rule::capacity_slot_key;
pub(crate) use rule::concurrency_slot_key;

/// Pre-computed parameters for the rule-check-and-claim transaction.
/// Built from a task's rules and metadata before entering the transaction,
/// so no references to the Task are needed inside the closure.
struct RuleQueryParams {
    /// Unified slot list (Audit 2, D1 / 7.3b): `(canonical_key, increment, threshold)`,
    /// covering **both** Concurrency (`conc:` key, increment `1`, threshold
    /// `max_concurency`) and Capacity (`cap:` key, increment = the task's capacity
    /// **charge**, threshold `max_capacity`) rules. Sorted by key and deduped
    /// (BTreeMap): two rules of the same candidate producing the SAME key collapse to a
    /// single slot (keeping the MOST restrictive / lowest threshold), and the sorted
    /// order gives every worker one canonical slot-lock acquisition order (A9 deadlock
    /// discipline). `cap:` sorts before `conc:` lexicographically — consistent, so the
    /// global slot-lock order stays well-defined across both prefixes.
    slots: Vec<(String, i32, i32, SlotKind)>,
    /// The capacity charge to persist in `task.capacity_charge` — `Some(charge)` when the
    /// task has ≥1 Capacity rule (charge is task-level, identical for every `cap:` key),
    /// `None` when it has none (column stays/goes NULL).
    capacity_charge: Option<i32>,
}

impl RuleQueryParams {
    /// Build query parameters from a task's rules and metadata.
    /// Returns `Err(ClaimResult::RuleBlocked)` if a rule cannot be evaluated
    /// (missing metadata field, missing expected_count for capacity, or a
    /// non-positive `max_capacity`).
    fn from_task(t: &Task) -> Result<Self, ClaimResult> {
        let rules = &t.start_condition.0;
        let task_id = t.id;

        // BTreeMap: dedup by canonical key + deterministic (sorted) iteration order.
        // Value = (increment, threshold).
        let mut slot_map: std::collections::BTreeMap<String, (i32, i32, SlotKind)> =
            std::collections::BTreeMap::new();
        let mut has_capacity = false;
        let mut capacity_charge: i32 = 0;

        for strategy in rules {
            match strategy {
                Strategy::Concurency(concurrency_rule) => {
                    // Canonical, collision-free slot key. A missing required metadata
                    // field blocks the claim (same as the pre-slot advisory path).
                    let key = match concurrency_slot_key(concurrency_rule, &t.metadata) {
                        Ok(k) => k,
                        Err(field) => {
                            log::warn!(
                                "Task {} missing metadata field '{}' required by concurrency rule, blocking",
                                task_id,
                                field
                            );
                            return Err(ClaimResult::RuleBlocked);
                        }
                    };

                    // NEW SEMANTICS (Audit 2, D1 / §7): the slot counts only tasks that
                    // consumed it (claimed WITH a rule producing this key), and the
                    // candidate ALWAYS consumes it — so the candidate counts itself and
                    // the threshold is simply its own `max_concurency` (allow while
                    // `used < max`). Concurrency increments the slot by exactly 1.
                    let threshold = concurrency_rule.max_concurency;
                    slot_map
                        .entry(key)
                        .and_modify(|(_inc, thr, _kind)| *thr = (*thr).min(threshold))
                        .or_insert((1, threshold, SlotKind::Concurrency));
                }
                Strategy::Capacity(capacity_rule) => {
                    // Canonical `cap:` slot key. A missing required metadata field blocks.
                    let key = match capacity_slot_key(capacity_rule, &t.metadata) {
                        Ok(k) => k,
                        Err(field) => {
                            log::warn!(
                                "Task {} missing metadata field '{}' required by capacity rule, blocking",
                                task_id,
                                field
                            );
                            return Err(ClaimResult::RuleBlocked);
                        }
                    };

                    // Candidate must have expected_count set.
                    let expected = match t.expected_count {
                        Some(e) => e,
                        None => {
                            log::warn!(
                                "Task {} has a Capacity rule but no expected_count, blocking",
                                task_id,
                            );
                            return Err(ClaimResult::RuleBlocked);
                        }
                    };

                    // Rust-side guard (7.3b): a non-positive `max_capacity` must block.
                    // The conditional-upsert's fresh-INSERT arm has no `WHERE used < max`
                    // check, so a brand-new slot would otherwise admit a candidate that
                    // the old `sum(0) >= max_cap(0)` probe blocked. This guard preserves
                    // the old admission semantics.
                    if capacity_rule.max_capacity <= 0 {
                        log::warn!(
                            "Task {} has a Capacity rule with non-positive max_capacity {}, blocking",
                            task_id,
                            capacity_rule.max_capacity,
                        );
                        return Err(ClaimResult::RuleBlocked);
                    }

                    // Charge = the candidate's remaining work. i64 arithmetic avoids an
                    // int overflow when success + failures exceeds i32::MAX; the clamped
                    // result is <= expected_count, so it fits back into i32.
                    let charge =
                        (expected as i64 - t.success as i64 - t.failures as i64).max(0) as i32;
                    has_capacity = true;
                    capacity_charge = charge; // task-level, identical for every cap: key
                    let threshold = capacity_rule.max_capacity;
                    slot_map
                        .entry(key)
                        .and_modify(|(inc, thr, _kind)| {
                            *inc = charge;
                            *thr = (*thr).min(threshold);
                        })
                        .or_insert((charge, threshold, SlotKind::Capacity));
                }
            }
        }

        Ok(RuleQueryParams {
            // BTreeMap iterates in ascending key order → already sorted + deduped.
            slots: slot_map
                .into_iter()
                .map(|(k, (inc, thr, kind))| (k, inc, thr, kind))
                .collect(),
            capacity_charge: if has_capacity {
                Some(capacity_charge)
            } else {
                None
            },
        })
    }
}

/// Non-error outcomes of the claim transaction that must ROLLBACK. diesel-async's
/// `transaction` only rolls back on `Err`, so a blocked rule / already-claimed task
/// (which must undo any slot increments already applied in the tx) is encoded as an
/// `Err` here and translated back to an `Ok(ClaimResult)` after the tx returns.
enum ClaimTxAbort {
    /// A concurrency slot was at its limit, or capacity was blocked.
    RuleBlocked(Option<BlockedConcurrency>),
    /// The task left `pending` before the claim UPDATE (another worker won).
    AlreadyClaimed,
    /// A genuine DB error — surfaces to the caller as `Err`.
    Db(DbError),
}

impl From<diesel::result::Error> for ClaimTxAbort {
    fn from(e: diesel::result::Error) -> Self {
        ClaimTxAbort::Db(DbError::from(e))
    }
}
impl From<DbError> for ClaimTxAbort {
    fn from(e: DbError) -> Self {
        ClaimTxAbort::Db(e)
    }
}

/// Atomically evaluate a task's rules and claim it (Pending -> Claimed) in ONE
/// transaction.
///
/// **Concurrency + Capacity (Audit 2, D1 — 7.3a for Concurrency, 7.3b for Capacity):**
/// each of the candidate's rules maps to a `rule_slot` row. In sorted key order (A9
/// deadlock discipline) the claim increments each slot with a conditional upsert
/// (`ON CONFLICT DO UPDATE SET used = used + $inc WHERE used < $threshold RETURNING used`):
/// Concurrency increments by `1` against `max_concurency`; Capacity increments by the
/// candidate's **charge** (`GREATEST(expected_count - success - failures, 0)`) against
/// `max_capacity`. A slot that returns no row is at its limit ⇒ the whole transaction
/// rolls back (undoing earlier increments) ⇒ `RuleBlocked`. The row lock taken by the
/// upsert (and the unique-index insert lock for a brand-new key) serializes concurrent
/// claimers of the same slot, so the advisory-lock + CTE-SUM layer Capacity used before
/// is gone. The consumed keys are persisted into `task.claimed_slot_keys` and the charge
/// into `task.capacity_charge` by the same claim UPDATE, so both can be released
/// precisely later (release reads the stored charge — never recomputed).
///
/// Capacity admission semantics preserved: allowed iff **others' current sum <
/// max_capacity** (the candidate's own charge is not counted in the check — the
/// fresh-INSERT / `used < threshold` gate matches the old `sum >= max_cap` probe;
/// overshoot when a large candidate lands on a near-full slot is allowed, as before).
///
/// The final claim `UPDATE ... WHERE id = $ AND status = 'pending'` matching 0 rows
/// (task already claimed) also rolls back ⇒ `AlreadyClaimed` (no slot consumed).
pub async fn claim_task_with_rules<'a>(
    conn: &mut Conn<'a>,
    t: &Task,
) -> Result<ClaimResult, DbError> {
    Ok(claim_task_with_rules_detailed(conn, t).await?.result)
}

/// Detailed variant used by the scheduler. It performs the same single claim attempt as
/// [`claim_task_with_rules`] but also reports the exact saturated Concurrency slot, when
/// that is what blocked the transaction. No detail is returned for Capacity failures or
/// for unevaluable rules, so those outcomes cannot contaminate the concurrency prefilter.
pub(crate) async fn claim_task_with_rules_detailed<'a>(
    conn: &mut Conn<'a>,
    t: &Task,
) -> Result<RuleClaimOutcome, DbError> {
    // No rules — just do a plain claim (no slot needed). A rule-less task never held a
    // capacity charge (start_condition is immutable), so capacity_charge stays NULL.
    if t.start_condition.0.is_empty() {
        return Ok(match claim_task(conn, &t.id).await? {
            true => RuleClaimOutcome {
                result: ClaimResult::Claimed,
                blocked_concurrency: None,
            },
            false => RuleClaimOutcome {
                result: ClaimResult::AlreadyClaimed,
                blocked_concurrency: None,
            },
        });
    }

    // Pre-compute everything we need before entering the transaction closure.
    let task_id = t.id;
    let params = match RuleQueryParams::from_task(t) {
        Ok(p) => p,
        Err(result) => {
            return Ok(RuleClaimOutcome {
                result,
                blocked_concurrency: None,
            });
        }
    };

    let RuleQueryParams {
        slots,
        capacity_charge,
    } = params;

    let tx: Result<(), ClaimTxAbort> = conn
        .transaction(async move |conn: &mut Conn<'a>| {
            // 1. Slot upserts (Concurrency + Capacity unified): conditional upsert per
            //    key, in sorted order. No row returned ⇒ at limit ⇒ abort (rollback
            //    undoes earlier increments). `$2` = increment (1 for conc:, charge for
            //    cap:), `$3` = threshold (max_concurency / max_capacity).
            #[derive(diesel::QueryableByName)]
            struct SlotUsed {
                #[diesel(sql_type = diesel::sql_types::Integer)]
                #[allow(dead_code)]
                used: i32,
            }
            for (key, increment, threshold, kind) in &slots {
                let row: Option<SlotUsed> = diesel::sql_query(
                    "INSERT INTO rule_slot AS rs (lock_key, used) VALUES ($1, $2)
                     ON CONFLICT (lock_key) DO UPDATE SET used = rs.used + $2
                     WHERE rs.used < $3
                     RETURNING used",
                )
                .bind::<diesel::sql_types::Text, _>(key)
                .bind::<diesel::sql_types::Integer, _>(*increment)
                .bind::<diesel::sql_types::Integer, _>(*threshold)
                .get_result::<SlotUsed>(&mut *conn)
                .await
                .optional()?;
                if row.is_none() {
                    let blocked = (*kind == SlotKind::Concurrency).then(|| BlockedConcurrency {
                        key: key.clone(),
                        threshold: *threshold,
                    });
                    return Err(ClaimTxAbort::RuleBlocked(blocked));
                }
            }

            // 2. Claim UPDATE, persisting the consumed slot keys AND the capacity charge.
            //    A non-empty start_condition always yields ≥1 slot (every rule produces a
            //    key or blocks), so `keys` is non-empty here. `capacity_charge` is bound
            //    NULL when the task carries no Capacity rule (explicit, paranoia against a
            //    stale value on a re-claim after release).
            let keys: Vec<String> = slots.iter().map(|(k, _, _, _)| k.clone()).collect();
            let claimed_rows = diesel::sql_query(
                "UPDATE task
                 SET status = 'claimed', last_updated = now(),
                     claimed_slot_keys = $2, capacity_charge = $3
                 WHERE id = $1 AND status = 'pending'",
            )
            .bind::<diesel::sql_types::Uuid, _>(task_id)
            .bind::<diesel::sql_types::Array<diesel::sql_types::Text>, _>(&keys)
            .bind::<diesel::sql_types::Nullable<diesel::sql_types::Integer>, _>(capacity_charge)
            .execute(&mut *conn)
            .await?;
            if claimed_rows == 0 {
                return Err(ClaimTxAbort::AlreadyClaimed);
            }

            Ok::<(), ClaimTxAbort>(())
        })
        .await;

    match tx {
        Ok(()) => Ok(RuleClaimOutcome {
            result: ClaimResult::Claimed,
            blocked_concurrency: None,
        }),
        Err(ClaimTxAbort::RuleBlocked(blocked_concurrency)) => Ok(RuleClaimOutcome {
            result: ClaimResult::RuleBlocked,
            blocked_concurrency,
        }),
        Err(ClaimTxAbort::AlreadyClaimed) => Ok(RuleClaimOutcome {
            result: ClaimResult::AlreadyClaimed,
            blocked_concurrency: None,
        }),
        Err(ClaimTxAbort::Db(e)) => Err(e),
    }
}

/// Release the concurrency + capacity slots (Audit 2, D1) held by the given tasks. For
/// every id in `task_ids` whose `claimed_slot_keys` is non-NULL, decrement
/// `rule_slot.used` (clamped at 0) by the amount that task contributed to each key — `1`
/// for a `conc:` key, the task's stored `capacity_charge` for a `cap:` key — then NULL
/// both `claimed_slot_keys` and `capacity_charge`.
///
/// The capacity charge is READ from the `capacity_charge` column (never recomputed:
/// expected_count/metadata are mutable while Running, so recomputation could diverge and
/// leak a slot). Progress flushed by the batch_updater has already shrunk the charge (and
/// decremented the slot by the shrink), so releasing the *remaining* stored charge here
/// exactly zeroes out this task's contribution.
///
/// **Exactly-once / no double-release.** Callers pass the ids they just terminalized
/// (or requeued). The `claimed_slot_keys IS NOT NULL` gate is what encodes "only a task
/// that actually consumed a slot releases one": a task that was Pending/Waiting/Paused
/// (never claimed with a rule) has NULL keys, so passing it is a harmless no-op — which
/// is why the terminal sites can pass their whole `terminal_ids` slice without a
/// per-id status check. Setting the column to NULL in the same statement makes a second
/// release call (should one ever reach the same id) a no-op, and each release is
/// coupled to the SUCCESS of a status-guarded transition (like the D2 batch decrement),
/// so a task can only be released once. Keys are read back from the column and NEVER
/// recomputed (metadata is mutable while Running).
///
/// Must run inside the same transaction as the terminal/requeue transition. One
/// statement: a CTE NULLs + returns the released key arrays, aggregates per-key counts
/// (a slot shared by two released tasks decrements by 2), and decrements `rule_slot`.
pub async fn release_slots_for_tasks<'a>(
    conn: &mut Conn<'a>,
    task_ids: &[uuid::Uuid],
) -> Result<(), DbError> {
    if task_ids.is_empty() {
        return Ok(());
    }
    // A9: pre-lock the affected `rule_slot` rows in one globally-ordered statement
    // BEFORE the join-driven decrement below. The claim side acquires slot row locks
    // one key at a time in sorted order; without this, `slot_dec`'s `UPDATE … FROM`
    // acquires them in join order, and a multi-key release racing a multi-key claim
    // (or another release) could take the same two slots in opposite orders and
    // deadlock. Locking here in `ORDER BY lock_key` makes every slot-lock acquisition
    // in the system sorted. Held until COMMIT (caller-invariant: in-tx).
    diesel::sql_query(
        "SELECT lock_key FROM rule_slot
         WHERE lock_key IN (
             SELECT DISTINCT unnest(claimed_slot_keys)
             FROM task
             WHERE id = ANY($1) AND claimed_slot_keys IS NOT NULL
         )
         ORDER BY lock_key
         FOR UPDATE",
    )
    .bind::<diesel::sql_types::Array<diesel::sql_types::Uuid>, _>(task_ids)
    .execute(&mut *conn)
    .await?;

    // NOTE: the keys are READ (and row-locked) in `to_release` BEFORE `cleared` NULLs
    // the column — a single `UPDATE ... SET keys = NULL RETURNING keys` would RETURN the
    // just-NULLed value and release nothing. All CTEs share one snapshot, so `key_counts`
    // aggregates the pre-NULL keys while `cleared` clears them and `slot_dec` decrements.
    diesel::sql_query(
        "WITH to_release AS (
            SELECT id, claimed_slot_keys, capacity_charge
            FROM task
            WHERE id = ANY($1) AND claimed_slot_keys IS NOT NULL
            FOR UPDATE
         ),
         key_counts AS (
            SELECT k AS lock_key,
                   SUM(CASE WHEN k LIKE 'cap:%' THEN COALESCE(capacity_charge, 0) ELSE 1 END)::int AS cnt
            FROM to_release, unnest(claimed_slot_keys) AS k
            GROUP BY k
         ),
         slot_dec AS (
            UPDATE rule_slot rs
            SET used = GREATEST(rs.used - kc.cnt, 0)
            FROM key_counts kc
            WHERE rs.lock_key = kc.lock_key
            RETURNING rs.lock_key
         ),
         cleared AS (
            UPDATE task
            SET claimed_slot_keys = NULL, capacity_charge = NULL
            WHERE id IN (SELECT id FROM to_release)
            RETURNING id
         )
         SELECT 1",
    )
    .bind::<diesel::sql_types::Array<diesel::sql_types::Uuid>, _>(task_ids)
    .execute(conn)
    .await?;
    Ok(())
}
