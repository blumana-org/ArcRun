use crate::models::StatusKind;
use diesel::deserialize::{self, FromSql};
use diesel::expression::AsExpression;
use diesel::pg::{Pg, PgValue};
use diesel::serialize::{self, IsNull, Output};
use diesel::{deserialize::FromSqlRow, serialize::ToSql, sql_types::Jsonb};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::hash::{Hash, Hasher};
use std::io::Write;
use utoipa::ToSchema;

/// A concurrency control strategy. Discriminated by the `type` JSON field.
///
/// Two types are supported:
/// - `Concurency`: limits Claimed/Running tasks admitted through the same reservation key.
/// - `Capacity`: admits a task when the existing reserved remaining work is below a threshold.
///   The candidate charge is added after this check, so the total may exceed the threshold.
///   `matcher.status` must be `Running`, and tasks using Capacity must set `expected_count`.
///
/// Both can be combined on the same task (AND semantics: all rules must pass).
///
/// ## Concurency Example
/// ```json
/// {
///   "type": "Concurency",
///   "max_concurency": 1,
///   "matcher": {
///     "status": "Running",
///     "kind": "clustering",
///     "fields": ["projectId"]
///   }
/// }
/// ```
///
/// ## Capacity Example
/// ```json
/// {
///   "type": "Capacity",
///   "max_capacity": 500,
///   "matcher": {
///     "status": "Running",
///     "kind": "clustering",
///     "fields": ["projectId"]
///   }
/// }
/// ```
#[derive(Debug, Clone, Serialize, PartialEq, Deserialize, Hash, Eq, ToSchema)]
#[serde(tag = "type")]
pub enum Strategy {
    /// Limits concurrent claims admitted through the same reservation key.
    Concurency(ConcurencyRule),
    /// Gates admission on remaining work reserved through the same key.
    /// The candidate is allowed when the existing charge is below max_capacity.
    Capacity(CapacityRule),
}

/// Array of concurrency strategies attached to a task. Empty array means no concurrency constraints.
#[derive(AsExpression, FromSqlRow, Debug, Clone, Serialize, Deserialize, PartialEq, ToSchema)]
#[diesel(sql_type = Jsonb)]
#[derive(Default)]
pub struct Rules(pub Vec<Strategy>);

/// Limits the number of Claimed/Running tasks admitted through the same matcher key.
#[derive(Debug, Clone, Serialize, PartialEq, Deserialize, Hash, Eq, ToSchema)]
pub struct ConcurencyRule {
    /// Maximum number of concurrent tasks allowed. Must be a positive integer.
    /// Example: 1 means "only one at a time" (mutual exclusion).
    pub max_concurency: i32,
    /// Matcher used to build the shared reservation key. Matching tasks without this rule do not count.
    pub matcher: Matcher,
}

/// Admits a task when the existing capacity charge under the matcher key is below max_capacity.
///
/// `remaining(task) = GREATEST(COALESCE(expected_count, 0) - success - failures, 0)`
///
/// The candidate charge is added after admission; only existing reservations count in the check. The candidate MUST have `expected_count` set (non-null),
/// otherwise it is blocked. `matcher.status` must be `Running`.
#[derive(Debug, Clone, Serialize, PartialEq, Deserialize, Hash, Eq, ToSchema)]
pub struct CapacityRule {
    /// Admission threshold for existing reserved work. Adding a candidate may exceed this value.
    pub max_capacity: i32,
    /// Matcher used to build the capacity reservation key. Only claims through this key count.
    pub matcher: Matcher,
}

/// Criteria for matching tasks. Used in concurrency rules and deduplication.
///
/// Two tasks "match" if they have the same `status`, same `kind`, and identical values for all
/// keys listed in `fields` (looked up in the task's `metadata` JSON).
#[derive(Debug, Clone, Serialize, PartialEq, Deserialize, Hash, Eq, ToSchema)]
pub struct Matcher {
    /// The task status to match on. For concurrency rules, this is typically `Running`.
    /// When set to `Running`, Claimed tasks are also counted to avoid oversubscription.
    pub status: StatusKind,
    /// The task kind to match on. Must be non-empty.
    pub kind: String,
    /// Metadata field keys to compare. Two tasks match only if their metadata values are equal
    /// for ALL listed keys. Example: `["projectId", "environment"]` means two tasks match only
    /// if both their `projectId` and `environment` metadata values are identical.
    pub fields: Vec<String>,
}

impl Matcher {
    /// Extract the metadata field values specified by this matcher from the given metadata.
    /// Returns `Ok(json_object)` with only the matched fields, or `Err(field_name)` if a
    /// required field is missing from the metadata.
    ///
    /// If `fields` is empty, returns `Ok({})`.
    /// If `metadata` is null/not-an-object and fields are required, returns `Err`.
    pub fn extract_metadata_fields(
        &self,
        metadata: &serde_json::Value,
    ) -> Result<serde_json::Value, String> {
        let mut result = json!({});
        for field in &self.fields {
            match metadata.get(field) {
                Some(v) => {
                    result[field] = v.clone();
                }
                None => {
                    return Err(field.clone());
                }
            }
        }
        Ok(result)
    }
}

/// Compute a deterministic i64 advisory lock key from a matcher and task metadata.
/// The key is derived from the matcher's kind, status, and the task's metadata values
/// for the matcher's fields.
fn compute_lock_key(
    prefix: Option<&str>,
    matcher: &Matcher,
    task_metadata: &serde_json::Value,
) -> i64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    if let Some(p) = prefix {
        p.hash(&mut hasher);
    }
    matcher.kind.hash(&mut hasher);
    matcher.status.hash(&mut hasher);
    for field in &matcher.fields {
        field.hash(&mut hasher);
        if let Some(val) = task_metadata.get(field) {
            val.to_string().hash(&mut hasher);
        }
    }
    hasher.finish() as i64
}

/// Compute a deterministic i64 advisory lock key from a concurrency rule and task metadata.
pub fn concurrency_lock_key(rule: &ConcurencyRule, task_metadata: &serde_json::Value) -> i64 {
    compute_lock_key(None, &rule.matcher, task_metadata)
}

/// Compute the **canonical, collision-free textual** slot key (Audit 2, D1) for a
/// Concurrency rule + task metadata. Unlike the i64 [`concurrency_lock_key`] (which
/// only serializes an advisory lock and tolerates hash collisions), this key
/// *identifies a shared counter row* in the `rule_slot` table — a collision would
/// merge two unrelated rules onto one counter and corrupt the count, so the key must
/// be exact, injective, and deterministic.
///
/// It is a JSON-encoded tuple `conc:{"fields":<matched-fields>,"kind":<kind>,"status":<status>}`.
/// The matched-fields object is [`Matcher::extract_metadata_fields`] (serde_json's Map
/// is a `BTreeMap`, so its keys serialize in sorted order — canonical), and JSON
/// escaping makes the encoding injective even when `kind` or a field value contains a
/// delimiter character. `Err(field)` when a required metadata field is missing — the
/// caller then blocks the claim, exactly as the advisory path did.
pub fn concurrency_slot_key(
    rule: &ConcurencyRule,
    task_metadata: &serde_json::Value,
) -> Result<String, String> {
    let matched = rule.matcher.extract_metadata_fields(task_metadata)?;
    let payload = json!({
        "kind": rule.matcher.kind,
        "status": rule.matcher.status,
        "fields": matched,
    });
    Ok(format!("conc:{payload}"))
}

/// Compute the **canonical, collision-free textual** slot key (Audit 2, D1 / 7.3b) for
/// a Capacity rule + task metadata. Mirrors [`concurrency_slot_key`] but with a distinct
/// `cap:` prefix (vs `conc:`), so the two rule kinds can never share a `rule_slot` row
/// even on an identical matcher. Same canonicality guarantees: a JSON-encoded tuple
/// `cap:{"fields":<matched-fields>,"kind":<kind>,"status":<status>}` (serde_json's Map is
/// a `BTreeMap` ⇒ sorted keys ⇒ canonical), injective under JSON escaping. `Err(field)`
/// when a required metadata field is missing — the caller then blocks the claim.
pub fn capacity_slot_key(
    rule: &CapacityRule,
    task_metadata: &serde_json::Value,
) -> Result<String, String> {
    let matched = rule.matcher.extract_metadata_fields(task_metadata)?;
    let payload = json!({
        "kind": rule.matcher.kind,
        "status": rule.matcher.status,
        "fields": matched,
    });
    Ok(format!("cap:{payload}"))
}

/// Compute a deterministic i64 advisory lock key for a dedupe matcher and task
/// metadata. Used by `handle_dedupe` to serialize the check-then-act dedupe window
/// between concurrent `POST /task` requests (Audit 2 A8).
///
/// Uses a distinct "dedupe" prefix so its keys don't collide with concurrency
/// (no prefix) or capacity ("capacity") lock keys. A hash collision between two
/// *different* dedupe keys is harmless: it only causes unnecessary serialization,
/// never a false dedupe (the `COUNT(*)` remains the source of truth).
pub fn dedupe_lock_key(matcher: &Matcher, task_metadata: &serde_json::Value) -> i64 {
    compute_lock_key(Some("dedupe"), matcher, task_metadata)
}

impl ToSql<Jsonb, Pg> for Rules {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, Pg>) -> serialize::Result {
        // <serde_json::Value as FromSql<Jsonb, Pg>>::to_sql(
        // we should be able to use jsonb implementation but the borrow checker
        // doesn't like it
        out.write_all(&[1])?; // JSONB version
        out.write_all(&serde_json::to_vec(&self)?)?;
        Ok(IsNull::No)
    }
}

impl FromSql<Jsonb, Pg> for Rules {
    fn from_sql(bytes: PgValue<'_>) -> deserialize::Result<Self> {
        let value = <serde_json::Value as FromSql<Jsonb, Pg>>::from_sql(bytes)?;
        Ok(serde_json::from_value(value)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn matcher(kind: &str, fields: &[&str]) -> Matcher {
        Matcher {
            status: StatusKind::Running,
            kind: kind.to_string(),
            fields: fields.iter().map(|s| s.to_string()).collect(),
        }
    }

    /// `capacity_slot_key` is canonical (stable, sorted-field JSON) and carries the
    /// `cap:` prefix.
    #[test]
    fn capacity_slot_key_is_canonical_and_prefixed() {
        let rule = CapacityRule {
            max_capacity: 500,
            matcher: matcher("cluster", &["b", "a"]),
        };
        let meta = json!({"a": 1, "b": 2, "ignored": 9});
        let key = capacity_slot_key(&rule, &meta).unwrap();
        assert!(key.starts_with("cap:"), "must carry the cap: prefix: {key}");
        // Field order in the matcher does not change the key (extract uses matcher order,
        // but serde_json::Map serializes its keys sorted, so both encode {"a":1,"b":2}).
        let rule2 = CapacityRule {
            max_capacity: 500,
            matcher: matcher("cluster", &["a", "b"]),
        };
        assert_eq!(key, capacity_slot_key(&rule2, &meta).unwrap());
    }

    /// A Capacity key and a Concurrency key built from the SAME matcher+metadata differ
    /// only by prefix — so the two rule kinds can never share a `rule_slot` row.
    #[test]
    fn capacity_and_concurrency_keys_are_prefix_distinct() {
        let m = matcher("cluster", &["projectId"]);
        let meta = json!({"projectId": 42});
        let cap = capacity_slot_key(
            &CapacityRule {
                max_capacity: 10,
                matcher: m.clone(),
            },
            &meta,
        )
        .unwrap();
        let conc = concurrency_slot_key(
            &ConcurencyRule {
                max_concurency: 10,
                matcher: m,
            },
            &meta,
        )
        .unwrap();
        assert_ne!(cap, conc);
        assert_eq!(
            cap.strip_prefix("cap:").unwrap(),
            conc.strip_prefix("conc:").unwrap(),
            "same matcher payload, only the prefix differs"
        );
    }

    /// A missing required metadata field is an `Err(field)` — the caller blocks the claim.
    #[test]
    fn capacity_slot_key_errors_on_missing_field() {
        let rule = CapacityRule {
            max_capacity: 10,
            matcher: matcher("cluster", &["projectId"]),
        };
        let err = capacity_slot_key(&rule, &json!({"other": 1})).unwrap_err();
        assert_eq!(err, "projectId");
    }
}
