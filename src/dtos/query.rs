use serde::{Deserialize, Serialize};
use utoipa::IntoParams;

use crate::{
    config::Config,
    models::{StatusKind, WebhookExecutionStatus},
};

/// Resolved pagination parameters ready for DB queries.
pub struct Pagination {
    pub offset: i64,
    pub limit: i64,
}

/// Pagination parameters for list endpoints.
#[derive(Debug, Serialize, Deserialize, Default, IntoParams)]
pub struct PaginationDto {
    /// Page number (0-indexed). Defaults to 0.
    pub page: Option<i64>,
    /// Number of items per page. Defaults to 50, maximum 100. Values above the max are clamped.
    pub page_size: Option<i64>,
}

impl PaginationDto {
    /// Resolve raw query params into validated offset + limit, enforcing config limits.
    pub fn resolve(self, config: &Config) -> Pagination {
        let mut page_size = self.page_size.unwrap_or(config.pagination.default_per_page);
        if page_size > config.pagination.max_per_page {
            page_size = config.pagination.max_per_page;
        }
        if page_size <= 0 {
            page_size = config.pagination.default_per_page;
        }

        let mut page = self.page.unwrap_or(0).max(0);

        // Prevent overflow when computing offset = page * page_size
        if page_size > 0 && page > 0 {
            let max_page = i64::MAX / page_size;
            if page > max_page {
                page = max_page;
            }
        }

        let offset = page.saturating_mul(page_size);

        Pagination {
            offset,
            limit: page_size,
        }
    }
}

/// Filter parameters for task listing. All filters are optional and combined with AND logic.
#[derive(Debug, Serialize, Deserialize, Default, IntoParams)]
pub struct FilterDto {
    /// Filter by task name (substring match).
    pub name: Option<String>,
    /// Filter by task kind (substring match). Example: "ci", "deploy".
    pub kind: Option<String>,
    /// Filter by task status. Example: "Running", "Pending", "Success".
    pub status: Option<StatusKind>,
    /// Filter by timeout value (exact match).
    pub timeout: Option<i32>,
    /// Filter by metadata JSONB containment (`@>`). Must be a valid JSON object,
    /// e.g. `?metadata={"env":"prod"}`; matches tasks whose metadata contains it.
    /// A malformed value is rejected with 400 (not silently ignored).
    pub metadata: Option<String>,
    /// Filter by batch UUID. Use this to get all tasks from a specific batch/DAG.
    pub batch_id: Option<uuid::Uuid>,
}

/// Resolved filter with escaped/parsed values ready for DB queries.
pub struct Filter {
    pub name: String,
    pub kind: String,
    pub metadata: Option<serde_json::Value>,
    pub status: Option<StatusKind>,
    pub timeout: Option<i32>,
    pub batch_id: Option<uuid::Uuid>,
}

/// Filter parameters for `GET /webhook-deliveries`.
#[derive(Debug, Serialize, Deserialize, Default, IntoParams)]
pub struct WebhookDeliveryFilterDto {
    /// Filter by delivery status (case-insensitive). One of: `pending`, `success`,
    /// `failure`, `exhausted`. Example: `?status=exhausted`.
    pub status: Option<String>,
}

impl WebhookDeliveryFilterDto {
    /// Parse the optional `status` string (case-insensitive) into the enum.
    /// Returns `Ok(None)` when no filter is supplied, `Err` on an unknown value.
    pub fn resolve_status(&self) -> Result<Option<WebhookExecutionStatus>, String> {
        match self.status.as_deref() {
            None => Ok(None),
            Some(s) => match s.trim().to_ascii_lowercase().as_str() {
                "pending" => Ok(Some(WebhookExecutionStatus::Pending)),
                "success" => Ok(Some(WebhookExecutionStatus::Success)),
                "failure" => Ok(Some(WebhookExecutionStatus::Failure)),
                "exhausted" => Ok(Some(WebhookExecutionStatus::Exhausted)),
                other => Err(format!(
                    "invalid status '{}': expected one of pending, success, failure, exhausted",
                    other
                )),
            },
        }
    }
}

impl FilterDto {
    /// Resolve raw query params into escaped/parsed values ready for DB queries.
    ///
    /// A10: a malformed `metadata` filter is now a hard error (`Err`) instead of being
    /// silently dropped. Previously `serde_json::from_str(..).ok()` swallowed the parse
    /// error, turning `?metadata={bad}` into "no metadata filter" — so the request
    /// returned ALL tasks instead of rejecting the bad input. This mirrors how an
    /// invalid `status` is rejected (serde fails the query extraction → 400).
    pub fn resolve(self) -> Result<Filter, String> {
        let metadata =
            match self.metadata {
                None => None,
                Some(f) => Some(serde_json::from_str(&f).map_err(|e| {
                    format!("invalid `metadata` filter: expected valid JSON ({})", e)
                })?),
            };
        Ok(Filter {
            name: super::escape_like_pattern(&self.name.unwrap_or_default()),
            kind: super::escape_like_pattern(&self.kind.unwrap_or_default()),
            metadata,
            status: self.status,
            timeout: self.timeout,
            batch_id: self.batch_id,
        })
    }
}
