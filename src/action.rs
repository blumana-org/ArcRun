use std::collections::HashMap;
use std::net::{IpAddr, ToSocketAddrs};

use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::{
    config::SecurityConfig,
    dtos::NewActionDto,
    metrics,
    models::{Action, ActionKindEnum, Task, TriggerCondition, TriggerKind},
};

/// HTTP method for webhook calls.
#[derive(Debug, Deserialize, Serialize, ToSchema)]
pub enum HttpVerb {
    /// HTTP GET
    Get,
    /// HTTP POST (most common for webhooks)
    Post,
    /// HTTP DELETE
    Delete,
    /// HTTP PUT
    Put,
    /// HTTP PATCH
    Patch,
}

impl From<HttpVerb> for reqwest::Method {
    fn from(verb: HttpVerb) -> Self {
        match verb {
            HttpVerb::Get => reqwest::Method::GET,
            HttpVerb::Post => reqwest::Method::POST,
            HttpVerb::Delete => reqwest::Method::DELETE,
            HttpVerb::Put => reqwest::Method::PUT,
            HttpVerb::Patch => reqwest::Method::PATCH,
        }
    }
}

/// Parameters for a Webhook action. This is the structure expected in `NewActionDto.params`
/// when `kind` is `Webhook`.
///
/// When the webhook is called, the ArcRun appends a `?handle=<host>/task/<task_uuid>`
/// query parameter to the URL. Your webhook handler should use this URL to report task
/// completion via `PATCH` or `PUT`.
///
/// ## Example
/// ```json
/// {
///   "url": "https://my-service.com/start-job",
///   "verb": "Post",
///   "body": {"job_type": "build", "ref": "main"},
///   "headers": {"Authorization": "Bearer secret-token"}
/// }
/// ```
#[derive(Debug, Deserialize, Serialize, ToSchema)]
pub struct WebhookParams {
    /// The URL to call. Must be a valid HTTP(S) URL. Internal/private IPs are blocked (SSRF protection).
    pub url: String,
    /// HTTP method to use.
    pub verb: HttpVerb,
    /// Optional JSON body to send with the request.
    pub body: Option<serde_json::Value>,
    /// Optional HTTP headers to include. Example: `{"Authorization": "Bearer xxx", "X-Custom": "value"}`.
    pub headers: Option<HashMap<String, String>>,
}

/// Build an idempotency key for a webhook trigger event.
///
/// Format:
/// - Start → `"{task_id}:start"`
/// - End+Success → `"{task_id}:end:success"`
/// - End+Failure → `"{task_id}:end:failure"`
/// - Cancel → `"{task_id}:cancel"`
pub fn idempotency_key(
    task_id: uuid::Uuid,
    trigger: &TriggerKind,
    condition: &TriggerCondition,
) -> String {
    match trigger {
        TriggerKind::Start => format!("{}:start", task_id),
        TriggerKind::End => {
            let cond = match condition {
                TriggerCondition::Success => "success",
                TriggerCondition::Failure => "failure",
            };
            format!("{}:end:{}", task_id, cond)
        }
        TriggerKind::Cancel => format!("{}:cancel", task_id),
        // BatchComplete is batch-level (not keyed by a task id); callers must use
        // `batch_complete_idempotency_key` instead. We return a clearly-marked key to
        // avoid a silent collision if this branch is ever hit by mistake.
        TriggerKind::BatchComplete => format!("{}:batch_complete_misuse", task_id),
    }
}

/// Idempotency key for a batch-complete webhook event: `batch:<batch_id>:complete`.
/// One per batch — the unique constraint makes concurrent batch-complete detection
/// (two tasks finishing "at the same time") enqueue at most one outbox row.
pub fn batch_complete_idempotency_key(batch_id: uuid::Uuid) -> String {
    format!("batch:{}:complete", batch_id)
}

/// Extra fields merged into the webhook request body for end/cancel notifications,
/// so the consumer learns the task's final state without a follow-up GET.
///
/// Merge strategy (backwards compatible): the fields below are injected into the
/// JSON body under a reserved top-level object key `arcrun`. If the action defines
/// a custom `body` object, we add the `arcrun` key alongside the existing fields
/// (existing keys are never overwritten — only a pre-existing `arcrun` key would be
/// replaced). If there is no custom body, the request body is `{"arcrun": {...}}`.
#[derive(Debug, Clone, Serialize)]
pub struct WebhookEnrichment {
    /// Final task status at the moment of the transition (e.g. "Success", "Failure", "Canceled").
    pub status: crate::models::StatusKind,
    /// When the task reached its terminal state.
    pub ended_at: Option<chrono::DateTime<chrono::Utc>>,
    /// The lifecycle trigger that produced this notification ("end" or "cancel").
    pub trigger: String,
}

/// Merge the optional [`WebhookEnrichment`] into a webhook body.
///
/// - No enrichment: returns the original body unchanged.
/// - Body is a JSON object: inserts an `arcrun` key holding the enrichment (any
///   pre-existing keys are preserved; only a prior `arcrun` key is overwritten).
/// - Body is absent or not an object: returns `{"arcrun": {...}}` (when the body is
///   a non-object value we wrap it under `body` to avoid losing it).
fn merge_enrichment(
    body: Option<serde_json::Value>,
    enrichment: Option<serde_json::Value>,
) -> Option<serde_json::Value> {
    let Some(enrichment_val) = enrichment else {
        return body;
    };
    match body {
        Some(serde_json::Value::Object(mut map)) => {
            map.insert("arcrun".to_string(), enrichment_val);
            Some(serde_json::Value::Object(map))
        }
        Some(other) => Some(serde_json::json!({ "arcrun": enrichment_val, "body": other })),
        None => Some(serde_json::json!({ "arcrun": enrichment_val })),
    }
}

#[derive(Clone)]
pub struct ActionContext {
    pub host_address: String,
    /// Controls how long a pending webhook execution can remain uncompleted
    /// before being eligible for retry.
    pub webhook_idempotency_timeout: std::time::Duration,
}

#[derive(Clone)]
pub struct ActionExecutor {
    pub ctx: ActionContext,
    pub client: reqwest::Client,
}

/// DNS resolver that refuses to hand reqwest any address in an internal/reserved
/// range (Audit 2, A5 — anti-DNS-rebinding).
///
/// Creation-time SSRF validation ([`crate::validation::validate_webhook_url`])
/// only sees the *name* a webhook was registered with; reqwest resolves that name
/// again at delivery time — possibly hours later, across retries. A rebinding
/// attacker registers `evil.example` → a public IP, passes validation, then flips
/// the record to `169.254.169.254` (cloud metadata) before delivery. This resolver
/// closes that window by re-checking every resolved IP at the moment of the request:
/// if any is internal, resolution fails, the delivery errors, and the outbox retries
/// it later (the existing at-least-once behaviour).
///
/// Operator allowlists (`ALLOWED_HOSTNAMES` / `ALLOWED_CIDRS`) are honoured here
/// too: an allowlisted hostname is resolved without any IP check, and resolved IPs
/// inside an allowed CIDR are accepted even though they are private. Without them,
/// every webhook to an internal docker/k8s service name fails at delivery while
/// passing creation-time validation (the name is not on any blocklist).
///
/// Scope / non-goals:
/// - **IP-literal URLs never reach this resolver.** reqwest's connector handles a
///   literal host directly without DNS, so `http://169.254.169.254/` is caught only
///   by the creation-time check (which now also handles IPv6 literals, A5 fix 1).
/// - **Blocked hostnames / suffixes stay a creation-time concern.** This resolver
///   filters purely on the resolved IP, never on the name (allowlist aside).
#[derive(Debug, Clone)]
struct SsrfGuardResolver {
    security: std::sync::Arc<SecurityConfig>,
}

impl reqwest::dns::Resolve for SsrfGuardResolver {
    fn resolve(&self, name: reqwest::dns::Name) -> reqwest::dns::Resolving {
        let security = self.security.clone();
        Box::pin(async move {
            let host = name.as_str().to_owned();
            let allowlisted =
                crate::validation::is_allowed_hostname(&host.to_lowercase(), &security);
            // getaddrinfo blocks — run it on the blocking pool. Port 0: reqwest
            // overrides the port from the URL afterwards.
            let addrs: Vec<std::net::SocketAddr> = tokio::task::spawn_blocking(move || {
                (host.as_str(), 0u16)
                    .to_socket_addrs()
                    .map(|it| it.collect::<Vec<_>>())
            })
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

            if !allowlisted {
                let ips: Vec<IpAddr> = addrs.iter().map(|a| a.ip()).collect();
                if let Err(bad) = crate::validation::check_resolved_ips(&ips, &security) {
                    return Err(format!(
                        "SSRF: '{}' resolved to internal/reserved IP {} — refusing to connect \
                         (allow it via ALLOWED_HOSTNAMES or ALLOWED_CIDRS if this target is trusted)",
                        name.as_str(),
                        bad
                    )
                    .into());
                }
            }

            Ok(Box::new(addrs.into_iter()) as reqwest::dns::Addrs)
        })
    }
}

impl ActionExecutor {
    /// Build an executor honouring the process-global security config
    /// ([`crate::validation`]). Used by the running server.
    pub fn new(ctx: ActionContext) -> Self {
        Self::with_security_config(ctx, &crate::validation::get_security_config())
    }

    /// Build an executor with an explicit [`SecurityConfig`].
    ///
    /// When `skip_ssrf_validation` is set (the debug/test default) the client uses
    /// reqwest's stock resolver — identical to the historical behaviour, so tests
    /// that webhook to `127.0.0.1` keep working. Otherwise the client installs
    /// [`SsrfGuardResolver`], which rejects delivery to any name resolving to an
    /// internal IP (anti-DNS-rebinding). This is also the test seam for building a
    /// strict executor without mutating the global `OnceLock`.
    pub fn with_security_config(ctx: ActionContext, security: &SecurityConfig) -> Self {
        let mut builder = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(
                crate::config::WEBHOOK_HTTP_TIMEOUT_SECS,
            ))
            .redirect(reqwest::redirect::Policy::none());
        if !security.skip_ssrf_validation {
            builder = builder.dns_resolver(SsrfGuardResolver {
                security: std::sync::Arc::new(security.clone()),
            });
        }
        let client = builder.build().expect("Failed to build HTTP client");
        Self { ctx, client }
    }

    #[tracing::instrument(name = "webhook_execute", level = "debug", skip(self, action, task), fields(task_id = %task.id, action_id = %action.id))]
    pub async fn execute(
        &self,
        action: &Action,
        task: &Task,
        idem_key: Option<&str>,
    ) -> Result<Option<NewActionDto>, String> {
        self.execute_with_enrichment(action, task, idem_key, None)
            .await
    }

    /// Like [`execute`], but optionally merges [`WebhookEnrichment`] (final task
    /// status + ended_at + trigger) into the request body under the `arcrun` key.
    /// Used by the delivery loop for end/cancel notifications.
    #[tracing::instrument(name = "webhook_execute", level = "debug", skip(self, action, task, enrichment), fields(task_id = %task.id, action_id = %action.id))]
    pub async fn execute_with_enrichment(
        &self,
        action: &Action,
        task: &Task,
        idem_key: Option<&str>,
        enrichment: Option<&WebhookEnrichment>,
    ) -> Result<Option<NewActionDto>, String> {
        match action.kind {
            ActionKindEnum::Webhook => {
                let params: WebhookParams = serde_json::from_value(action.params.clone())
                    .map_err(|e| format!("Failed to parse webhook params: {}", e))?;
                let trigger_str = match action.trigger {
                    TriggerKind::Start => "start",
                    TriggerKind::End => "end",
                    TriggerKind::Cancel => "cancel",
                    TriggerKind::BatchComplete => "batch_complete",
                };
                // Task-level webhooks carry a `?handle=` callback URL and X-Task-* headers.
                let handle = format!("{}/task/{}", &self.ctx.host_address, &task.id);
                let body = merge_enrichment(
                    params.body.clone(),
                    enrichment.map(serde_json::to_value).and_then(Result::ok),
                );
                self.send_webhook(
                    params,
                    body,
                    Some(&handle),
                    idem_key,
                    trigger_str,
                    Some(&task.id.to_string()),
                )
                .await
            }
        }
    }

    /// Execute a single batch-level webhook (`on_batch_complete`). Unlike task-level
    /// webhooks there is NO `?handle=` callback (no task to drive) and no X-Task-Id;
    /// the `arcrun` enrichment object (batch_id / counts / completed_at) is merged into
    /// the body. The `Idempotency-Key` is the batch-complete key.
    pub async fn execute_batch_action(
        &self,
        action: &NewActionDto,
        idem_key: Option<&str>,
        enrichment_value: serde_json::Value,
    ) -> Result<(), String> {
        match action.kind {
            ActionKindEnum::Webhook => {
                let params: WebhookParams = serde_json::from_value(action.params.clone())
                    .map_err(|e| format!("Failed to parse webhook params: {}", e))?;
                let body = merge_enrichment(params.body.clone(), Some(enrichment_value));
                self.send_webhook(params, body, None, idem_key, "batch_complete", None)
                    .await
                    .map(|_| ())
            }
        }
    }

    /// Core HTTP execution shared by task-level and batch-level webhooks.
    ///
    /// `handle` adds the `?handle=` query param (task-level only); `task_id_header`
    /// adds the `X-Task-Id` header. `body` is the already-merged JSON body (the
    /// caller is responsible for merging any `arcrun` enrichment).
    async fn send_webhook(
        &self,
        params: WebhookParams,
        body: Option<serde_json::Value>,
        handle: Option<&str>,
        idem_key: Option<&str>,
        trigger_str: &str,
        task_id_header: Option<&str>,
    ) -> Result<Option<NewActionDto>, String> {
        let url = params.url;
        let started_at = std::time::Instant::now();
        let mut request = self.client.request(params.verb.into(), &url);
        if let Some(handle) = handle {
            request = request.query(&[("handle", handle)]);
        }
        if let Some(body) = body {
            request = request.json(&body);
        }
        if let Some(headers) = params.headers {
            for (key, value) in headers {
                request = request.header(key, value);
            }
        }
        if let Some(key) = idem_key {
            request = request.header("Idempotency-Key", key);
        }
        if let Some(tid) = task_id_header {
            request = request.header("X-Task-Id", tid);
        }
        request = request.header("X-Task-Trigger", trigger_str);

        let response = request.send().await.map_err(|e| {
            metrics::record_webhook_execution(
                trigger_str,
                "failure",
                started_at.elapsed().as_secs_f64(),
            );
            format!("Failed to send request: {}", error_chain(&e))
        })?;
        let status = response.status();
        if status.is_redirection() {
            metrics::record_webhook_execution(
                trigger_str,
                "failure",
                started_at.elapsed().as_secs_f64(),
            );
            log::warn!(
                "Webhook returned redirect status {} — redirects are disabled for SSRF protection",
                status
            );
            return Err(format!(
                "Webhook returned redirect status {} — redirects are disabled",
                status
            ));
        }
        if status.is_success() {
            let result = match read_response_body_limited(response).await {
                Ok((body, false)) => {
                    log::debug!(
                        "Webhook returned a successful response body ({} bytes)",
                        body.len()
                    );
                    match serde_json::from_str(&body) {
                        Ok(dto) => Ok(Some(dto)),
                        Err(e) => {
                            log::debug!("Response body did not parse as NewActionDto: {}", e);
                            Ok(None)
                        }
                    }
                }
                Ok((_body, true)) if trigger_str != "start" => {
                    // Notification responses do not drive control flow. A large body is
                    // ignored after the cap instead of allocating it or retrying a
                    // delivery that already succeeded remotely.
                    log::warn!(
                        "Webhook response body exceeded {} bytes and was truncated",
                        MAX_WEBHOOK_RESPONSE_BYTES
                    );
                    Ok(None)
                }
                Ok((_body, true)) => Err(format!(
                    "on_start response body exceeds {} bytes",
                    MAX_WEBHOOK_RESPONSE_BYTES
                )),
                Err(e) => Err(e),
            };
            metrics::record_webhook_execution(
                trigger_str,
                if result.is_ok() { "success" } else { "failure" },
                started_at.elapsed().as_secs_f64(),
            );
            result
        } else {
            metrics::record_webhook_execution(
                trigger_str,
                "failure",
                started_at.elapsed().as_secs_f64(),
            );
            let body = read_response_body_limited(response)
                .await
                .map(|(body, truncated)| {
                    if truncated {
                        format!("{body}…[truncated]")
                    } else {
                        body
                    }
                })
                .unwrap_or_default();
            log::error!("Webhook response status {} (body capped): {}", status, body);
            Err(format!("Request failed with status: {}", status))
        }
    }
}

/// Hard cap for response bodies controlled by webhook targets. The on_start body may
/// contain one cancel action, so 64 KiB is ample while preventing an untrusted peer from
/// forcing an unbounded allocation or log write.
const MAX_WEBHOOK_RESPONSE_BYTES: usize = 64 * 1024;

async fn read_response_body_limited(response: reqwest::Response) -> Result<(String, bool), String> {
    let mut stream = response.bytes_stream();
    let mut body = Vec::with_capacity(4096);
    let mut truncated = false;

    while let Some(chunk) = stream.next().await {
        let chunk =
            chunk.map_err(|e| format!("Failed to read webhook response: {}", error_chain(&e)))?;
        let remaining = MAX_WEBHOOK_RESPONSE_BYTES.saturating_sub(body.len());
        if chunk.len() > remaining {
            body.extend_from_slice(&chunk[..remaining]);
            truncated = true;
            break;
        }
        body.extend_from_slice(&chunk);
    }

    Ok((String::from_utf8_lossy(&body).into_owned(), truncated))
}

/// Render an error with its full `source()` chain (`a: b: c`). reqwest's `Display`
/// stops at "error sending request for url (...)" and hides the actual cause — a
/// refused SSRF resolution, a connect timeout, a TLS failure — which made webhook
/// failures undiagnosable from the logs.
fn error_chain(err: &dyn std::error::Error) -> String {
    let mut out = err.to_string();
    let mut cur = err.source();
    while let Some(src) = cur {
        let msg = src.to_string();
        if !out.ends_with(&msg) {
            out.push_str(": ");
            out.push_str(&msg);
        }
        cur = src.source();
    }
    out
}

#[cfg(test)]
mod error_chain_tests {
    use super::error_chain;

    #[derive(Debug)]
    struct Leaf;
    impl std::fmt::Display for Leaf {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "resolved to internal IP")
        }
    }
    impl std::error::Error for Leaf {}

    #[derive(Debug)]
    struct Mid(Leaf);
    impl std::fmt::Display for Mid {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "dns error")
        }
    }
    impl std::error::Error for Mid {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            Some(&self.0)
        }
    }

    #[test]
    fn renders_every_level() {
        assert_eq!(
            error_chain(&Mid(Leaf)),
            "dns error: resolved to internal IP"
        );
        assert_eq!(error_chain(&Leaf), "resolved to internal IP");
    }
}
