use super::gate::{HttpLimits, NetGate, host_of};
use async_trait::async_trait;
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;
use tinytools::{PermissionLevel, Tool, ToolResult};

use tinyagents_harness::url_guard::{ValidatedUrl, normalize_allowed_domains};

use super::gate::validate_destination;

/// What a [`PaymentHook`] hands back to retry a `402 Payment Required`.
pub struct PaymentAttempt {
    /// Extra request headers that carry the payment (added to the original set).
    pub headers: Vec<(String, String)>,
    /// Called once with how the retried request came out, so the host can
    /// settle whatever ledger entry it opened when it produced `headers`.
    pub settle: Box<dyn FnOnce(PaymentOutcome) + Send>,
}

impl std::fmt::Debug for PaymentAttempt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PaymentAttempt")
            .field("headers", &self.headers.len())
            .finish_non_exhaustive()
    }
}

/// How the request retried with payment headers came out.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PaymentOutcome {
    /// HTTP status of the retried response.
    pub status: u16,
    /// Whether that status is a success.
    pub success: bool,
    /// The retried response's `PAYMENT-RESPONSE` header, verbatim, when it had one.
    pub payment_response: Option<String>,
}

/// A host's answer to a `402 Payment Required` that names a payment scheme.
///
/// The tool sees the 402, asks the hook to pay, and retries once with the
/// headers it returns. What paying means (a wallet, a ledger, a budget) is
/// entirely the host's.
#[async_trait]
pub trait PaymentHook: std::fmt::Debug + Send + Sync {
    /// Produce the payment headers for a 402 from `url`.
    ///
    /// `response_headers` are the 402 response's headers, which carry the
    /// payment requirements.
    ///
    /// # Errors
    ///
    /// The message returned to the model as the tool error.
    async fn pay(
        &self,
        url: &str,
        response_headers: &reqwest::header::HeaderMap,
    ) -> Result<PaymentAttempt, String>;
}

/// HTTP request tool for API interactions.
/// Supports GET, POST, PUT, DELETE methods with configurable security.
#[derive(Debug)]
pub struct HttpRequestTool {
    gate: Arc<dyn NetGate>,
    allowed_domains: Vec<String>,
    max_response_size: usize,
    timeout_secs: u64,
    payment: Option<Arc<dyn PaymentHook>>,
}

impl HttpRequestTool {
    /// An `http_request` tool over `gate`.
    ///
    /// A `0` `max_response_size` or `timeout_secs` falls back to `defaults`.
    pub fn new(
        gate: Arc<dyn NetGate>,
        allowed_domains: Vec<String>,
        max_response_size: usize,
        timeout_secs: u64,
        defaults: HttpLimits,
    ) -> Self {
        // Treat `0` as "use default": a 0-byte cap or 0-second timeout is never
        // a meaningful limit, only a footgun (see migration 5→6). Pull the
        // fallbacks from `HttpRequestConfig::default()` so the tool, the schema
        // default, and the migration share one source and can't drift. A `0`
        // here means a stale/invalid config slipped past the migration, so
        // surface it with a stable, grep-friendly, non-sensitive log line.
        let max_response_size = if max_response_size == 0 {
            log::warn!(
                "[tool.http_request] coercing invalid limit field=max_response_size \
                 from=0 to={} (stale/invalid config — see migration 5→6)",
                defaults.max_response_size
            );
            defaults.max_response_size
        } else {
            max_response_size
        };
        let timeout_secs = if timeout_secs == 0 {
            log::warn!(
                "[tool.http_request] coercing invalid limit field=timeout_secs \
                 from=0 to={} (stale/invalid config — see migration 5→6)",
                defaults.timeout_secs
            );
            defaults.timeout_secs
        } else {
            timeout_secs
        };
        Self {
            gate,
            allowed_domains: normalize_allowed_domains(allowed_domains),
            max_response_size,
            timeout_secs,
            payment: None,
        }
    }

    /// Answer a `402 Payment Required` through `hook` instead of returning it unpaid.
    #[must_use]
    pub fn with_payment_hook(mut self, hook: Arc<dyn PaymentHook>) -> Self {
        self.payment = Some(hook);
        self
    }

    async fn validate_url(
        &self,
        raw_url: &str,
        method: &str,
        headers: &[(String, String)],
        body: Option<&str>,
    ) -> anyhow::Result<ValidatedUrl> {
        validate_destination(
            &*self.gate,
            raw_url,
            &self.allowed_domains,
            method,
            headers,
            body,
        )
        .await
    }

    fn validate_method(&self, method: &str) -> anyhow::Result<reqwest::Method> {
        match method.to_uppercase().as_str() {
            "GET" => Ok(reqwest::Method::GET),
            "POST" => Ok(reqwest::Method::POST),
            "PUT" => Ok(reqwest::Method::PUT),
            "DELETE" => Ok(reqwest::Method::DELETE),
            "PATCH" => Ok(reqwest::Method::PATCH),
            "HEAD" => Ok(reqwest::Method::HEAD),
            "OPTIONS" => Ok(reqwest::Method::OPTIONS),
            _ => anyhow::bail!(
                "Unsupported HTTP method: {method}. Supported: GET, POST, PUT, DELETE, PATCH, HEAD, OPTIONS"
            ),
        }
    }

    fn parse_headers(&self, headers: &serde_json::Value) -> Vec<(String, String)> {
        let mut result = Vec::new();
        if let Some(obj) = headers.as_object() {
            for (key, value) in obj {
                if let Some(str_val) = value.as_str() {
                    result.push((key.clone(), str_val.to_string()));
                }
            }
        }
        result
    }

    #[allow(dead_code)]
    fn redact_headers_for_display(headers: &[(String, String)]) -> Vec<(String, String)> {
        headers
            .iter()
            .map(|(key, value)| {
                let lower = key.to_lowercase();
                let is_sensitive = lower.contains("authorization")
                    || lower.contains("api-key")
                    || lower.contains("apikey")
                    || lower.contains("token")
                    || lower.contains("secret");
                if is_sensitive {
                    (key.clone(), "***REDACTED***".into())
                } else {
                    (key.clone(), value.clone())
                }
            })
            .collect()
    }

    fn is_safe_response_header(name: &str) -> bool {
        matches!(
            name.to_ascii_lowercase().as_str(),
            "content-type"
                | "content-length"
                | "retry-after"
                | "x-ratelimit-limit"
                | "x-ratelimit-remaining"
                | "x-ratelimit-reset"
        )
    }

    async fn execute_request(
        &self,
        target: &ValidatedUrl,
        method: reqwest::Method,
        headers: Vec<(String, String)>,
        body: Option<&str>,
    ) -> anyhow::Result<reqwest::Response> {
        let builder = reqwest::Client::builder()
            .timeout(Duration::from_secs(self.timeout_secs))
            .connect_timeout(Duration::from_secs(10))
            .user_agent(super::gate::USER_AGENT)
            .redirect(reqwest::redirect::Policy::none());
        let builder = self.gate.prepare_client("tool.http_request", builder);
        // Connect only to the vetted addresses; the URL keeps its host name so
        // `Host`, SNI and certificate checks are unchanged.
        let builder = builder.resolve_to_addrs(&target.host, &target.addrs);
        let client = builder.build()?;

        let mut request = client.request(method, &target.url);

        for (key, value) in headers {
            request = request.header(&key, &value);
        }

        if let Some(body_str) = body {
            request = request.body(body_str.to_string());
        }

        Ok(request.send().await?)
    }

    /// Retry a `402 Payment Required` once with the headers `hook` produced.
    async fn handle_payment_required(
        &self,
        hook: &Arc<dyn PaymentHook>,
        initial_response: reqwest::Response,
        target: &ValidatedUrl,
        method: reqwest::Method,
        headers: Vec<(String, String)>,
        body: Option<&str>,
    ) -> Result<reqwest::Response, String> {
        log::debug!(
            "[tool.http_request] 402 received with PAYMENT-REQUIRED, attempting x402 payment for {}",
            target.url
        );

        let attempt = hook.pay(&target.url, initial_response.headers()).await?;

        let mut retry_headers = headers;
        retry_headers.extend(attempt.headers);

        let response = self
            .execute_request(target, method, retry_headers, body)
            .await
            .map_err(|e| format!("x402 retry request failed: {e}"))?;

        (attempt.settle)(PaymentOutcome {
            status: response.status().as_u16(),
            success: response.status().is_success(),
            payment_response: response
                .headers()
                .get("PAYMENT-RESPONSE")
                .and_then(|v| v.to_str().ok())
                .map(str::to_string),
        });

        Ok(response)
    }

    async fn format_response(&self, response: reqwest::Response) -> anyhow::Result<ToolResult> {
        let status = response.status();
        let status_code = status.as_u16();

        // Name *and* value. This printed the name twice — `{}: {:?}` over
        // `(k.as_str(), k.as_str())` — so every line read
        // `x-ratelimit-remaining: "x-ratelimit-remaining"` and the header block
        // carried no information at all. The redaction beside it only makes
        // sense if values were meant to be shown, which is the reading taken
        // here: rate-limit and retry-after headers are exactly what a caller
        // needs on the failures this block renders.
        let headers_text = response
            .headers()
            .iter()
            .map(|(name, value)| {
                // Response headers are untrusted and service-specific headers
                // can carry credentials under arbitrary names. Only expose
                // values from this small set of useful diagnostic headers.
                if Self::is_safe_response_header(name.as_str()) {
                    format!(
                        "{}: {}",
                        name.as_str(),
                        value.to_str().unwrap_or("<binary>")
                    )
                } else {
                    format!("{}: ***REDACTED***", name.as_str())
                }
            })
            .collect::<Vec<_>>()
            .join(", ");

        let response_text = match response.text().await {
            Ok(text) => self.truncate_response(&text),
            Err(e) => format!("[Failed to read response body: {e}]"),
        };

        let output = format!(
            "Status: {} {}\nResponse Headers: {}\n\nResponse Body:\n{}",
            status_code,
            status.canonical_reason().unwrap_or("Unknown"),
            headers_text,
            response_text
        );

        if status.is_success() {
            Ok(ToolResult::success(output))
        } else {
            // The same `output` on both arms. This returned the bare string
            // `HTTP 403` — twenty characters — having already built the status
            // line, headers and body and then dropped them, so the one part of
            // the response that said *why* never reached the caller. A real
            // case: GitHub's 403 names the missing `User-Agent` header outright,
            // with a documentation link, and all of it was discarded.
            Ok(ToolResult::error(output))
        }
    }

    fn truncate_response(&self, text: &str) -> String {
        if text.len() > self.max_response_size {
            let mut truncated = text
                .chars()
                .take(self.max_response_size)
                .collect::<String>();
            truncated.push_str("\n\n... [Response truncated due to size limit] ...");
            truncated
        } else {
            text.to_string()
        }
    }
}

#[async_trait]
impl Tool for HttpRequestTool {
    fn name(&self) -> &'static str {
        "http_request"
    }

    fn description(&self) -> &'static str {
        "Make HTTP requests to external APIs. Supports GET, POST, PUT, DELETE, PATCH, HEAD, OPTIONS methods. \
        Security constraints: allowlist-only domains, no local/private hosts, configurable timeout and response size limits."
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "url": {
                    "type": "string",
                    "description": "HTTP or HTTPS URL to request"
                },
                "method": {
                    "type": "string",
                    "description": "HTTP method (GET, POST, PUT, DELETE, PATCH, HEAD, OPTIONS)",
                    "default": "GET"
                },
                "headers": {
                    "type": "object",
                    "description": "Optional HTTP headers as key-value pairs (e.g., {\"Authorization\": \"Bearer token\", \"Content-Type\": \"application/json\"})",
                    "default": {}
                },
                "body": {
                    "type": "string",
                    "description": "Optional request body (for POST, PUT, PATCH requests)"
                }
            },
            "required": ["url"]
        })
    }

    /// Rich HTTP semantics (methods, headers, request bodies, and x402 retry)
    /// are the same Network-class risk as `curl`: read-only autonomy is blocked
    /// in `execute`, and supervised/full tiers route through ApprovalGate.
    fn external_effect_with_args(&self, _args: &serde_json::Value) -> bool {
        self.gate.network_needs_approval()
    }

    fn permission_level(&self) -> PermissionLevel {
        PermissionLevel::Write
    }

    async fn execute(&self, args: serde_json::Value) -> anyhow::Result<ToolResult> {
        let url = args
            .get("url")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("Missing 'url' parameter"))?;

        let method_str = args.get("method").and_then(|v| v.as_str()).unwrap_or("GET");
        let headers_val = args.get("headers").cloned().unwrap_or(json!({}));
        let body = args.get("body").and_then(|v| v.as_str());

        if !self.gate.can_act() {
            return Ok(ToolResult::error(
                "[policy-blocked] Action blocked: autonomy is read-only",
            ));
        }

        if !self.gate.record_action() {
            return Ok(ToolResult::error("Action blocked: rate limit exceeded"));
        }

        // Local-only enforcement (privacy epic S7, #4441): mirror the read-only
        // `can_act()` deny above — under LocalOnly, refuse the outbound request
        // before URL validation / DNS so nothing (not even a DNS lookup for the
        // host) leaves the device. The post-validation `emit_external_transfer`
        // below stays the S2 disclosure point for permitted requests.
        {
            let host = host_of(url);
            if let Some(msg) = self.gate.local_only_block(&host) {
                return Ok(ToolResult::error(msg));
            }
        }

        let request_headers = self.parse_headers(&headers_val);
        let target = match self
            .validate_url(url, method_str, &request_headers, body)
            .await
        {
            Ok(v) => v,
            Err(e) => return Ok(ToolResult::error(e.to_string())),
        };
        let url = target.url.clone();

        // Egress spine (privacy epic S2, #4436): an agent-driven HTTP request to
        // an allowlisted host leaves the device — disclose the destination and
        // everything that rides with it (body + custom headers) before the
        // round-trip.
        {
            let has_headers = headers_val.as_object().is_some_and(|h| !h.is_empty());
            self.gate
                .disclose(&host_of(&url), body.is_some(), has_headers);
        }

        let method = match self.validate_method(method_str) {
            Ok(m) => m,
            Err(e) => return Ok(ToolResult::error(e.to_string())),
        };

        let response = match self
            .execute_request(&target, method.clone(), request_headers.clone(), body)
            .await
        {
            Ok(r) => r,
            Err(e) => return Ok(ToolResult::error(format!("HTTP request failed: {e}"))),
        };

        // Payment-required: if the server returns 402 with a PAYMENT-REQUIRED
        // header and the host installed a hook, attempt to pay and retry. With
        // no hook a 402 passes through unpaid.
        let response = match &self.payment {
            Some(hook)
                if response.status() == reqwest::StatusCode::PAYMENT_REQUIRED
                    && (response.headers().get("PAYMENT-REQUIRED").is_some()
                        || response.headers().get("X-PAYMENT-REQUIRED").is_some()) =>
            {
                match self
                    .handle_payment_required(hook, response, &target, method, request_headers, body)
                    .await
                {
                    Ok(paid_response) => paid_response,
                    Err(msg) => return Ok(ToolResult::error(msg)),
                }
            }
            _ => response,
        };

        self.format_response(response).await
    }
}

#[cfg(test)]
#[path = "http_request_tests.rs"]
mod tests;
