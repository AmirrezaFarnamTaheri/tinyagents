//! `web_fetch` — fetch a URL and return its text body.
//!
//! Coding-harness baseline tool (issue #1205). Distinct from
//! `http_request` (full method/header surface) and `curl` (writes to
//! disk). `web_fetch` is the single-purpose "GET and read" primitive
//! the agent reaches for when researching: returns the response body
//! as text, capped, with a tiny preamble (status + final URL).
//!
//! A 4xx/5xx response is a failed fetch: it comes back as an error result
//! (`is_error`) naming the status and a short body excerpt, so a host that
//! budgets or retries on tool errors sees blocked and rate-limited pages for
//! what they are. 3xx responses are not followed and stay successful reports.

use super::gate::{HttpLimits, NetGate, USER_AGENT, host_of};
use crate::url_guard::{ValidatedUrl, normalize_allowed_domains};

use super::gate::validate_destination;
use async_trait::async_trait;
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;
use tinytools::{PermissionLevel, Tool, ToolResult};

/// How `web_fetch` turns an HTML page into something a model should read.
///
/// Content transforms are somebody else's specialty (the host wires in
/// whichever engine it has), so the tool asks instead of owning one.
pub trait HtmlExtractor: std::fmt::Debug + Send + Sync {
    /// Whether `body`, which arrived with no usable `Content-Type`, is HTML.
    fn looks_like_html(&self, body: &str) -> bool;

    /// Convert an HTML document to Markdown (links kept, scripts dropped).
    fn to_markdown(&self, html: &str) -> String;
}

/// An asynchronous, fallible HTML transform supplied by the host.
///
/// Use this interface when extraction runs in a separate service or module.
/// Failures propagate to the fetch caller; the tool does not retry locally.
#[async_trait]
pub trait AsyncHtmlExtractor: std::fmt::Debug + Send + Sync {
    /// Whether a response without a usable content type contains HTML.
    ///
    /// # Errors
    /// Returns the host's detection failure.
    async fn looks_like_html(&self, body: &str) -> anyhow::Result<bool>;

    /// Convert HTML to Markdown, preserving links and dropping scripts.
    ///
    /// # Errors
    /// Returns the host's extraction failure.
    async fn to_markdown(&self, html: &str) -> anyhow::Result<String>;
}

#[derive(Debug)]
enum HtmlProvider {
    Local(Arc<dyn HtmlExtractor>),
    Async {
        extractor: Arc<dyn AsyncHtmlExtractor>,
        timeout: Duration,
    },
}

impl HtmlProvider {
    async fn looks_like_html(&self, body: &str) -> anyhow::Result<bool> {
        match self {
            Self::Local(html) => Ok(html.looks_like_html(body)),
            Self::Async { extractor, timeout } => {
                tokio::time::timeout(*timeout, extractor.looks_like_html(body))
                    .await
                    .map_err(|_| anyhow::anyhow!("HTML detection timed out"))?
            }
        }
    }

    async fn to_markdown(&self, body: &str) -> anyhow::Result<String> {
        match self {
            Self::Local(html) => Ok(html.to_markdown(body)),
            Self::Async { extractor, timeout } => {
                tokio::time::timeout(*timeout, extractor.to_markdown(body))
                    .await
                    .map_err(|_| anyhow::anyhow!("HTML extraction timed out"))?
            }
        }
    }
}

/// Fetches a URL and returns its text body.
#[derive(Debug)]
pub struct WebFetchTool {
    gate: Arc<dyn NetGate>,
    allowed_domains: Vec<String>,
    max_bytes: usize,
    timeout_secs: u64,
    html: HtmlProvider,
    extra_properties: Vec<(String, serde_json::Value)>,
}

impl WebFetchTool {
    /// A `web_fetch` tool over `gate`.
    ///
    /// Both `None` and `Some(0)` for the limits mean "use `defaults`".
    pub fn new(
        gate: Arc<dyn NetGate>,
        allowed_domains: Vec<String>,
        max_bytes: Option<usize>,
        timeout_secs: Option<u64>,
        defaults: HttpLimits,
        html: Arc<dyn HtmlExtractor>,
    ) -> Self {
        Self::with_provider(
            gate,
            allowed_domains,
            max_bytes,
            timeout_secs,
            defaults,
            HtmlProvider::Local(html),
        )
    }

    /// A `web_fetch` tool with asynchronous HTML detection and extraction.
    ///
    /// Limits and raw-response handling match [`Self::new`]. Extraction errors
    /// propagate without invoking another provider. Both asynchronous detection
    /// and conversion are bounded by the configured request timeout.
    #[must_use]
    pub fn new_async(
        gate: Arc<dyn NetGate>,
        allowed_domains: Vec<String>,
        max_bytes: Option<usize>,
        timeout_secs: Option<u64>,
        defaults: HttpLimits,
        html: Arc<dyn AsyncHtmlExtractor>,
    ) -> Self {
        let resolved_timeout = match timeout_secs {
            Some(0) => {
                log::warn!(
                    "[tool.web_fetch] coercing invalid limit field=timeout_secs from=0 to={} \
                     (stale/invalid config — see migration 5→6)",
                    defaults.timeout_secs
                );
                defaults.timeout_secs
            }
            Some(seconds) => seconds,
            None => defaults.timeout_secs,
        };
        Self::with_provider(
            gate,
            allowed_domains,
            max_bytes,
            Some(resolved_timeout),
            defaults,
            HtmlProvider::Async {
                extractor: html,
                timeout: Duration::from_secs(resolved_timeout),
            },
        )
    }

    fn with_provider(
        gate: Arc<dyn NetGate>,
        allowed_domains: Vec<String>,
        max_bytes: Option<usize>,
        timeout_secs: Option<u64>,
        defaults: HttpLimits,
        html: HtmlProvider,
    ) -> Self {
        // Treat both `None` and `Some(0)` as "use default": callers wire these
        // from `[http_request]`, and a 0-byte cap truncates every body to
        // nothing while a 0-second timeout fails every request instantly.
        // Stale-zero configs are repaired on load (migration 5→6); this clamp
        // is the always-on guard at the point of use. The fallbacks come from
        // the host's `defaults` so the tool shares one source with the schema +
        // migration (no cross-layer drift). `Some(0)` is a genuine
        // misconfiguration, so log it (grep-friendly, no payload); a bare
        // `None` is a normal "use default" call and stays quiet.
        let max_bytes = match max_bytes {
            Some(0) => {
                log::warn!(
                    "[tool.web_fetch] coercing invalid limit field=max_bytes \
                     from=0 to={} (stale/invalid config — see migration 5→6)",
                    defaults.max_response_size
                );
                defaults.max_response_size
            }
            Some(n) => n,
            None => defaults.max_response_size,
        };
        let timeout_secs = match timeout_secs {
            Some(0) => {
                log::warn!(
                    "[tool.web_fetch] coercing invalid limit field=timeout_secs \
                     from=0 to={} (stale/invalid config — see migration 5→6)",
                    defaults.timeout_secs
                );
                defaults.timeout_secs
            }
            Some(n) => n,
            None => defaults.timeout_secs,
        };
        Self {
            gate,
            allowed_domains: normalize_allowed_domains(allowed_domains),
            max_bytes,
            timeout_secs,
            html,
            extra_properties: Vec::new(),
        }
    }

    /// Add a property to the advertised parameter schema (not `required`).
    ///
    /// The host uses this to opt the tool into arguments it interprets itself
    /// before the call reaches the tool.
    #[must_use]
    pub fn with_schema_property(mut self, name: &str, schema: serde_json::Value) -> Self {
        self.extra_properties.push((name.to_string(), schema));
        self
    }
}

#[async_trait]
impl Tool for WebFetchTool {
    fn name(&self) -> &'static str {
        "web_fetch"
    }

    fn description(&self) -> &'static str {
        "GET a URL and read the page. HTML returns as Markdown (links kept, \
         scripts dropped); `raw: true` for the body as sent. For POST or \
         custom headers use `http_request`."
    }

    fn parameters_schema(&self) -> serde_json::Value {
        let mut schema = json!({
            "type": "object",
            "properties": {
                "url": { "type": "string", "description": "Absolute http(s) URL." },
                "max_bytes": {
                    "type": "integer",
                    "description": "Cap the returned text at this many bytes \
                     (default 1_000_000). Bounds the OUTPUT — the extracted \
                     markdown, or the raw body with raw:true — never the markup \
                     the extractor reads, so lowering it cannot cost you content \
                     the page actually had.",
                    "minimum": 1
                },
                "raw": {
                    "type": "boolean",
                    "description": "Return the body as sent."
                }
            },
            "required": ["url"]
        });
        if let Some(properties) = schema["properties"].as_object_mut() {
            for (name, property) in &self.extra_properties {
                properties.insert(name.clone(), property.clone());
            }
        }
        schema
    }

    fn permission_level(&self) -> PermissionLevel {
        PermissionLevel::ReadOnly
    }

    /// Idempotent GET — safe to fan out across parallel `web_fetch`
    /// calls. Targets that throttle aggressively are the user's
    /// concern; we don't try to second-guess at the tool layer.
    fn is_concurrency_safe(&self, _args: &serde_json::Value) -> bool {
        true
    }

    /// How much of a page reaches the model in one result.
    ///
    /// Extraction does most of the work — after HTML→Markdown a long
    /// documentation page is usually a few thousand chars — so this
    /// bites only on genuinely large documents. What it no longer does
    /// is throw the remainder away: `ToolOutputMiddleware` spills the
    /// full extracted page to an artifact and returns the `file_read`
    /// call that pages it, which is what this comment used to
    /// recommend while the cap itself disabled the affordance.
    ///
    /// 24k rather than the old 50k because the point of reference
    /// moved: 50k was a bound on raw markup, this is clean Markdown.
    /// Hermes budgets 15,000 chars of extracted text for the same job;
    /// Codex caps every tool result at ~10,000 tokens.
    fn max_result_size_chars(&self) -> Option<usize> {
        Some(24_000)
    }

    async fn execute(&self, args: serde_json::Value) -> anyhow::Result<ToolResult> {
        let raw_url = args
            .get("url")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("Missing 'url' parameter"))?;
        let max_bytes = args
            .get("max_bytes")
            .and_then(serde_json::Value::as_u64)
            .map_or(self.max_bytes, |n| (n as usize).max(1));
        let raw_requested = args
            .get("raw")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false);

        if self.gate.is_rate_limited() {
            return Ok(ToolResult::error(
                "Rate limit exceeded: too many actions in the last hour",
            ));
        }
        if !self.gate.record_action() {
            return Ok(ToolResult::error(
                "Rate limit exceeded: action budget exhausted",
            ));
        }

        // Local-only enforcement (privacy epic S7, #4441): refuse the fetch under
        // LocalOnly before URL validation / DNS. The post-validation
        // `emit_external_transfer` below stays the S2 disclosure point.
        {
            let host = host_of(raw_url);
            if let Some(msg) = self.gate.local_only_block(&host) {
                return Ok(ToolResult::error(msg));
            }
        }

        let target = match validate_destination(
            &*self.gate,
            raw_url,
            &self.allowed_domains,
            "GET",
            &[],
            None,
        )
        .await
        {
            Ok(u) => u,
            Err(e) => return Ok(ToolResult::error(format!("URL rejected: {e}"))),
        };
        let url = target.url.clone();

        // Egress spine (privacy epic S2, #4436): disclose the fetch destination
        // before contacting the host.
        self.gate.disclose(&host_of(&url), false, false);

        self.fetch_validated(&target, max_bytes, raw_requested)
            .await
    }
}

impl WebFetchTool {
    /// Issue the GET for a URL that already passed the gate and the SSRF
    /// guard, and render the response for the model.
    async fn fetch_validated(
        &self,
        target: &ValidatedUrl,
        max_bytes: usize,
        raw_requested: bool,
    ) -> anyhow::Result<ToolResult> {
        // Disable automatic redirect following: reqwest follows up to 10
        // redirects by default, and a redirect target may be on a host
        // outside the allowed-domains list. We surface 3xx responses to
        // the caller so they can decide whether to refetch the new URL.
        let client = match reqwest::Client::builder()
            .timeout(Duration::from_secs(self.timeout_secs))
            .user_agent(USER_AGENT)
            .redirect(reqwest::redirect::Policy::none())
            // Connect only to the vetted addresses; the URL keeps its host name.
            .resolve_to_addrs(&target.host, &target.addrs)
            .build()
        {
            Ok(c) => c,
            Err(e) => return Ok(ToolResult::error(format!("Failed to build client: {e}"))),
        };

        let url = target.url.as_str();
        let resp = match client.get(url).send().await {
            Ok(r) => r,
            Err(e) => return Ok(ToolResult::error(format!("Request failed: {e}"))),
        };
        let status = resp.status();
        let final_url = resp.url().to_string();
        let location = resp
            .headers()
            .get(reqwest::header::LOCATION)
            .and_then(|v| v.to_str().ok())
            .map(str::to_string);
        let content_type = resp
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .map(str::to_string);
        let retry_after = resp
            .headers()
            .get(reqwest::header::RETRY_AFTER)
            .and_then(|v| v.to_str().ok())
            .map(str::to_string);
        let body = match resp.text().await {
            Ok(b) => b,
            Err(e) => return Ok(ToolResult::error(format!("Failed to read body: {e}"))),
        };

        if let Some(loc) = &location
            && status.is_redirection()
        {
            return Ok(ToolResult::success(format!(
                "status={} url={} location={loc}\n[redirect not followed — re-call web_fetch with the location URL if it's an allowed domain]",
                status.as_u16(),
                final_url
            )));
        }

        // A 4xx/5xx is a failed fetch, not a page: returning it as a success
        // let a blocked or rate-limited site count as research done. (3xx is
        // handled above and stays a successful "not followed" report.)
        if status.is_client_error() || status.is_server_error() {
            let host = host_of(&final_url);
            log::debug!(
                "[tool.web_fetch] http error status={} host={host} retry_after_present={}",
                status.as_u16(),
                retry_after.is_some()
            );
            let excerpt =
                error_body_excerpt(&self.html, &body, content_type.as_deref(), raw_requested)
                    .await?;
            return Ok(ToolResult::error(http_error_message(
                status,
                &host,
                retry_after.as_deref(),
                &excerpt,
            )));
        }

        let downloaded = body.len();

        // Markdown by default. A page's prose is a small fraction of its
        // bytes; handing the raw document to the model (and to the payload
        // summarizer behind it) is how one research turn came to cost
        // 1,083,069 input tokens. The host's `HtmlExtractor` owns
        // content transforms.
        let converted =
            !raw_requested && is_html(&self.html, &body, content_type.as_deref()).await?;
        let rendered = render_body(&self.html, body, converted, max_bytes).await?;

        let extracted = rendered.extracted;
        let mut header = format!("status={} url={final_url}", status.as_u16());
        if converted {
            header.push_str(" content=markdown");
        }
        // `output_capped_at`, not the old `download_capped_at`: nothing here
        // ever capped a download — `resp.text()` above materialises the whole
        // body regardless — and naming it that sent a reader looking in the
        // wrong place for the content that went missing.
        if rendered.output_capped {
            header.push_str(&format!(" output_capped_at={max_bytes}B"));
        }
        append_markup_truncation_header(&mut header, &rendered);
        if converted && extracted < downloaded {
            header.push_str(&format!(" extracted={extracted}B_of_{downloaded}B"));
        }
        header.push('\n');
        let content = rendered.content;

        // Full extracted content. Bounding it — the head/tail window, the
        // spill to an artifact and the paging handle — belongs to
        // `ToolOutputMiddleware`, which applies one rule to every tool.
        // Codex enforces exactly this invariant at a single chokepoint
        // (`context_manager/history.rs`), which is why a tool there cannot
        // leak an unbounded payload however it misbehaves.
        Ok(ToolResult::success(format!("{header}{content}")))
    }
}

/// How much of an error response's body is quoted back to the model.
const ERROR_EXCERPT_CHARS: usize = 300;

/// The model-facing text for a 4xx/5xx response: what happened, why it
/// matters, and what to do next, plus a short excerpt of the body.
fn http_error_message(
    status: reqwest::StatusCode,
    host: &str,
    retry_after: Option<&str>,
    excerpt: &str,
) -> String {
    let code = status.as_u16();
    let reason = status.canonical_reason().unwrap_or("Unknown Status");
    let mut msg = format!("HTTP {code} {reason} from {host}; ");
    match code {
        429 => {
            msg.push_str("the site is rate limiting requests.");
            if let Some(wait) = retry_after.map(str::trim).filter(|w| !w.is_empty()) {
                msg.push_str(&format!(" Retry-After: {wait}."));
            }
            msg.push_str(" Try another source, or retry later.");
        }
        401 | 403 => msg.push_str("the site refused the request. Try another source."),
        404 | 410 => {
            msg.push_str(
                "the page does not exist at this URL. Check the URL or try another source.",
            );
        }
        500..=599 => {
            msg.push_str(
                "the server failed to handle the request. Retry later or try another source.",
            );
        }
        _ => msg.push_str("the server rejected the request. Try another source."),
    }
    if !excerpt.is_empty() {
        msg.push_str("\nResponse excerpt: ");
        msg.push_str(excerpt);
    }
    msg
}

/// A short, single-line, text-only excerpt of an error response body.
///
/// HTML goes through the host extractor (unless the caller asked for `raw`)
/// so the model reads the page's words rather than its markup.
async fn error_body_excerpt(
    extractor: &HtmlProvider,
    body: &str,
    content_type: Option<&str>,
    raw_requested: bool,
) -> anyhow::Result<String> {
    let (bounded_body, _) = cap_extractor_input(body);
    let text = if !raw_requested && is_html(extractor, bounded_body, content_type).await? {
        extractor.to_markdown(bounded_body).await?
    } else {
        body.to_string()
    };
    let collapsed = text.split_whitespace().collect::<Vec<_>>().join(" ");
    Ok(match collapsed.char_indices().nth(ERROR_EXCERPT_CHARS) {
        Some((cut, _)) => format!("{}...", &collapsed[..cut]),
        None => collapsed,
    })
}

/// Raw markup handed to the HTML extractor, at most.
///
/// Not a byte budget — the caller's `max_bytes` owns that, applied to the
/// output. This exists only so a pathological document cannot cost unbounded
/// CPU in `to_markdown`, a cost the old pre-truncation ordering hid by
/// accident. 8 MiB is ~19x the largest real page measured here (a 434 KB
/// client-rendered spreadsheet) and ~8x the default `max_response_size`, so no
/// realistic page reaches it.
const EXTRACTOR_INPUT_CEILING: usize = 8 * 1024 * 1024;

/// A body turned into what the caller reads.
struct RenderedBody {
    /// The text to return, bounded by the caller's `max_bytes`.
    content: String,
    /// Length *before* that bound, so the header's ratio describes the
    /// extraction rather than the truncation.
    extracted: usize,
    /// Whether `content` was cut to fit `max_bytes`.
    output_capped: bool,
    /// Whether the markup was cut before the extractor saw it, which only
    /// happens past [`EXTRACTOR_INPUT_CEILING`].
    markup_truncated: bool,
}

/// Add the extractor input ceiling to a fetch header when markup was cut.
fn append_markup_truncation_header(header: &mut String, rendered: &RenderedBody) {
    if rendered.markup_truncated {
        header.push_str(&format!(" markup_truncated_at={EXTRACTOR_INPUT_CEILING}B"));
    }
}

/// Convert, **then** bound.
///
/// The order is the whole of this function. It used to be the other way round,
/// and the cap was therefore destroying the thing it was meant to measure: a
/// 432,864-byte client-rendered page fetched with `max_bytes: 50000` was cut at
/// byte 50,000 — mid-tag, mid-DOM — and the readability pass, handed that
/// wreckage, recovered only the `<title>`. The result was
/// `extracted=48B_of_432864B`, reported as `status=200`. The same URL with no
/// `max_bytes` yields `extracted=37243B_of_433638B`: the real document.
/// Identical tool, identical extractor, 776x the content, one parameter.
///
/// So a caller setting a sensible cost bound silently lost the page, and the
/// knob it would then reach for — raising the cap — was the right knob turned
/// too timidly, which is the worst case for learning anything from the failure.
///
/// Bounding the output is also what the schema promises, and it costs no
/// memory: the caller has already materialised the whole body. Only the
/// extractor's input needs a ceiling, and that is for CPU.
async fn render_body(
    html: &HtmlProvider,
    body: String,
    converted: bool,
    max_bytes: usize,
) -> anyhow::Result<RenderedBody> {
    let (markup_truncated, body) = if converted {
        let (body, truncated) = cap_extractor_input(&body);
        (truncated, body.to_string())
    } else {
        (false, body)
    };
    let full = if converted {
        html.to_markdown(&body).await?
    } else {
        body
    };
    let extracted = full.len();
    let (content, output_capped) = if extracted > max_bytes {
        let cut = floor_char_boundary(&full, max_bytes);
        (full[..cut].to_string(), true)
    } else {
        (full, false)
    };
    Ok(RenderedBody {
        content,
        extracted,
        output_capped,
        markup_truncated,
    })
}

/// Bound every body before it crosses into a host supplied extractor.
fn cap_extractor_input(body: &str) -> (&str, bool) {
    if body.len() > EXTRACTOR_INPUT_CEILING {
        let cut = floor_char_boundary(body, EXTRACTOR_INPUT_CEILING);
        (&body[..cut], true)
    } else {
        (body, false)
    }
}

/// The largest index at or below `index` that is a char boundary of `s`.
fn floor_char_boundary(s: &str, index: usize) -> usize {
    if index >= s.len() {
        return s.len();
    }
    let mut end = index;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    end
}

/// Is this HTML? The server's own `Content-Type` is authoritative when it
/// says so; otherwise fall back to the host's content detection, which
/// already distinguishes HTML from JSON, diffs and code.
async fn is_html(
    extractor: &HtmlProvider,
    body: &str,
    content_type: Option<&str>,
) -> anyhow::Result<bool> {
    if let Some(ct) = content_type {
        let ct = ct.to_ascii_lowercase();
        let mime = ct.split(';').next().unwrap_or("").trim().to_string();
        // An explicit non-HTML type is a statement, not a guess: a JSON API
        // that happens to embed markup must come back verbatim.
        if !mime.is_empty() && mime != "text/html" && mime != "application/xhtml+xml" {
            return Ok(false);
        }
        if !mime.is_empty() {
            return Ok(true);
        }
    }
    let (body, _) = cap_extractor_input(body);
    extractor.looks_like_html(body).await
}

/// The HTTP status of a `web_fetch` error result. This remains available from
/// the producer module, while the single parser lives in `tinytools::status`
/// so lightweight hosts can consume the result contract without depending on
/// `tinytools-std`.
pub use tinytools::status::web_fetch_status as status_of;

#[cfg(test)]
#[path = "web_fetch_tests.rs"]
mod tests;
