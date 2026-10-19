#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

use super::*;
use crate::network::test_support::{DEFAULT_LIMITS, TestHtml, TestNetGate, pinned_to_local};
use std::sync::Mutex;

fn test_security() -> Arc<TestNetGate> {
    TestNetGate::supervised()
}

fn fetch(
    gate: Arc<TestNetGate>,
    allowed: Vec<String>,
    max: Option<usize>,
    timeout: Option<u64>,
) -> WebFetchTool {
    WebFetchTool::new(
        gate,
        allowed,
        max,
        timeout,
        DEFAULT_LIMITS,
        Arc::new(TestHtml),
    )
}

#[test]
fn web_fetch_name_and_schema() {
    let tool = fetch(test_security(), vec!["example.com".into()], None, None);
    assert_eq!(tool.name(), "web_fetch");
    let schema = tool.parameters_schema();
    assert!(schema["properties"]["url"].is_object());
    assert!(
        schema["required"]
            .as_array()
            .unwrap()
            .contains(&json!("url"))
    );
}

#[test]
fn a_host_can_opt_the_schema_into_extra_optional_arguments() {
    let tool = fetch(test_security(), vec!["example.com".into()], None, None).with_schema_property(
        "summary_focus",
        json!({"type": "string", "description": "What you need."}),
    );
    let schema = tool.parameters_schema();
    assert_eq!(schema["properties"]["summary_focus"]["type"], "string");
    assert!(
        !schema["required"]
            .as_array()
            .unwrap()
            .contains(&json!("summary_focus"))
    );
    // Without the opt-in the schema is exactly the base one.
    let plain = fetch(test_security(), vec![], None, None).parameters_schema();
    assert!(plain["properties"].get("summary_focus").is_none());
}

#[test]
fn zero_and_none_limits_fall_back_to_defaults() {
    // Callers wire these from `[http_request]`; a stale `Some(0)` is a
    // 0-byte cap (empty bodies) and a 0-second timeout (instant failure).
    // Both `None` and `Some(0)` must coerce to the shared schema defaults.
    let defaults = DEFAULT_LIMITS;
    let from_zero = fetch(
        test_security(),
        vec!["example.com".into()],
        Some(0),
        Some(0),
    );
    assert_eq!(from_zero.max_bytes, defaults.max_response_size);
    assert_eq!(from_zero.timeout_secs, defaults.timeout_secs);
    assert_ne!(from_zero.timeout_secs, 0);
    assert_ne!(from_zero.max_bytes, 0);

    let from_none = fetch(test_security(), vec!["example.com".into()], None, None);
    assert_eq!(from_none.max_bytes, defaults.max_response_size);
    assert_eq!(from_none.timeout_secs, defaults.timeout_secs);
}

#[test]
fn nonzero_limits_are_preserved() {
    let tool = fetch(
        test_security(),
        vec!["example.com".into()],
        Some(4096),
        Some(15),
    );
    assert_eq!(tool.max_bytes, 4096);
    assert_eq!(tool.timeout_secs, 15);
}

#[tokio::test]
async fn web_fetch_rejects_disallowed_domain() {
    let tool = fetch(test_security(), vec!["example.com".into()], None, None);
    let result = tool
        .execute(json!({ "url": "https://evil.test/path" }))
        .await
        .unwrap();
    assert!(result.is_error);
    assert!(result.output().contains("URL rejected"));
}

#[tokio::test]
async fn web_fetch_rejects_invalid_url() {
    let tool = fetch(test_security(), vec!["example.com".into()], None, None);
    let result = tool.execute(json!({ "url": "not-a-url" })).await.unwrap();
    assert!(result.is_error);
}

#[tokio::test]
async fn web_fetch_blocked_under_local_only_privacy_mode() {
    // Privacy epic S7 (#4441): under LocalOnly the fetch is refused with a
    // `[policy-blocked]` result before any URL validation / network.
    let tool = fetch(
        TestNetGate::local_only(),
        vec!["example.com".into()],
        None,
        None,
    );
    let result = tool
        .execute(json!({ "url": "https://example.com/data" }))
        .await
        .unwrap();
    assert!(result.is_error);
    assert!(
        result.output().contains("[policy-blocked]"),
        "got: {}",
        result.output()
    );
    assert!(
        result.output().contains("Local-only"),
        "got: {}",
        result.output()
    );
}

#[tokio::test]
async fn test_web_fetch_truncation_utf8() {
    // Mock body with multi-byte char exactly at budget
    let body = "Hello 🦀 World"; // 🦀 is at index 6-9
    let max_bytes = 8;
    // Should truncate at index 6
    let cut = floor_char_boundary(body, max_bytes);
    assert_eq!(cut, 6);
    assert_eq!(&body[..cut], "Hello ");
}

// --- content extraction ----------------------------------------------------

async fn html(body: &str, content_type: Option<&str>) -> bool {
    is_html(&HtmlProvider::Local(Arc::new(TestHtml)), body, content_type)
        .await
        .unwrap()
}

#[tokio::test]
async fn an_explicit_html_content_type_selects_markdown_conversion() {
    assert!(html("<p>hi</p>", Some("text/html; charset=utf-8")).await);
    assert!(html("<p>hi</p>", Some("application/xhtml+xml")).await);
}

#[tokio::test]
async fn an_explicit_non_html_content_type_is_taken_at_its_word() {
    // A JSON API that happens to quote markup must come back verbatim —
    // the server said what it sent, so we don't second-guess it by sniffing.
    let body = r#"{"html": "<div><p>one</p><span>two</span><a href="/x">two</a></div>"}"#;
    assert!(!html(body, Some("application/json")).await);
    assert!(!html("<p>x</p>", Some("text/plain")).await);
}

#[tokio::test]
async fn a_missing_content_type_falls_back_to_content_detection() {
    assert!(html("<!DOCTYPE html><html><body><p>hi</p></body></html>", None).await);
    assert!(!html("# Just a README\n\nSome prose.\n", None).await);
}

#[tokio::test]
async fn an_empty_content_type_does_not_veto_detection() {
    assert!(html("<!DOCTYPE html><html><body>x</body></html>", Some("")).await);
}

#[test]
fn the_schema_offers_the_raw_escape_hatch() {
    let tool = fetch(test_security(), vec![], None, None);
    let schema = tool.parameters_schema();
    assert!(
        schema["properties"].get("raw").is_some(),
        "a caller must be able to opt out of conversion: {schema}"
    );
    assert!(
        tool.description().contains("Markdown"),
        "the model needs to know what it will get back: {}",
        tool.description()
    );
}

#[test]
fn the_declared_cap_is_sized_for_extracted_markdown_not_raw_markup() {
    let tool = fetch(test_security(), vec![], None, None);
    let cap = tool
        .max_result_size_chars()
        .expect("web_fetch declares a cap");
    assert!(
        (8_000..=32_000).contains(&cap),
        "cap should sit in the same range as Hermes (15k chars) and Codex \
         (~10k tokens) budget for one result, got {cap}"
    );
}

#[tokio::test]
async fn html_is_converted_through_the_host_extractor_only_when_it_is_html() {
    let body = "<!DOCTYPE html><html><body><p>hi</p></body></html>";
    assert!(html(body, None).await);
    assert_eq!(TestHtml.to_markdown(body), "hi");
}

#[tokio::test]
async fn execute_blocks_when_rate_limited() {
    let tool = fetch(
        TestNetGate::with(crate::network::test_support::AutonomyLevel::Supervised, 0),
        vec!["example.com".into()],
        None,
        None,
    );
    let result = tool
        .execute(json!({ "url": "https://example.com/data" }))
        .await
        .unwrap();
    assert!(result.is_error);
    assert!(result.output().contains("Rate limit exceeded"));
}

// --- HTTP error statuses ---------------------------------------------------
//
// Loopback is refused by the SSRF guard, so these drive `fetch_validated`
// (everything after validation) against a one-shot local server.

/// Serves one canned response and returns the base URL.
async fn serve_once(response: &str) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let response = response.to_string();
    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut buf = vec![0u8; 8192];
        let _ = socket.read(&mut buf).await.unwrap();
        socket.write_all(response.as_bytes()).await.unwrap();
        let _ = socket.shutdown().await;
    });
    format!("http://{addr}/page")
}

#[tokio::test]
async fn an_outgoing_request_carries_a_user_agent() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let seen = Arc::new(std::sync::Mutex::new(String::new()));
    let request_log = Arc::clone(&seen);
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = Vec::new();
        let mut buf = [0u8; 1024];
        while !request.windows(4).any(|window| window == b"\r\n\r\n") {
            let n = socket.read(&mut buf).await.unwrap();
            assert!(
                n != 0,
                "peer closed before sending the complete HTTP headers"
            );
            request.extend_from_slice(&buf[..n]);
        }
        *request_log.lock().unwrap() = String::from_utf8_lossy(&request).to_string();
        socket
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
            .await
            .unwrap();
    });

    let tool = fetch(test_security(), vec![], None, None);
    let result = tool
        .fetch_validated(
            &pinned_to_local(&format!("http://{addr}/")),
            1_000_000,
            false,
        )
        .await
        .unwrap();
    assert!(!result.is_error, "got: {}", result.output());
    server.await.unwrap();

    let request = seen.lock().unwrap().to_ascii_lowercase();
    let user_agent = request
        .lines()
        .find_map(|line| line.strip_prefix("user-agent: "));
    assert_eq!(
        user_agent,
        Some(concat!("tinytools/", env!("CARGO_PKG_VERSION")))
    );
}

fn http_response(status_line: &str, headers: &str, body: &str) -> String {
    format!(
        "HTTP/1.1 {status_line}\r\n{headers}Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
}

async fn fetch_canned(response: String) -> ToolResult {
    let url = serve_once(&response).await;
    fetch(test_security(), vec![], None, None)
        .fetch_validated(&pinned_to_local(&url), 1_000_000, false)
        .await
        .unwrap()
}

#[tokio::test]
async fn a_200_is_a_successful_result_with_the_status_header() {
    let result = fetch_canned(http_response(
        "200 OK",
        "Content-Type: text/plain\r\n",
        "hello",
    ))
    .await;
    assert!(!result.is_error, "got: {}", result.output());
    assert!(result.output().starts_with("status=200 url="));
    assert!(result.output().ends_with("hello"));
}

#[tokio::test]
async fn a_403_is_an_error_naming_the_status_and_suggesting_another_source() {
    let result = fetch_canned(http_response(
        "403 Forbidden",
        "Content-Type: text/plain\r\n",
        "Access denied by bot protection",
    ))
    .await;
    assert!(result.is_error, "got: {}", result.output());
    let out = result.output();
    assert!(
        out.contains("HTTP 403 Forbidden from 127.0.0.1"),
        "got: {out}"
    );
    assert!(out.contains("refused the request"), "got: {out}");
    assert!(out.contains("another source"), "got: {out}");
    assert!(
        out.contains("Access denied by bot protection"),
        "got: {out}"
    );
}

#[tokio::test]
async fn a_429_is_an_error_that_reports_rate_limiting_and_retry_after() {
    let result = fetch_canned(http_response(
        "429 Too Many Requests",
        "Retry-After: 120\r\n",
        "",
    ))
    .await;
    assert!(result.is_error, "got: {}", result.output());
    let out = result.output();
    assert!(out.contains("HTTP 429 Too Many Requests"), "got: {out}");
    assert!(out.contains("rate limit"), "got: {out}");
    assert!(out.contains("Retry-After: 120"), "got: {out}");
}

#[tokio::test]
async fn a_429_without_retry_after_still_reports_rate_limiting() {
    let result = fetch_canned(http_response("429 Too Many Requests", "", "")).await;
    assert!(result.is_error);
    assert!(result.output().contains("rate limit"));
    assert!(!result.output().contains("Retry-After"));
}

#[tokio::test]
async fn a_404_is_an_error() {
    let result = fetch_canned(http_response(
        "404 Not Found",
        "Content-Type: text/html\r\n",
        "<!DOCTYPE html><html><body><p>No such page</p></body></html>",
    ))
    .await;
    assert!(result.is_error, "got: {}", result.output());
    let out = result.output();
    assert!(out.contains("HTTP 404 Not Found"), "got: {out}");
    // The excerpt is the page's text, not its markup.
    assert!(out.contains("No such page"), "got: {out}");
    assert!(!out.contains("<p>"), "got: {out}");
}

#[tokio::test]
async fn a_5xx_is_an_error() {
    let result = fetch_canned(http_response("503 Service Unavailable", "", "")).await;
    assert!(result.is_error);
    assert!(result.output().contains("HTTP 503 Service Unavailable"));
}

#[tokio::test]
async fn an_error_body_excerpt_is_short() {
    let long = "x".repeat(5_000);
    let result = fetch_canned(http_response(
        "500 Internal Server Error",
        "Content-Type: text/plain\r\n",
        &long,
    ))
    .await;
    assert!(result.is_error);
    assert!(
        result.output().len() < 1_000,
        "excerpt must be bounded, got {} bytes",
        result.output().len()
    );
}

#[tokio::test]
async fn a_redirect_is_still_reported_as_a_successful_result() {
    let result = fetch_canned(http_response(
        "301 Moved Permanently",
        "Location: https://example.com/new\r\n",
        "",
    ))
    .await;
    assert!(!result.is_error, "got: {}", result.output());
    assert!(result.output().contains("status=301"));
    assert!(result.output().contains("location=https://example.com/new"));
}

// --- The cap bounds the output, not the extractor's input ------------------
//
// `TestHtml::to_markdown` strips tags, so a document whose prose sits past the
// cap offset is the shape that discriminates: cutting the markup first loses
// the prose outright, cutting the rendered text keeps it.

/// Markup whose readable text sits behind `filler` bytes of attribute.
fn page_with_prose_after(filler: usize) -> String {
    format!(
        "<!DOCTYPE html><html><head><title>T</title></head><body>\
         <div data-pad=\"{}\"></div><p>the prose that matters</p></body></html>",
        "f".repeat(filler)
    )
}

#[tokio::test]
async fn the_cap_applies_to_the_extracted_text_not_the_markup() {
    // The regression this function exists for. A 432,864-byte page fetched at
    // `max_bytes: 50000` used to come back as `extracted=48B_of_432864B` — the
    // whole document reduced to its `<title>` — because the markup was cut
    // mid-DOM before the extractor ever saw it.
    let body = page_with_prose_after(4_000);
    assert!(body.len() > 1_000, "the prose must sit past the cap");

    let rendered = render_body(&HtmlProvider::Local(Arc::new(TestHtml)), body, true, 1_000)
        .await
        .unwrap();
    assert!(
        rendered.content.contains("the prose that matters"),
        "cutting the markup first would have lost this: {:?}",
        rendered.content
    );
}

#[test]
fn cutting_the_markup_first_really_does_destroy_the_extraction() {
    // The counterfactual, so the test above cannot pass vacuously: the old
    // ordering, performed by hand, loses the prose from the same document.
    let body = page_with_prose_after(4_000);
    let truncated = &body[..1_000];
    assert!(
        !TestHtml
            .to_markdown(truncated)
            .contains("the prose that matters"),
        "a pre-extraction cut is what destroyed the content"
    );
}

#[tokio::test]
async fn the_reported_length_is_the_extraction_not_the_truncation() {
    // `extracted` is pre-cap on purpose: the header's `extracted=XB_of_YB`
    // ratio describes how much of the document the extractor found, which is
    // the number that reveals a collapse. Measuring post-cap would report the
    // cap back to the caller as if it were the page.
    let body = page_with_prose_after(4_000);
    let full = TestHtml.to_markdown(&body);
    let rendered = render_body(&HtmlProvider::Local(Arc::new(TestHtml)), body, true, 4)
        .await
        .unwrap();
    assert_eq!(rendered.extracted, full.len());
    assert!(rendered.output_capped);
    assert!(rendered.content.len() <= 4);
}

#[tokio::test]
async fn an_output_within_the_cap_is_returned_whole_and_unflagged() {
    let body = page_with_prose_after(16);
    let rendered = render_body(
        &HtmlProvider::Local(Arc::new(TestHtml)),
        body,
        true,
        1_000_000,
    )
    .await
    .unwrap();
    assert!(!rendered.output_capped);
    assert!(!rendered.markup_truncated);
    assert!(rendered.content.contains("the prose that matters"));
}

#[tokio::test]
async fn markup_truncated_input_is_reported_in_the_fetch_header() {
    // Drive the body across the extractor's independent input ceiling. The
    // large attribute keeps extracted text small, while proving that the
    // extractor input itself was bounded and reported to the caller.
    let body = format!(
        "<!DOCTYPE html><html><body><div data-pad=\"{}\"></div><p>visible</p></body></html>",
        "x".repeat(EXTRACTOR_INPUT_CEILING)
    );
    assert!(body.len() > EXTRACTOR_INPUT_CEILING);
    let rendered = render_body(&HtmlProvider::Local(Arc::new(TestHtml)), body, true, 1_000)
        .await
        .unwrap();
    assert!(rendered.markup_truncated);
    let mut output = "status=200 url=https://example.com content=markdown".to_string();
    append_markup_truncation_header(&mut output, &rendered);
    assert!(
        output.contains(&format!("markup_truncated_at={EXTRACTOR_INPUT_CEILING}B")),
        "header should disclose the extractor input ceiling: {output}"
    );
}

#[tokio::test]
async fn raw_output_is_bounded_by_the_same_cap() {
    // With `raw: true` there is no extraction, so the cap applies to the body
    // itself — the one case where cutting the input and cutting the output are
    // the same act.
    let rendered = render_body(
        &HtmlProvider::Local(Arc::new(TestHtml)),
        "abcdefghij".to_string(),
        false,
        4,
    )
    .await
    .unwrap();
    assert_eq!(rendered.content, "abcd");
    assert!(rendered.output_capped);
    assert_eq!(rendered.extracted, 10);
}

#[test]
fn the_markup_ceiling_is_far_above_any_real_page() {
    // Sized against the measured worst case, not picked round. A ceiling near
    // the old default would reintroduce the bug for ordinary documents.
    const {
        assert!(EXTRACTOR_INPUT_CEILING >= 8 * 1024 * 1024);
        assert!(EXTRACTOR_INPUT_CEILING > 433_638 * 10);
    }
}

#[test]
fn the_schema_says_which_side_of_the_extractor_it_bounds() {
    // The description is a caller's only account of what the knob does, and
    // the old wording ("Truncate body at this many bytes") is what made
    // lowering it look free.
    let tool = fetch(test_security(), vec![], None, None);
    let schema = tool.parameters_schema();
    let desc = schema["properties"]["max_bytes"]["description"]
        .as_str()
        .expect("max_bytes documents itself");
    assert!(desc.contains("OUTPUT"), "{desc}");
    assert!(desc.contains("never the markup"), "{desc}");
}

#[derive(Debug)]
struct RemoteHtml {
    fail_detection: bool,
    fail_extraction: bool,
}

#[async_trait]
impl AsyncHtmlExtractor for RemoteHtml {
    async fn looks_like_html(&self, body: &str) -> anyhow::Result<bool> {
        tokio::task::yield_now().await;
        anyhow::ensure!(!self.fail_detection, "remote detection unavailable");
        Ok(TestHtml.looks_like_html(body))
    }

    async fn to_markdown(&self, body: &str) -> anyhow::Result<String> {
        tokio::task::yield_now().await;
        anyhow::ensure!(!self.fail_extraction, "remote extraction unavailable");
        Ok(TestHtml.to_markdown(body))
    }
}

fn remote_fetch(fail_detection: bool, fail_extraction: bool) -> WebFetchTool {
    WebFetchTool::new_async(
        test_security(),
        vec![],
        None,
        None,
        DEFAULT_LIMITS,
        Arc::new(RemoteHtml {
            fail_detection,
            fail_extraction,
        }),
    )
}

#[tokio::test]
async fn remote_extraction_reads_before_applying_the_output_cap() {
    let tool = remote_fetch(false, false);
    let body = page_with_prose_after(4_000);
    let url = serve_once(&http_response("200 OK", "", &body)).await;
    let result = tool
        .fetch_validated(&pinned_to_local(&url), 1_000, false)
        .await
        .unwrap();
    assert!(result.output().contains("the prose that matters"));
    assert!(result.output().contains("content=markdown"));
}

#[tokio::test]
async fn remote_detection_failure_propagates_without_returning_markup() {
    let tool = remote_fetch(true, false);
    let url = serve_once(&http_response("200 OK", "", "<html>private</html>")).await;
    let error = tool
        .fetch_validated(&pinned_to_local(&url), 1_000, false)
        .await
        .unwrap_err();
    assert_eq!(error.to_string(), "remote detection unavailable");
}

#[tokio::test]
async fn remote_extraction_failure_propagates_for_success_and_error_responses() {
    let tool = remote_fetch(false, true);
    for status in ["200 OK", "403 Forbidden"] {
        let url = serve_once(&http_response(
            status,
            "Content-Type: text/html\r\n",
            "<p>private</p>",
        ))
        .await;
        let error = tool
            .fetch_validated(&pinned_to_local(&url), 1_000, false)
            .await
            .unwrap_err();
        assert_eq!(error.to_string(), "remote extraction unavailable");
    }
}

#[tokio::test]
async fn raw_and_explicit_non_html_responses_do_not_call_the_remote_provider() {
    let tool = remote_fetch(true, true);
    for (headers, raw) in [("Content-Type: application/json\r\n", false), ("", true)] {
        let url = serve_once(&http_response("200 OK", headers, "<p>raw</p>")).await;
        let result = tool
            .fetch_validated(&pinned_to_local(&url), 1_000, raw)
            .await
            .unwrap();
        assert!(result.output().ends_with("<p>raw</p>"));
        assert!(!result.output().contains("content=markdown"));
    }
}

#[derive(Debug)]
struct PendingHtml;

#[async_trait]
impl AsyncHtmlExtractor for PendingHtml {
    async fn looks_like_html(&self, _body: &str) -> anyhow::Result<bool> {
        std::future::pending().await
    }

    async fn to_markdown(&self, _body: &str) -> anyhow::Result<String> {
        std::future::pending().await
    }
}

#[tokio::test(start_paused = true)]
async fn pending_remote_detection_and_extraction_are_bounded() {
    let tool = WebFetchTool::new_async(
        test_security(),
        vec![],
        None,
        Some(2),
        DEFAULT_LIMITS,
        Arc::new(PendingHtml),
    );
    let error = is_html(&tool.html, "<html>body</html>", None)
        .await
        .unwrap_err();
    assert_eq!(error.to_string(), "HTML detection timed out");
    let error = render_body(&tool.html, "<p>body</p>".into(), true, 1_000)
        .await
        .err()
        .unwrap();
    assert_eq!(error.to_string(), "HTML extraction timed out");
    // Error-response excerpts are governed by the same provider deadline.
    let error = error_body_excerpt(&tool.html, "<p>body</p>", Some("text/html"), false)
        .await
        .unwrap_err();
    assert_eq!(error.to_string(), "HTML extraction timed out");
}

#[derive(Debug)]
struct InputLengthHtml(Arc<Mutex<Vec<usize>>>);

#[async_trait]
impl AsyncHtmlExtractor for InputLengthHtml {
    async fn looks_like_html(&self, body: &str) -> anyhow::Result<bool> {
        self.0.lock().unwrap().push(body.len());
        Ok(true)
    }

    async fn to_markdown(&self, body: &str) -> anyhow::Result<String> {
        self.0.lock().unwrap().push(body.len());
        Ok(String::new())
    }
}

#[tokio::test]
async fn every_remote_extractor_call_receives_at_most_the_input_ceiling() {
    let lengths = Arc::new(Mutex::new(Vec::new()));
    let extractor = HtmlProvider::Async {
        extractor: Arc::new(InputLengthHtml(lengths.clone())),
        timeout: Duration::from_secs(1),
    };
    let body = "x".repeat(EXTRACTOR_INPUT_CEILING + 1);

    // Missing Content-Type takes the remote detection path.
    assert!(is_html(&extractor, &body, None).await.unwrap());
    // Error bodies can call detection and extraction directly.
    error_body_excerpt(&extractor, &body, None, false)
        .await
        .unwrap();
    // An explicit HTML error response skips detection but still caps extraction.
    error_body_excerpt(&extractor, &body, Some("text/html"), false)
        .await
        .unwrap();

    assert_eq!(
        *lengths.lock().unwrap(),
        vec![
            EXTRACTOR_INPUT_CEILING,
            EXTRACTOR_INPUT_CEILING,
            EXTRACTOR_INPUT_CEILING,
            EXTRACTOR_INPUT_CEILING,
        ]
    );
}

#[test]
fn remote_provider_timeout_uses_the_same_zero_and_none_defaults_as_http() {
    for configured in [Some(0), None, Some(7)] {
        let tool = WebFetchTool::new_async(
            test_security(),
            vec![],
            None,
            configured,
            DEFAULT_LIMITS,
            Arc::new(PendingHtml),
        );
        let HtmlProvider::Async { timeout, .. } = tool.html else {
            panic!("expected remote provider")
        };
        assert_eq!(timeout, Duration::from_secs(tool.timeout_secs));
    }
}

#[test]
fn status_of_reads_the_leading_web_fetch_error_shape() {
    assert_eq!(
        status_of("HTTP 403 Forbidden from example.com; try another source\nbody"),
        Some(403)
    );
    assert_eq!(
        status_of("  HTTP 503 Service Unavailable from a.b; retry later"),
        Some(503)
    );
}

#[test]
fn status_of_rejects_quoted_bare_or_non_error_statuses() {
    assert_eq!(status_of("403 Forbidden"), None);
    assert_eq!(status_of("HTTP 403"), None);
    assert_eq!(status_of("HTTP 200 OK from example.com; fine"), None);
    assert_eq!(status_of("HTTP 99 x from h; g"), None);
    assert_eq!(status_of("HTTP 404 Not Found from two words; g"), None);
    assert_eq!(
        status_of("error\nHTTP 403 Forbidden from example.com; quoted"),
        None
    );
}

/// As for `http_request`: the fetch connects to the vetted address and keeps
/// the host name, rather than resolving the name again.
#[tokio::test]
async fn the_fetch_connects_to_the_pinned_address() {
    let url = serve_once(&http_response(
        "200 OK",
        "Content-Type: text/plain\r\n",
        "pinned",
    ))
    .await;
    let port = reqwest::Url::parse(&url).unwrap().port().unwrap();
    let target = ValidatedUrl {
        url: format!("http://pinned.invalid:{port}/page"),
        host: "pinned.invalid".to_string(),
        addrs: vec![format!("127.0.0.1:{port}").parse().unwrap()],
    };
    let result = fetch(test_security(), vec![], None, None)
        .fetch_validated(&target, 1_000_000, false)
        .await
        .unwrap();
    assert!(!result.is_error, "{result:?}");
}
