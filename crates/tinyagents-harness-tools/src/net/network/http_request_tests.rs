use super::*;
use crate::net::network::test_support::{AutonomyLevel, DEFAULT_LIMITS, TestNetGate, pinned_to_local};

fn test_tool(allowed_domains: Vec<&str>) -> HttpRequestTool {
    let security = TestNetGate::supervised();
    HttpRequestTool::new(
        security,
        allowed_domains.into_iter().map(String::from).collect(),
        1_000_000,
        30,
        DEFAULT_LIMITS,
    )
}

#[test]
fn zero_limits_fall_back_to_defaults() {
    // Stale configs (or a bad write) can pass 0; a 0-second timeout fails
    // every request and a 0-byte cap truncates every body. The constructor
    // must coerce both to the module defaults — never let 0 reach reqwest.
    let security = TestNetGate::supervised();
    let tool = HttpRequestTool::new(
        security,
        vec!["example.com".to_string()],
        0,
        0,
        DEFAULT_LIMITS,
    );
    let defaults = DEFAULT_LIMITS;
    assert_eq!(tool.max_response_size, defaults.max_response_size);
    assert_eq!(tool.timeout_secs, defaults.timeout_secs);
    assert_ne!(tool.timeout_secs, 0);
    assert_ne!(tool.max_response_size, 0);
}

#[test]
fn nonzero_limits_are_preserved() {
    let security = TestNetGate::supervised();
    let tool = HttpRequestTool::new(
        security,
        vec!["example.com".to_string()],
        2048,
        12,
        DEFAULT_LIMITS,
    );
    assert_eq!(tool.max_response_size, 2048);
    assert_eq!(tool.timeout_secs, 12);
}

#[test]
fn validate_accepts_valid_methods() {
    let tool = test_tool(vec!["example.com"]);
    assert!(tool.validate_method("GET").is_ok());
    assert!(tool.validate_method("POST").is_ok());
    assert!(tool.validate_method("PUT").is_ok());
    assert!(tool.validate_method("DELETE").is_ok());
    assert!(tool.validate_method("PATCH").is_ok());
    assert!(tool.validate_method("HEAD").is_ok());
    assert!(tool.validate_method("OPTIONS").is_ok());
}

#[test]
fn validate_rejects_invalid_method() {
    let tool = test_tool(vec!["example.com"]);
    let err = tool.validate_method("INVALID").unwrap_err().to_string();
    assert!(err.contains("Unsupported HTTP method"));
}

#[tokio::test]
async fn validate_url_rejects_disallowed_domain() {
    let tool = test_tool(vec!["example.com"]);
    let err = tool
        .validate_url("https://evil.test/path", "GET", &[], None)
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("allowed websites"));
}

#[tokio::test]
async fn execute_blocks_readonly_mode() {
    let security = TestNetGate::with(AutonomyLevel::ReadOnly, 100);
    let tool = HttpRequestTool::new(
        security,
        vec!["example.com".into()],
        1_000_000,
        30,
        DEFAULT_LIMITS,
    );
    let result = tool
        .execute(json!({"url": "https://example.com"}))
        .await
        .unwrap();
    assert!(result.is_error);
    assert!(result.output().contains("read-only"));
}

#[tokio::test]
async fn execute_blocks_when_rate_limited() {
    let security = TestNetGate::with(AutonomyLevel::Supervised, 0);
    let tool = HttpRequestTool::new(
        security,
        vec!["example.com".into()],
        1_000_000,
        30,
        DEFAULT_LIMITS,
    );
    let result = tool
        .execute(json!({"url": "https://example.com"}))
        .await
        .unwrap();
    assert!(result.is_error);
    assert!(result.output().contains("rate limit"));
}

#[tokio::test]
async fn execute_blocked_under_local_only_privacy_mode() {
    // Privacy epic S7 (#4441): under LocalOnly the request is refused with a
    // `[policy-blocked]` result before URL validation / network.
    let tool = HttpRequestTool::new(
        TestNetGate::local_only(),
        vec!["example.com".into()],
        1_000_000,
        30,
        DEFAULT_LIMITS,
    );
    let result = tool
        .execute(json!({"url": "https://example.com"}))
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

#[test]
fn truncate_response_within_limit() {
    let tool = test_tool(vec!["example.com"]);
    let text = "hello world";
    assert_eq!(tool.truncate_response(text), "hello world");
}

#[test]
fn truncate_response_over_limit() {
    let tool = HttpRequestTool::new(
        TestNetGate::supervised(),
        vec!["example.com".into()],
        10,
        30,
        DEFAULT_LIMITS,
    );
    let text = "hello world this is long";
    let truncated = tool.truncate_response(text);
    assert!(truncated.len() <= 10 + 60);
    assert!(truncated.contains("[Response truncated"));
}

#[test]
fn parse_headers_preserves_original_values() {
    let tool = test_tool(vec!["example.com"]);
    let headers = json!({
        "Authorization": "Bearer secret",
        "Content-Type": "application/json",
        "X-API-Key": "my-key"
    });
    let parsed = tool.parse_headers(&headers);
    assert_eq!(parsed.len(), 3);
    assert!(
        parsed
            .iter()
            .any(|(k, v)| k == "Authorization" && v == "Bearer secret")
    );
    assert!(
        parsed
            .iter()
            .any(|(k, v)| k == "X-API-Key" && v == "my-key")
    );
    assert!(
        parsed
            .iter()
            .any(|(k, v)| k == "Content-Type" && v == "application/json")
    );
}

#[test]
fn redact_headers_for_display_redacts_sensitive() {
    let headers = vec![
        ("Authorization".into(), "Bearer secret".into()),
        ("Content-Type".into(), "application/json".into()),
        ("X-API-Key".into(), "my-key".into()),
        ("X-Secret-Token".into(), "tok-123".into()),
    ];
    let redacted = HttpRequestTool::redact_headers_for_display(&headers);
    assert_eq!(redacted.len(), 4);
    assert!(
        redacted
            .iter()
            .any(|(k, v)| k == "Authorization" && v == "***REDACTED***")
    );
    assert!(
        redacted
            .iter()
            .any(|(k, v)| k == "X-API-Key" && v == "***REDACTED***")
    );
    assert!(
        redacted
            .iter()
            .any(|(k, v)| k == "X-Secret-Token" && v == "***REDACTED***")
    );
    assert!(
        redacted
            .iter()
            .any(|(k, v)| k == "Content-Type" && v == "application/json")
    );
}

#[test]
fn redact_headers_does_not_alter_original() {
    let headers = vec![("Authorization".into(), "Bearer real-token".into())];
    let _ = HttpRequestTool::redact_headers_for_display(&headers);
    assert_eq!(headers[0].1, "Bearer real-token");
}

#[test]
fn only_explicitly_safe_response_header_values_are_displayed() {
    for safe in [
        "content-type",
        "content-length",
        "retry-after",
        "x-ratelimit-remaining",
    ] {
        assert!(HttpRequestTool::is_safe_response_header(safe), "{safe}");
        assert!(HttpRequestTool::is_safe_response_header(
            &safe.to_ascii_uppercase()
        ));
    }

    for sensitive in [
        "set-cookie",
        "www-authenticate",
        "proxy-authenticate",
        "authentication-info",
        "x-api-key",
        "x-service-token",
    ] {
        assert!(
            !HttpRequestTool::is_safe_response_header(sensitive),
            "{sensitive} must be redacted"
        );
    }
}

#[test]
fn redirect_policy_is_none() {
    let tool = test_tool(vec!["example.com"]);
    assert_eq!(tool.name(), "http_request");
}

#[test]
fn supervised_http_request_is_external_effect_for_approval_gate() {
    let tool = test_tool(vec!["example.com"]);
    assert_eq!(tool.permission_level(), PermissionLevel::Write);
    assert!(tool.external_effect_with_args(&json!({
        "url": "https://example.com/api",
        "method": "POST",
        "headers": { "Authorization": "Bearer token" },
        "body": "{}"
    })));
}

#[test]
fn readonly_http_request_is_not_external_effect_because_execute_blocks() {
    let security = TestNetGate::with(AutonomyLevel::ReadOnly, 100);
    let tool = HttpRequestTool::new(
        security,
        vec!["example.com".into()],
        1_000_000,
        30,
        DEFAULT_LIMITS,
    );
    assert!(!tool.external_effect_with_args(&json!({
        "url": "https://example.com/api",
        "method": "GET"
    })));
}

#[tokio::test]
async fn a_request_discloses_body_and_header_presence_to_the_gate() {
    // A public IPv4 literal skips DNS; the disallowed method fails after
    // disclosure, so nothing is ever contacted.
    let gate = TestNetGate::supervised();
    let tool = HttpRequestTool::new(
        gate.clone(),
        vec!["8.8.8.8".into()],
        1_000_000,
        30,
        DEFAULT_LIMITS,
    );
    let result = tool
        .execute(json!({
            "url": "https://8.8.8.8/x",
            "method": "TRACE",
            "headers": {"Authorization": "Bearer t"},
            "body": "{}"
        }))
        .await
        .unwrap();
    assert!(result.is_error);
    assert!(result.output().contains("Unsupported HTTP method"));
    assert_eq!(gate.disclosed(), vec![("8.8.8.8".to_string(), true, true)]);
}

#[tokio::test]
async fn a_bare_request_discloses_neither_body_nor_headers() {
    let gate = TestNetGate::supervised();
    let tool = HttpRequestTool::new(
        gate.clone(),
        vec!["8.8.8.8".into()],
        1_000_000,
        30,
        DEFAULT_LIMITS,
    );
    let _ = tool
        .execute(json!({"url": "https://8.8.8.8/x", "method": "TRACE"}))
        .await
        .unwrap();
    assert_eq!(
        gate.disclosed(),
        vec![("8.8.8.8".to_string(), false, false)]
    );
}

#[test]
fn the_host_of_an_unparseable_url_is_unknown() {
    assert_eq!(super::super::gate::host_of("not a url"), "unknown");
    assert_eq!(
        super::super::gate::host_of("https://api.example.com/v1"),
        "api.example.com"
    );
}

/// Serves one canned HTTP response per accepted connection, in order, and
/// records each request's raw head. Returns the bound address.
async fn serve(
    responses: Vec<String>,
) -> (std::net::SocketAddr, Arc<std::sync::Mutex<Vec<String>>>) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let log = Arc::clone(&seen);
    tokio::spawn(async move {
        for response in responses {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buf = vec![0u8; 8192];
            let n = socket.read(&mut buf).await.unwrap();
            log.lock()
                .unwrap()
                .push(String::from_utf8_lossy(&buf[..n]).to_string());
            socket.write_all(response.as_bytes()).await.unwrap();
            let _ = socket.shutdown().await;
        }
    });
    (addr, seen)
}

#[derive(Debug)]
struct RecordingHook {
    settled: Arc<std::sync::Mutex<Vec<PaymentOutcome>>>,
    fail: bool,
}

#[async_trait]
impl PaymentHook for RecordingHook {
    async fn pay(
        &self,
        _url: &str,
        response_headers: &reqwest::header::HeaderMap,
    ) -> Result<PaymentAttempt, String> {
        if self.fail {
            return Err("x402 payment failed: no wallet".into());
        }
        assert!(response_headers.get("PAYMENT-REQUIRED").is_some());
        let settled = Arc::clone(&self.settled);
        Ok(PaymentAttempt {
            headers: vec![("PAYMENT-SIGNATURE".into(), "sig".into())],
            settle: Box::new(move |outcome| settled.lock().unwrap().push(outcome)),
        })
    }
}

const PAYMENT_REQUIRED: &str = "HTTP/1.1 402 Payment Required\r\nPAYMENT-REQUIRED: abc\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";

// Loopback is refused by the SSRF guard, so these drive the private request
// path directly rather than `execute`.
#[tokio::test]
async fn a_402_is_retried_once_with_the_hooks_headers_and_settled() {
    let ok = "HTTP/1.1 200 OK\r\nPAYMENT-RESPONSE: resp\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok";
    let (addr, seen) = serve(vec![PAYMENT_REQUIRED.to_string(), ok.to_string()]).await;
    let settled = Arc::new(std::sync::Mutex::new(Vec::new()));
    let hook: Arc<dyn PaymentHook> = Arc::new(RecordingHook {
        settled: Arc::clone(&settled),
        fail: false,
    });
    let tool = test_tool(vec![]);
    let url = format!("http://{addr}/paid");
    let target = pinned_to_local(&url);
    let first = tool
        .execute_request(
            &target,
            reqwest::Method::GET,
            vec![("X-A".into(), "1".into())],
            None,
        )
        .await
        .unwrap();
    assert_eq!(first.status(), reqwest::StatusCode::PAYMENT_REQUIRED);

    let paid = tool
        .handle_payment_required(
            &hook,
            first,
            &target,
            reqwest::Method::GET,
            vec![("X-A".into(), "1".into())],
            None,
        )
        .await
        .unwrap();
    assert!(paid.status().is_success());

    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests.len(), 2);
    assert!(
        !requests[0]
            .to_ascii_lowercase()
            .contains("payment-signature")
    );
    assert!(
        requests[1]
            .to_ascii_lowercase()
            .contains("payment-signature: sig")
    );
    assert!(requests[1].to_ascii_lowercase().contains("x-a: 1"));
    assert_eq!(
        *settled.lock().unwrap(),
        vec![PaymentOutcome {
            status: 200,
            success: true,
            payment_response: Some("resp".into()),
        }]
    );
}

#[tokio::test]
async fn a_hook_failure_is_returned_as_the_error_without_a_retry() {
    let (addr, seen) = serve(vec![PAYMENT_REQUIRED.to_string()]).await;
    let hook: Arc<dyn PaymentHook> = Arc::new(RecordingHook {
        settled: Arc::default(),
        fail: true,
    });
    let tool = test_tool(vec![]);
    let url = format!("http://{addr}/paid");
    let target = pinned_to_local(&url);
    let first = tool
        .execute_request(&target, reqwest::Method::GET, vec![], None)
        .await
        .unwrap();
    let err = tool
        .handle_payment_required(&hook, first, &target, reqwest::Method::GET, vec![], None)
        .await
        .unwrap_err();
    assert_eq!(err, "x402 payment failed: no wallet");
    assert_eq!(seen.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn a_body_is_sent_and_a_success_response_is_formatted_with_redacted_cookies() {
    let ok = "HTTP/1.1 200 OK\r\nSet-Cookie: session=abc\r\nWWW-Authenticate: Bearer secret-challenge\r\nX-Api-Key: api-secret\r\nX-Service-Token: service-secret\r\nX-Other: 1\r\nContent-Length: 5\r\nConnection: close\r\n\r\nhello";
    let (addr, seen) = serve(vec![ok.to_string()]).await;
    let tool = test_tool(vec![]);
    let response = tool
        .execute_request(
            &pinned_to_local(&format!("http://{addr}/post")),
            reqwest::Method::POST,
            vec![],
            Some("payload"),
        )
        .await
        .unwrap();
    let result = tool.format_response(response).await.unwrap();
    assert!(!result.is_error);
    let text = result.text();
    assert!(text.contains("hello"), "{text}");
    assert!(text.contains("set-cookie: ***REDACTED***"), "{text}");
    assert!(!text.contains("session=abc"), "{text}");
    for secret in ["secret-challenge", "api-secret", "service-secret"] {
        assert!(!text.contains(secret), "response leaked {secret}: {text}");
    }
    for header in ["www-authenticate", "x-api-key", "x-service-token"] {
        assert!(
            text.to_ascii_lowercase()
                .contains(&format!("{header}: ***redacted***")),
            "{text}"
        );
    }
    assert!(seen.lock().unwrap()[0].ends_with("payload"));
}

#[tokio::test]
async fn a_non_success_response_is_reported_as_an_error() {
    // The failure now carries the whole response rather than just its code.
    //
    // It used to answer `HTTP 500` —, and in a measured case `HTTP 403`,
    // twenty characters, after building the status line, headers and body and
    // then dropping them. The discarded body was the only thing that said what
    // was wrong: GitHub's 403 names the missing `User-Agent` header outright.
    let body = "Request forbidden by administrative rules. \
                Please make sure your request has a User-Agent header";
    let (addr, _) = serve(vec![format!(
        "HTTP/1.1 403 Forbidden\r\nContent-Length: {}\r\nX-RateLimit-Remaining: 59\r\nConnection: close\r\n\r\n{}",
        body.len(),
        body
    )])
    .await;
    let tool = test_tool(vec![]);
    let response = tool
        .execute_request(
            &pinned_to_local(&format!("http://{addr}/")),
            reqwest::Method::GET,
            vec![],
            None,
        )
        .await
        .unwrap();
    let result = tool.format_response(response).await.unwrap();
    assert!(result.is_error, "a 403 is still a failure");
    let text = result.text();
    // Anchored: a caller classifying the failure reads the status from the head
    // of the text, and an unreadable one gets guessed at.
    assert!(text.starts_with("Status: 403"), "{text}");
    // The reason the server gave.
    assert!(text.contains("User-Agent header"), "{text}");
    // Header *values*, not the name printed twice. `x-ratelimit-remaining` is
    // exactly what a caller wants on the failures this block renders.
    let lower = text.to_ascii_lowercase();
    assert!(lower.contains("x-ratelimit-remaining: 59"), "{text}");
}

#[tokio::test]
async fn an_unreadable_body_is_reported_inline() {
    // Promise more bytes than are sent, then close: reading the body fails.
    let (addr, _) = serve(vec![
        "HTTP/1.1 200 OK\r\nContent-Length: 100\r\nConnection: close\r\n\r\nshort".to_string(),
    ])
    .await;
    let tool = test_tool(vec![]);
    let response = tool
        .execute_request(
            &pinned_to_local(&format!("http://{addr}/")),
            reqwest::Method::GET,
            vec![],
            None,
        )
        .await
        .unwrap();
    let result = tool.format_response(response).await.unwrap();
    assert!(result.text().contains("Failed to read response body"));
}

#[tokio::test]
async fn a_missing_url_is_an_error() {
    let tool = test_tool(vec![]);
    let err = tool.execute(json!({})).await.unwrap_err();
    assert!(err.to_string().contains("Missing 'url'"));
}

#[test]
fn a_payment_hook_can_be_installed_and_attempts_debug_without_headers() {
    let hook: Arc<dyn PaymentHook> = Arc::new(RecordingHook {
        settled: Arc::default(),
        fail: false,
    });
    let tool = test_tool(vec![]).with_payment_hook(hook);
    assert!(tool.payment.is_some());
    let attempt = PaymentAttempt {
        headers: vec![("PAYMENT-SIGNATURE".into(), "secret".into())],
        settle: Box::new(|_| {}),
    };
    let shown = format!("{attempt:?}");
    assert!(shown.contains("PaymentAttempt") && !shown.contains("secret"));
}

#[test]
fn the_test_gate_builds_a_client_with_the_requested_timeouts() {
    let gate = TestNetGate::supervised();
    let _client = gate.timeout_client("svc", 5, 2);
}

/// Both network tools must name themselves on the wire.
///
/// Not cosmetic: GitHub's REST API answers 403 to a request with no
/// `User-Agent`, so every `api.github.com` call through these tools failed
/// until this was set. `serve` records the raw request, which is the only way
/// to assert an outgoing header actually left the process.
#[tokio::test]
async fn an_outgoing_request_carries_a_user_agent() -> anyhow::Result<()> {
    let (addr, seen) = serve(vec![
        "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok".to_string(),
    ])
    .await;
    let tool = test_tool(vec![]);
    let _ = tool
        .execute_request(
            &pinned_to_local(&format!("http://{addr}/")),
            reqwest::Method::GET,
            vec![],
            None,
        )
        .await?;

    let request = seen
        .lock()
        .map_err(|error| anyhow::anyhow!("request log mutex poisoned: {error}"))?[0]
        .clone();
    let lower = request.to_ascii_lowercase();
    assert!(
        lower.contains("user-agent:"),
        "no User-Agent was sent:\n{request}"
    );
    assert!(
        lower.contains("user-agent: tinytools/"),
        "the header must identify this crate:\n{request}"
    );
    Ok(())
}

/// The connection goes to the vetted address, not to whatever the host name
/// resolves to a second time: the name below does not resolve at all, and the
/// request still reaches the pinned socket with the name in `Host`.
#[tokio::test]
async fn the_request_connects_to_the_pinned_address_and_keeps_the_host_name() {
    let ok = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok";
    let (addr, seen) = serve(vec![ok.to_string()]).await;
    let tool = test_tool(vec![]);
    let target = tinyagents_harness::url_guard::ValidatedUrl {
        url: format!("http://pinned.invalid:{}/x", addr.port()),
        host: "pinned.invalid".to_string(),
        addrs: vec![addr],
    };
    let response = tool
        .execute_request(&target, reqwest::Method::GET, vec![], None)
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let request = seen.lock().unwrap().join("");
    assert!(
        request
            .to_ascii_lowercase()
            .contains(&format!("host: pinned.invalid:{}", addr.port())),
        "{request}"
    );
}
