use super::*;
use std::net::SocketAddr;
use std::sync::Mutex;

/// What the gate was asked: url, host, port, method, headers, body.
type Asked = (
    String,
    String,
    u16,
    String,
    Vec<(String, String)>,
    Option<String>,
);

/// A gate that answers `authorize_destination` with a canned result and keeps
/// what it was asked.
#[derive(Debug)]
struct Authorizing {
    answer: Option<Result<Vec<SocketAddr>, String>>,
    seen: Mutex<Vec<Asked>>,
}

impl Authorizing {
    fn new(answer: Option<Result<Vec<SocketAddr>, String>>) -> Self {
        Self {
            answer,
            seen: Mutex::new(Vec::new()),
        }
    }
}

impl NetGate for Authorizing {
    fn can_act(&self) -> bool {
        true
    }
    fn is_rate_limited(&self) -> bool {
        false
    }
    fn record_action(&self) -> bool {
        true
    }
    fn network_needs_approval(&self) -> bool {
        false
    }
    fn local_only_block(&self, _host: &str) -> Option<String> {
        None
    }
    fn disclose(&self, _host: &str, _has_body: bool, _has_headers: bool) {}
    fn prepare_client(
        &self,
        _service: &str,
        builder: reqwest::ClientBuilder,
    ) -> reqwest::ClientBuilder {
        builder
    }
    fn timeout_client(
        &self,
        _service: &str,
        _timeout_secs: u64,
        _connect_timeout_secs: u64,
    ) -> reqwest::Client {
        reqwest::Client::new()
    }
    fn authorize_destination<'a>(
        &'a self,
        destination: &'a Destination<'a>,
    ) -> DestinationFuture<'a> {
        if let Ok(mut seen) = self.seen.lock() {
            seen.push((
                destination.url.to_string(),
                destination.host.to_string(),
                destination.port,
                destination.method.to_string(),
                destination.headers.to_vec(),
                destination.body.map(str::to_string),
            ));
        }
        Box::pin(std::future::ready(self.answer.clone()))
    }
}

fn public() -> SocketAddr {
    "93.184.216.34:443".parse().unwrap()
}

#[tokio::test]
async fn a_gate_that_takes_no_part_leaves_the_tools_own_check_in_force() {
    // The trait default is `None`, so the shared DNS check runs; an IP literal
    // needs no lookup and is returned as the single pinned address.
    let gate = Authorizing::new(None);
    let target = validate_destination(&gate, "http://93.184.216.34/x", &[], "GET", &[], None)
        .await
        .unwrap();
    assert_eq!(target.addrs, vec!["93.184.216.34:80".parse().unwrap()]);
    let blocked = validate_destination(&gate, "http://169.254.169.254/", &[], "GET", &[], None)
        .await
        .unwrap_err();
    assert!(blocked.to_string().contains("Blocked"), "{blocked}");
}

#[tokio::test]
async fn an_authorizing_gate_supplies_the_addresses_and_sees_the_request() {
    let gate = Authorizing::new(Some(Ok(vec![public()])));
    let headers = vec![("X-A".to_string(), "1".to_string())];
    let target = validate_destination(
        &gate,
        "https://example.test/path?q=1",
        &[],
        "POST",
        &headers,
        Some("payload"),
    )
    .await
    .unwrap();
    assert_eq!(target.url, "https://example.test/path?q=1");
    assert_eq!(target.host, "example.test");
    assert_eq!(target.addrs, vec![public()]);
    let seen = gate.seen.lock().unwrap();
    assert_eq!(
        seen[0],
        (
            "https://example.test/path?q=1".to_string(),
            "example.test".to_string(),
            443,
            "POST".to_string(),
            headers,
            Some("payload".to_string())
        )
    );
}

#[tokio::test]
async fn the_gates_refusal_reason_reaches_the_caller() {
    let gate = Authorizing::new(Some(Err("egress denied: Privacy".to_string())));
    let error = validate_destination(&gate, "https://example.test/", &[], "GET", &[], None)
        .await
        .unwrap_err();
    assert_eq!(error.to_string(), "egress denied: Privacy");
}

#[tokio::test]
async fn lexical_checks_run_before_the_gate_is_asked() {
    let gate = Authorizing::new(Some(Ok(vec![public()])));
    for url in [
        "ftp://example.test/",
        "http://127.0.0.1/",
        "http://localhost/",
    ] {
        assert!(
            validate_destination(&gate, url, &[], "GET", &[], None)
                .await
                .is_err(),
            "{url}"
        );
    }
    let allowed = vec!["allowed.test".to_string()];
    assert!(
        validate_destination(&gate, "https://other.test/", &allowed, "GET", &[], None)
            .await
            .is_err()
    );
    assert!(
        gate.seen.lock().unwrap().is_empty(),
        "the gate must not see a URL the tool already refused"
    );
}

#[tokio::test]
async fn a_gate_cannot_widen_the_tool_to_private_or_empty_answers() {
    for bad in [
        vec!["127.0.0.1:443".parse().unwrap()],
        vec!["10.0.0.5:443".parse().unwrap()],
        vec![public(), "169.254.169.254:443".parse().unwrap()],
        vec![],
    ] {
        let gate = Authorizing::new(Some(Ok(bad)));
        assert!(
            validate_destination(&gate, "https://example.test/", &[], "GET", &[], None)
                .await
                .is_err()
        );
    }
}
