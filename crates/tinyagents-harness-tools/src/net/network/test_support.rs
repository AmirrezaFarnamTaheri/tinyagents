//! A small, self-contained [`NetGate`] for the network tools' unit tests.
//!
//! It models just enough of a host policy to exercise the tools: an autonomy
//! tier, an action budget, an approval flag, a local-only switch and a record
//! of what the tool disclosed. It is *not* a copy of any host's policy; the
//! host's own semantics are tested in the host, through its adapter.

#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    missing_docs,
    unreachable_pub
)]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use super::gate::{HttpLimits, NetGate};
use super::web_fetch::HtmlExtractor;

/// Prefix the fake puts on refusals a real host would mark as policy-blocked.
pub const POLICY_BLOCKED_MARKER: &str = "[policy-blocked]";

/// Autonomy tier of the fake gate.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AutonomyLevel {
    /// Nothing may run.
    ReadOnly,
    /// Outbound requests ask for approval.
    Supervised,
    /// Outbound requests run unprompted.
    Full,
}

/// The defaults the tests hand to constructors that take [`HttpLimits`].
pub const DEFAULT_LIMITS: HttpLimits = HttpLimits {
    max_response_size: 1_000_000,
    timeout_secs: 30,
};

/// A fake host policy.
#[derive(Debug)]
pub struct TestNetGate {
    autonomy: AutonomyLevel,
    max_actions: usize,
    used: AtomicUsize,
    local_only: bool,
    disclosed: Mutex<Vec<(String, bool, bool)>>,
}

impl TestNetGate {
    /// Supervised gate with an effectively unlimited budget.
    pub fn supervised() -> Arc<Self> {
        Self::with(AutonomyLevel::Supervised, 1_000_000)
    }

    /// Gate with an explicit tier and action budget.
    pub fn with(autonomy: AutonomyLevel, max_actions: usize) -> Arc<Self> {
        Arc::new(Self {
            autonomy,
            max_actions,
            used: AtomicUsize::new(0),
            local_only: false,
            disclosed: Mutex::new(Vec::new()),
        })
    }

    /// Supervised gate whose privacy mode refuses every outbound request.
    pub fn local_only() -> Arc<Self> {
        Arc::new(Self {
            autonomy: AutonomyLevel::Supervised,
            max_actions: 1_000_000,
            used: AtomicUsize::new(0),
            local_only: true,
            disclosed: Mutex::new(Vec::new()),
        })
    }

    /// Everything the tool has disclosed so far: `(host, has_body, has_headers)`.
    pub fn disclosed(&self) -> Vec<(String, bool, bool)> {
        self.disclosed.lock().unwrap().clone()
    }
}

impl NetGate for TestNetGate {
    fn can_act(&self) -> bool {
        self.autonomy != AutonomyLevel::ReadOnly
    }

    fn is_rate_limited(&self) -> bool {
        self.used.load(Ordering::SeqCst) >= self.max_actions
    }

    fn record_action(&self) -> bool {
        self.used.fetch_add(1, Ordering::SeqCst) < self.max_actions
    }

    fn network_needs_approval(&self) -> bool {
        self.autonomy == AutonomyLevel::Supervised
    }

    fn local_only_block(&self, host: &str) -> Option<String> {
        self.local_only.then(|| {
            format!("{POLICY_BLOCKED_MARKER} Local-only privacy mode blocks contacting {host}.")
        })
    }

    fn disclose(&self, host: &str, has_body: bool, has_headers: bool) {
        self.disclosed
            .lock()
            .unwrap()
            .push((host.to_string(), has_body, has_headers));
    }

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
        timeout_secs: u64,
        connect_timeout_secs: u64,
    ) -> reqwest::Client {
        reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(timeout_secs))
            .connect_timeout(std::time::Duration::from_secs(connect_timeout_secs))
            .build()
            .unwrap_or_default()
    }
}

/// An [`HtmlExtractor`] that recognises `<html` / `<!doctype html` and
/// "converts" by stripping angle-bracket tags.
#[derive(Debug, Default)]
pub struct TestHtml;

impl HtmlExtractor for TestHtml {
    fn looks_like_html(&self, body: &str) -> bool {
        let head = body.trim_start().to_ascii_lowercase();
        head.starts_with("<!doctype html") || head.starts_with("<html")
    }

    fn to_markdown(&self, html: &str) -> String {
        let mut out = String::new();
        let mut in_tag = false;
        for ch in html.chars() {
            match ch {
                '<' => in_tag = true,
                '>' => in_tag = false,
                c if !in_tag => out.push(c),
                _ => {}
            }
        }
        out
    }
}

/// A [`ValidatedUrl`] for a loopback test server: the host is the IP literal in
/// `url`, pinned to that one socket address, as the SSRF guard would return it
/// for a public literal.
pub fn pinned_to_local(url: &str) -> tinyagents_harness::url_guard::ValidatedUrl {
    let parsed = reqwest::Url::parse(url).expect("test URL parses");
    let host = parsed.host_str().expect("test URL has a host").to_string();
    let ip: std::net::IpAddr = host.parse().expect("test URL host is an IP literal");
    let port = parsed.port_or_known_default().expect("test URL has a port");
    tinyagents_harness::url_guard::ValidatedUrl {
        url: url.to_string(),
        host,
        addrs: vec![std::net::SocketAddr::new(ip, port)],
    }
}
