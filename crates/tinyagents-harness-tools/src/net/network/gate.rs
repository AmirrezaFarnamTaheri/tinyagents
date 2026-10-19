//! The host's say over every network tool call.
//!
//! The tools in this module reach the internet, but they do not decide whether
//! they are *allowed* to. Whether the agent may act at all, how many actions it
//! has left this hour, whether an outbound request needs a human's approval,
//! whether a privacy mode forbids leaving the device, and how the process-wide
//! proxy is applied to a client all belong to the host whose threat model and
//! configuration they depend on. A tool asks a [`NetGate`] instead.
//!
//! Like [`crate::filesystem::FsGate`], the trait carries no decision *types* of
//! its own, only booleans, strings and the `reqwest` builder the tool was
//! already holding, so a host maps its own policy onto it without translating
//! a vocabulary.

/// What these tools call themselves on the wire.
///
/// reqwest sends no `User-Agent` unless told to, and a missing one is not a
/// cosmetic omission. GitHub's REST API refuses the request outright:
///
/// ```text
/// 403 Request forbidden by administrative rules.
///     Please make sure your request has a User-Agent header
/// ```
///
/// Reproducible on demand — the same URL in the same second answers 403 with
/// no header and 200 with one — so every `api.github.com` call through these
/// tools failed, always, and the 403 was then read as a credentials problem.
/// Several other APIs require one too, and anonymous traffic is the first a
/// rate limiter penalises.
///
/// Identifying rather than disguised: a server that wants to throttle or block
/// this traffic should be able to name it.
pub(super) const USER_AGENT: &str = concat!("tinytools/", env!("CARGO_PKG_VERSION"));

/// The host policy a network tool consults before it acts.
///
/// Implementations must be cheap to call: the tools ask on every invocation.
/// All methods take `&self`; an implementation that counts actions keeps that
/// state behind interior mutability.
pub trait NetGate: std::fmt::Debug + Send + Sync {
    /// Whether the host permits the agent to act at all right now.
    ///
    /// `false` blocks the tool before it touches the network.
    fn can_act(&self) -> bool;

    /// Whether the action budget is already spent, without consuming any of it.
    fn is_rate_limited(&self) -> bool;

    /// Consume one unit of the action budget.
    ///
    /// Returns `false` when the call is over budget and must be refused.
    fn record_action(&self) -> bool;

    /// Whether an outbound request must be confirmed by a human before it runs.
    ///
    /// Drives the tools' `external_effect_with_args` flag, which routes the
    /// call through the host's approval flow.
    fn network_needs_approval(&self) -> bool;

    /// The refusal message when the host's privacy mode forbids contacting
    /// `host`, or `None` when the request may proceed.
    ///
    /// Asked before URL validation and DNS, so a refused request leaks nothing,
    /// not even a lookup.
    fn local_only_block(&self, host: &str) -> Option<String>;

    /// Tell the host that a request is about to leave the device for `host`.
    ///
    /// Observe-only: called after enforcement has already let the request
    /// through. `has_body` is true when a request body rides along and
    /// `has_headers` when caller-supplied headers do.
    fn disclose(&self, host: &str, has_body: bool, has_headers: bool);

    /// Apply the host's process-wide proxy settings for `service` to a client
    /// builder the tool has already configured.
    fn prepare_client(
        &self,
        service: &str,
        builder: reqwest::ClientBuilder,
    ) -> reqwest::ClientBuilder;

    /// A ready-made client for `service` with the given total and connect
    /// timeouts, proxied and TLS-configured the way the host does it.
    fn timeout_client(
        &self,
        service: &str,
        timeout_secs: u64,
        connect_timeout_secs: u64,
    ) -> reqwest::Client;

    /// Let the host authorize the destination of a request and say which socket
    /// addresses the connection may use.
    ///
    /// Called after the lexical checks (scheme, allowlist, literal private
    /// host) and before any DNS or connection. The answer decides how the tool
    /// proceeds:
    ///
    /// * `None` (the default): the host takes no part, and the tool resolves and
    ///   vets the host itself ([`validate_url_with_dns_check`]).
    /// * `Some(Ok(addrs))`: the host authorized the destination. The tool
    ///   connects only to `addrs` (no second DNS lookup) while keeping the URL's
    ///   host for the `Host` header, SNI and certificate checks. Addresses that
    ///   are private or local are refused regardless, so a host cannot widen the
    ///   tool beyond the public internet.
    /// * `Some(Err(reason))`: the host refused; the tool returns `reason`.
    ///
    /// A host that already holds a decision engine for destinations (a security
    /// module that resolves, applies an address floor and pins) answers here so
    /// there is one URL guard, not one per tool.
    ///
    /// [`validate_url_with_dns_check`]: crate::url_guard::validate_url_with_dns_check
    fn authorize_destination<'a>(
        &'a self,
        destination: &'a Destination<'a>,
    ) -> DestinationFuture<'a> {
        let _ = destination;
        Box::pin(std::future::ready(None))
    }
}

/// A request a tool is about to make, as the host sees it when it authorizes
/// the destination. Borrowed from the call; nothing here is retained.
#[derive(Debug, Clone, Copy)]
pub struct Destination<'a> {
    /// The URL that passed the lexical checks.
    pub url: &'a str,
    /// Its host (a name or an IPv4 literal).
    pub host: &'a str,
    /// Its port (the scheme default when the URL names none).
    pub port: u16,
    /// HTTP method.
    pub method: &'a str,
    /// Caller-supplied headers.
    pub headers: &'a [(String, String)],
    /// Request body, if any.
    pub body: Option<&'a str>,
}

/// The host's answer to [`NetGate::authorize_destination`]; see there.
pub type DestinationFuture<'a> = std::pin::Pin<
    Box<
        dyn std::future::Future<Output = Option<Result<Vec<std::net::SocketAddr>, String>>>
            + Send
            + 'a,
    >,
>;

/// Lexically validate `raw_url`, then resolve it: through the gate when the
/// host authorizes destinations, otherwise with the shared DNS check. The
/// returned addresses are the only ones the tool may connect to.
pub(super) async fn validate_destination(
    gate: &dyn NetGate,
    raw_url: &str,
    allowed_domains: &[String],
    method: &str,
    headers: &[(String, String)],
    body: Option<&str>,
) -> anyhow::Result<crate::url_guard::ValidatedUrl> {
    use crate::url_guard::{
        ValidatedUrl, extract_host, extract_port, is_private_or_local_host, validate_url,
        validate_url_with_dns_check,
    };
    let url = validate_url(raw_url, allowed_domains)?;
    let host = extract_host(&url)?;
    let port = extract_port(&url)?;
    let destination = Destination {
        url: &url,
        host: &host,
        port,
        method,
        headers,
        body,
    };
    match gate.authorize_destination(&destination).await {
        None => validate_url_with_dns_check(raw_url, allowed_domains).await,
        Some(Err(reason)) => anyhow::bail!("{reason}"),
        Some(Ok(addrs)) => {
            if addrs.is_empty() {
                anyhow::bail!("the host authorized '{host}' without an address");
            }
            if let Some(blocked) = addrs
                .iter()
                .find(|addr| is_private_or_local_host(&addr.ip().to_string()))
            {
                anyhow::bail!(
                    "Blocked local/private address {} for '{host}'",
                    blocked.ip()
                );
            }
            Ok(ValidatedUrl { url, host, addrs })
        }
    }
}

/// The host of `url`, or `"unknown"` when it does not parse or has none.
///
/// The value handed to [`NetGate::local_only_block`] and
/// [`NetGate::disclose`].
pub(crate) fn host_of(url: &str) -> String {
    reqwest::Url::parse(url)
        .ok()
        .and_then(|u| u.host_str().map(str::to_string))
        .unwrap_or_else(|| "unknown".to_string())
}

/// Response-size and timeout limits a tool falls back to when it is
/// constructed with a `0` (or absent) value.
///
/// A `0` byte cap truncates every body to nothing and a `0` second timeout
/// fails every request instantly, so neither is ever a meaningful limit; it
/// only shows up when a stale configuration slips through. The host owns what
/// the sensible default is and passes it in.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HttpLimits {
    /// Response-body cap, in bytes.
    pub max_response_size: usize,
    /// Whole-request timeout, in seconds.
    pub timeout_secs: u64,
}

#[cfg(test)]
#[path = "gate_tests.rs"]
mod tests;
