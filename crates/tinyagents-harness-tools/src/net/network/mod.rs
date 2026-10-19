//! Network tools: `http_request`, `web_fetch`, `curl` and `pushover`.
//!
//! Every tool takes an `Arc<dyn NetGate>`: the tool performs the I/O, the
//! host's gate decides whether it may (autonomy, action budget, approval,
//! privacy mode) and how the process-wide proxy is applied. The gate trait
//! ([`NetGate`]) is the whole seam; nothing here knows what an autonomy level,
//! an approval prompt or an egress descriptor is. URL allowlisting and SSRF
//! checks are [`crate::url_guard`]'s.
//!
//! Two more seams keep host-specific behavior out of the tools: a
//! [`PaymentHook`] answers a `402 Payment Required` for `http_request`, and an
//! [`HtmlExtractor`] turns pages into Markdown for `web_fetch`.

// These tools moved here verbatim from a host crate; their behavior, messages
// and control flow are pinned by tests and by the fixtures in `fixtures/`, so
// the purely stylistic pedantic lints below are allowed rather than reshaping
// working code (long `execute` bodies, `usize as f64` size labels, and so on).
#![allow(
    clippy::too_many_lines,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::similar_names,
    clippy::used_underscore_binding,
    clippy::manual_let_else,
    clippy::format_push_string,
    clippy::unwrap_used,
    clippy::unused_self,
    clippy::unused_async,
    clippy::needless_pass_by_value,
    clippy::doc_markdown,
    clippy::unnecessary_wraps
)]

mod curl;
mod gate;
mod http_request;
mod pushover;
pub mod web_fetch;

#[cfg(test)]
#[path = "contract_tests.rs"]
mod contract_test;
#[cfg(test)]
mod test_support;

pub use curl::CurlTool;
pub use gate::{Destination, DestinationFuture, HttpLimits, NetGate};
pub use http_request::{HttpRequestTool, PaymentAttempt, PaymentHook, PaymentOutcome};
pub use pushover::PushoverTool;
pub use web_fetch::{AsyncHtmlExtractor, HtmlExtractor, WebFetchTool};
