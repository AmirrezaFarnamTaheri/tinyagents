//! Unit tests for URL validation, SSRF host classification, and the DNS
//! check's vetted addresses.

use super::*;

/// Turns an expected rejection into its message, and an unexpected success
/// into a test failure, without `unwrap_err`.
trait Rejection {
    fn rejection(self) -> anyhow::Result<String>;
}

impl<T: std::fmt::Debug> Rejection for anyhow::Result<T> {
    fn rejection(self) -> anyhow::Result<String> {
        match self {
            Ok(value) => anyhow::bail!("expected a rejection, got {value:?}"),
            Err(err) => Ok(err.to_string()),
        }
    }
}

#[test]
fn normalize_domain_strips_scheme_path_and_case() {
    let got = normalize_domain("  HTTPS://Docs.Example.com/path ");
    assert_eq!(got.as_deref(), Some("docs.example.com"));
}

#[test]
fn normalizes_http_domains_and_rejects_empty_hosts() {
    assert_eq!(
        normalize_domain("http://Example.com:8080/path"),
        Some("example.com".into())
    );
    assert_eq!(normalize_domain("https://"), None);
    assert!(extract_host("http:///path").is_err());
    assert!(extract_host("http://:80/path").is_err());
}

#[test]
fn rejects_malformed_ports() {
    assert!(extract_port("http://example.com:abc").is_err());
    assert!(extract_port("http://example.com:65536").is_err());
}

#[tokio::test]
async fn system_dns_resolves_numeric_loopback_without_external_network() -> anyhow::Result<()> {
    let resolved = super::resolve_host_ips("127.0.0.1".to_string(), 80).await?;
    assert_eq!(resolved, vec!["127.0.0.1".parse::<std::net::IpAddr>()?]);
    assert!(super::resolve_host_ips(String::new(), 80).await.is_err());
    Ok(())
}

#[test]
fn normalize_allowed_domains_deduplicates() {
    let got = normalize_allowed_domains(vec![
        "example.com".into(),
        "EXAMPLE.COM".into(),
        "https://example.com/".into(),
    ]);
    assert_eq!(got, vec!["example.com".to_string()]);
}

#[test]
fn validate_accepts_exact_domain() -> anyhow::Result<()> {
    let allow = vec!["example.com".to_string()];
    let got = validate_url("https://example.com/docs", &allow)?;
    assert_eq!(got, "https://example.com/docs");
    Ok(())
}

#[test]
fn validate_accepts_http() {
    let allow = vec!["example.com".to_string()];
    assert!(validate_url("http://example.com", &allow).is_ok());
}

#[test]
fn validate_accepts_subdomain() {
    let allow = vec!["example.com".to_string()];
    assert!(validate_url("https://api.example.com/v1", &allow).is_ok());
}

#[test]
fn validate_rejects_allowlist_miss() -> anyhow::Result<()> {
    let allow = vec!["example.com".to_string()];
    let err = validate_url("https://google.com", &allow).rejection()?;
    assert!(err.contains("allowed websites"));
    Ok(())
}

#[test]
fn validate_wildcard_allows_any_public_host() {
    let allow = vec!["*".to_string()];
    assert!(validate_url("https://example.com/docs", &allow).is_ok());
    assert!(validate_url("https://www.cnbc.com/markets", &allow).is_ok());
    assert!(validate_url("https://sub.deep.example.org", &allow).is_ok());
}

#[test]
fn validate_wildcard_still_blocks_local_and_private() -> anyhow::Result<()> {
    // "Allow all sites" must NOT defeat the SSRF guard.
    let allow = vec!["*".to_string()];
    assert!(
        validate_url("https://localhost:8080", &allow)
            .rejection()?
            .contains("local/private")
    );
    assert!(
        validate_url("https://192.168.1.5", &allow)
            .rejection()?
            .contains("local/private")
    );
    Ok(())
}

#[test]
fn validate_rejects_localhost() -> anyhow::Result<()> {
    let allow = vec!["localhost".to_string()];
    let err = validate_url("https://localhost:8080", &allow).rejection()?;
    assert!(err.contains("local/private"));
    Ok(())
}

#[test]
fn validate_rejects_private_ipv4() -> anyhow::Result<()> {
    let allow = vec!["192.168.1.5".to_string()];
    let err = validate_url("https://192.168.1.5", &allow).rejection()?;
    assert!(err.contains("local/private"));
    Ok(())
}

#[test]
fn validate_rejects_whitespace() -> anyhow::Result<()> {
    let allow = vec!["example.com".to_string()];
    let err = validate_url("https://example.com/hello world", &allow).rejection()?;
    assert!(err.contains("whitespace"));
    Ok(())
}

#[test]
fn validate_rejects_userinfo() -> anyhow::Result<()> {
    let allow = vec!["example.com".to_string()];
    let err = validate_url("https://user@example.com", &allow).rejection()?;
    assert!(err.contains("userinfo"));
    Ok(())
}

// Empty allowed_domains = open mode: any public host is permitted.
// This keeps web-fetch working when no domain list is configured and
// makes behaviour consistent between default and external-LLM routing.
// (#2700)
#[test]
fn validate_empty_allowlist_allows_public_host() {
    assert!(validate_url("https://example.com", &[]).is_ok());
    assert!(validate_url("https://www.cnbc.com/markets", &[]).is_ok());
}

#[test]
fn validate_empty_allowlist_still_blocks_private_hosts() -> anyhow::Result<()> {
    let err = validate_url("https://192.168.1.5", &[]).rejection()?;
    assert!(err.contains("local/private"));

    let err = validate_url("https://localhost", &[]).rejection()?;
    assert!(err.contains("local/private"));
    Ok(())
}

// ── normalize_allowed_domains: fail-closed on malformed-only input ──

#[test]
fn normalize_all_invalid_entries_stays_fail_closed() {
    // A non-empty list that fully normalizes to nothing must NOT produce
    // an empty slice (which would silently enter open mode). (#2738)
    let got = normalize_allowed_domains(vec!["   ".into(), "https://".into()]);
    assert!(
        !got.is_empty(),
        "normalized result must be non-empty to stay in strict mode"
    );
    // The sentinel must not match any real public host.
    assert!(
        !host_matches_allowlist("example.com", &got),
        "sentinel must not grant access to real hosts"
    );
    assert!(
        !host_matches_allowlist("api.example.com", &got),
        "sentinel must not grant access to subdomains"
    );
}

#[test]
fn normalize_empty_input_stays_empty_for_open_mode() {
    // Explicitly empty input should return empty (open mode is intentional).
    assert_eq!(normalize_allowed_domains(vec![]).len(), 0);
}

#[tokio::test]
async fn dns_check_with_empty_allowlist_allows_public_resolved_host() -> anyhow::Result<()> {
    // Open mode (empty allowlist) must still pass DNS check for public IPs.
    let got = validate_url_with_dns_check_with_resolver(
        "https://example.com",
        &[],
        |host, port| async move {
            assert_eq!(host, "example.com");
            assert_eq!(port, 443);
            Ok(vec!["93.184.216.34".parse()?])
        },
    )
    .await?;
    assert_eq!(got.url, "https://example.com");
    Ok(())
}

#[tokio::test]
async fn dns_check_with_empty_allowlist_blocks_private_resolved_ip() -> anyhow::Result<()> {
    // Even in open mode, DNS rebinding to a private IP must be blocked.
    let err = validate_url_with_dns_check_with_resolver("https://example.com", &[], |_, _| async {
        Ok(vec!["10.0.0.1".parse()?])
    })
    .await
    .rejection()?;
    assert!(err.contains("DNS rebinding blocked"));
    Ok(())
}

#[tokio::test]
async fn dns_check_resolver_failure_is_a_refusal_not_a_pass_through() -> anyhow::Result<()> {
    // A resolver error (NXDOMAIN, network down, timeout) must refuse the
    // fetch, not fall back to treating the host as unresolved-and-therefore-
    // allowed.
    let err = validate_url_with_dns_check_with_resolver(
        "https://this-host-does-not-exist.invalid",
        &[],
        |host, _port| async move { anyhow::bail!("DNS resolution failed for '{host}': NXDOMAIN") },
    )
    .await
    .rejection()?;
    assert!(err.contains("DNS resolution failed"));
    Ok(())
}

#[tokio::test]
async fn dns_check_resolver_returning_no_addresses_is_a_refusal() -> anyhow::Result<()> {
    // A resolver that answers with zero addresses (some stub resolvers do
    // this instead of erroring) must not be treated as "no IPs to check,
    // therefore allowed".
    let err = validate_url_with_dns_check_with_resolver("https://example.com", &[], |_, _| async {
        Ok(Vec::new())
    })
    .await
    .rejection()?;
    assert!(err.contains("DNS resolution returned no addresses"));
    Ok(())
}

#[test]
fn validate_rejects_ftp_scheme() -> anyhow::Result<()> {
    let allow = vec!["example.com".to_string()];
    let err = validate_url("ftp://example.com", &allow).rejection()?;
    assert!(err.contains("http://") || err.contains("https://"));
    Ok(())
}

#[test]
fn validate_rejects_empty_url() -> anyhow::Result<()> {
    let allow = vec!["example.com".to_string()];
    let err = validate_url("", &allow).rejection()?;
    assert!(err.contains("empty"));
    Ok(())
}

#[test]
fn validate_rejects_ipv6_host() -> anyhow::Result<()> {
    let allow = vec!["example.com".to_string()];
    let err = validate_url("http://[::1]:8080/path", &allow).rejection()?;
    assert!(err.contains("IPv6"));
    Ok(())
}

#[test]
fn blocks_multicast_ipv4() {
    assert!(is_private_or_local_host("224.0.0.1"));
    assert!(is_private_or_local_host("239.255.255.255"));
}

#[test]
fn blocks_broadcast() {
    assert!(is_private_or_local_host("255.255.255.255"));
}

#[test]
fn blocks_reserved_ipv4() {
    assert!(is_private_or_local_host("240.0.0.1"));
    assert!(is_private_or_local_host("250.1.2.3"));
}

#[test]
fn blocks_documentation_ranges() {
    // TEST-NET-1 is globally routable in this policy; only TEST-NET-2 and
    // TEST-NET-3 are classified as non-global here.
    assert!(!is_private_or_local_host("192.0.2.1"));
    assert!(is_private_or_local_host("198.51.100.1"));
    assert!(is_private_or_local_host("203.0.113.1"));
}

#[test]
fn blocks_benchmarking_range() {
    assert!(is_private_or_local_host("198.18.0.1"));
    assert!(is_private_or_local_host("198.19.255.255"));
}

#[test]
fn blocks_ipv6_localhost() {
    assert!(is_private_or_local_host("::1"));
    assert!(is_private_or_local_host("[::1]"));
}

#[test]
fn blocks_ipv6_multicast() {
    assert!(is_private_or_local_host("ff02::1"));
}

#[test]
fn blocks_ipv6_link_local() {
    assert!(is_private_or_local_host("fe80::1"));
}

#[test]
fn blocks_ipv6_unique_local() {
    assert!(is_private_or_local_host("fd00::1"));
}

#[test]
fn blocks_ipv4_mapped_ipv6() {
    assert!(is_private_or_local_host("::ffff:127.0.0.1"));
    assert!(is_private_or_local_host("::ffff:192.168.1.1"));
    assert!(is_private_or_local_host("::ffff:10.0.0.1"));
}

#[test]
fn allows_public_ipv4() {
    assert!(!is_private_or_local_host("8.8.8.8"));
    assert!(!is_private_or_local_host("1.1.1.1"));
    assert!(!is_private_or_local_host("93.184.216.34"));
}

#[test]
fn blocks_ipv6_documentation_range() {
    assert!(is_private_or_local_host("2001:db8::1"));
}

#[test]
fn blocks_nat64_translation_prefixes() {
    assert!(is_private_or_local_host("64:ff9b:1::7f00:1"));
    assert!(is_private_or_local_host("64:ff9b::7f00:1"));
    assert!(!is_private_or_local_host("64:ff9b::808:808"));
    assert!(!is_private_or_local_host("2001:4860:4860::8888"));
}

#[test]
fn classifies_well_known_nat64_embedded_addresses() {
    assert!(is_private_or_local_host("64:ff9b::a00:1"));
    assert!(is_private_or_local_host("64:ff9b::7f00:1"));
    assert!(is_private_or_local_host("64:ff9b::c633:6401"));
    assert!(!is_private_or_local_host("64:ff9b::808:808"));
    // This is outside the exact /96 translation prefix.
    assert!(!is_private_or_local_host("64:ff9b:0:0:0:1:a00:1"));
}

#[test]
fn classifies_6to4_embedded_destinations() {
    assert!(is_private_or_local_host("2002:a00:1::"));
    assert!(is_private_or_local_host("2002:7f00:1::"));
    assert!(!is_private_or_local_host("2002:808:808::"));
}

#[test]
fn classifies_teredo_client_address_without_rejecting_server_address() {
    // The last two segments are the client's IPv4 address with every bit inverted.
    assert!(is_private_or_local_host("2001:0:808:808:0:0:f5ff:fffe"));
    // The server is not the IPv4 destination represented by this endpoint.
    assert!(!is_private_or_local_host("2001:0:a00:1:0:0:f7f7:f7f7"));
    assert!(!is_private_or_local_host("2001:0:808:808:0:0:fefe:fefe"));
}

#[test]
fn classifies_mapped_and_compatible_ipv4_addresses() {
    assert!(is_private_or_local_host("::ffff:10.0.0.1"));
    assert!(!is_private_or_local_host("::ffff:8.8.8.8"));
    assert!(is_private_or_local_host("::10.0.0.1"));
    assert!(!is_private_or_local_host("::8.8.8.8"));
}

#[tokio::test]
async fn dns_check_rejects_private_ipv4_inside_transition_address() -> anyhow::Result<()> {
    let err = validate_url_with_dns_check_with_resolver("https://example.com", &[], |_, _| async {
        Ok(vec!["2002:a00:1::".parse()?])
    })
    .await
    .rejection()?;
    assert!(err.contains("DNS rebinding blocked"));

    let allowed =
        validate_url_with_dns_check_with_resolver("https://example.com", &[], |_, _| async {
            Ok(vec!["2002:808:808::".parse()?])
        })
        .await?;
    assert_eq!(allowed.addrs[0].ip().to_string(), "2002:808:808::");
    Ok(())
}

#[test]
fn allows_public_ipv6() {
    assert!(!is_private_or_local_host("2607:f8b0:4004:800::200e"));
}

#[test]
fn blocks_shared_address_space() {
    assert!(is_private_or_local_host("100.64.0.1"));
    assert!(is_private_or_local_host("100.127.255.255"));
    assert!(!is_private_or_local_host("100.63.0.1"));
    assert!(!is_private_or_local_host("100.128.0.1"));
}

#[test]
fn ssrf_blocks_loopback_127_range() {
    assert!(is_private_or_local_host("127.0.0.1"));
    assert!(is_private_or_local_host("127.0.0.2"));
    assert!(is_private_or_local_host("127.255.255.255"));
}

#[test]
fn ssrf_blocks_rfc1918_10_range() {
    assert!(is_private_or_local_host("10.0.0.1"));
    assert!(is_private_or_local_host("10.255.255.255"));
}

#[test]
fn ssrf_blocks_rfc1918_172_range() {
    assert!(is_private_or_local_host("172.16.0.1"));
    assert!(is_private_or_local_host("172.31.255.255"));
}

#[test]
fn ssrf_blocks_unspecified_address() {
    assert!(is_private_or_local_host("0.0.0.0"));
}

#[test]
fn ssrf_blocks_dot_localhost_subdomain() {
    assert!(is_private_or_local_host("evil.localhost"));
    assert!(is_private_or_local_host("a.b.localhost"));
}

#[test]
fn ssrf_blocks_dot_local_tld() {
    assert!(is_private_or_local_host("service.local"));
}

#[test]
fn ssrf_ipv6_unspecified() {
    assert!(is_private_or_local_host("::"));
}

// ── Defense-in-depth: alternate IP notations rejected by allowlist
//
// Rust's IpAddr::parse() rejects octal, hex, decimal, and
// zero-padded notations. They fall through as hostnames and get
// rejected by the allowlist instead. These tests pin that
// behaviour so a parser change can't silently re-open SSRF.

#[test]
fn ssrf_octal_loopback_not_parsed_as_ip() {
    assert!(!is_private_or_local_host("0177.0.0.1"));
}

#[test]
fn ssrf_hex_loopback_not_parsed_as_ip() {
    assert!(!is_private_or_local_host("0x7f000001"));
}

#[test]
fn ssrf_decimal_loopback_not_parsed_as_ip() {
    assert!(!is_private_or_local_host("2130706433"));
}

#[test]
fn ssrf_zero_padded_loopback_not_parsed_as_ip() {
    assert!(!is_private_or_local_host("127.000.000.001"));
}

#[test]
fn ssrf_alternate_notations_rejected_by_validate_url() -> anyhow::Result<()> {
    let allow = vec!["example.com".to_string()];
    for notation in [
        "http://0177.0.0.1",
        "http://0x7f000001",
        "http://2130706433",
        "http://127.000.000.001",
    ] {
        let err = validate_url(notation, &allow).rejection()?;
        assert!(
            err.contains("allowed websites"),
            "Expected allowlist rejection for {notation}, got: {err}"
        );
    }
    Ok(())
}

// ── DNS rebinding protection ─────────────────────────────────

#[tokio::test]
async fn dns_check_blocks_localhost_resolution() -> anyhow::Result<()> {
    // "localhost" resolves to 127.0.0.1 on most systems. Even if
    // someone adds it to the allowlist, the DNS check should block it.
    let allow = vec!["localhost".to_string()];
    // validate_url itself already blocks "localhost" via the hostname check,
    // but validate_url_with_dns_check should also catch it.
    let err = validate_url_with_dns_check("https://localhost", &allow)
        .await
        .rejection()?;
    assert!(
        err.contains("local/private") || err.contains("rebinding"),
        "Expected SSRF block for localhost, got: {err}"
    );
    Ok(())
}

#[tokio::test]
async fn dns_check_passes_for_public_resolved_ip() -> anyhow::Result<()> {
    let allow = vec!["example.com".to_string()];
    let got = validate_url_with_dns_check_with_resolver(
        "https://example.com",
        &allow,
        |host, port| async move {
            assert_eq!(host, "example.com");
            assert_eq!(port, 443);
            Ok(vec!["93.184.216.34".parse()?])
        },
    )
    .await?;
    assert_eq!(got.url, "https://example.com");
    Ok(())
}

#[tokio::test]
async fn dns_check_blocks_private_resolved_ip() -> anyhow::Result<()> {
    let allow = vec!["example.com".to_string()];
    let err =
        validate_url_with_dns_check_with_resolver("https://example.com", &allow, |_, _| async {
            Ok(vec!["127.0.0.1".parse()?])
        })
        .await
        .rejection()?;
    assert!(err.contains("DNS rebinding blocked"));
    Ok(())
}

#[tokio::test]
async fn dns_check_uses_explicit_port_for_resolution() -> anyhow::Result<()> {
    let allow = vec!["api.example.com".to_string()];
    let got = validate_url_with_dns_check_with_resolver(
        "http://api.example.com:8080/status",
        &allow,
        |host, port| async move {
            assert_eq!(host, "api.example.com");
            assert_eq!(port, 8080);
            Ok(vec!["93.184.216.34".parse()?])
        },
    )
    .await?;
    assert_eq!(got.url, "http://api.example.com:8080/status");
    Ok(())
}

#[tokio::test]
async fn dns_check_returns_resolver_failure() -> anyhow::Result<()> {
    let allow = vec!["example.com".to_string()];
    let err = validate_url_with_dns_check_with_resolver(
        "https://example.com",
        &allow,
        |host, _| async move {
            anyhow::bail!("DNS resolution failed for '{host}': resolver unavailable")
        },
    )
    .await
    .rejection()?;
    assert!(err.contains("DNS resolution failed"));
    Ok(())
}

#[tokio::test]
async fn dns_check_rejects_ip_literal_private() -> anyhow::Result<()> {
    let allow = vec!["10.0.0.1".to_string()];
    let err = validate_url_with_dns_check("https://10.0.0.1", &allow)
        .await
        .rejection()?;
    assert!(err.contains("local/private"));
    Ok(())
}

#[test]
fn wildcard_allows_any_host() {
    let any = vec!["*".to_string()];
    assert!(host_matches_allowlist("docs.rs", &any));
    assert!(host_matches_allowlist("api.github.com", &any));
    assert!(host_matches_allowlist("whatever.example.org", &any));
}

#[tokio::test]
async fn wildcard_still_blocks_private_hosts() -> anyhow::Result<()> {
    // `*` opens public hosts only — SSRF block on private/local hosts stays.
    let any = vec!["*".to_string()];
    let err = validate_url_with_dns_check("https://127.0.0.1", &any)
        .await
        .rejection()?;
    assert!(err.contains("local/private"), "got: {err}");
    Ok(())
}

#[test]
fn exported_ssrf_predicates_classify_non_global_ips_accurately() -> anyhow::Result<()> {
    use std::net::{Ipv4Addr, Ipv6Addr};

    // IPv4 Non-global checks
    assert!(is_non_global_v4(Ipv4Addr::LOCALHOST));
    assert!(is_non_global_v4(Ipv4Addr::new(10, 0, 0, 1)));
    assert!(is_non_global_v4(Ipv4Addr::new(172, 16, 0, 1)));
    assert!(is_non_global_v4(Ipv4Addr::new(192, 168, 1, 1)));
    assert!(is_non_global_v4(Ipv4Addr::new(169, 254, 1, 1)));
    assert!(is_non_global_v4(Ipv4Addr::new(100, 64, 0, 1))); // CGNAT
    assert!(is_non_global_v4(Ipv4Addr::new(240, 0, 0, 1))); // Class E
    assert!(!is_non_global_v4(Ipv4Addr::new(192, 0, 2, 1))); // TEST-NET-1 is globally routable in this policy
    assert!(is_non_global_v4(Ipv4Addr::new(198, 51, 100, 1))); // TEST-NET-2
    assert!(is_non_global_v4(Ipv4Addr::new(203, 0, 113, 1))); // TEST-NET-3
    assert!(is_non_global_v4(Ipv4Addr::new(192, 88, 99, 1))); // 6to4 anycast
    assert!(is_non_global_v4(Ipv4Addr::UNSPECIFIED)); // 0.0.0.0/8
    assert!(is_non_global_v4(Ipv4Addr::new(0, 1, 2, 3))); // 0.0.0.0/8

    // IPv4 Global public IPs
    assert!(!is_non_global_v4(Ipv4Addr::new(8, 8, 8, 8)));
    assert!(!is_non_global_v4(Ipv4Addr::new(1, 1, 1, 1)));
    assert!(!is_non_global_v4(Ipv4Addr::new(140, 82, 121, 4)));
    // Non-TEST-NET IPs in adjacent /24 blocks should not be classified as non-global
    assert!(!is_non_global_v4(Ipv4Addr::new(198, 51, 1, 1)));
    assert!(!is_non_global_v4(Ipv4Addr::new(203, 0, 1, 1)));
    assert!(!is_non_global_v4(Ipv4Addr::new(192, 88, 98, 1)));

    // IPv6 Non-global checks
    assert!(is_non_global_v6(Ipv6Addr::LOCALHOST));
    assert!(is_non_global_v6(Ipv6Addr::UNSPECIFIED));
    assert!(is_non_global_v6("fc00::1".parse()?));
    assert!(is_non_global_v6("fe80::1".parse()?));
    assert!(is_non_global_v6("2001:db8::1".parse()?));
    assert!(is_non_global_v6("100::1".parse()?));
    assert!(is_non_global_v6("100:0:0:1::1".parse()?));
    assert!(is_non_global_v6("2001:2::1".parse()?));
    assert!(is_non_global_v6("3fff::1".parse()?));
    assert!(is_non_global_v6("5f00::1".parse()?));

    // IPv6 Global public IPs
    assert!(!is_non_global_v6("2606:4700:4700::1111".parse()?));
    assert!(!is_non_global_v6("101::1".parse()?));
    assert!(!is_non_global_v6("100:0:0:2::1".parse()?));
    assert!(!is_non_global_v6("2001:3::1".parse()?));
    assert!(!is_non_global_v6("4000::1".parse()?));
    assert!(!is_non_global_v6("5f01::1".parse()?));

    // Host helper checks (including ASCII case-insensitivity and trailing dot)
    assert!(is_private_or_local_host("localhost"));
    assert!(is_private_or_local_host("LOCALHOST"));
    assert!(is_private_or_local_host("localhost."));
    assert!(is_private_or_local_host("my-service.localhost"));
    assert!(is_private_or_local_host("MY-SERVICE.LOCALHOST"));
    assert!(is_private_or_local_host("device.local"));
    assert!(is_private_or_local_host("DEVICE.LOCAL"));
    assert!(is_private_or_local_host("device.local."));
    assert!(is_private_or_local_host("127.0.0.1"));
    assert!(is_private_or_local_host("[::1]"));
    assert!(!is_private_or_local_host("github.com"));
    assert!(!is_private_or_local_host("api.openai.com"));
    Ok(())
}

// ── WHATWG parser differentials ─────────────────────────────

#[test]
fn validate_rejects_backslash_authority_smuggling() -> anyhow::Result<()> {
    // A WHATWG parser treats `\` as `/` for http(s), so a real client
    // connects to 127.0.0.1 while a naive split sees `*.example.com`.
    let allow = vec!["example.com".to_string()];
    let smuggled = "http://127.0.0.1\\.example.com/";
    let err = validate_url(smuggled, &allow).rejection()?;
    assert!(err.contains("backslash"), "got: {err}");
    let err = validate_url(smuggled, &[]).rejection()?;
    assert!(err.contains("backslash"), "got: {err}");
    Ok(())
}

#[test]
fn validate_rejects_backslash_anywhere() -> anyhow::Result<()> {
    let err = validate_url("https://example.com/a\\b", &[]).rejection()?;
    assert!(err.contains("backslash"), "got: {err}");
    Ok(())
}

#[test]
fn extract_host_and_port_reject_backslash() {
    let smuggled = "http://127.0.0.1\\.example.com:8080/";
    assert!(extract_host(smuggled).is_err());
    assert!(extract_port(smuggled).is_err());
}

#[test]
fn validate_rejects_percent_encoded_host() -> anyhow::Result<()> {
    // WHATWG percent-decodes the host, so this is 127.0.0.1 on the wire.
    let err = validate_url("http://%31%32%37.0.0.1/", &[]).rejection()?;
    assert!(err.contains("percent-encoded"), "got: {err}");
    let allow = vec!["example.com".to_string()];
    let err = validate_url("http://evil%2eexample.com/", &allow).rejection()?;
    assert!(err.contains("percent-encoded"), "got: {err}");
    Ok(())
}

#[test]
fn extract_host_and_port_reject_percent_encoded_authority() {
    assert!(extract_host("http://%31%32%37.0.0.1/").is_err());
    assert!(extract_port("http://example.com:%38%30/").is_err());
}

#[test]
fn validate_allows_percent_encoding_outside_the_authority() -> anyhow::Result<()> {
    let got = validate_url("https://example.com/search?q=a%20b#x%2F", &[])?;
    assert_eq!(got, "https://example.com/search?q=a%20b#x%2F");
    Ok(())
}

#[tokio::test]
async fn dns_check_returns_exactly_the_vetted_addresses() -> anyhow::Result<()> {
    let got = validate_url_with_dns_check_with_resolver(
        "https://API.example.com:8443/v1",
        &[],
        |_, _| async {
            Ok(vec![
                "93.184.216.34".parse()?,
                "2606:4700:4700::1111".parse()?,
            ])
        },
    )
    .await?;
    assert_eq!(
        got,
        ValidatedUrl {
            url: "https://API.example.com:8443/v1".to_string(),
            host: "api.example.com".to_string(),
            addrs: vec![
                "93.184.216.34:8443".parse()?,
                "[2606:4700:4700::1111]:8443".parse()?,
            ],
        }
    );
    Ok(())
}

#[tokio::test]
async fn dns_check_pins_an_ip_literal_host_to_itself() -> anyhow::Result<()> {
    // IP literals skip DNS entirely, so this stays network-free.
    let got = validate_url_with_dns_check("http://93.184.216.34/page", &[]).await?;
    assert_eq!(got.host, "93.184.216.34");
    assert_eq!(got.addrs, vec!["93.184.216.34:80".parse()?]);
    Ok(())
}
