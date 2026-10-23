use super::*;

/// The real reason reaches the log and progress event, not a constant
/// (openhuman#7830).
#[test]
fn terminal_error_keeps_the_reason() {
    assert_eq!(
        terminal_error_message("  provider returned 429: rate limited \n"),
        "provider returned 429: rate limited"
    );
}

/// A credential echoed in the error must not reach the log or the host UI.
#[test]
fn terminal_error_scrubs_credentials() {
    let message = terminal_error_message(
        "request to https://api.example/v1?token=aB3dE5fG7hJ9kL1mN3pQ failed",
    );
    assert!(!message.contains("aB3dE5fG7hJ9kL1mN3pQ"), "{message}");
    assert!(message.contains("failed"), "{message}");
}

/// A huge error (a dumped response body) is bounded.
#[test]
fn terminal_error_is_truncated() {
    let message = terminal_error_message(&"x".repeat(TERMINAL_ERROR_MAX_CHARS * 4));
    assert!(message.ends_with("… [truncated]"), "{message}");
    assert!(message.chars().count() < TERMINAL_ERROR_MAX_CHARS + 20);
}
