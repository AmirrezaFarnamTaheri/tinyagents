use super::*;

#[test]
fn a_long_multibyte_message_is_cut_on_a_character_boundary() {
    // '€' is 3 bytes and 2000 is not a multiple of 3, so byte 2000 lands
    // strictly inside a character: slicing at the raw offset panics.
    let content = "€".repeat(1500);
    assert!(content.len() > DEFAULT_FTS_SNIPPET_BYTES);
    assert!(!content.is_char_boundary(DEFAULT_FTS_SNIPPET_BYTES));
    let cut = snippet(&content, DEFAULT_FTS_SNIPPET_BYTES);
    assert_eq!(cut.len(), 1998, "the largest whole-character prefix under the cap");
    assert!(cut.chars().all(|c| c == '€'));
}

#[test]
fn an_ascii_message_longer_than_the_cap_truncates_to_exactly_the_cap() {
    let content = "a".repeat(5000);
    assert_eq!(snippet(&content, 2000).len(), 2000);
    assert_eq!(snippet("short", 2000), "short");
    assert_eq!(snippet("", 0), "");
    assert_eq!(snippet("abc", 0), "");
}

#[test]
fn the_cap_defaults_to_two_thousand_bytes() {
    // The cap is process-wide, so a test that changed it would race every
    // other test that indexes a message; only the default is asserted here.
    assert_eq!(DEFAULT_FTS_SNIPPET_BYTES, 2000);
    assert_eq!(fts_snippet_bytes(), DEFAULT_FTS_SNIPPET_BYTES);
}
