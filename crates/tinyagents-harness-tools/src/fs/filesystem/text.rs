//! Small text helpers shared by the filesystem tools.

/// Truncate `s` to at most `max_bytes` bytes, appending a single-character
/// ellipsis `…` (3 bytes) if truncated. The returned string's total byte
/// length never exceeds `max_bytes`.
pub(super) fn truncate_at_byte_boundary(s: &str, max_bytes: usize) -> String {
    if s.len() <= max_bytes {
        return s.to_string();
    }
    let ellipsis = "…";
    let ellipsis_len = ellipsis.len();
    if max_bytes < ellipsis_len {
        return String::new();
    }
    let mut end = max_bytes - ellipsis_len;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}{}", &s[..end], ellipsis)
}

#[cfg(test)]
#[path = "text_test_tests.rs"]
mod test;
