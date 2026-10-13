//! Shared full-text indexing configuration for the session history.

/// Default cap on how much of a message body is copied into the search index,
/// in bytes.
///
/// Indexing whole messages would let the FTS table grow without bound next to
/// the message rows themselves, so only a leading snippet is indexed — which
/// means **a match beyond this offset is not findable**. That is a real
/// behavioural limit, not an implementation detail, so it is public and
/// overridable rather than a private constant nobody could see.
pub const DEFAULT_FTS_SNIPPET_BYTES: usize = 2000;

/// The live snippet cap. Read through [`fts_snippet_bytes`], set through
/// [`set_fts_snippet_bytes`].
static FTS_SNIPPET_BYTES: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(DEFAULT_FTS_SNIPPET_BYTES);

/// Returns the current FTS snippet cap in bytes.
pub fn fts_snippet_bytes() -> usize {
    FTS_SNIPPET_BYTES.load(std::sync::atomic::Ordering::Relaxed)
}

/// Overrides the FTS snippet cap.
///
/// Process-wide and takes effect for subsequently indexed content only —
/// raising it does not retroactively widen what is already indexed. Pair it
/// with [`crate::retention::reindex_fts`] to rebuild the index at the new cap.
pub fn set_fts_snippet_bytes(bytes: usize) {
    tracing::debug!("[session_db] fts snippet cap set to {bytes} bytes");
    FTS_SNIPPET_BYTES.store(bytes, std::sync::atomic::Ordering::Relaxed);
}

/// Truncates `content` to at most `limit` bytes on a character boundary.
///
/// Slicing at a raw byte offset panics whenever the offset lands inside a
/// multi-byte character, which any long non-ASCII message can trigger.
pub fn snippet(content: &str, limit: usize) -> &str {
    if content.len() <= limit {
        return content;
    }
    let mut cutoff = limit;
    while cutoff > 0 && !content.is_char_boundary(cutoff) {
        cutoff -= 1;
    }
    &content[..cutoff]
}

#[cfg(test)]
#[path = "fts_tests.rs"]
mod tests;
