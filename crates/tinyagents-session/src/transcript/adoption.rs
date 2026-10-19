//! The result of folding a conversation's pre-identity transcripts into its
//! session ([`TranscriptLocator::adopt_legacy`](super::TranscriptLocator::adopt_legacy)).

use std::path::PathBuf;

#[cfg(test)]
#[path = "adoption_tests.rs"]
mod tests;

/// What adoption did for one session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionAdoption {
    /// The transcript now backing the session.
    pub path: PathBuf,
    /// The legacy roots folded into it, oldest first.
    pub adopted: Vec<PathBuf>,
    /// Messages carried over.
    pub messages: usize,
}
