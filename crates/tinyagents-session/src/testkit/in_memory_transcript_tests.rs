use super::*;

#[test]
fn in_memory_transcript_starts_empty() {
    let locator = in_memory_transcript();

    assert!(
        locator
            .thread_transcripts("fixture-thread", None)
            .expect("list empty in-memory locator")
            .roots
            .is_empty()
    );
}
