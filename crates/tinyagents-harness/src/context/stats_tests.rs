use super::*;

#[test]
fn context_window_projection_preserves_unknown_and_zero() {
    let unknown = context_window_statistics(17, None);
    assert_eq!(unknown.context_window_tokens, None);
    assert_eq!(unknown.utilization(), None);

    let zero = context_window_statistics(17, Some(0));
    assert_eq!(zero.context_window_tokens, Some(0));
    assert_eq!(zero.utilization(), None);

    let known = context_window_statistics(25, Some(100));
    assert_eq!(known.utilization(), Some(0.25));
}
