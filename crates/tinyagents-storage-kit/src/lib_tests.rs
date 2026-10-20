use super::*;

#[test]
fn composite_keys_keep_the_legacy_encoding_for_unicode_and_reserved_delimiters() {
    assert_eq!(
        composite_key(["~owner", "a/b", "100%", "☃"]).unwrap(),
        "%7Eowner/a%2Fb/100%25/☃"
    );
    let long = "x".repeat(513);
    assert_eq!(composite_key([long.as_str()]).unwrap().len(), 53);
}
