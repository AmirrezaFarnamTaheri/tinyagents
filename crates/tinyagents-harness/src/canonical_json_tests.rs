use super::*;

#[test]
fn sorting_is_recursive_preserves_arrays_and_does_not_mutate_the_caller() {
    let original: Value =
        serde_json::from_str(r#"{"z":[{"b":2,"a":1},null,true,"text",5],"a":{"z":0,"a":-1}}"#)
            .unwrap();
    let before = serde_json::to_string(&original).unwrap();
    let expected = r#"{"a":{"a":-1,"z":0},"z":[{"a":1,"b":2},null,true,"text",5]}"#;
    assert_eq!(to_string(&original).unwrap(), expected);
    assert_eq!(to_vec(&original).unwrap(), expected.as_bytes());
    assert_eq!(
        serde_json::from_str::<Value>(&to_string_pretty(&original).unwrap()).unwrap(),
        original,
    );
    assert_eq!(serde_json::to_string(&original).unwrap(), before);
}

#[test]
fn scalar_encoding_and_array_order_remain_unchanged() {
    for value in [
        serde_json::json!(null),
        serde_json::json!(true),
        serde_json::json!("quotes\" and Unicode λ"),
        serde_json::json!(1.5),
        serde_json::json!([3, 2, 1]),
    ] {
        assert_eq!(
            to_string(&value).unwrap(),
            serde_json::to_string(&value).unwrap()
        );
    }
}
