use super::*;

#[test]
fn description_catalogue_matches_the_frozen_channel_prompt_bytes() {
    let tools = [
        ("file_read", "Read UTF-8 text → bytes"),
        ("tool_search", "Find a tool.\nPreserve line breaks."),
    ];
    assert_eq!(
        render_description_catalogue(&tools),
        include_str!("catalog_channel.golden.txt")
    );
}

#[test]
fn an_empty_catalogue_emits_no_heading_or_protocol() {
    assert_eq!(render_description_catalogue(&[]), "");
}
