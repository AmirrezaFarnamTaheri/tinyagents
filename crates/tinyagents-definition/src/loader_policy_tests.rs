use super::*;

#[test]
fn parse_str_stamps_host_provenance_without_requiring_a_prompt() {
    let source = "id='embedded'\nwhen_to_use='embedded definition'\n";
    let definition = parse_str::<Extensions>(source, DefinitionSource::Builtin).unwrap();

    assert_eq!(definition.id, "embedded");
    assert_eq!(definition.source, DefinitionSource::Builtin);
    assert_eq!(
        definition.system_prompt,
        PromptSource::Inline(String::new())
    );
}

#[test]
fn parse_str_reports_malformed_toml() {
    let error = parse_str::<Extensions>("id = [", DefinitionSource::Builtin).unwrap_err();

    assert!(error.to_string().contains("parsing agent definition TOML"));
}
use crate::Extensions;

#[test]
fn explicit_inline_admission_preserves_authored_whitespace_and_other_prompt_sources() {
    let dir = tempfile::tempdir().unwrap();
    for (name, prompt) in [
        ("whitespace", "inline='  '"),
        ("file", "file={path=''}"),
        ("verbatim", "verbatim=''"),
    ] {
        let path = dir.path().join(format!("{name}.toml"));
        std::fs::write(
            &path,
            format!("id='{name}'\nwhen_to_use='specialist'\n[system_prompt]\n{prompt}\n"),
        )
        .unwrap();
        assert!(load_file::<Extensions>(&path).is_err());
        let definition = load_file_with_validation::<Extensions>(
            &path,
            DefinitionValidation::NonemptyInlinePrompt,
        )
        .unwrap();
        assert_eq!(definition.id, name);
        assert_eq!(definition.source, DefinitionSource::File(path));
    }
    let mut definitions = Vec::<AgentDefinition>::new();
    load_dir_with_validation(
        dir.path(),
        &mut definitions,
        DefinitionValidation::NonemptyInlinePrompt,
    )
    .unwrap();
    assert_eq!(definitions.len(), 3);
}

#[test]
fn both_loader_policies_reject_an_omitted_or_empty_inline_prompt() {
    let dir = tempfile::tempdir().unwrap();
    for prompt in ["", "[system_prompt]\ninline=''\n"] {
        let path = dir.path().join("empty.toml");
        std::fs::write(
            &path,
            format!("id='empty'\nwhen_to_use='specialist'\n{prompt}"),
        )
        .unwrap();
        for validation in [
            DefinitionValidation::Diagnostics,
            DefinitionValidation::NonemptyInlinePrompt,
        ] {
            assert!(
                load_file_with_validation::<Extensions>(&path, validation)
                    .unwrap_err()
                    .to_string()
                    .contains("missing")
            );
        }
    }
}
