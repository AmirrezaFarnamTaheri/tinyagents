# Released Claude Code persistence fixtures

These files were written by the old file-backed public APIs from TinyAgents
commit `d5ef63ce7ece369e8faaba69ab32a881540dde78`, immediately before the
storage-port migration. `claude-code-sessions.json` comes from
`session_store::SessionStore::open`, `set`, and `record_delivered`; it includes
an orphan delivered-turn entry because the old API allowed recording turns
before a session UUID existed. `claude_code_settings.json` comes from
`settings::save`.

Regenerate them from the TinyAgents repository root with:

```sh
bash crates/tinyagents-harness/src/providers/claude_code/fixtures/generate-legacy-fixtures.sh
```

The script extracts those exact source files from the archived commit, builds
the small generator in `target/`, and copies the resulting old-writer bytes
back here. The tests import these captured bytes directly; they do not rebuild
the old schema locally.
