# Structural code tools

Enable the `code` Cargo feature and call `tinyagents_harness_tools::code_tools(gate)` to
obtain six `Box<dyn Tool>` values. `code_tools_with_options` accepts
`CodeToolOptions` when a host needs tighter resource limits. Construction is
fallible because options and the bundled catalog are validated.

The tools use the pure `tinytools-code` engine for parsing and rendering. This
module owns native filesystem mechanics; the supplied `FsGate` owns policy.
Neither module executes generated code, installs declared dependencies, resolves
semantic types, or treats a syntax check as a successful build or test run.

## Availability and authorization

All six tools are deferred, belong to family `code`, and carry tags `code`,
`structural`, and `pack:code`. Every static minimum permission is `ReadOnly` so
hosts can retain preview access. `ast_patch` and `chunk_apply` require `Write`
unless the invocation contains the literal JSON boolean `dry_run: true`.
Missing, false, null, string and numeric flags never lower the declaration.
Nonboolean flags fail strict decoding before any filesystem mutation.

Write calls consult the original gate's autonomy and action budget. The host's
`write_needs_approval` controls external-effect routing. Previews neither prompt
as writes nor consume actions. A trusted `ToolRunContext` workspace descriptor
scopes path validation per call without changing the original gate or moving
its accounting into a separate policy object.

## Arguments

Unknown fields are rejected. `language` can explicitly select `rust`,
`typescript`, `tsx`, `javascript`, `python`, `go`, `json`, or `toml`; otherwise
inspection and patch tools infer it from a supported filename extension.

| Tool | Required arguments | Optional arguments |
| --- | --- | --- |
| `ast_outline` | `path` | `language`, `level`, `offset`, `limit`, `start_line`, `line_count` |
| `ast_symbol` | `path`, `selector` | `language`, `start_line`, `line_count` |
| `ast_patch` | `path`, `expected_revision`, `operation`, `code` | `language`, `selector`, `dry_run`, `start_line`, `line_count` |
| `chunk_search` | None | `query`, `language`, `offset`, `limit` |
| `chunk_inspect` | Exactly one of `id` or `manifest_path` | `offset`, `limit` |
| `chunk_apply` | `path`, `chunk_revision`, exactly one of `id` or `manifest_path`, plus the destination guard below | `bindings`, `selector`, `dry_run`, `start_line`, `line_count` |

`selector` accepts a revision-bound symbol ID, an unambiguous qualified name,
or an unambiguous local name. A truncated symbol collection only permits IDs.
The patch operations are `replace_body`, `replace_symbol`, `insert_import`,
`append_member`, and `wrap`. All except `insert_import` need a selector; a wrap
uses exactly one `{{node}}` marker. Source revisions are 64 lowercase hexadecimal
digits returned by the engine, not Git commit hashes.

For `chunk_apply`, edit an existing file with `expected_revision`, or create a
new file with `create_new: true` and no `expected_revision`. Creation refuses any
existing target and requires its parent directory to exist. A complete-file
chunk replaces the complete file and accepts no selector. Function-body and
member chunks require an existing file and a selector, and use the same pure
patch engine as `ast_patch`. `bindings` has two optional objects: `parameters`
for typed values and `slots` for supplied code strings.

`chunk_search` searches the twelve bundled patterns. To use a custom pattern,
inspect its `manifest_path` and pass that same path to `chunk_apply`. The
manifest's template path must be portable and relative to the manifest's
directory. Both files pass the gate, and the resolved template must remain
inside that directory, including after symlink resolution. A returned
`chunk_revision` binds the exact manifest and template contents. Reformatting a
custom manifest changes this content identity even when its normalized
`manifest_revision` remains the same.

## Receipts and paging

Successful results contain a JSON block. List pages use this shape:

```json
{"items": [], "offset": 0, "total": 0, "next_offset": null}
```

`offset` is zero-based. `limit` is positive and bounded by `max_page_items`.
`chunk_search` defaults to five results, or the configured `max_page_items` if
that is smaller; an explicit `limit` can select any positive size up to that
bound. Other record lists default to `max_page_items`.
Outline symbols, references and diagnostics each have their own page object.
Their separate truncation flags disclose parser collection limits; a page's
`total` counts retained records, not records beyond those limits.

`ast_outline` accepts `level: 0` for metadata only, with no `skeleton` field.
The default `level: 1` includes a folded source page alongside that metadata.
Other levels and noninteger values are rejected before reading the file.

Source and folded-skeleton pages use this shape:

```json
{"text": "fn example() {}\n", "start_line": 1, "total_lines": 1,
 "next_start_line": null, "truncated": false}
```

`start_line` is one-based, relative to the returned source selection: the file
for mutation receipts, the folded source for outlines, or the declaration for
`ast_symbol`. Symbol ranges independently preserve absolute source coordinates.
`line_count` must be positive and no greater than `max_page_lines`. A single
oversized line can exceed the output budget even on a one-line page; that call
returns a bounded error instead of silently dropping content.

Mutation receipts include `path`, `language`, `before_revision` (null for
creation), `revision`, `changed`, `written`, `dry_run`, and a source page. Chunk
receipts also include `chunk_id`, `chunk_revision`, `rendered_revision`, and
informational `dependencies`. `written: true` means publication succeeded.
An unchanged result reports `written: false` and consumes no action.

For example, preview a body edit after reading the current revision:

```json
{"path":"src/lib.rs","expected_revision":"<64 lowercase hex digits>",
 "selector":"calculate","operation":"replace_body","code":"42",
 "dry_run":true}
```

## Resource bounds

| `CodeToolOptions` field | Default |
| --- | --- |
| `limits` | `CodeLimits::default()`; 1 MiB source, 32 cached entries, 8 MiB retained source |
| `max_argument_bytes` | 1 MiB |
| `max_manifest_bytes` | 64 KiB |
| `max_template_bytes` | 256 KiB |
| `max_output_bytes` | 64 KiB |
| `max_page_items` | 32 |
| `max_page_lines` | 80 |

Argument validation counts serialized UTF-8 and JSON escape bytes with a writer
that stops at the limit, without allocating a second complete argument buffer.
Native inputs must be regular UTF-8 files. Reads take at most the configured
bound plus one byte so a file growing during a read cannot cause an unbounded
allocation. The output bound measures the actual pretty-printed model-visible
JSON. Errors are truncated only at UTF-8 boundaries. The complete success
receipt is checked before any staging file or action-budget consumption.

## Publication and concurrency

The tools share a mutation mutex and participate in `file_state`'s path lock
when that coordinator is enabled. Successful publications record the current
file-state agent's write. Exact source revisions remain required regardless of
whether the optional coordinator is enabled.

Edits stage a complete file in the destination's validated directory. Existing
permission bits are retained; new files use the temporary-file library's
private creation permissions. Before publication, the adapter reauthorizes the
original destination path and rechecks target, manifest and template revisions.
The original gate's write authority is checked after action reservation and
again after the final asynchronous path and revision checks; the reserved
action is charged only once.
Creation uses no-clobber publication, and replacement uses an atomic rename.
Failed checks and cancellation drop the staged file, preserving the target.
The final atomic publication is a short synchronous operation: cancellation
cannot expose a partially written destination.

These checks coordinate cooperating tools and detect changes observed before
publication. They do not claim an operating-system compare-and-swap against an
unrelated writer that changes a file between the last revision check and rename,
or power-loss durability of a directory entry. Filesystem confinement remains
the host gate's responsibility.

See the accepted [specification](../../../../docs/specs/structural-code.md) and
[implementation plan](../../../../docs/plans/structural-code.md).
