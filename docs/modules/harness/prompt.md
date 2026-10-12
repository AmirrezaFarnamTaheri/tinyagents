# Harness Prompt Feature

The prompt feature owns reusable prompt templates, dynamic system prompts,
message placeholders, few-shot examples, rendered prompt caching, and injection
of runtime context into prompts.

## Source Inspiration

LangChain prompt primitives include string prompts, chat prompts, message
templates, image prompts, few-shot prompts, structured prompts, prompt loading,
and prompt unit tests:

- prompt core:
  <https://github.com/langchain-ai/langchain/tree/master/libs/core/langchain_core/prompts>
- prompt tests:
  <https://github.com/langchain-ai/langchain/tree/master/libs/core/tests/unit_tests/prompts>
- dynamic prompt middleware:
  <https://github.com/langchain-ai/langchain/blob/master/libs/langchain_v1/langchain/agents/middleware/types.py>

## Responsibilities

- Render system, user, assistant, and tool message templates.
- Support message placeholders.
- Support runtime context variables.
- Support few-shot examples.
- Support multimodal prompt content.
- Support structured prompt schemas.
- Validate required variables.
- Keep rendered prompts observable.
- Cache rendered prompts when safe.
- Support dynamic prompts through middleware.

## Template Rules

Prompt rendering should be explicit and typed. A template should declare:

- required variables
- optional variables and defaults
- output message role
- output content blocks
- whether rendered output is cacheable
- redaction hints for rendered content

Prompt rendering errors should happen before model invocation and should be
classified separately from provider errors.

## Runtime Context

Dynamic prompts may use:

- run metadata
- thread id
- user/application context
- selected model profile
- retrieved context
- memory summaries
- current date/time when explicitly configured

Dynamic prompts should not access global state implicitly. Time, random ids, and
external data should come from `RunContext` or configured providers so tests can
remain deterministic.

## Stable object ordering

PromptBuilder fingerprints serialize JSON objects with recursively sorted keys,
including objects inside arrays. This preserves the existing default
serde_json fingerprint bytes when a downstream dependency enables
serde_json/preserve_order. Equal object contents produce equal fingerprints
regardless of insertion order. Arrays retain their order; changing an array's
order still changes the fingerprint.

The same rule applies when the harness turns tool schemas or assistant tool
calls into prompt text: compact signatures and argument notes, skill disclosure,
deferred search results, and text-mode call replay. Caller-owned schemas and
arguments are not mutated. Scalars use serde_json's normal encoding; this is
sorted-key serialization, not RFC 8785 numeric canonicalization.

These guarantees cover harness fingerprints and prompt renderers, not provider
wire serializers. Enabling insertion-ordered JSON can also enlarge public
result representations and trigger enum-size lint failures. This ordering fix
does not change those representations or establish complete Mongo/harness
production feature compatibility.
