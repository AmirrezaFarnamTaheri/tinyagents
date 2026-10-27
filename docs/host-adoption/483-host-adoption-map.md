# OpenHuman adoption map for #483

This leaf places generic transcript conversions and legacy row serialization in
TinyAgents. OpenHuman keeps attachment policy, product presentation, and its
provider response adapters.

| OpenHuman copy | TinyAgents API | OpenHuman work after the gitlink is merged |
| --- | --- | --- |
| `crates/openhuman-core/src/agent/messages.rs::history_wire` and `history_wire::option` | `tinyagents_session::transcript::legacy::history_wire::{serialize, deserialize}` and `...::history_wire::option::{serialize, deserialize}` | Replace the local serializer/deserializer and keep the Serde field attributes pointed at the TinyAgents modules. Both adapters continue to read and write only `{role, content}`; reads normalize legacy tool envelopes and private `[OH_IMAGE:…]` rows, while writes use `legacy_content()` or host-visible `display_content()` for rows with typed parts. Invalid row shapes remain Serde errors. Delete `messages.rs`'s generic `wire_content`, `lifted`, and `serialize_rows` functions and their tests after adoption. |
| `crates/openhuman-core/src/agent/message_convert.rs::chat_message_to_message` | `tinyagents_runtime::transcript::transcript_to_message(&TranscriptMessage) -> Message` | Replace the wrapper with the runtime call. The runtime preserves typed tool calls/results and media parts and now also lifts legacy `[IMAGE:<reference>]` markers in untyped user text. |
| `message_convert.rs::history_to_messages` | `transcript_to_messages(&[TranscriptMessage]) -> Vec<Message>` | Replace the per-row map. This is a projection only and deliberately does not validate a complete native tool-call sequence, matching the host helper. Use `native_transcript_to_messages` only where complete native history validation is intended. |
| `message_convert.rs::messages_to_history` | `messages_to_display_transcript(&[Message]) -> Vec<TranscriptMessage>` | Replace the per-row display projection. Custom messages remain omitted; assistant tool declarations and user media remain omitted just as in the current display projection. Keep native persistence on `message_to_native_transcript`. |
| `message_convert.rs::messages_to_conversation` | `messages_to_conversation(&[Message]) -> Vec<tinyagents_harness::tinytools_agent::dialect::TranscriptEntry>` | Replace the implementation. It retains typed assistant tool declarations, groups adjacent tool results, preserves reasoning metadata, flushes results before a chat entry, and omits custom messages. The dialect types use the harness's public `tinytools_agent` re-export. |
| `message_convert.rs::messages_since_last_user` and `messages_since_request` | `messages_since_last_user(&[Message]) -> &[Message]` and `messages_since_request(&[Message], usize) -> &[Message]` | Replace both helpers. The first returns the suffix after the latest user or all messages if there is no user. The second slices from the captured request length, clamped to the transcript length. This keeps tool rounds and mid-turn user steering after the request boundary. |
| `message_convert.rs::user_content_blocks` and image-marker parsing in `user_message_from_text` | `user_content_blocks(String) -> Vec<ContentBlock>` and `user_message_from_text(&str) -> Message` | Use the runtime parser for generic `[IMAGE:…]` text. Keep the `crate::attachments` branch that recognizes OpenHuman upload markers and maps local audio, video, and document attachments. Pass its ordinary text segments through the TinyAgents parser. Runtime parsing retains malformed/empty marker text, handles multiple images in source order, and leaves prose intact. |

Keep `user_text_with_markers` and `messages_to_history_projection` in the host.
They are presentation adapters: the former chooses the user's display string,
and the latter recognizes OpenHuman's replayed textual `[Tool results]` frame
as a dialect `ToolResults` entry. Keep `provider_messages_from_conversation`,
`row_to_dialect_message`, and the `ChatResponse` conversion at the provider
boundary as well; those adapt OpenHuman-owned provider responses and product
metadata rather than generic transcript rows.

The focused TinyAgents tests cover known legacy `{role, content}` fixtures,
typed assistant/tool envelope round trips, image marker ordering and malformed
markers, plain text preservation, tool-result grouping, and both history cursor
boundaries. OpenHuman should retain integration coverage for upload markers,
attachment authorization, and product presentation, then delete the duplicate
generic helper tests from `message_convert_tests.rs`.
