//! Public types for the prompt assembly module.
//!
//! These template and builder types let prompts be assembled from runtime values
//! rather than baked in, which is what makes prompt construction itself a
//! programmable step in the recursive harness — a model or graph node can render
//! the prompt for the sub-call it is about to make.
//!
//! All user-visible structs and enums live here so that [`super`] can provide
//! clean implementations without mixing type definitions and method bodies.

use serde::{Deserialize, Serialize};

use tinyinference_llm::message::Message;
use tinyinference_llm::model::{PromptSegment, ResponseFormat};
use tinyinference_llm::tool::ToolSchema;

/// The role a rendered message will take in the conversation.
///
/// Used by [`PromptTemplate::render_message`] and [`MessagesTemplate`] to
/// determine which [`Message`] variant to produce.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TemplateRole {
    /// System / developer instructions.
    System,
    /// User / human turn.
    User,
    /// Assistant / model turn.
    Assistant,
}

/// A string template that supports `{placeholder}` substitution.
///
/// # Syntax
///
/// * `{name}` – replaced with the string value of `name` from the variable map.
/// * `{{` – literal `{`.
/// * `}}` – literal `}`.
///
/// Rendering fails with [`crate::error::TinyAgentsError::Validation`] if a
/// `{name}` placeholder is present in the template but `name` is not found in
/// the provided variable map, or if a placeholder is left unclosed.
///
/// # Examples
///
/// ```rust
/// use tinyagents_harness::prompt::PromptTemplate;
/// use serde_json::{json, Map};
///
/// let t = PromptTemplate::new("Hello, {name}!");
/// let mut vars = Map::new();
/// vars.insert("name".to_string(), json!("world"));
/// assert_eq!(t.render(&vars).unwrap(), "Hello, world!");
/// ```
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PromptTemplate {
    /// The raw template string.
    pub template: String,
}

/// An ordered sequence of (role, template) pairs that renders to a
/// [`Vec<Message>`].
///
/// Each entry produces one [`Message`] when [`MessagesTemplate::render`] is
/// called with a variable map.  The entries are rendered in declaration order.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct MessagesTemplate {
    /// Ordered (role, template) entries.
    pub entries: Vec<(TemplateRole, PromptTemplate)>,
}

/// Assembles a [`ModelRequest`](tinyinference_llm::model::ModelRequest) while tracking prompt-cache segments.
///
/// Callers push segments in logical order — system, tools, instructions (all
/// cacheable), then history and volatile context (not cacheable).  The stable
/// prefix is kept at the head so providers can apply KV-cache reuse.
///
/// Call [`PromptBuilder::build`] to finalize the request, optionally appending
/// extra tail messages (e.g. the current user turn).
///
/// # Cache-segment ordering
///
/// | Push method              | [`SegmentRole`]  | cacheable |
/// |--------------------------|------------------|-----------|
/// | [`push_system`]          | `System`         | `true`    |
/// | [`push_tools_segment`]   | `Tools`          | `true`    |
/// | [`push_instructions`]    | `Instructions`   | `true`    |
/// | [`push_history`]         | `History`        | `false`   |
/// | [`push_volatile`]        | `Volatile`       | `false`   |
///
/// [`push_system`]: PromptBuilder::push_system
/// [`push_tools_segment`]: PromptBuilder::push_tools_segment
/// [`push_instructions`]: PromptBuilder::push_instructions
/// [`push_history`]: PromptBuilder::push_history
/// [`push_volatile`]: PromptBuilder::push_volatile
/// [`SegmentRole`]: tinyinference_llm::model::SegmentRole
#[derive(Clone, Debug, Default)]
pub struct PromptBuilder {
    /// Accumulated segments, in push order.
    pub(crate) segments: Vec<BuiltSegment>,
    /// Tool declarations gathered from [`push_tools_segment`] calls.
    ///
    /// [`push_tools_segment`]: PromptBuilder::push_tools_segment
    pub(crate) tools: Vec<ToolSchema>,
    /// Optional response-format override applied in [`build`].
    ///
    /// [`build`]: PromptBuilder::build
    pub(crate) response_format: Option<ResponseFormat>,
}

/// One behavior-free, already-rendered prompt section.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PromptSection {
    /// Stable caller-owned section label used in truncation provenance.
    pub name: String,
    /// Text to append in caller order.
    pub content: String,
}

impl PromptSection {
    /// Creates a named section.
    pub fn new(name: impl Into<String>, content: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            content: content.into(),
        }
    }
}

/// Evidence that a section was shortened to satisfy a caller-supplied cap.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PromptTruncation {
    /// Name of the section that did not fit.
    pub section: String,
    /// Number of source bytes omitted at a UTF-8 boundary.
    pub omitted_bytes: usize,
}

/// Ordered assembly output and truncation provenance.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PromptAssembly {
    /// The bounded assembled text.
    pub text: String,
    /// Names of whole sections included before any truncation.
    pub included_sections: Vec<String>,
    /// The first truncated section, if the byte cap was reached.
    pub truncation: Option<PromptTruncation>,
}

/// Caller-supplied byte and token ceilings for pure prompt assembly.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PromptBudget {
    /// Maximum total bytes of assembled text.
    pub max_bytes: usize,
    /// Maximum total tokens of assembled text, per the caller's tokenizer.
    pub max_tokens: usize,
}

/// Relative stability of a system-prompt section. Sections are emitted in
/// this order so a changing tail cannot invalidate an earlier cacheable prefix.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum PromptTier {
    /// Build- and agent-stable instructions.
    Stable,
    /// Context fixed for a session, such as project instructions.
    Context,
    /// Content that may change between turns.
    Volatile,
}

/// One rendered prompt slice assigned to a cache tier.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TieredPromptPart {
    /// Cache tier for this slice.
    pub tier: PromptTier,
    /// Already-rendered UTF-8 text.
    pub content: String,
}

impl TieredPromptPart {
    /// Creates a prompt slice for `tier`.
    pub fn new(tier: PromptTier, content: impl Into<String>) -> Self {
        Self {
            tier,
            content: content.into(),
        }
    }
}

/// A section whose output depends on caller-owned context.
///
/// Implementations should return only non-empty slices. The composer calls
/// each section once per build and groups slices by tier after rendering.
pub trait DynamicPromptSection<C>: Send + Sync {
    /// Stable section label, used for diagnostics and future provenance.
    fn name(&self) -> &str;
    /// Render this section using the context supplied by the host.
    fn render(&self, context: &C) -> crate::Result<Vec<TieredPromptPart>>;
}

impl<C, F> DynamicPromptSection<C> for F
where
    F: Fn(&C) -> crate::Result<Vec<TieredPromptPart>> + Send + Sync,
{
    fn name(&self) -> &str {
        "dynamic"
    }
    fn render(&self, context: &C) -> crate::Result<Vec<TieredPromptPart>> {
        self(context)
    }
}

impl<C, T> DynamicPromptSection<C> for std::sync::Arc<T>
where
    T: DynamicPromptSection<C> + ?Sized,
{
    fn name(&self) -> &str {
        (**self).name()
    }
    fn render(&self, context: &C) -> crate::Result<Vec<TieredPromptPart>> {
        (**self).render(context)
    }
}

/// Fully rendered system prompt with stable/context/volatile cache boundaries.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TieredPrompt {
    /// Flattened text, with one trailing newline when non-empty.
    pub text: String,
    /// Byte offsets at the ends of stable and context tiers when later bytes follow.
    pub breakpoints: Vec<usize>,
    /// Rendered tier content in stable, context, volatile order.
    pub parts: Vec<(PromptTier, String)>,
}

// ---------------------------------------------------------------------------
// Private internals
// ---------------------------------------------------------------------------

/// A single assembled segment held by [`PromptBuilder`].
#[derive(Clone, Debug)]
pub(crate) struct BuiltSegment {
    /// The messages that belong to this segment (empty for a tools segment).
    pub(crate) messages: Vec<Message>,
    /// Segment metadata propagated to [`ModelRequest::cache_segments`].
    pub(crate) meta: PromptSegment,
}
