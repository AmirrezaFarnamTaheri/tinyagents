# harness::observability::spans

This module is the host-neutral input model and Langfuse exporter for mapped
run, step, span, generation, and event observations. Hosts retain ownership of
collection, identity, content redaction/capture, and mapping their native events
into [`Span`](../spans.rs). The exporter owns the stable Langfuse JSON envelope
and delegates OTLP projection, request sizing, credentials, HTTP status and
partial-failure handling to the existing observability helpers and
[`LangfuseClient`](../langfuse/README.md).

## Lifecycle and errors

Export is pull-based: build a batch from the spans currently available and send
it at the host's chosen boundary. The exporter has no background queue, flush,
or close lifecycle. Ingestion sends one batch; OTLP sends bounded requests in
order and stops on the first error. A transport error is returned to the host.
The existing client reports failed HTTP responses, legacy `207` partial
failures, and OTLP `partialSuccess.rejectedSpans` through the shared error
path.

## Shape

`build_ingestion_batch` places one `trace-create` envelope first and preserves
the caller's span order afterward. Each observation keeps its supplied stable
id, parent id, timing, status, input/output, usage details, and metadata. Trace
ids default to `root_run_id`, then `run_id`, on the first span. Hosts should
pass a root span when they need the full run boundary represented in the
Langfuse tree.

`build_otlp_requests` projects that same batch with
`trace_export::ingestion_otlp`; callers choose both the resource service name
and instrumentation scope. The projection and client preserve Langfuse v4
parentage, payload, usage, trace attributes, request bounds, and failure
semantics.
