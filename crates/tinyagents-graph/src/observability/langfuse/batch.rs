//! Transport-independent Langfuse graph payload construction.

use super::*;

/// Build one graph ingestion batch without constructing a transport client.
///
/// Use this when the host supplies authentication, endpoint selection, or an
/// export sink separately. The result has the same trace, structural run span,
/// graph spans, point events, and health metadata as
/// [`GraphLangfuseExporter::build_ingestion_batch`]. No environment variables,
/// credentials, or network access are required.
///
/// `span_metadata_fn` receives each span's coordinate observation. Its keys
/// override the built-in coordinate metadata, matching the exporter's hook.
/// An empty observation slice returns [`TinyAgentsError::Validation`].
pub fn build_graph_langfuse_batch(
    trace: LangfuseTraceConfig,
    observations: &[GraphObservation],
    span_metadata_fn: Option<&SpanMetadataFn>,
) -> Result<Value> {
    if observations.is_empty() {
        return Err(TinyAgentsError::Validation(
            "at least one observation is required".to_string(),
        ));
    }
    let trace_id = resolve_trace_id(&trace, observations);
    let first = &observations[0];
    let trace_ts = iso_ms(first.ts_ms);
    let health = GraphHealthSummary::from_observations(observations);

    let mut batch = Vec::with_capacity(observations.len() + 2);
    batch.push(trace_create(&trace_id, &trace_ts, &trace, first, &health));

    // A structural span for the graph run itself, sitting directly under the
    // trace. Every step, subgraph, and point event nests under it, and — key
    // for a unified trace — its id follows the same `{trace}:run:{run_id}`
    // scheme the harness exporter parents its run spans to. A sub-agent that
    // a graph node spawns carries `parent_run_id == graph run id`, so its
    // harness-exported run span nests directly under this graph-run span
    // rather than floating at the trace root.
    let run_parent = run_span_id(&trace_id, first.root_run_id.as_str());
    batch.push(graph_run_span(&trace_id, &run_parent, observations, first));

    let mut consumed = vec![false; observations.len()];
    push_span_events(
        &trace_id,
        &run_parent,
        observations,
        &mut consumed,
        &mut batch,
        span_metadata_fn,
    );
    push_point_events(&trace_id, &run_parent, observations, &consumed, &mut batch);

    Ok(json!({ "batch": batch }))
}
