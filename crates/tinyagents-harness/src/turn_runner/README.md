# Turn runner

`TurnRunner` contains the reusable lifecycle around one channel-agnostic
conversation turn. The host implements session checkout and fingerprint
validation, supplies origin metadata, executes the turn, and checks the
session back in. Separate ports own ingress policy, reply persistence,
progress and terminal delivery, failure classification, and grounded closing
or suggestions.

Ingress policies run in a fixed order: stage attachments, apply the guardrail,
intercept approval replies, then run before-submit hooks. A consumed approval
reply or a policy rejection never starts a turn. Accepted `Steer`, `Followup`,
and `Collect` work is handed to the host queue; `Interrupt` cancels the prior
primary generation; `Parallel` uses its own request-scoped cancellation slot
and does not serialize against the primary session lock.

Primary sessions serialize by `(agent_id, profile_id, thread_id)`. The active
slot is installed before waiting for that lock, so an interrupt can stop the
current owner and stale cancellation ids cannot stop its replacement.
Host-authored turns use the same session checkout and reply store but never
occupy the user cancellation slot. The runner persists a deterministic reply
id before emitting exactly one terminal event.

The idle guard defaults to 900 seconds and resets on progress. Hosts signal
`WorkStarted` and `WorkFinished` around model or tool work so a quiet but
healthy request is not treated as idle. The optional wall deadline remains a
separate total ceiling. Either guard cancels child work and asks the
`Summarizer` for a bounded grounded close before classifying the turn as failed.

`AgentProgress` is the canonical channel-independent progress vocabulary.
Hosts translate it to their own event wire format; this module does not define
channel event names or transport payloads.
