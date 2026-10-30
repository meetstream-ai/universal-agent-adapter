# Architecture and contract v1

The package has one session host, one shared tool executor and four optional runtime adapters. MeetStream owns meeting presence, audio capture/transport and chat delivery. The default local pilot uses its supported external-agent connection; no MIA backend change is required. The SDK accepts final transcripts from MeetStream callbacks or one explicitly selected shared Deepgram connection. Guided streaming startup selects Deepgram. The latter is an alternative public-bridge input stage; it does not modify hosted MIA. The public bridge owns a single shared speech stage and chat delivery. Each framework owns its model/tool loop. `NativeMIAOutput` lets a caller-owned MIA pipeline retain speech ownership instead.

```
MeetStream final transcript -> authenticated callback -> turn policy -> SessionHost
                                                           |
                         OpenAI Agents / LangGraph / ADK / CrewAI
                                      | shared ToolExecutor
                                      v
                       ordered events -> common speech/chat output
MeetStream audio -> speaker/energy gate -> cancellation + playback clear
```

`AgentAdapter.open_session(context, tools)` returns a `RuntimeSession` with capabilities, an asynchronous `run_turn(turn, history)` iterator and `close()`. The host owns the running task and cancellation. An adapter must propagate cancellation and close its iterator in `finally`; it must never substitute a completed answer split into artificial tokens.

Every accepted turn has an immutable ID, stable input-event ID and trusted session context. The host assigns event sequence numbers and generation numbers. Events include `turn.started`, `assistant.text.delta`, `assistant.message.completed`, tool lifecycle events and exactly one of `turn.completed`, `turn.cancelled`, `turn.failed`. Message completion carries the accumulated text for verification; speech consumes deltas and flushes its existing buffer without speaking the full message again.

Cancellation increments the generation before awaiting cleanup or network I/O. Every media submission rechecks generation inside its send lock. Runtime cleanup has a one-second deadline; common output clearing uses bounded worker and socket waits. A runtime that ignores cancellation poisons that session and cannot accept another turn. Failure from an obsolete runtime cannot clear a newer turn's audio. Reasoning completion and playback status are distinct: interruption can still clear speech after `turn.completed` without publishing another terminal event.

All adapters use fresh invocation state with bounded common history (40 messages; 64,000 characters before inference). Interrupted input and a delivery marker are preserved; partial assistant output is not committed as a completed answer. This initial release does not retain private framework memory/checkpoints across turns. Applications requiring those features must implement and test their own state reconciliation before claiming full support.

Only selected answer text is public. LangGraph exposes the `answer` node, ADK excludes thought parts and other authors, OpenAI Agents rejects handoffs, and CrewAI uses an explicit ReAct executor and exposes text after `Final Answer:` when tools are available. CrewAI's no-tool native path streams its direct response without a ReAct marker. Multi-agent delegation, private tool inventories and native persistent-memory integration are outside this release's supported profile.

The executor validates JSON Schema, enforces the allowlist, injects workspace/session scope and imposes a deadline and 16-call limit. It tracks native tool tasks in the turn's execution scope. Cancellation and deadline cleanup cancel and await those tasks as well as the framework runner; an unfinished task poisons the session. A SQLite operation ledger records a write before execution. Completed repeats with the same scoped call ID return the stored result; changed arguments are rejected. Interrupted or uncertain writes become `outcome_unknown` and are never automatically replayed. Tool handlers must use the supplied operation ID when the downstream service supports idempotency. Read tools do not require a durable operation record.

OpenAI and ADK preserve native call IDs. LangGraph and CrewAI allocate host IDs at the wrapper boundary: deduplication does not imply that a model's new call with equivalent arguments is the same operation. Automatic model/transport retries are disabled where supported; there is no host retry loop. A meeting display name is never an authorization identity. Demo writes are local sandbox action items, not production task creation.

Limits: 8 sessions by default, one active reasoning turn per session, 32 pending inputs, 1,024 distinct input events per session, 512 retained output events, 16,000 input characters, 64,000 output characters, eight queued speech phrases capped at 120 text characters, 30 seconds of audio per phrase and a 60-second turn deadline. Callback JSON is limited to 64 KiB, binary audio to 256 KiB. Unknown audio message types are ignored; malformed length prefixes and partial samples are rejected.

The public audio stream is **mixed** audio. Dominant-speaker metadata does not isolate individual speech. The conservative energy gate excludes this bot and `NoSpeaker`, requires 120 ms above threshold in 20 ms windows and triggers once per speech segment. It preserves silence inside large capture frames and distinguishes a normal one-second capture cadence from a missing-frame gap. This is an initial barge-in detector, not speaker verification or a full acoustic VAD model. Known attribution and real platform evidence are required for audible interruption acceptance.

## Hosted MIA tool connection

Hosted MIA invokes the bridge's authenticated `delegate_agent`, `switch_agent` and `interrupt_agent` HTTPS functions. The envelope must match the `X-Bot-ID` header. A dedicated tool token cannot authorize management routes. Durable request receipts reject changed arguments, replay recorded results and prevent re-execution of uncertain requests after a restart. Authenticated activity renews the session's idle lease.

The function returns completed JSON to MIA, which owns its speech and audible interruption. It does not stream voice chunks through a JSON response. The separate media bridge rejects a hosted MIA configuration during bot creation to prevent two independent reasoning/speech owners on the same bot. Applications embedding the SDK can instead provide their own delivery callbacks through `NativeMIAOutput`.

## Runtime switching

`POST /v1/sessions/{sid}/runtime` prepares a replacement, then fences old output, awaits cancellation and closes the old runtime before admitting new work. Bot identity, sockets, speech, authorized tools, write ledger and common history stay attached. Queued input from the previous runtime is discarded. Preparation failure keeps the existing runtime; unconfirmed commit cleanup marks the session unsafe. `runtime.switched` is a control event with the old/new profile and selection counter, separate from turn terminal events.

## Process lifecycle

Guided startup holds one OS-backed lock per local ledger, prebinds its socket and checks public reachability before bot creation. Private local instance state lets `npm stop` reach only this checkout's process using a fresh random token. Ctrl+C, authenticated stop and POSIX service termination use the same session/tunnel cleanup. A stopped instance releases its lock and removes its control state; a crash leaves durable bot-departure obligations for the next start. POSIX fixed ports can be reused after normal shutdown. Windows uses exclusive port ownership; the default free-port selection avoids waiting for a previous TCP connection to expire.

Sources: [audio wire format](https://docs.meetstream.ai/guides/websockets/real-time-audio-streaming), [control and interruption](https://docs.meetstream.ai/guides/websockets/meeting-control-patterns), [bridge architecture](https://docs.meetstream.ai/guides/websockets/bridge-server-architecture), [MeetStream Python SDK](https://github.com/meetstream-ai/meetstream-python), [MIA configuration](https://docs.meetstream.ai/guides/mia/mia-custom-configurations).

The optional Deepgram input sends a brief initial silence before bot admission, keeps its authenticated connection alive, accumulates final segments and commits at a current 2.5-second utterance boundary. Boundaries older than recognized interim/final words and the provider's stale `last_word_end=-1` are ignored. A short `speech_final` endpoint alone cannot execute a request. [Gap events can fire while speech continues](https://developers.deepgram.com/docs/utterance-end); semantic completeness still requires live validation. Mixed frames with `NoSpeaker` reach STT, while conservative acoustic interruption requires known attribution. Outbound 24 kHz provider PCM is converted to 48 kHz with a bounded sample-hold converter.


`output="events"` is the media-free reasoning sidecar mode for a caller-owned MIA pipeline. The event journal/SSE stream is the output; bridge speech/chat and the bridge-only chat tool are disabled. See [native owner integration](native-mia.md). An explicitly interrupted completed response with unconfirmed delivery also adds a reconciliation marker, even when the SDK's speech worker has finished submitting audio. Shared chat-tool sends use the same generation predicate inside the send lock as normal assistant output.

Unknown mixed frames are suppressed during possible self playback and for a short post-submission echo window. Known participant onset can still interrupt playback and enable the participant stream. This cannot replace per-speaker tracks or acoustic echo cancellation: simultaneous speech and incorrect dominant-speaker labels remain platform limitations.

The published [protocol schema](protocol-v1.schema.json) is generated from the Pydantic contract. Run `uv run --extra dev python scripts/protocol_schema.py --check` to verify drift, or omit `--check` to regenerate. Public `trace_id` and `state_strategy` metadata remain compatible v1 fields; no new tracing or persistent-framework-memory guarantee is implied.


Incoming audio accepts both length-prefixed `0x01` frames (48 kHz) and rate-tagged `0x04` mono PCM16 frames. Type-4 input is normalized to 48 kHz before common STT and speech-onset detection; the live Google Meet acceptance observed 24 kHz type-4 frames. Unsupported rates/channels and malformed metadata are rejected. The documented legacy JSON `PCMChunk` envelope is normalized into the same input path. Audio and control sockets use channel-specific acknowledgements. See [control/audio formats](https://docs.meetstream.ai/guides/websockets/meeting-control-patterns).

The voice acceptance observer has its own bot name for self-audio exclusion. It must hear the assistant; reusing the assistant's self-filter would incorrectly suppress the very speech the test is supposed to verify.

Speech inserts word boundaries in camel-case identifiers such as `GammaPilot`, `LangGraph` and `CrewAI` before synthesis. SDK text events, meeting chat and tool arguments keep their original text. This improves spoken word boundaries without rewriting transcripts or stored business data.

Shutdown fences reasoning and input and closes local sockets before reconciling owned-bot departure. Each removal request has a hard deadline. A slow or lost response triggers an independent check. If the status endpoint remains cached at `InMeeting`, a matching bot-detail response with a terminal `Status` can confirm departure. Uncertain exits retain their durable cleanup entry.
