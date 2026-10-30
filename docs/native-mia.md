# Optional native MIA owner integration

**Optional integration for applications that already own speech.** Use [hosted custom functions or the public media bridge](runtime-switching.md) for normal deployment. The instructions below apply only to an application that already owns the speech pipeline.

The MIA backend means the private service that receives finalized meeting speech, runs its AI and sends voice responses. It is operated by the MIA owner; a website that creates hosted agent configurations does not contain that service. The SDK cannot change this private reasoning pipeline through the documented public bot API. Its owner must disable the original reasoning stage and wire one replacement. Two supported implementation boundaries are provided; they both use the same `SessionHost` and runtime adapters.

For a Python MIA backend, use [the in-process example](../examples/native_mia.py). Supply finalized transcripts, streaming output delivery, playback clearing and a pending-playback query. Delivery callbacks must recheck the host's `active` predicate immediately before every media submission. Clearing must finish cooperatively and fence obsolete queued media; it must not wait indefinitely.

For a JavaScript backend, run the Python SDK as a private reasoning sidecar with `output="events"`, then use [the Node.js example](../examples/native_mia.mjs). This mode publishes normalized v1 events over authenticated SSE and sends no speech/chat itself. It omits the bridge-only chat tool. The two demonstration tools remain project lookup and sandbox action-item creation. Real application tool handlers belong in the shared executor; the in-process SDK accepts `ToolSpec` handlers directly.

```toml
profile = "adk"
model = "gemini-3.5-flash-lite"
output = "events"
workspace = "your-workspace"
ledger = "native-mia.sqlite"
```

```js
import { attachMia } from './examples/native_mia.mjs';
const agent = await attachMia({
  server: 'http://127.0.0.1:8080',
  key: process.env.ADAPTER_API_KEY,
  botId: existingMeetingBotId,
  deliver: miaDeliverEvent,       // Existing MIA streaming speech/chat integration.
  clearPlayback: miaClearQueue,   // Existing MIA queue clear/abort integration.
});
// Monitor agent.events in the MIA supervisor. A disconnect requires restart.
// Feed stable event_id/timestamp and MIA-accepted finalized text into
// agent.turn(turn). Invoke agent.interrupt() on
// participant speech onset, and agent.close() at meeting end.
```

Node.js 22 or later is required; the example uses only native fetch, streams and crypto. The events-only service accepts these authenticated finalized turns without a second wake-word gate and never opens its own STT/TTS clients, even if an inherited transcript setting names Deepgram. MIA must serialize its per-session `turn`, `interrupt` and `close` commands. The client fences local delivery before awaiting network operations, rejects duplicate/stale transcript events, and rejects configurations that also own speech/chat. The MIA supervisor must monitor the event-stream promise and stop accepting turns after transport failure. It must independently clear playback on meeting lifecycle changes. SSE heartbeats keep idle connections active; a proxy must preserve streaming and permit the session's idle lifetime.

Creating a session with an existing `bot_id` does not create, reconfigure or remove that bot. The MIA owner retains presence, STT, media and lifecycle ownership. Keep the management credential on the backend. Remote sidecars require HTTPS; loopback HTTP is allowed. Runtime selection remains sidecar configuration (`AGENT_RUNTIME`/`AGENT_MODEL`), with the same MIA caller code for every runtime.

For this private events-only mode, `doctor` requires the management key and selected model credentials; a public callback origin and MeetStream bot-creation key are unnecessary. The events-only service rejects new meeting creation; MIA supplies its existing bot ID and retains meeting presence.

These are usable SDK boundaries, not evidence that the hosted MIA backend has been modified. Native acceptance still requires the actual backend hooks, their authorization policy, and measured real playback cancellation. The supplied callback tests establish generation fencing and protocol behavior only.

For a packaged private deployment, run guided setup in service mode, then use `docker compose up -d --build`. `config.hosted.toml` selects the events-only mode; Compose keeps its port on loopback and persists `/data`. Applications choosing this optional native integration supply their own callbacks. Hosted MIA uses the public custom functions and needs no owner-side backend hook. See [production acceptance](release-blockers.md).

The private ASGI service accepts owner-authorized tools through `create_app(settings, tool_specs=your_tool_specs)`. Each entry is the existing `ToolSpec` with its JSON schema, async handler, write flag and timeout. The shared executor validates arguments and supplies trusted workspace/session/bot context. This replaces the demonstration tools rather than adding a second tool executor; the same registered tools are available to every selected runtime. The default Compose service deliberately contains sandbox demonstration tools until its owner supplies application handlers.

Owner-supplied tools use framework-neutral instructions rather than the demo's project/action-item instructions. Set `instructions` in Settings/TOML to supply the application's own instructions. The generic ToolExecutor creates only its operation ledger; demo tables initialize with demo setup and bot-cleanup tables with the bridge. Existing ledger tables and records are preserved.
