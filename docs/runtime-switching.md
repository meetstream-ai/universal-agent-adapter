# Bridge runtime switching

The bridge routes requests to **OpenAI Agents SDK, Google ADK, CrewAI or LangGraph** and supports switching during a meeting. MIA retains its existing backend and speech pipeline.

## Two public connection paths

| Path | Meeting/speech owner | Bridge interface | Streaming boundary |
|---|---|---|---|
| Hosted MIA custom functions | Existing hosted MIA agent | `delegate_agent`, `switch_agent`, `interrupt_agent` HTTPS tools | Framework streams internally; MIA receives a completed JSON answer and owns speech |
| Public media bridge | MeetStream bot plus this shared speech module | Existing audio/control sockets, callbacks and management API | Incremental answer text reaches shared speech output |

Both use the same four adapters, tool executor and SessionHost. Neither needs a MIA backend patch. The optional in-process/events-client integration remains available for applications that already own speech; it is not a prerequisite.

The [official custom-function guide](https://docs.meetstream.ai/guides/mia/mia-custom-configurations) documents the JSON envelope, authentication and bot context. This is a tool integration, not an undocumented replacement of MIA's built-in model. MIA continues to select tools and deliver their results.

## Streaming pilot (default)

`npm start` uses MeetStream's supported external-agent connection. MeetStream owns the meeting bot, audio transport and chat; the bridge shares one STT/TTS stage across the four runtimes. Text deltas feed the speech queue as they arrive, and interruption fences the runtime and clears MeetStream playback. The same application switches OpenAI Agents SDK, Google ADK, CrewAI or LangGraph through configuration or authenticated controls. There is no hosted MIA backend change and no hosted router generating a second answer.

Use `npm start -- --runtime langgraph` to select the initial runtime; say **“Okay assistant, switch to ADK”** to change during the meeting. All four runtimes are available with the appropriate provider credentials. Deepgram supplies shared input/output speech.

## Hosted MIA setup

`npm start -- --mode mia` selects the optional hosted MIA custom-function pilot. It asks for missing model-provider, MeetStream and ngrok credentials privately, starts the events bridge, validates public HTTPS, and registers an owned Google realtime MIA agent with all three tools and the tested routing/interruption settings. It asks for the meeting URL and automatically joins with **Adapter MIA Pilot**. Admit the bot if the meeting requires host approval; keep the bridge running. Noninteractive startup requires `--meeting <URL>`. No function-file copying, prompt editing or backend patch is needed.

ADK is the fresh-install default; later starts remember the selected runtime. All four choices appear in the hosted function schema and routing prompt. Use `--runtime` to choose one at startup and `--model` to override its model. Setup requests missing provider credentials. Provider errors do not trigger automatic fallback.

For a restricted Google-only session, use `--no-openai`. This excludes the OpenAI runtime from the available choices and resets a saved OpenAI startup selection to ADK. It cannot be combined with `--runtime openai`.

The launcher persists a private unpredictable ownership name in `.adapter/mia-pilot.json` before creating a cloud configuration. It records the configuration ID, verifies the saved Google provider and tool URLs/schemas/headers, and cleans only configurations with that exact ownership name. An interrupted or lost create response is reconciled by name. Each new start reconciles the previous registration and registers the current HTTPS address. Existing unrelated MIA configurations are preserved. Start only one launcher per project state directory and keep its private ownership state until cleanup is confirmed.

`npm stop` or Ctrl+C cancels bridge sessions, removes the owned bot and confirms departure before removing the registration, deletes the transient function export and closes the bridge/tunnel. Shutdown waits for in-flight registration and bot creation before removing its result. If the cloud is unavailable, the pending ownership record is retained; `npm stop` can retry even after the local process has exited. A confirmed-clean repeated stop needs no cloud access. Shutdown can wait for cloud requests and local draining; the stop command bounds its overall wait to 150 seconds. A durable bot ID, create key and unique ownership attributes allow stop to recover after a crash or lost create response. Accepted leave requests are polled until independent status confirms departure. If creation remains unconfirmed, cleanup stays pending rather than risking a new bot or removing an unrelated one.

The private `.adapter/mia-functions.json` export remains available while running for diagnostics; it contains a tool token and is removed on shutdown. The saved environment-file path lets cleanup recover credentials for custom `--env-file` starts without copying provider secrets into the ownership record. The account's Gemini integration must already be configured; bridge `.env` values do not provision hosted account integrations. Registered HTTPS hostnames must resolve publicly.

### Hosted MIA voice configuration

Save your Google key once under **MeetStream → Integrations → Models → Gemini**. Automatic registration selects **realtime**, provider **google**, model **gemini-2.5-flash-native-audio-preview-12-2025**, and voice **Puck**. Google supplies reasoning and voice. The launcher applies the functions, cancellation-before-acknowledgement routing prompt and settings below. These fields are documented in the [MIA API reference](https://docs.meetstream.ai/guides/mia/mia-api-guide).

The hosted voice configuration is independent of the selected external runtime. All four adapters use the same registered tools and switching interface. Verify audible interruption when changing hosted settings; the bridge cancels external work while MIA controls its own playback.

The successful Google meeting acceptance used cancellation-before-acknowledgement routing and these settings, now applied automatically alongside the generated `custom_functions`:

```json
{
  "enable_interruptions": true,
  "interruption_mode": "vad",
  "interruptions": {"min_duration_seconds": 0.15, "word_threshold": 1},
  "resume_false_interruption": false,
  "false_interruption_timeout": 0.5,
  "response_modality": "audio"
}
```

This setting favours prompt interruption over ignoring brief background speech. Tune it for your meeting acoustics and repeat acceptance. A weaker prompt initially said “Stopped” without cancelling external work; acknowledging the phrase is not evidence of cancellation. Verify the `interrupt_agent` result and `turn.cancelled` journal entry for a pending turn, plus actual cessation and recovery of hosted speech. These are hosted agent settings, not MIA backend changes. When updating an agent through the API, preserve its existing functions and other behaviour settings; the `agent` block is replaced rather than merged.

For non-interactive startup:

```sh
npm start -- --mode mia --runtime adk
```

For an existing MIA agent or an unattended deployment, the separately operated private Compose service also serves these tools when `.env` contains `MIA_BRIDGE_TOKEN` and `PUBLIC_BASE_URL`, with `output="events"`. Your HTTPS proxy must route `/mia/` to the service. Retrieve the configuration through authenticated `GET /v1/mia/functions`; do not forward the management key to MIA. `MIA_BRIDGE_TOKEN` authorizes only the tool routes. Each request must have matching `X-Bot-ID` and `bot.bot_id`.

## Switching the public media bridge during a meeting

Guided **meeting** mode enables explicit voice switching. Say **“Okay assistant, switch to ADK”**, **LangGraph**, **CrewAI** or **OpenAI**. The meeting chat confirms the selection. Whole switch commands recognize “land graph”/“line graph” for LangGraph, “a dee kay”/“A D kay” for ADK, and “crew eye”/“Crew I” for CrewAI, including punctuation between initials. The control also accepts ASR “switched” for “switch,” “run time” for “runtime,” and an explicit “switch the provider to …” command. An unknown runtime name produces a clarification in chat and keeps the current runtime; it never becomes an agent task or project lookup. Quoted/discussed instructions and ordinary chart requests do not select a runtime. Every participant allowed by the configured turn policy can request this control; custom deployments opt in with `voice_switching=true`. Management switching remains authenticated separately.

Direct meeting startup also prints the session ID and a switch command. While it stays running:

```sh
uv run --frozen --all-extras --no-extra dev meetstream-adapter --env-file .env switch YOUR_SESSION_ID --profile langgraph --provider google --server http://127.0.0.1:YOUR_PORT
```

Or send an authenticated management request:

```http
POST /v1/sessions/YOUR_SESSION_ID/runtime
Authorization: Bearer YOUR_MANAGEMENT_KEY
Content-Type: application/json

{"profile":"langgraph","provider":"google"}
```

Optional `model` selects an explicit model. `provider` selects LangGraph's model backend or the default CrewAI model prefix; ADK uses Google and the OpenAI adapter uses its configured OpenAI-compatible API. Runtime credentials/base URLs remain operator configuration, never model-supplied tool arguments. `GET /v1/runtimes` lists permitted frameworks and their installation status, and session status reports the selected runtime/model/revision.

## Switch behavior

Preparation checks local credentials, imports and adapter capabilities while leaving the current runtime intact. It does not spend a model turn proving provider quota/model availability; those can still fail on the next request.

Commit fences old output, cancels/awaits native and tool tasks, clears pending local/platform playback, closes the old runtime and activates the replacement. Bot identity, sockets, shared speech configuration, authorized tools, durable write ledger and bounded common conversation history stay attached to the same session. Queued old input is discarded, including a turn already dequeued while the switch prepared. The interrupted request is not automatically replayed.

If preparation fails, the existing runtime remains usable. If commit cleanup cannot be confirmed, the session is marked unsafe and requires restart; the bridge does not run two agents concurrently. The SSE journal records `runtime.switched` as a control operation with previous/new profile and revision, separate from turn terminal events.

Common history transfers; private framework checkpoints/memory do not. Interrupted writes remain `outcome_unknown` and are not retried automatically. MIA tool request receipts persist across process restarts: identical request IDs return recorded results, changed arguments are rejected, and uncertain requests are not re-executed. Keep the database persistent and define retention before deleting receipts.

A switch receipt is returned only while the same session ID and selection/revision remain current and the runtime is safe. Unsafe state requires session restart. An old switch retry after a later switch or process restart returns 409; submit a new request ID to select it again. Authenticated MIA activity renews the idle session lease. Idle expiry or restart ends in-memory conversation sessions, while completed write receipts remain available.

The JSON MIA boundary cannot revoke an answer already handed to MIA or provide incremental voice chunks through the custom-function response. Explicit bridge interruption, HTTP cancellation/disconnect and switch calls cancel pending external work; MIA remains responsible for audible interruption of its own speech. Full streaming/playback behavior is supported on the public media path and must be measured in live acceptance.

## Validation

Controlled native runners exercise every framework across runtime switches, including an active write, streaming next-turn recovery and common ledger preservation. HTTP tests cover same-bot/same-socket selection, per-meeting isolation, failed preparation, stale queued input, pending post-generation speech, MIA envelope/authentication, retry receipts and private launcher export. These tests do not certify paid provider availability or real hosted MIA playback. See [current validation](validation.md).
