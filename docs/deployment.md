# Authentication and deployment

For the portable guided launcher, use the [quickstart](../quickstart.md). The commands below are for developers operating the service directly.

Use Python 3.12 or 3.13. Install only the framework extras you require, or use the committed lockfile for the exact tested dependency graph:

```sh
uv sync --frozen --all-extras
cp -n .env.example .env
chmod 600 .env
# Set the required credentials locally and generate a random management key.
uv run meetstream-adapter --env-file .env doctor
uv run meetstream-adapter --env-file .env serve
```

`doctor` reports credential presence, not values. Local serving defaults to `127.0.0.1:8080`; choose another port if occupied. A TLS reverse proxy must route HTTPS and WSS to this same service, preserve upgrades and disable response buffering for `/events/stream`. Set `PUBLIC_BASE_URL` to its HTTPS origin. Do not point a tunnel at an occupied port until confirming which application owns it.

Management routes require `Authorization: Bearer <ADAPTER_API_KEY>`, with a random key of at least 32 characters. `/health` and `/ready` contain no meeting data. A created session receives a fresh random callback token, an expiry and an expected bot binding. Callback paths and socket handshakes must match that binding. Token rotation is performed by creating a new session and closing the old one. `--session-file` creates a private file exclusively and never prints its token.

Framework run tracing is disabled for these meeting invocations. The CrewAI adapter defaults its documented `CREWAI_DISABLE_TELEMETRY=true` flag without changing the embedding application's global OpenTelemetry switch. An explicit operator environment value is preserved.

Store callback files as `session.json`, `*.session.json` or inside `.sessions/`; these are ignored by Git and the container context. Source distributions include only the documented project directories/files and exclude session files. Review archives before sharing, especially if you place secrets inside source/documentation directories.

Per-bot callbacks are unsigned, so the token-bearing URL is their authentication. These URLs must not enter access logs, tracing or error reports. The CLI disables Uvicorn access logs and INFO connection logs and suppresses provider SDK diagnostics. Configure equivalent redaction at your load balancer and tunnel provider, including path captures. Use a controlled test account; do not expose participant data in development tunnel inspection logs.

Workspace callbacks at `/webhooks/workspace` separately require `MEETSTREAM_WEBHOOK_SECRET`, raw-body HMAC SHA-256 in `X-MeetStream-Signature: sha256=<hex>`, and a timestamp inside the signed JSON body within five minutes. An unsigned timestamp header cannot establish freshness. Transcript callbacks are deduplicated before wake-word handling; no stable timestamp means the input is rejected. Lost transcripts cannot be reconstructed from the event journal. Lifecycle status is reconciled every 30 seconds because webhook delivery is best effort.

Run one application process per ledger/session set. This release uses in-memory sessions and SQLite WAL for sandbox writes. A restart deliberately ends sessions rather than replaying turns or ambiguous actions. Multiple worker processes are not supported: bot affinity and a shared session store would require a separate deployment design. Mount a persistent ledger for operation deduplication. Retention/deletion of action-item data is the operator's responsibility; no raw audio is retained by this bridge.

```sh
docker build -t meetstream-agent-adapter .
docker run --rm --env-file .env -p 127.0.0.1:8080:8080 \
  -v "$PWD/deploy-config.toml:/app/config.toml:ro" \
  -v "$PWD/data:/data" meetstream-agent-adapter
```

In `deploy-config.toml`, set `ledger="/data/actions.sqlite"`. Make the volume writable by UID 10001. The image runs as that user. It contains all four frameworks so configuration switching does not need a rebuild. This is a container recipe; see the validation report for build and runtime evidence.

The default remains the full-runtime image. For a smaller, deliberately single-runtime deployment, set `IMAGE_RUNTIME=adk` (or `openai`, `langgraph`, `crewai`) in the Compose environment before building. Alternatively, build with `docker build --build-arg AGENT_RUNTIME=adk -t mia-reasoning:adk .`. Configure the running service to use that same runtime. Switching to an uninstalled runtime requires a new image; use the default image when configuration-only switching is required.

For a smaller local installation, an explicit runtime extra also works:

```sh
uv run --frozen --extra adk --extra quickstart meetstream-adapter start --runtime adk
```

The desktop launchers keep all four runtime choices available but exclude the `dev` extra. The free offline demo can run with core dependencies alone: `uv run --frozen meetstream-adapter demo --friendly`. Test/lint commands still explicitly install `dev` or all extras.

The removed `mia_transport_only` option is now rejected as an unknown setting. Delete it from old configs; use `output="events"` and actual MIA owner integration instead. `mia_config_id` remains an explicit rejection guard for unsupported hosted-agent/public-bridge combinations.

Private reports, recordings, local databases and credential files stay out of Git, Docker and release archives. Publish reviewed copies of evidence deliberately; do not delete existing databases to clean the source tree because they may contain pending bot exits or unresolved writes.

Credentials, model endpoints and initial defaults are server configuration. Per-session runtime selection uses authenticated control routes or the explicit MIA switch tool; tool arguments never supply model endpoints or credentials. `AGENT_RUNTIME` selects `openai`, `langgraph`, `adk` or `crewai`; `AGENT_MODEL` overrides the default model. For OpenAI Agents and LangGraph, `MODEL_BASE_URL` and `MODEL_KEY_ENV` select an explicitly configured compatible provider. OpenAI Agents additionally accepts `MODEL_API=chat_completions`. The default uses OpenAI Responses. These settings are not an automatic fallback policy.

The common speech stage independently selects `SPEECH_PROVIDER=openai` (default), `gemini` or `deepgram`. Set the matching `SPEECH_MODEL`, `SPEECH_VOICE` and credential; `SPEECH_API_KEY` overrides the normal provider key. Do not set it to an OpenAI credential while selecting Gemini. Changing the reasoning runtime leaves the speech settings unchanged. Google ADK uses `GOOGLE_API_KEY` (or SDK-supported Vertex configuration supplied by the application); CrewAI's provider selection follows its model prefix.

Treat returned audio status `submitted_unconfirmed` as submission evidence only. Socket send success cannot prove audible delivery. Google Meet's external `interrupt` command clears its queue according to current documentation; Zoom and Teams require their own measured playback support before advertising parity.

See [architecture](architecture.md), [compatibility](compatibility.md) and [demo procedure](demos.md). Do not enable hosted MIA reasoning simultaneously with external reasoning.


For Deepgram speech set `SPEECH_PROVIDER=deepgram` and `SPEECH_MODEL=aura-2-thalia-en`, with `DEEPGRAM_API_KEY` or an explicit `SPEECH_API_KEY`. Aura's model name selects the voice; `speech_voice` is unused for that provider. The reasoning runtime has no control over speech credentials. Default MeetStream transcript callbacks and `TRANSCRIPT_SOURCE=deepgram` are mutually exclusive. Selecting local Deepgram disables the bot's MeetStream live-transcription request, preventing duplicate STT owners. Local STT costs and credentials then belong to this service. Neither path stores raw meeting audio in the bridge.

Bot cleanup obligations are recorded in the same persistent ledger immediately after bot creation. Failed removal remains pending and is retried by maintenance, including on service restart; only owned bot IDs are targeted. On shutdown the service fences output, closes provider streams and requests bot removal. Persistent ledger storage is necessary for cleanup after a process crash. A session's TTL is bounded to 24 hours.

Loss of a previously connected audio or control channel interrupts the active output. New turns are rejected during reconnection. The session closes after the configured grace period if either lost channel is still absent, even if the other remains connected. Replacing a socket does not let its obsolete connection close the replacement.

Known participant speech also clears submitted, unconfirmed voice playback after the local speech worker finishes. Submission completion cannot establish that the meeting platform's queue is empty. This conservative clear reconciles the previous answer as potentially interrupted; it does not claim an audible completion timestamp. The onset detector resets after a missing-frame gap because mixed-audio transports can omit silence.

Before fresh speech, the bridge sends `resume_audio` with the current turn ID. The [current control guide](https://docs.meetstream.ai/guides/websockets/meeting-control-patterns) requires this after interruption on Zoom/Teams; Google Meet ignores it. Command ordering is covered by a cancellation/recovery regression. Live Zoom/Teams acknowledgment and audible playback still require platform validation. The energy detector examines 20 ms windows rather than averaging a potentially one-second frame, and its missing-frame reset allows the normal capture duration plus 250 ms of jitter. This keeps short onsets and internal silence boundaries visible without treating every large frame as a fresh speech segment.

Speech pacing starts from each available PCM chunk, so provider startup or mid-stream stalls cannot accumulate catch-up time and burst audio into the meeting queue. A timed regression verifies spacing after a delayed provider response.

For LangGraph with native Gemini, set `AGENT_RUNTIME=langgraph`, `MODEL_PROVIDER=google` and `AGENT_MODEL=gemini-3.5-flash-lite`, with `GOOGLE_API_KEY` supplied privately. For CrewAI, `AGENT_RUNTIME=crewai` with `MODEL_PROVIDER=google` selects a Gemini-prefixed default model; an explicit `AGENT_MODEL` overrides that default and its prefix selects the provider. LangGraph and CrewAI use OpenAI defaults when no provider is configured. There is no automatic provider fallback; `MODEL_BASE_URL` and `MODEL_KEY_ENV` configure the OpenAI-compatible path for OpenAI Agents/LangGraph. Use the authenticated per-session runtime route to switch during a meeting. Restart only when changing process-level credentials or defaults.

For optional local testing, `uv run --frozen --all-extras --with ngrok==1.7.0 python scripts/tunnel.py --env-file .env --port 8097` reads `NGROK_AUTHTOKEN` privately and forwards only the selected loopback port. It prints the public origin, never the token, and closes the listener on Control + C. Ngrok is not a production SDK dependency. The live harness checks public health before creating bots; an offline tunnel fails that preflight. Use production TLS and configure traffic-log redaction on any tunnel/proxy carrying callback paths.

`POST /v1/sessions/{session_id}/stop` closes reasoning and media immediately. It returns HTTP 202 with `bot_removal_pending=true` when the owned platform bot has not yet reached a departure status; keep the service running to reconcile cleanup. Accepted remove requests do not clear the persistent obligation until independent status confirms departure. An already-absent owned bot is treated as cleaned up. The same ledger resumes cleanup after a service restart.

## Supplied Compose deployment

`compose.yaml` builds the existing non-root production image, loads `.env`, mounts `config.hosted.toml` read-only, persists `/data` and checks `/ready`. It runs `doctor` before the server so missing required credentials prevent startup. The management key is required by Compose; Google/OpenAI keys can come from `.env` or the invoking environment. The default is native ADK; the selected `AGENT_RUNTIME` and `AGENT_MODEL` may override it. The events-only service has no STT/TTS ownership. Port 8080 binds only to loopback; a MIA backend on that host can use loopback. For another host/container, supply an authenticated HTTPS route; the JavaScript client deliberately requires HTTPS outside loopback. One service process owns the persisted ledger.

Guided local startup likewise takes an OS-backed single-instance lock for its ledger and bounds ngrok connection setup to 20 seconds. It chooses a free port without probing and rebinding, validates the public endpoint before bot creation, and lets asyncio own Control+C so cleanup runs immediately. Optional `--mode mia` asks for the meeting URL, registers an owned Google-backed agent and creates its hosted MIA meeting bot; direct meeting mode creates one normal assistant. Neither starts the scripted validation speaker.

## Hosted MIA public tool bridge

No backend patch is required. `npm start` defaults to the streaming external-agent pilot with Google reasoning and shared Deepgram speech. MeetStream retains meeting presence and media/chat transport. `--mode mia` optionally registers and verifies hosted MIA custom functions, routing and voice/interruption settings. `npm stop` closes sessions, makes owned bots leave and shuts down the tunnel. It reconciles pending departures from the private ledger after a process crash; hosted MIA mode additionally verifies departure before removing its registration. Meeting admission may still require host approval; no MeetStream dashboard launch or stop is required. A durable private ownership record supports interrupted-create and cleanup recovery. All four runtimes are available with the appropriate provider credentials and model access. Use `--no-openai` to explicitly restrict a run to Google-backed runtimes, including management switches. MIA voice stays on Google. See [registration lifecycle](runtime-switching.md#hosted-mia-setup).

The separately operated events-only Compose service exposes the general four-runtime tools with `MIA_BRIDGE_TOKEN` and `PUBLIC_BASE_URL`; register those tools in the existing MIA agent yourself. Compose does not invoke the local pilot lifecycle. Preserve the agent's other tools/voice settings. Keep the dedicated tool token separate from management/provider keys. Authenticated `/v1/mia/functions` returns private configuration; keep it private.

The bridge supports authenticated live `switch` operations per session; model/credential/import preparation failure leaves the current runtime intact. A failed commit requires session restart. See [runtime switching](runtime-switching.md) for exact behavior and the completed-JSON versus incremental-media boundaries.

## Dependency security

The 23 October security refresh updates certifi to 2026.10.19 and CI/container build tooling to uv 0.13.5. The compromised emSign ECC Root CA - G3 is absent from the selected certifi bundle. The full-runtime environment still contains advisory-bearing ChromaDB, MCP, OpenTelemetry and uv versions constrained by CrewAI/ADK. See the exact constraints and exposure assessment in [release acceptance](release-blockers.md#dependency-advisories).

Guided npm and desktop startup use the maintained certifi bundle through `SSL_CERT_FILE` when falling back to environment-local uv. This avoids that old uv's affected embedded roots without violating CrewAI CLI's dependency range. Explicit `SSL_CERT_FILE` or `SSL_CERT_DIR` overrides are preserved: operators must maintain those trust stores and remove the compromised root themselves. A missing local bundle fails startup instead of silently reverting to bundled roots. Stop remains offline-safe. Use uv 0.13.4 or newer for first installation, manual uv commands and CI; do not invoke the environment-local old uv directly for network operations without an appropriate custom trust store.

The application exposes neither ChromaDB nor MCP server routes and adds no inbound OpenTelemetry instrumentation. A regression covers those absent routes and verifies that attacker-supplied trace headers never reach `TraceState.from_header`. Installing server or tracing integrations changes the exposure and requires a fresh review. After starting Docker Desktop, the updated Linux ARM64 image built successfully and passed all 208 Python tests. Separate non-root service checks verified health/readiness, management authentication, all four runtime selections on one session, interrupt/stop controls, absent dependency-server routes and graceful SIGTERM shutdown. Model execution tests used controlled models with external networking disconnected. The image's Python dependency audit retains the same four flagged packages; this is not a clean advisory scan or a live provider/voice test.

For deployments that only need ADK or LangGraph, build the corresponding single-runtime image and install only that extra; those profiles do not require the CrewAI/OpenAI MCP dependency chain. This changes which runtimes are available, so it is not equivalent to accepting all four in production.
