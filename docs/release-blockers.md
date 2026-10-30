# Release acceptance and deployment limits

The bridge supports four native runtimes, shared tools, streaming and cancellation, per-meeting switching, one-command startup/shutdown and hosted MIA custom functions. **No MIA backend change is required.**

The selected rollout is a **local pilot**. Default `npm start` uses the documented external-agent audio bridge, asks for the meeting URL and joins automatically. Hosted MIA registration is available through `--mode mia`; its functional checks below are separate. `npm stop`, Ctrl+C and post-crash cleanup have live command evidence. Continuous production hosting is outside the selected pilot scope; its requirements are recorded separately for a future rollout.

## Local-pilot acceptance

The project owner accepted the local pilot on **24 October 2026**, treating MeetStream's delayed bot-departure status as an external-service exception. It does not block this pilot release. All nine voice/runtime checks passed; the full harness retains its factual 9/10 result. The bridge keeps uncertain cleanup entries and retries them on stop/start. Existing dependency advisories and future deployment limits below remain documented separately.

## Verified streaming pilot behaviour

On **21 October 2026**, the default external-agent bridge passed all eight checks in the finite Google Meet voice harness: ADK tools/voice, LangGraph tools/voice, spoken runtime switching on the same bot/session, pending-tool cancellation, audible-speech interruption, fresh-turn recovery and owned-resource cleanup. The independent listener heard the actual meeting audio. Both bots subsequently reported `Done`. This meets the original two-runtime local-pilot acceptance through the supported media bridge, without MIA backend changes or OpenAI usage. See [validation](validation.md) for evidence and reproduction.

This verifies functional streaming delivery and interruption; it does not establish a quantitative latency target or production uptime. Native framework conformance covers all four adapters, while funded live OpenAI acceptance remains an account prerequisite below.

## Verified hosted MIA behaviour

On **20 October 2026**, an isolated Google Meet used Google realtime for hosted MIA voice and Google models for the external runtimes. No OpenAI inference or speech services were used.

- Spoken requests reached ADK and LangGraph through MIA's actual HTTPS custom functions. Both performed lookup/write operations and MIA spoke their results. ADK → LangGraph preserved one bot and one bridge session.
- A separate spoken ADK → CrewAI switch and sandbox write succeeded. This supplements the five earlier CrewAI provider checks; the meeting write alone does not establish that every requested read was executed.
- With the explicit [Google agent settings and routing prompt](runtime-switching.md#hosted-mia-voice-configuration), an explicit spoken interrupt cut a four-step answer partway through. The interrupt receipt reported safe cancellation; the next spoken request recovered correctly. This is functional meeting evidence, not sample-accurate playback latency measurement.
- A spoken stop cancelled a deliberately delayed 20-second lookup before it completed. The bridge recorded `turn.cancelled` with `reason=mia_tool_request` and `runtime_stopped=true`. The same bot/session then switched to LangGraph, performed a fresh lookup and exactly one write, and spoke the result.
- All eight owned test bots reached `Done`. All four temporary agent configurations were deleted, and the original three account configurations were preserved. The Google integration remains connected. Test tunnels/services were stopped. Earlier automatic-registration checks created no bots. Additional command lifecycle checks confirmed automatic MIA arrival and departure after `npm stop` and Ctrl+C, preserving the original three configurations and removing owned test registrations.

The first weak routing prompt failed: hosted MIA continued speech or acknowledged cancellation without calling the bridge. The successful configuration is documented; do not substitute an acknowledgement for cancellation evidence. A receipt-classification race found in the final logs is also fixed: delegation waits for interruption cleanup before recording `turn_cancelled`, rather than incorrectly storing `agent_failed`.

## Follow-up departure confirmation

ADK and CrewAI each pass the nine voice/runtime follow-up checks, including phonetic runtime names, cancellation and recovery. Their full scenario reports remain failed because departure confirmation exceeded the cleanup deadline. The bridge now handles the observed stale status endpoint by checking terminal status in the matching bot-detail response, with bounded removal requests and retained ownership for uncertain exits. This does not guarantee that the platform will publish a terminal state within a fixed time. See [validation](validation.md#adk-and-crewai-voice-follow-up) for the original reports and independent later cleanup checks. Do not advertise these follow-ups as all-green single-run acceptance.

## CrewAI full-run revalidation

The 24 October full CrewAI runs passed **nine of ten checks**, including fresh-turn voice recovery. The remaining failure is owned-bot departure confirmation: MeetStream accepted removal and the bot visibly left, but its status/detail endpoints continued reporting `InMeeting` beyond the cleanup deadline. Transient retries and requesting departure before socket teardown are tested; neither removes this platform-state inconsistency. The ledger retains uncertain exits for `npm stop` reconciliation. The project owner has waived this external-service check as a gate for the local pilot. A future 10/10 claim still requires terminal API status and an empty ledger in the same run. See [latest validation](validation.md#crewai-voice-and-cleanup-revalidation-24-october-latest); later cleanup cannot retroactively pass a failed run.

## Provider requirements

Configure the credentials and model access required by each selected runtime. Use `--runtime` to choose the starting runtime or switch during the meeting. Reasoning providers and shared speech are configured separately. See [runtime configuration](runtime-switching.md) and [validation evidence](validation.md) for provider-specific coverage.

## Production deployment prerequisite

A continuously running production host/account has not been selected or deployed. Supply that target, a fixed HTTPS route, persistent ledger storage, secret management and the registered MIA tool configuration before advertising a hosted production release. The local Google model key authorizes model usage; it does not authorize deploying a server. An ngrok meeting test establishes connectivity, not production uptime. Temporary test-agent configurations are deliberately not left pointing to a stopped bridge.

The built-in write tool is a sandbox example. Supply authorized application tool handlers before enabling real business writes. Keep management routes private and use separate management/tool tokens. These are bridge deployment tasks, not MIA backend modifications.

## Dependency advisories

Rechecked **23 October 2026** against package registry metadata, maintainer advisories and a fresh full-environment audit. Updated certifi to **2026.10.19**, with that minimum enforced for new installations. CI and the container build now use **uv 0.13.5**. These changes preserve all four native runtime versions and compatible dependency constraints.

| Remaining package | Why its patched release cannot currently be selected | Current bridge exposure |
|---|---|---|
| MCP 1.28.1 | CrewAI 1.15.28 requires `mcp~=1.28.1`; the session-exhaustion fix starts at 1.30.0 | No MCP server is mounted or started |
| ChromaDB 1.5.9 | CrewAI requires `chromadb~=1.5.9`; no fixed release is published for the four distinct reported server advisories | No ChromaDB server/routes; Crew memory is disabled and no knowledge sources are supplied |
| OpenTelemetry API 1.42.1 | ADK 2.12.0 requires `opentelemetry-api<=1.42.1`; the tracestate-parser fix is 1.45.1 | The bridge installs no inbound HTTP tracing instrumentation and does not parse incoming trace headers. A regression fails if those headers reach the affected parser |
| uv 0.11.33 inside the environment | CrewAI CLI requires `uv~=0.11.6`; the bundled-root fix starts at 0.13.4 | Guided npm/desktop startup supplies the maintained certifi CA file to local uv instead of its affected bundled roots; offline stop performs no certificate/network setup |

Explicit `SSL_CERT_FILE`/`SSL_CERT_DIR` settings remain operator-owned and are preserved. Custom bundles must also exclude the compromised emSign root. Initial setup and direct uv commands should use uv 0.13.4 or newer; the local fallback mitigation does not upgrade the installed uv package. See [deployment](deployment.md#dependency-security).

The fresh post-change scanner still reports **8 entries across 4 packages**, representing **7 distinct advisory IDs** (the ChromaDB pre-authentication finding appears twice). No findings are ignored or removed from the report. The scan remains non-clean despite the bounded bridge exposure and TLS mitigation. A zero-advisory policy requires compatible upstream fixes; forced dependency overrides would violate current SDK constraints. This review does not certify arbitrary embedded applications or future MCP, ChromaDB or tracing instrumentation.

Sources: [MCP advisory](https://github.com/modelcontextprotocol/python-sdk/security/advisories/GHSA-84m7-p3x7-pcfv), [ChromaDB advisories](https://osv.dev/vulnerability/PYSEC-2026-3814), [OpenTelemetry advisory](https://github.com/open-telemetry/opentelemetry-python/security/advisories/GHSA-xcq7-p3pf-p88h), [uv advisory and custom-CA workaround](https://github.com/astral-sh/uv/security/advisories/GHSA-hq85-5cpj-mr62), [certifi advisory](https://github.com/certifi/python-certifi/security/advisories/GHSA-6q48-6c7w-9gw2), [CrewAI metadata](https://pypi.org/pypi/crewai/1.15.28/json), [ADK metadata](https://pypi.org/pypi/google-adk/2.12.0/json). Private before/after audits are `reports/dependency-audit-2026-10-23-before.json` and `reports/dependency-audit-2026-10-23.json`.

## Verification limits

Google Meet streaming-bridge and optional hosted voice have functional switching and interruption evidence. Quantified audible latency/soak, Zoom/Teams, and MeetStream-managed transcription require their own acceptance before being advertised as verified. Windows checks are configured in CI but have not been executed locally.

OpenAI live inference is excluded under the credit constraint. Its current native SDK conformance passes controlled-model checks; the stored 11 October provider report records exhausted quota. That is not evidence of a funded live OpenAI run. Google-backed ADK/LangGraph/CrewAI acceptance does not certify OpenAI availability.

Hosted custom functions return completed JSON. They cannot stream external answer tokens into MIA speech or revoke an answer already handed to MIA. MIA owns that playback; the public media bridge supplies incremental speech itself. Choose and validate the intended connection path.
