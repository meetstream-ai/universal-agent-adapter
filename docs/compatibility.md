# Tested compatibility

| Component | Version | Boundary |
|---|---|---|
| Python | 3.12.14 / 3.13.14 | Local validation interpreter; package supports 3.12–3.13 |
| OpenAI Agents SDK | 0.20.0 | Native `Runner.run_streamed`, shared FunctionTool, immediate cancellation |
| LangGraph | 1.2.16 | Compiled StateGraph, messages stream, ToolNode, async cleanup |
| langchain-openai | 1.7.2 | OpenAI-compatible chat model |
| langchain-google-genai | 4.4.2 | Explicit native Gemini model for LangGraph; tool-call signatures retained |
| Google ADK | 2.12.0 | Native Runner, SSE partial/final events, shared BaseTool |
| CrewAI | 1.15.28 | Native `akickoff`, explicit `CrewAgentExecutor`, ReAct answer filter |
| OpenAI Python | 2.54.0 | Shared compatibility range selected by both OpenAI Agents and CrewAI |
| MeetStream Python SDK | 1.0.0 | Public bot create/status/remove APIs |

The lockfile is a tested compatible set, not an instruction to install every newest release together. OpenAI Agents releases requiring OpenAI Python 3.x cannot share CrewAI's `openai<3` environment. CrewAI also constrains Pydantic and MCP. Upgrade frameworks together and rerun streaming/tool/cancellation tests before accepting a new resolution.

Registry checks and conformance on **19 October 2026** cover ADK 2.12.0, CrewAI 1.15.28, LangGraph 1.2.16, langchain-openai 1.7.2 and langchain-google-genai 4.4.2. The resolved Google GenAI client is 2.31.0. OpenAI Agents remains at the compatible 0.20.0; its latest 0.24.0 requires OpenAI Python 3.x, while CrewAI requires `<3`. CrewAI still requires `mcp~=1.28.1` and `chromadb~=1.5.9`, so its patch update does not resolve those dependency advisories. See [production acceptance](release-blockers.md). Package sources: [CrewAI metadata](https://pypi.org/pypi/crewai/json), [ADK metadata](https://pypi.org/pypi/google-adk/json), [Agents metadata](https://pypi.org/pypi/openai-agents/json).

CrewAI's explicit `CrewAgentExecutor` is deprecated in this release. It is selected because the default executor made synchronous model calls in the cancellation spike. The supported configuration avoids worker-thread model/tool execution. Remove this compatibility choice only after the replacement executor passes the same cleanup and private-output tests. Arbitrary crews, delegation and concurrent tasks are not covered.

The **23 October 2026** security refresh retains the four framework pins above and updates certifi to **2026.10.19**. CI/container build tooling uses **uv 0.13.5**; that version successfully checks the same lockfile. CrewAI CLI still requires environment-local `uv~=0.11.6`, so guided startup supplies the maintained certifi trust bundle to that fallback. ADK's `opentelemetry-api<=1.42.1` currently prevents selecting the patched 1.45.1. The current audit therefore also includes OpenTelemetry and local uv; earlier two-package audit statements describe their dated checks, not the latest complete finding set. See [current dependency advisories](release-blockers.md#dependency-advisories) for fixed versions, mitigations and remaining constraints.

The four conformance profiles run actual installed SDKs with controlled models, not mocked runners. They verify at least two incremental participant deltas, one shared sandbox write, no private reasoning in output and bounded native cleanup. These checks do not establish provider availability or audible playback.

Model defaults are configuration conveniences, not version guarantees. The Google API rejected `gemini-2.5-flash` for the available account; ADK now defaults to `gemini-3.5-flash-lite`, the model exercised by the native provider/tool and shared voice demos. Operators can explicitly select other models from the [Google catalog](https://ai.google.dev/gemini-api/docs/models) after validating their availability and turn latency. Google's [streaming speech reference](https://ai.google.dev/gemini-api/docs/generate-content/speech-generation) defines 24 kHz mono headerless PCM for the selected TTS path. Availability, quotas and latency still need live checks.


Live ADK and CrewAI reasoning/tool checks passed with Gemini 3.5 Flash Lite. Gemini's OpenAI-compatible endpoint did not reliably preserve tool-call thought signatures in the tested OpenAI Agents/LangGraph loops. Do not infer tool compatibility from a successful single text response; those configurations are not accepted by this release's live evidence. Use ADK’s native Google path or LangGraph with `MODEL_PROVIDER=google`; the latter passed a strict real-provider streaming/two-tool/write check on 12 October. Its regression runs the actual Google integration and graph, verifies signature replay between model/tool steps, hides thought output, and checks native response-stream cancellation, recovery and owned-client cleanup. This explicit selection is not a fallback. Separately validate any OpenAI-compatible endpoint.

CrewAI exposes no public provider-close interface in the pinned version. Its owned `_client`/`_async_client` fields are closed at session end; caller-supplied LLMs remain caller-owned. This version-specific boundary must be rechecked on upgrade. ADK and OpenAI sessions also close SDK-owned network clients, and LangGraph closes its owned HTTP client or its native Google model through the integration’s public `aclose()`. Google request configuration applies a 60-second timeout and one attempt; caller-supplied models remain caller-owned.

CrewAI's Gemini provider forwards `client_params` to the native Google client, but does not apply generic `LLM(timeout=..., max_retries=...)` there. The adapter now supplies a 60-second Google `HttpOptions.timeout` and one retry attempt (the initial request only), verified against the actual owned SDK client. It also requests `include_thoughts=false`, including in the direct no-tools path. The host's turn deadline remains independent. This fixes configuration and private-output exposure; it cannot guarantee provider latency or tool-selection accuracy. See the [native Gemini provider source](https://github.com/crewAIInc/crewAI/blob/1.15.28/lib/crewai/src/crewai/llms/providers/gemini/completion.py).

The optional common Deepgram Aura REST speech stage requests headerless 24 kHz mono linear16 PCM using the [official speak API](https://developers.deepgram.com/reference/text-to-speech/speak-request). Its model identifier includes the voice. Gemini speech was exercised successfully before its daily free-tier quota was exhausted; the final shared demo profile uses Deepgram speech.

CI uses [checkout](https://github.com/actions/checkout/releases/tag/v7.0.1), [setup-uv](https://github.com/astral-sh/setup-uv/releases/tag/v10.3.0) and [setup-node](https://github.com/actions/setup-node/releases/tag/v7.1.0). Their official action metadata specifies Node 24 for the action runtime, supported by GitHub-hosted `ubuntu-latest`; the JavaScript sidecar remains tested with Node 22 or later.

ADK now uses the Google SDK’s bounded native HTTP retry: at most two attempts for initial HTTP 500/502/503/504 responses, under the host’s independent turn deadline. A regression keeps the actual SDK retry and SSE-decoding layers, verifies one initial retry, and verifies no replay after a stream starts and no retry for HTTP 429. Whole turns and tool execution are never retried automatically. See the [pinned streaming-request source](https://github.com/googleapis/python-genai/blob/v2.31.0/google/genai/_api_client.py). This does not eliminate errors reported inside an established provider stream.

ADK requests native `GenerateContentConfig(temperature=0)` to reduce response variability in meeting/tool workflows. This is passed through the actual ADK runner and checked at the model boundary; it does not guarantee deterministic provider output.

The shared external Deepgram ingress now uses `nova-3`, following the current [streaming API](https://developers.deepgram.com/reference/speech-to-text/listen-streaming). It keeps the same 48 kHz PCM framing, bounded queues and final-utterance policy. This replaces the older Nova-2 model after observed recognition errors; transcripts are never rewritten to satisfy tests. MeetStream-managed transcription configuration remains a separate path with its documented Nova-2 profile and pending live acceptance.

Checks on 19–20 October pass for ADK and native-Google LangGraph: each streams an answer, performs the lookup and creates exactly one sandbox action with the requested title/owner. A CrewAI failure was traced to the bridge's ReAct wrapper dropping CrewAI's per-call stop-marker scope. The wrapper now forwards that scope to the native provider without mutating shared model settings. The native-provider regression fails with the old wrapper and passes with the fix; five planned real-Google CrewAI checks then pass with both requested tools and exact write arguments. This is evidence for that workflow, not a guarantee that all model choices or voice latency budgets are production-certified.

Provider-specific validation results and their limits are recorded in [validation](validation.md).

Nova-3 input includes the four runtime names as separate `keyterm` query parameters, using [Deepgram keyterm prompting](https://developers.deepgram.com/docs/keyterm). These vocabulary hints improve domain recognition without find-and-replace rules or rewriting finalized transcripts. Shared control parsing still handles known variants and rejects unknown switch targets before reasoning.
