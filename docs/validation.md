# Validation

Implementation cross-check updated **24 October 2026**. Live meeting evidence below remains dated separately; no new funded OpenAI inference is claimed.

## Local-pilot release decision

On 24 October 2026, the project owner accepted the local pilot with MeetStream's delayed departure status treated as an external-service exception, not a bridge release blocker. The nine voice/runtime checks passed. The raw ten-check report remains 9/10 because terminal platform status was not confirmed within the deadline; this release decision does not change or relabel that result. Cleanup ownership and retry behavior remain enabled.

## Developer commands

```sh
uv sync --frozen --all-extras
uv run ruff check src tests scripts examples
uv run pytest -q
uv run python scripts/protocol_schema.py --check
node --test --test-timeout=10000 tests/test_native_mia.mjs tests/test_project_commands.mjs
uv build
```

## CrewAI voice and cleanup revalidation (24 October, latest)

Two complete voice sequences passed **all nine voice/runtime checks** in one run each: initial ADK lookup/write with audible response, CrewAI lookup/write with audible response, same bot/session, canonical and phonetic switches, unknown-runtime rejection, pending-tool cancellation, audible speech interruption followed by silence, and a fresh lookup/write with independently heard recovery. Neither is an all-green acceptance run: **platform departure confirmation still exceeds the cleanup deadline**. The host visibly saw both bots leave; the platform accepted removal but continued reporting the primary bot as `InMeeting`. Accepted removal and visual departure are not substituted for terminal API status. Failed reports are preserved unchanged.

The reruns fixed one observed ADK speech variant (`Gribble ADK`) using the existing explicit alias list. Unknown names and compound instructions remain rejected. The voice observer now accepts the exact assistant acknowledgement `Stop` or `Stopped`, because ASR dropped the final consonants; it rejects participant speech, older acknowledgements and longer unrelated phrases. The 30-second acknowledgement deadline, cancellation receipts, playback-clear, silence and recovery checks remain unchanged.

Shutdown now requests owned-bot departure before explicitly closing its meeting sockets, with input fenced during teardown. Both successful and failed-removal ordering have regressions; failures retain the ownership ledger. This ordering change did **not** eliminate the observed platform status delay and is not advertised as a complete cleanup fix.

Verification passes **216 Python tests** locally and inside `meetstream-agent-adapter:pilot-crewai-2026-10-24` with external networking disabled, **6 JavaScript tests**, Ruff and protocol drift. Container HTTP smoke checks also pass authentication, same-session switching across all four native runtimes, interrupt/stop and graceful shutdown, using fixture keys with external networking disabled. No OpenAI inference was used. Normal startup still enables all four runtimes; only the live test harness excludes OpenAI.

Final reconciliation confirmed eight of this session's ten test bots terminal. Two primary bots remain reported `InMeeting` despite visible departure and accepted removal; their two durable cleanup entries are retained. The host left the disposable meeting, and all test processes, containers and temporary tunnels are stopped. Private reconciliation evidence is `reports/crewai-owned-cleanup-2026-10-24.json`.

Private evidence: `reports/streaming-voice-crewai-2026-10-24-acceptance.json`, `reports/streaming-voice-crewai-2026-10-24-departure.json`, `reports/crewai-final-tests-2026-10-24.log`, `reports/container-crewai-final-tests-2026-10-24.log` and `reports/container-crewai-final-smoke-2026-10-24.json`. An earlier attempt rejected the ADK variant, another hit the audible-confirmation deadline, and one could not start after the temporary test tunnel expired. These attempts remain failures; no reports were combined into a passing result.

## CrewAI cleanup and voice rerun (23 October)

The cleanup loop now retains ownership and retries temporary MeetStream connection, timeout and server errors within its existing deadline, instead of aborting confirmation on the first failed status request. Matching terminal bot details remain the fallback for stale status responses; neither an accepted removal request nor a timeout is treated as proof of departure. Three regressions cover a failed confirmation followed by confirmed departure.

The live rerun exposed additional voice-path issues. Shared speech now uses bounded two-second PCM packets instead of quarter-second packets; a short answer is sent as a single packet. This adds buffering before the first audio packet, while phrase-level streaming and playback clearing remain supported. Deepgram speech retries connection establishment only, never partial playback or tool writes. Transcription accepts corrected final word timings for the same segment while retaining the guard against stale boundaries from older speech. The regression suite covers that correction and continuing speech. Private failure diagnostics record exception classes without credentials.

The latest local checks pass **212 Python tests**, **6 JavaScript tests**, Ruff and protocol drift. The rebuilt `meetstream-agent-adapter:pilot-crewai-cleanup-2026-10-23` image also passes all **212 Python tests** with external networking disabled. The best new live attempt passed **eight of ten checks**, including ADK/CrewAI exact tool writes with independently heard responses, canonical/variant runtime switching, unknown-name rejection, pending-tool cancellation and audible interruption. It then failed during recovery with a speech connection timeout; MeetStream confirmation also encountered connection errors. Google Meet visibly lost the host network connection during subsequent attempts. These are failed full-run results, not a replacement for complete acceptance. No OpenAI inference was used.

All **24 owned test bots** were independently confirmed departed after connectivity recovered, and local ledgers have **zero pending exits**. The temporary tunnel and test meeting were closed. The previous failed reports remain unchanged. A fully passing CrewAI voice run **still requires a stable meeting/network connection and a fresh complete run**; later cleanup cannot retroactively make an interrupted run pass. The occupied ngrok endpoint was left untouched; the test used a temporary external tunnel through the harness's new `--public-url`/`--port` options. Normal `npm start`/`npm stop` remain unchanged and all four runtimes remain available by default; this test harness explicitly disables it.

Private evidence: `reports/streaming-voice-crewai-2026-10-23-acceptance.json`, `reports/streaming-voice-crewai-2026-10-23-release.json`, `reports/crewai-owned-cleanup-2026-10-23.json` and `reports/crewai-cleanup-final-tests-2026-10-23.log`. Earlier diagnostic attempts include a corrected test-wrapper error reading Deepgram's utterance-end channel field; those attempts are preserved as failures.

## Original scope cross-check

| Requirement | Implementation and evidence |
|---|---|
| Framework-neutral adapter interface | `contract.py` defines `AgentAdapter`, `RuntimeSession`, capabilities and ordered common events without framework imports |
| OpenAI Agents SDK, Google ADK, CrewAI and LangGraph | Four native adapters under `adapters/`; native framework tests exercise streaming, shared tools, cancellation and recovery for each |
| Common transport and tools | `server.py` supplies authenticated HTTP/WebSocket/SSE paths; `ToolExecutor` validates and scopes calls and retains durable write receipts |
| Configuration-only runtime switching | One session host replaces its adapter while keeping bot identity, sockets, shared history and tools; all 12 directed switches between the four native runners are tested, including cancelling an active write |
| Streaming turns and interruption | Native deltas feed the shared speech queue; generation fences, cooperative cancellation and playback clearing prevent obsolete output after interruption. The ADK/LangGraph live meeting acceptance below supplies independent audible evidence |
| Error handling | Missing credentials or unsupported capabilities preserve the current runtime during switch preparation; unsafe cleanup blocks further turns. OpenAI quota/error recovery is covered through the real SDK with intercepted HTTP responses |
| Authentication and deployment guidance | Separate management/tool tokens, scoped callback tokens and persistent cleanup/operation ledgers are implemented; [deployment](deployment.md) documents local and container operation |
| Voice demos and plug-and-play operation | The finite [voice harness](demos.md) exercises real meeting audio; guided `npm start` installs dependencies, prompts for credentials/link and joins. `npm stop`/Ctrl+C reconcile owned bots and close local resources |

The 23 October local verification, including the subsequent dependency-security refresh, passes **208 Python tests** and **6 JavaScript tests**, Ruff, protocol schema drift, installed dependency compatibility, source/wheel builds and `npm start -- --mode demo`. Controlled-model switching tests use the actual installed framework runners and make no paid model calls. Native stress results are recorded separately in private `reports/scope-native-stress-2026-10-23.json`.

The cross-check fixed environment-based CrewAI selection ignoring `MODEL_PROVIDER=google` when no model override was supplied. It also fixed `npm stop` incorrectly reporting no work when the Python environment was deleted but a media-bot cleanup ledger remained. Regression tests cover both paths. The normal stop command reconciled the previously saved owned-bot obligation; the local ledger now has zero pending bot exits. Historical failed voice-run results below remain unchanged.

The original two-runtime local-pilot acceptance has recorded passing ADK/LangGraph meeting evidence. All four adapters are implemented and tested; funded OpenAI voice acceptance and platform-specific rollout limits remain distinct from implementation support.

The security refresh updates certifi to 2026.10.19, removes its scanner finding and verifies that its trust store lacks the compromised emSign ECC Root CA - G3. CI/container build tooling is pinned to uv 0.13.5; the new uv successfully validates the same lockfile. Guided local/desktop startup provides the maintained certificate file to CrewAI's older pinned uv, preserves explicit operator TLS overrides, and refuses a missing or pre-root-removal bundle. Tests verify that startup does not fall back to old uv roots when certificate setup fails, while stop remains offline-safe. Public-route checks also assert that MCP/ChromaDB endpoints are absent and malicious trace headers never enter OpenTelemetry's affected parser. The remaining **8 scanner entries across 4 packages (7 distinct IDs)** remain visible in the [security assessment](release-blockers.md#dependency-advisories); no finding is suppressed. This refresh creates no meeting bots and uses no paid model inference.

## Updated container validation (23 October)

Docker Desktop was started and `meetstream-agent-adapter:pilot-security-2026-10-23` built successfully with a freshly pulled Python 3.12 slim base. The Linux ARM64 image runs Python 3.12.15 as UID 10001. Its certificate bundle is certifi 2026.10.19 with the compromised emSign root absent; global build tooling is uv 0.13.5. CrewAI's separate environment-local uv remains at its required 0.11.33.

- All **208 Python tests pass inside the image**, including native framework streaming, tools, cancellation, recovery and all 12 directed runtime switches. Test dependencies were installed only in a temporary container directory; external networking was disconnected before tests. The shipped image was not modified to add test packages.
- Separate real HTTP service checks pass for health/readiness, unauthenticated management rejection, ADK → LangGraph → CrewAI → OpenAI → ADK on the same synthetic bot/session, interrupt/stop controls, absent MCP/ChromaDB endpoints and graceful SIGTERM exit. Fixture keys and disabled external networking prevent paid model calls or meeting creation.
- Installed dependency compatibility passes. The image excludes local credentials, ownership state, reports and tests. A scan of its 167 installed Python distributions retains the same four flagged packages and seven distinct advisory IDs; it is not a zero-advisory image or an OS-package vulnerability assessment.
- Test containers were removed. Docker Desktop remains running as requested; no bridge or test meeting is left running.

Private evidence: `reports/container-security-build-2026-10-23.log`, `reports/container-security-tests-2026-10-23.log`, `reports/container-security-smoke-2026-10-23.json`, `reports/container-security-audit-2026-10-23.json` and `reports/container-security-validation-2026-10-23.json`. An initial test invocation omitted the project's pytest configuration and did not execute async cases correctly; its failed log is preserved separately. The corrected full run supplies that configuration and passes all 208 tests. No application fix was needed for that harness error.

## Default OpenAI startup image recheck (23 October)

After enabling OpenAI by default, `meetstream-agent-adapter:pilot-openai-default-2026-10-23` was rebuilt. All **208 Python tests pass inside this image**, with external networking disconnected, including default OpenAI credentials, remembered startup selection, hosted four-runtime schemas and explicit Google-only opt-out. Separate network-disabled HTTP smoke checks pass for authenticated service access, ADK → LangGraph → CrewAI → OpenAI → ADK on one synthetic session, interruption, stop, non-root execution and completed application shutdown. No paid model calls or meeting bots were created. The smoke harness enables lifecycle logging to verify shutdown; the normal CLI suppresses those logs, so exit-code-only checks were insufficient.

The README, quickstart and source ZIP describe the same default: `npm start` enables all four runtimes, ADK is the fresh-install selection, and `--no-openai` is an explicit opt-out. Docker Compose remains the separately configured events service; it does not perform the guided npm meeting-join flow. Private evidence is in `reports/container-openai-default-tests-2026-10-23.log`, `reports/container-openai-default-smoke-2026-10-23.json` and `reports/container-openai-default-release-2026-10-23.json`. Historical image and live-voice results above retain their original scope.

## Automated checks (21 October baseline)

| Check | Evidence |
|---|---|
| Python 3.12 and 3.13 | 197 tests pass on each version. Full suites cover four SDK runners, shared tools, streaming, cancellation, switching, receipts, authentication and lifecycle cleanup |
| Core-only installation | Framework-independent suite passes; optional-framework cases skip explicitly |
| JavaScript clients and launcher | 4 tests pass: delivery ownership, stop-before-first-start, custom uv environments and missing-environment cleanup protection |
| Static checks | Ruff, generated protocol schema drift and installed-package compatibility |
| Process lifecycle | Authenticated private stop, checkout isolation, repeated stop, POSIX restart and SIGTERM cleanup |
| Linux ARM64 container | Earlier same-day build: all 123 then-existing tests pass; non-root HTTP/MIA receipt smoke test |
| Native stress | Each framework: 100 controlled-model turns and 10 cancellation/recovery cycles |
| Packaging | Source/wheel builds, credential/private-file scans and documentation-link checks |
| Dependency security | Upstream MCP/ChromaDB advisories remain; see [production acceptance](release-blockers.md) |

The private `reports/verification-openai-readiness-status-2026-10-20.json` records an earlier baseline; the 23 October results above supersede its test counts. Reports, meeting media and credentials are excluded from release archives. Configured CI jobs do not establish that hosted Windows CI has run.

Regressions include historical switch receipts matching a different/unsafe session, stop during startup preflight, exceptional lifespan cleanup, custom uv environments, bounded ngrok setup, and transient new-tunnel health failures. Startup retries only unauthenticated health requests for its own newly opened tunnel; it never retries bot creation or tool writes as part of this check. A dedicated interruption regression delays playback cleanup and proves that cancellation receipts wait for the correct terminal event, remain idempotent, and allow recovery.

## Default streaming bridge voice acceptance

On **21 October 2026**, the finite Google-only harness passed all eight acceptance checks in a real Google Meet:

- ADK and LangGraph each executed a fresh lookup and exactly one matching sandbox write. A separate listener heard the answer over meeting audio; both turns emitted multiple SDK text deltas.
- A spoken LangGraph switch changed the runtime while retaining the same meeting bot and session.
- A spoken stop cancelled a deliberately delayed tool handler and produced a safe cancellation terminal.
- A stop during an audible spacecraft story cleared playback and interrupted the answer. The bot acknowledged the stop, stayed quiet and then completed a fresh lookup/write with spoken confirmation.
- Both owned bots left; independent subsequent API checks reported `Done` for each. No OpenAI calls were made and no MIA backend was changed.

Private evidence: `reports/streaming-voice-acceptance-2026-10-21.json` (`passed: true`, eight positive checks) and `reports/runtime-switch-caption-fix-2026-10-21.png`. Reproduce with [the finite voice harness](demos.md). Earlier failures remain in separate reports.

The live tests exposed and fixed three integration problems: rate-tagged 24 kHz audio was ignored, the independent listener filtered out the assistant as self-audio, and “land graph”/“line graph” switch transcripts fell through to reasoning. Both PCM header formats now reach the shared 48 kHz ingress. Whole switch commands recognize the observed variants; unknown targets receive clarification without invoking reasoning or tools. Regression tests preserve ordinary chart requests and discussed/quoted instructions.

The default streams SDK deltas into a shared phrase speech queue and delivers PCM through MeetStream. Optional hosted MIA custom-function mode still returns completed JSON. Fast model generation can finish before speech begins; these checks do not assert a measured token-to-audio overlap, latency target or long-duration soak.

## ADK and CrewAI voice follow-up

The Google-only follow-up exercised LangGraph → ADK and ADK → CrewAI, including switching back and selecting the target again with a phonetic variant. **Both passed all nine voice/runtime checks**: initial tools/voice, target tools/voice, unchanged bot/session, canonical switch, variant switch, safe unknown-name handling, pending-tool cancellation, audible-speech interruption and fresh-turn recovery.

Observed inputs included “a DK,” “Crew I,” “switched” and “run time.” The shared parser handles these whole-command forms. Nova-3 now receives the four framework names as vocabulary hints; finalized transcripts are not rewritten. The tests still require exact sandbox write fields and independently heard speech. An early Greek-letter title was misheard as “data pilot”; that failure is preserved. Final scenarios use ordinary phrases (`review notes`, `check budget`, `send summary`) rather than weakening the exact-data checks.

The full follow-up runs remain marked **failed overall** because platform departure confirmation exceeded their cleanup deadline. The bots left the Meet UI. The status API also reproduced a stale `InMeeting` response after the matching bot-detail API reported `Done`; cleanup now checks terminal bot details as an independent fallback and bounds removal requests. A separate follow-up reconciles late terminal states without changing the original results. Inspect private `reports/provider-voice-cleanup-followup-2026-10-21.json` for the latest per-bot confirmation; do not present these as all-green single-run acceptance reports.

Private voice evidence: `reports/streaming-voice-adk-2026-10-21.json`, `reports/streaming-voice-crewai-2026-10-21-stale-status-cleanup-failure.json` and `reports/provider-voice-switch-recovery-2026-10-21.png`. Earlier ASR, pronunciation and admission failures remain separate. No OpenAI credits were used. Normal `npm start`/`npm stop` usage is unchanged.

## Automatic local MIA meeting lifecycle

The optional `npm start -- --mode mia` path uses Google-only MIA registration, including the tools, routing prompt and interruption settings. The following registration checks were performed on 20 October; ordinary `npm start` now uses the streaming external-agent path. Real command checks verify successful registration, cloud roundtrip, public rejection of an OpenAI switch, `npm stop` cleanup, refresh on a second start, Ctrl+C cleanup, and recovery through `npm stop` after an owned test process-group crash. All original three cloud configurations remain unchanged; no meeting bots or OpenAI inference calls were created during these registration checks.

Regression tests cover interrupted creation, a lost create response, unavailable cleanup retaining ownership state, stale-process cleanup, rejection of an unexpected hosted provider, custom-environment credential scoping, safe repeated stop, and draining the local service before cloud deletion. The Google pilot guard covers both MIA and management runtime switches; the separately operated general service retains four-runtime support.

Current command evidence is in private `reports/automatic-mia-live-2026-10-20.json` and `reports/automatic-mia-crash-recovery-2026-10-20.json`. Earlier voice/switch/interruption acceptance below remains separate from these lifecycle checks.

`npm start -- --mode mia` additionally prompts for a meeting link and creates the hosted MIA bot automatically using a durable idempotency key. A real Google Meet confirmed automatic arrival, host admission, `InMeeting` status, and departure after `npm stop`. Restart with `--meeting` and Ctrl+C also confirmed departure and registration cleanup. All three original configurations remain unchanged. No OpenAI services were used. Private evidence: `reports/automatic-meeting-lifecycle-2026-10-20.json`.

Bot lifecycle regressions cover stop/Ctrl+C during creation, idempotent HTTP 507 replay, lost create responses recovered by ownership attributes, rejected creation, unknown creation retaining cleanup obligations, and HTTP 202 removal remaining pending until status confirms departure. A pending bot exit prevents deleting its MIA configuration.

## Actual Google-backed hosted MIA meeting

Google was connected under MeetStream Integrations and used for hosted realtime model/voice. External ADK, LangGraph and CrewAI used Google models. The finite test speaker used Deepgram speech. No OpenAI model or speech calls were made. No backend code or unrelated cloud configurations were changed.

The actual Google Meet checks established:

1. Spoken delegation to ADK and LangGraph, with both lookup and exact sandbox writes, streamed SDK text events and spoken MIA results.
2. ADK → LangGraph selection through MIA's `switch_agent`, retaining the meeting bot and bridge session. A separate ADK → CrewAI meeting switch/write also succeeded.
3. Active-speech interruption: an explicit spoken interrupt invoked `interrupt_agent`; the old four-step answer stopped partway through, and MIA answered a new request correctly. Observation used live meeting captions and bridge receipts; no sample-accurate audible-latency claim is made.
4. Pending-work cancellation: a natural spoken stop invoked `interrupt_agent` during an intentionally delayed lookup. The journal recorded `mia_tool_request` cancellation, not a timeout. After switching the same session to LangGraph, a fresh lookup and one exact write succeeded and MIA spoke the result.
5. Cleanup: independent platform status confirmed all eight test bots `Done`. Four temporary MIA configurations were removed, the original three were preserved, and all owned tunnels/services stopped. The saved Google integration remains connected.

Initial attempts are preserved as failures. A private harness inspection bug, a transient ngrok health 404, and a weak routing prompt were found and addressed. One pending-tool attempt reached the tool timeout and therefore did not count as cancellation evidence. The successful pending-work test uses a longer tool deadline and verifies an explicit cancellation event. The final logs also reproduced an `agent_failed`/`turn_cancelled` receipt race; the code now synchronizes classification with interruption cleanup. See the [tested hosted prompt and settings](runtime-switching.md#hosted-mia-voice-configuration).

Private evidence: `reports/hosted-voice-switch-first-run-2026-10-20.json`, `reports/hosted-voice-interruption-recovery-2026-10-20.json`, `reports/hosted-acceptance-2026-10-20.json`, meeting screenshots and independent cleanup status. These are finite isolated tests; normal startup does not launch a test speaker.

## Provider and public-route evidence

Real Google ADK and native-Google LangGraph lookup/write/streaming checks pass. CrewAI's scoped observation-stop forwarding fix passes all five planned real-provider checks with exact tool writes. Its meeting write supplements those checks; it does not prove every requested lookup was fresh.

Public HTTPS ADK → LangGraph → CrewAI routing passed with three exact writes, safe runtime state and receipt retries. An earlier repeat failed, and another stalled before readiness; those are retained rather than overwritten. A subsequent diagnostic passed all three runtimes with no failed turns. Newly opened tunnels now retry transient health failures within a fixed attempt limit and still release their instance lock on failure.

OpenAI Agents is tested through its actual SDK with controlled models and through the real `AsyncOpenAI` client, Responses SSE parser and native Agents runner using an intercepted HTTP transport. The transport tests verify the official `/v1/responses` request, API-key selection, model selection, streaming deltas, tool execution, client cleanup, cancellation/recovery, and one quota rejection followed by a successful fresh turn without repeated writes. They make no external requests. Launcher tests verify explicit `--runtime openai` and `--allow-openai` configuration, credentials, full four-runtime function schemas and matching hosted routing prompts; ordinary startup now enables all four runtimes, while explicit `--no-openai` retains the Google-only guard. Default startup/credential and remembered OpenAI selection regressions cover this change; earlier Google-only live evidence predates this default change. Stored funded-provider attempts reported exhausted OpenAI quota; no fresh funded OpenAI inference is claimed. After funding, run `npm start -- --runtime openai` and repeat spoken lookup/write, switch and interruption acceptance before claiming funded live OpenAI meeting validation. Gemini's OpenAI-compatible endpoint is not a validated substitute for the native Google integrations.

## Remaining rollout scope

The production bridge host, persistent HTTPS deployment and production tool permissions still need an owner-selected deployment target. Hosted Google voice acceptance does not establish deployed uptime, exact playback latency, Zoom/Teams parity or Windows execution. Earlier LangGraph/ADK voice soak evidence predates hot switching. See [production acceptance](release-blockers.md) for the specific limits and dependency exposure review.
