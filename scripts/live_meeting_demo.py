"""Explicit real-meeting harness: scripted speaker -> shared STT -> runtime -> voice.

Requires an existing disposable Google Meet, funded credentials and a TLS
origin pointing at this process. Admission is performed by the meeting host.
Only the two bots created by this run are stopped in cleanup.
"""

import argparse
import asyncio
import base64
import json
import logging
import math
import os
import re
import secrets
import struct
import time
from contextlib import asynccontextmanager, suppress
from dataclasses import replace
from pathlib import Path
from urllib.parse import urlparse

import httpx
import uvicorn
from fastapi import WebSocket
from meetstream import AsyncMeetStream, NotFoundError, NotReadyError, ServerError
from meetstream import ConnectionError as PlatformConnectionError
from meetstream import TimeoutError as PlatformTimeoutError

from meetstream_agent_adapter.cli import load_environment
from meetstream_agent_adapter.config import load_settings
from meetstream_agent_adapter.meetstream.output import create_speech, pcm_48k
from meetstream_agent_adapter.meetstream.protocol import acknowledgement, decode_audio, meeting_ended
from meetstream_agent_adapter.meetstream.transcription import DeepgramInput
from meetstream_agent_adapter.server import create_app


async def wait_for(predicate, seconds, label):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        result = predicate()
        if result:
            return result
        await asyncio.sleep(0.2)
    raise TimeoutError(label)


def sustained_count(text):
    """Reject short/refused answers as evidence of long-answer interruption."""
    numbers = "one two three four five six seven eight nine ten eleven twelve thirteen fourteen fifteen sixteen seventeen eighteen nineteen twenty thirty forty fifty sixty".split()
    found = set(re.findall(r"\b(?:\d{1,2}|" + "|".join(numbers) + r")\b", text.lower()))
    return len(found) >= 10


async def save_demo_video(api, bot_id, path):
    # Removal is asynchronous: recording endpoints may still return 404 while
    # the bot is leaving. Wait for its terminal status before SDK polling.
    await wait_for_departure(api, bot_id, seconds=120)
    # The live API also returned 404 after Done while video processing was
    # pending. The SDK retries 202 only; bound this observed 404 race as well.
    async with asyncio.timeout(600):
        for _ in range(60):
            try:
                metadata = await api.bots.wait_for_video(bot_id, timeout=600, interval=10)
                break
            except NotFoundError:
                await asyncio.sleep(10)
        else:
            raise TimeoutError("Demo recording did not become available")
    url = metadata["video_url"]
    parsed = urlparse(url)
    if (parsed.scheme != "https" or not (parsed.hostname or "").endswith(".amazonaws.com")
            or parsed.username or parsed.password or parsed.port not in {None, 443}):
        raise ValueError("Demo download requires the documented HTTPS S3 recording URL")
    created, total = False, 0
    try:
        with path.open("xb") as target:
            created = True
            path.chmod(0o600)
            async with httpx.AsyncClient(timeout=30) as client, client.stream("GET", url) as response:
                response.raise_for_status()
                async for chunk in response.aiter_bytes():
                    total += len(chunk)
                    if total > 512 * 1024 * 1024:
                        raise ValueError("Demo recording exceeded 512 MiB")
                    target.write(chunk)
        if not total:
            raise ValueError("Demo recording was empty")
    except BaseException:
        if created:
            path.unlink(missing_ok=True)
        raise
    return {"path": str(path.resolve()), "bytes": total}


async def stop_owned_bot(api, bot_id):
    error = None
    try:
        async with asyncio.timeout(10):
            await api.bots.remove(bot_id)
    except NotReadyError:
        pass  # HTTP 202 is accepted/pending; independent terminal status is required.
    except Exception as exc:
        error = type(exc).__name__
    # A rejected/timed-out remove is resolved only by independent platform
    # state. Likewise, an accepted request alone does not prove the bot left.
    status = await wait_for_departure(api, bot_id)
    return {"terminal_status": status.get("status"), "remove_requested": error is None,
            "remove_error_class": error}


async def wait_for_departure(api, bot_id, seconds=60):
    from meetstream_agent_adapter.meetstream.lifecycle import departure_status

    async with asyncio.timeout(seconds):
        for _ in range(seconds // 2):
            try:
                status = await departure_status(api, bot_id)
            except NotFoundError:
                return {"status": "Absent"}
            except (PlatformConnectionError, PlatformTimeoutError, ServerError):
                await asyncio.sleep(2)
                continue
            if meeting_ended(status):
                return status
            await asyncio.sleep(2)
    raise TimeoutError("Owned test bot did not reach terminal status")


async def stop_primary_session(client, api, session_id, bot_id):
    try:
        response = await client.post(f"/v1/sessions/{session_id}/stop")
        response.raise_for_status()
    except httpx.HTTPError:
        # A lost stop acknowledgement keeps a direct safety fallback; normal
        # cleanup has exactly one owner and no second removal request.
        return await stop_owned_bot(api, bot_id)
    status = await wait_for_departure(api, bot_id)
    return {"terminal_status": status.get("status"), "remove_requested": True,
            "remove_error_class": None}


@asynccontextmanager
async def delayed_fault(executor, output, scenario):
    """Restore test-only tool/speech delays even when the scenario is cancelled."""
    entered, cancelled = asyncio.Event(), asyncio.Event()
    spec = executor.specs["lookup_project_status"]
    speech_pcm = output.speech.pcm

    async def slow_lookup(arguments, context, operation, handler=spec.handler, started=entered, cleanup=cancelled):
        started.set()
        try:
            await asyncio.sleep(8)
            return await handler(arguments, context, operation)
        except asyncio.CancelledError:
            cleanup.set()
            raise

    async def delayed_speech(text, source=speech_pcm, started=entered, cleanup=cancelled):
        started.set()
        try:
            await asyncio.sleep(8)
            async for chunk in source(text):
                yield chunk
        except asyncio.CancelledError:
            cleanup.set()
            raise

    try:
        if scenario == "slow_tool":
            executor.specs[spec.name] = replace(spec, handler=slow_lookup)
        elif scenario == "delayed_speech":
            output.speech.pcm = delayed_speech
        else:
            raise ValueError("Unknown fault scenario")
        yield entered, cancelled
    finally:
        executor.specs[spec.name] = spec
        output.speech.pcm = speech_pcm


async def run(args):
    settings = load_settings(args.config)
    if (settings.output != "voice_and_chat" or settings.transcript_source != "deepgram"
            or "okay assistant" not in [word.lower().strip() for word in settings.wake_words]):
        raise ValueError("The voice harness requires the shared examples/voice-demo.toml configuration")
    print(f"Scripted voice test: the test speaker requests counting and stopping {args.interruptions} time(s). "
          "This is separate from the normal assistant. Control + C stops the test.", flush=True)
    api = AsyncMeetStream(timeout=15, max_retries=0)
    app = create_app(settings, api_client=api)
    nonce = secrets.token_urlsafe(32)
    speaker_id, speaker_ws = None, None
    bound, speaker_ready = asyncio.Event(), asyncio.Event()
    primary_id, session_id = None, None
    record = None
    provider = {"adk": "google", "langgraph": settings.model_provider,
                "crewai": settings.model.partition("/")[0], "openai": "openai"}.get(settings.profile, "offline")
    report = {"profile": settings.profile, "model": settings.model, "model_provider": provider,
              "kind": "real_google_meet_voice_loop",
              "stt_model": DeepgramInput.model,
              "speech_provider": settings.speech_provider, "speech_model": settings.speech_model,
              "voice": settings.speech_model if settings.speech_provider == "deepgram" else settings.speech_voice,
              "passed": False}

    @app.websocket("/test-speaker/{token}")
    @app.websocket("/{path_bot}/test-speaker/{token}")
    async def speaker_socket(ws: WebSocket, token: str, path_bot: str | None = None):
        nonlocal speaker_ws
        if not secrets.compare_digest(token, nonce):
            await ws.close(code=1008)
            return
        await ws.accept()
        try:
            async with asyncio.timeout(15):
                initial = await ws.receive_json()
                await bound.wait()
            if initial.get("bot_id") != speaker_id or initial.get("type") != "ready" or path_bot and path_bot != speaker_id:
                await ws.close(code=1008)
                return
            speaker_ws = ws
            await ws.send_json(acknowledgement(speaker_id))
            speaker_ready.set()
            while True:
                frame = await ws.receive()
                if frame["type"] == "websocket.disconnect":
                    break
        except Exception:
            pass
        finally:
            speaker_ws = None

    server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=args.port,
                                         log_level="warning", access_log=False, timeout_graceful_shutdown=20))
    serving = asyncio.create_task(server.serve())
    speech = create_speech(settings)

    async def synthesize(text):
        return [pcm async for pcm in speech.pcm(text)]

    async def say(pcm):
        deadline = time.monotonic()
        for chunk in pcm:
            if not speaker_ws:
                raise RuntimeError("Test speaker disconnected")
            await speaker_ws.send_json({"command": "sendaudio", "bot_id": speaker_id, "audiochunk": base64.b64encode(pcm_48k(chunk)).decode(),
                                        "sample_rate": 48000, "encoding": "pcm16", "channels": 1, "endianness": "little"})
            deadline += len(chunk) / 48000
            await asyncio.sleep(max(0, deadline - time.monotonic()))

    try:
        await wait_for(lambda: server.started or serving.done(), 10, "server startup")
        if serving.done():
            await serving
            raise RuntimeError("Server could not start; choose an unused port")
        report["stage"] = "public_endpoint_preflight"
        async with httpx.AsyncClient(timeout=10) as public_client:
            health = await public_client.get(settings.public_url + "/health")
            health.raise_for_status()
            if health.json() != {"status": "ok", "version": "0.1.0"}:
                raise RuntimeError("Public endpoint is not the adapter service")
        async with httpx.AsyncClient(base_url=f"http://127.0.0.1:{args.port}", timeout=30,
                                     headers={"Authorization": "Bearer " + os.environ["ADAPTER_API_KEY"]}) as client:
            result = await client.post("/v1/sessions", json={"meeting_link": args.meeting})
            result.raise_for_status()
            primary = result.json()
            session_id, primary_id = primary["session_id"], primary["bot_id"]
            ws_origin = settings.public_url.replace("https:", "wss:", 1)
            result = await api.bots.create({"meeting_link": args.meeting, "bot_name": "SDK Test Speaker", "video_required": args.record_video,
                **({"recording_config": {"video_layout": "speaker_view"}} if args.record_video else {}),
                "socket_connection_url": {"websocket_url": f"{ws_origin}/test-speaker/{nonce}"},
                "automatic_leave": {"waiting_room_timeout": 180, "everyone_left_timeout": 60}}, idempotency_key=secrets.token_hex(16))
            speaker_id = result["bot_id"]
            bound.set()
            report.update(primary_bot_id=primary_id, speaker_bot_id=speaker_id, session_id=session_id)
            # IDs are safe; callback tokens and token-bearing URLs are never printed.
            print(json.dumps({"stage": "admit_both_test_bots", "profile": settings.profile,
                              "primary_bot_id": primary_id, "speaker_bot_id": speaker_id}), flush=True)
            # Persist owned IDs immediately so a hard-killed harness can be cleaned up.
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(json.dumps(report, indent=2) + "\n")
            report["stage"] = "wait_for_test_bot_admission"
            deadline = time.monotonic() + 180
            while time.monotonic() < deadline:
                states = await asyncio.gather(api.bots.status(primary_id), api.bots.status(speaker_id))
                report["admission_statuses"] = {"adapter": states[0].get("status"), "speaker": states[1].get("status")}
                if any(meeting_ended(state) for state in states):
                    raise RuntimeError("A test bot ended before admission")
                if all(s.get("status") in {"InMeeting", "Recording"} for s in states) and speaker_ready.is_set():
                    break
                await asyncio.sleep(2)
            else:
                raise TimeoutError("test bot admission")
            record = app.state.records[session_id]
            await wait_for(lambda: record.output.socket and record.audio_socket, 15, "primary transport")
            # Isolated synthetic meeting only: retain bounded timing/classification
            # diagnostics, never audio, participant identities or callback tokens.
            stop_at = None
            onset = record.speech_gate.onset
            report["stop_audio_observations"] = []

            def observe_onset(frame):
                detected = onset(frame)
                if stop_at is not None and len(report["stop_audio_observations"]) < 128:
                    decoded = decode_audio(frame)
                    if decoded:
                        _, name, pcm = decoded
                        attribution = "self" if name == record.speech_gate.bot_name else "unknown" if name in {"", "NoSpeaker"} else "participant"
                        report["stop_audio_observations"].append({
                            "cycle": cycle + 1,
                            "phase": report.get("stage"),
                            "elapsed_seconds": round(time.monotonic() - stop_at, 3),
                            "attribution": attribution, "audio_seconds": round(len(pcm) / 96000, 3),
                            "onset": detected, "playback_pending": record.output.delivery_pending(),
                            "delivery": record.output.delivery})
                return detected

            record.speech_gate.onset = observe_onset
            print(json.dumps({"stage": "voice_tool_test", "profile": settings.profile}), flush=True)
            report["stage"] = "synthesize_test_utterance"
            pcm = await synthesize("Okay assistant, look up project adapter status. Also create an action item with title verify and owner Alex. Confirm both results briefly.")
            # Prime the fixture's virtual microphone before its first wake word.
            tone = b''.join(struct.pack('<h', int(1800 * math.sin(2 * math.pi * 440 * n / 24000))) for n in range(24000))
            await say([tone])
            await asyncio.sleep(1.5)
            print(json.dumps({"stage": "scripted_speech_start", "profile": settings.profile}), flush=True)
            await say(pcm)
            print(json.dumps({"stage": "scripted_speech_finished", "profile": settings.profile}), flush=True)
            report["stage"] = "wait_for_finalized_transcript_and_answer"
            await wait_for(lambda: any(e.type.value in {"turn.completed", "turn.failed"} for e in record.host.events), 90, "voice turn")
            events = list(record.host.events)
            report["voice_turn_terminal"] = events[-1].type.value
            report["voice_turn_error_code"] = events[-1].payload.get("code")
            report["delta_count"] = sum(e.type.value == "assistant.text.delta" for e in events)
            report["completed_tools"] = [e.payload["name"] for e in events if e.type.value == "tool.completed"]
            items = app.state.executor.db.execute("SELECT title, owner FROM action_items WHERE session=?", (session_id,)).fetchall()
            report["sandbox_action_items"] = len(items)
            report["sandbox_arguments_correct"] = len(items) == 1 and tuple(v.strip().lower() for v in items[0]) == ("verify", "alex")
            report["both_requested_tools_completed"] = {"lookup_project_status", "create_action_item"}.issubset(report["completed_tools"])
            await wait_for(lambda: not record.output.delivery_pending(), 60, "speech delivery")
            report["audio_frames"] = record.output.audio_frames
            report["submitted_audio_seconds"] = round(record.output.submitted_audio_seconds, 3)
            report["chat_messages"] = record.output.chat_messages
            report["delivery_status"] = record.output.delivery
            report["input_error"] = record.input_error
            if (report["voice_turn_terminal"] != "turn.completed" or report["sandbox_action_items"] != 1
                    or report["delta_count"] < 2 or not report["sandbox_arguments_correct"]
                    or not report["both_requested_tools_completed"]
                    or not report["audio_frames"] or record.output.error):
                raise RuntimeError("Initial voice/tool turn failed")
            # Prepare the stop phrase before starting the long answer.
            stop_pcm = await synthesize("Okay assistant, stop. Just say stopped.")
            report["long_answer_target"] = 100
            long_pcm = await synthesize("Okay assistant, count aloud from number one up to one hundred. Do not skip any numbers. Use commas between the numbers.")
            report["interruption_cycles"] = []
            for cycle in range(args.interruptions):
                baseline_frames = record.output.audio_frames
                previous_turn_id = record.host.run.turn.turn_id
                await say(long_pcm)
                report["stage"] = "wait_for_long_answer"
                await wait_for(lambda baseline=baseline_frames, previous=previous_turn_id:
                               record.output.audio_frames > baseline or record.output.error or
                               (record.host.run.turn.turn_id != previous and record.host.events[-1].type.value == "turn.failed"),
                               90, "long answer speech")
                if record.output.error or record.host.events[-1].type.value == "turn.failed":
                    report["long_answer_error_code"] = record.output.error or record.host.events[-1].payload.get("code")
                    raise RuntimeError("Long-answer provider or speech failed")
                interrupted_turn_id = record.host.run.turn.turn_id
                await wait_for(lambda: sustained_count(record.host.run.text) or record.host.run.task.done(),
                               15, "sustained answer content")
                if not sustained_count(record.host.run.text) or not record.output.delivery_pending():
                    raise RuntimeError("A short or already-submitted answer cannot prove long-answer barge-in")
                # Earlier onsets belong to the count request, not to barge-in.
                baseline_interruptions = record.host.metrics["speech_interruptions"]
                playback_pending_before_stop = record.output.delivery_pending()
                started = time.monotonic()
                stop_at = started
                await say(stop_pcm)
                report["stage"] = "wait_for_acoustic_interruption"
                await wait_for(lambda baseline=baseline_interruptions: record.host.metrics["speech_interruptions"] > baseline, 15, "speech-onset cancellation")
                report["speech_interruption_observed"] = True
                report["stop_utterance_elapsed_seconds"] = round(time.monotonic() - started, 3)
                await wait_for(lambda prior=interrupted_turn_id: record.host.run and record.host.run.turn.turn_id != prior
                               and record.host.run.task.done(), 90, "post-interruption turn")
                report["post_interruption_terminal"] = record.host.events[-1].type.value
                report["runtime_state_safe"] = not record.host.poisoned
                report["speech_interruptions"] = record.host.metrics["speech_interruptions"]
                report["interruption_cycles"].append({"cycle": cycle + 1,
                    "acoustic_interruption": report["speech_interruption_observed"],
                    "sustained_count_response": True, "playback_pending_before_stop": playback_pending_before_stop,
                    "post_terminal": report["post_interruption_terminal"], "state_safe": report["runtime_state_safe"]})
                if not report["runtime_state_safe"] or report["post_interruption_terminal"] != "turn.completed":
                    raise RuntimeError("Post-interruption state was not safe")
                await wait_for(lambda: not record.output.delivery_pending(), 60, "stop response delivery")
                stop_at = None
                print(json.dumps({"stage": "interruption_cycle_completed", "cycle": cycle + 1,
                                  "profile": settings.profile}), flush=True)
            accepted_inputs = [m["content"].lower() for m in record.host.history
                               if m["role"] == "user" and not m["content"].startswith("[")]
            report["accepted_turn_checks"] = {
                "count_request": any(("count" in text or "recite" in text) and ("hundred" in text or "100" in text) for text in accepted_inputs),
                "stop_request": any("stop" in text for text in accepted_inputs)}
            # This harness is restricted to isolated synthetic-speech meetings.
            report["accepted_test_turns"] = accepted_inputs[-4:]
            report["passed"] = (report["voice_turn_terminal"] == "turn.completed" and report["delta_count"] >= 2
                and report["sandbox_arguments_correct"]
                and report["both_requested_tools_completed"]
                and report["sandbox_action_items"] == 1 and report["audio_frames"] > 0 and report["chat_messages"] > 0
                and report["delivery_status"] == "submitted_unconfirmed" and report["runtime_state_safe"]
                and report["post_interruption_terminal"] == "turn.completed"
                and all(report["accepted_turn_checks"].values())
                and len(report["interruption_cycles"]) == args.interruptions)
            if args.fault_cases:
                report["fault_cases"] = []
                lookup_pcm = await synthesize("Okay assistant, look up project adapter status.")
                for scenario in ("slow_tool", "delayed_speech"):
                    async with delayed_fault(app.state.executor, record.output, scenario) as (entered, cancelled):
                        report["stage"] = scenario
                        await say(lookup_pcm if scenario == "slow_tool" else long_pcm)
                        await wait_for(entered.is_set, 90, scenario + " entry")
                        interrupted_turn_id = record.host.run.turn.turn_id
                        baseline = record.host.metrics["speech_interruptions"]
                        stop_at = time.monotonic()
                        await say(stop_pcm)
                        await wait_for(lambda prior=baseline: record.host.metrics["speech_interruptions"] > prior,
                                       15, scenario + " acoustic interruption")
                        await wait_for(cancelled.is_set, 2, scenario + " cooperative cleanup")
                    await wait_for(lambda prior=interrupted_turn_id: record.host.run.turn.turn_id != prior
                                   and record.host.run.task.done(), 90, scenario + " recovery")
                    if record.host.poisoned or record.host.events[-1].type.value != "turn.completed":
                        raise RuntimeError("Fault-case recovery failed")
                    await wait_for(lambda: not record.output.delivery_pending(), 60, scenario + " speech delivery")
                    stop_at = None
                    writes = app.state.executor.db.execute("SELECT COUNT(*) FROM action_items WHERE session=?", (session_id,)).fetchone()[0]
                    if writes != 1:
                        raise RuntimeError("Fault cases created an unexpected sandbox write")
                    report["fault_cases"].append({"scenario": scenario, "acoustic_interruption": True,
                                                  "cooperative_cleanup": True, "state_safe": True,
                                                  "post_terminal": "turn.completed", "sandbox_writes": writes})
                    print(json.dumps({"stage": "fault_case_completed", "scenario": scenario,
                                      "profile": settings.profile}), flush=True)
            if args.soak_seconds:
                report["stage"] = "soak"
                heartbeat_pcm = await synthesize("Okay assistant, just say ready.")
                began = time.monotonic()
                next_heartbeat = began + 240
                report["soak_heartbeat_count"] = 0
                while time.monotonic() - began < args.soak_seconds:
                    await asyncio.sleep(min(30, args.soak_seconds - (time.monotonic() - began)))
                    status = await api.bots.status(primary_id)
                    report["soak_elapsed_seconds"] = round(time.monotonic() - began, 2)
                    report["soak_last_bot_status"] = status.get("status")
                    if record.closed or record.host.poisoned or status.get("status") not in {"InMeeting", "Recording"}:
                        raise RuntimeError("Session failed during soak")
                    if time.monotonic() >= next_heartbeat:
                        # MeetStream deliberately leaves after 600 silent seconds.
                        # Exercise a real voice turn periodically during the soak.
                        prior = record.host.run.turn.turn_id
                        frames = record.output.audio_frames
                        await say(heartbeat_pcm)
                        await wait_for(lambda previous=prior: record.host.run.turn.turn_id != previous
                                       and record.host.run.task.done(), 90, "soak voice heartbeat")
                        await wait_for(lambda: not record.output.delivery_pending(), 60, "soak heartbeat speech")
                        if (record.host.events[-1].type.value != "turn.completed"
                                or record.output.audio_frames <= frames or record.output.error):
                            raise RuntimeError("Soak voice heartbeat failed")
                        report["soak_heartbeat_count"] += 1
                        next_heartbeat = time.monotonic() + 240
                    assert len(record.host.history) <= 40 and len(record.host.events) <= 512
                    print(json.dumps({"stage": "soak", "elapsed_seconds": round(time.monotonic() - began),
                                      "profile": settings.profile}), flush=True)
                report["soak_seconds"] = round(time.monotonic() - began, 2)
            # Give the host time to observe captions/chat and save UI proof before cleanup.
            print(json.dumps({"stage": "evidence_window", "profile": settings.profile, "passed": report["passed"]}), flush=True)
            report["stage"] = "completed"
            await asyncio.sleep(20)
    except asyncio.CancelledError:
        report["passed"] = False
        report["stage"] = "cancelled"
        report["error_class"] = "CancelledError"
        raise
    except Exception as exc:
        report["passed"] = False
        report["error_class"] = type(exc).__name__
        if record:
            report["diagnostics"] = {"metrics": dict(record.host.metrics), "input_error": record.input_error,
                                     "delivery_error": record.output.error,
                                     "delivery_error_details": record.output.error_details}
            report["accepted_test_turns"] = [m["content"] for m in record.host.history
                                              if m["role"] == "user" and not m["content"].startswith("[")][-4:]
            report["background_test_turns"] = record.policy.background[-4:]
            report["synthetic_assistant_answers"] = [m["content"] for m in record.host.history
                                                     if m["role"] == "assistant"][-4:]
            if record.transcription:
                report["diagnostics"]["stt_messages_received"] = record.transcription.messages_received
                report["diagnostics"]["pending_stt_segments"] = len(record.transcription.parts)
        # No exception string: provider errors may contain credentials or meeting data.
    finally:
        report["cleanup"] = {}
        for bot_id in [speaker_id, primary_id]:
            if bot_id:
                try:
                    if bot_id == primary_id:
                        async with httpx.AsyncClient(base_url=f"http://127.0.0.1:{args.port}", timeout=30,
                                                     trust_env=False, headers={"Authorization": "Bearer " + os.environ["ADAPTER_API_KEY"]}) as owner:
                            report["cleanup"][bot_id] = await stop_primary_session(owner, api, session_id, bot_id)
                    else:
                        report["cleanup"][bot_id] = await stop_owned_bot(api, bot_id)
                except Exception as exc:
                    report["cleanup"][bot_id] = {"terminal_status": "unconfirmed", "error_class": type(exc).__name__}
                    report["passed"] = False
        server.should_exit = True
        with suppress(Exception):
            await serving
        await speech.close()
        if args.record_video and speaker_id and report.get("stage") != "cancelled":
            print(json.dumps({"stage": "processing_demo_video", "profile": settings.profile}), flush=True)
            try:
                report["demo_video"] = await save_demo_video(api, speaker_id, args.output.with_suffix(".mp4"))
            except Exception as exc:
                report["video_error_class"] = type(exc).__name__
                report["passed"] = False
        await api.aclose()
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2), flush=True)
    return report["passed"]


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--env-file", default=".env")
    parser.add_argument("--config", default="config.toml")
    parser.add_argument("--meeting", required=True)
    parser.add_argument("--port", type=int, default=8097)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--interruptions", type=int, default=1)
    parser.add_argument("--soak-seconds", type=int, default=0)
    parser.add_argument("--fault-cases", action="store_true", help="Test spoken cancellation during a slow lookup and delayed speech")
    parser.add_argument("--record-video", action="store_true", help="Record the isolated demo via MeetStream and save a private MP4 beside the report")
    args = parser.parse_args()
    if not 1 <= args.interruptions <= 20 or not 0 <= args.soak_seconds <= 1800:
        parser.error("Choose 1–20 interruption cycles and 0–1800 soak seconds")
    if args.record_video and args.output.with_suffix(".mp4").exists():
        parser.error("Choose a new report name; its demo video already exists")
    logging.disable(logging.CRITICAL)
    load_environment(args.env_file)
    raise SystemExit(0 if asyncio.run(run(args)) else 1)
