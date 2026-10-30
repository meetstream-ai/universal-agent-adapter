import os
from pathlib import Path
from runpy import run_path
from types import SimpleNamespace

import httpx
import pytest
from meetstream import NotFoundError

harness = run_path(str(Path(__file__).resolve().parents[1] / "scripts/live_meeting_demo.py"))
sustained_count = harness["sustained_count"]


def test_long_answer_evidence_rejects_short_count_and_refusal():
    assert not sustained_count("I can't count to sixty; I can only count from one to six. One, two, three, four, five, six.")
    assert not sustained_count("Stopped.")
    assert sustained_count(", ".join(str(n) for n in range(1, 61)))
    assert sustained_count("One, two, three, four, five, six, seven, eight, nine, ten, eleven, twelve.")


async def test_cleanup_reconciles_failed_remove_only_with_independent_terminal_status(monkeypatch):
    calls, states = [], iter(["InMeeting", "Done"])

    async def remove(bot_id):
        calls.append(bot_id)
        raise TimeoutError("Synthetic lost response")

    async def status(bot_id):
        return {"status": next(states)}

    async def tick(seconds):
        assert seconds == 2

    monkeypatch.setattr(harness["asyncio"], "sleep", tick)
    api = SimpleNamespace(bots=SimpleNamespace(remove=remove, status=status))
    outcome = await harness["stop_owned_bot"](api, "owned-fixture")
    assert calls == ["owned-fixture"]
    assert outcome == {"terminal_status": "Done", "remove_requested": False, "remove_error_class": "TimeoutError"}

    async def still_in_meeting(bot_id):
        return {"status": "InMeeting"}

    api.bots.status = still_in_meeting
    with pytest.raises(TimeoutError):
        await harness["stop_owned_bot"](api, "other-owned-fixture")


async def test_primary_cleanup_uses_session_owner_and_verifies_departure():
    calls = []

    async def status(bot_id):
        calls.append(("status", bot_id))
        return {"status": "Done"}

    async def forbidden_remove(bot_id):
        pytest.fail("The harness must not remove the application's primary bot twice")

    def respond(request):
        calls.append((request.method, request.url.path))
        return httpx.Response(202, json={"closed": True, "bot_removal_pending": True})

    api = SimpleNamespace(bots=SimpleNamespace(status=status, remove=forbidden_remove))
    async with httpx.AsyncClient(base_url="http://127.0.0.1", transport=httpx.MockTransport(respond)) as client:
        result = await harness["stop_primary_session"](client, api, "session", "owned")
    assert result["terminal_status"] == "Done"
    assert calls == [("POST", "/v1/sessions/session/stop"), ("status", "owned")]


@pytest.mark.parametrize("scenario", ["slow_tool", "delayed_speech"])
async def test_fault_delays_cancel_cooperatively_and_restore_original_handlers(monkeypatch, scenario):
    import asyncio

    from meetstream_agent_adapter.tools import ToolSpec

    async def lookup(*args):
        return {"status": "ready"}

    async def pcm(text):
        yield b"\x00\x00"

    async def delay(seconds):
        assert seconds == 8
        await asyncio.Event().wait()

    spec = ToolSpec("lookup_project_status", "Lookup", {}, lookup)
    executor = SimpleNamespace(specs={spec.name: spec})
    output = SimpleNamespace(speech=SimpleNamespace(pcm=pcm))
    monkeypatch.setattr(harness["asyncio"], "sleep", delay)
    async with harness["delayed_fault"](executor, output, scenario) as (entered, cancelled):
        if scenario == "slow_tool":
            task = asyncio.create_task(executor.specs[spec.name].handler({}, None, "op"))
        else:
            task = asyncio.create_task(anext(output.speech.pcm("Synthetic")))
        await asyncio.wait_for(entered.wait(), 1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert cancelled.is_set()
    assert executor.specs[spec.name] is spec and output.speech.pcm is pcm


async def test_recording_export_rejects_untrusted_urls_and_preserves_existing_files(monkeypatch, tmp_path):
    url = "https://demo.s3.amazonaws.com/fixture.mp4?signature=synthetic"
    pending = True

    async def ready(bot_id, **kwargs):
        nonlocal pending
        if pending:
            pending = False
            raise NotFoundError("Recording is still processing", status=404)
        return {"video_url": url}

    async def stopped(bot_id):
        return {"status": "Done"}

    async def tick(seconds):
        assert seconds == 10

    monkeypatch.setattr(harness["asyncio"], "sleep", tick)

    def respond(request):
        assert "authorization" not in request.headers
        return httpx.Response(200, content=b"fixture-video")

    client = httpx.AsyncClient
    monkeypatch.setattr(httpx, "AsyncClient", lambda **kwargs: client(transport=httpx.MockTransport(respond), **kwargs))
    api = SimpleNamespace(bots=SimpleNamespace(wait_for_video=ready, status=stopped))
    path = tmp_path / "demo.mp4"
    save = harness["save_demo_video"]
    result = await save(api, "owned-fixture", path)
    assert result["bytes"] == 13 and path.read_bytes() == b"fixture-video"
    if os.name != "nt":
        assert path.stat().st_mode & 0o777 == 0o600
    with pytest.raises(FileExistsError):
        await save(api, "owned-fixture", path)
    assert path.read_bytes() == b"fixture-video"
    for bad_url in ["https://demo.s3.amazonaws.com.attacker.example/fixture.mp4",
                    "https://synthetic:secret@demo.s3.amazonaws.com/fixture.mp4"]:
        url = bad_url
        with pytest.raises(ValueError):
            await save(api, "owned-fixture", tmp_path / "untrusted.mp4")
    assert not (tmp_path / "untrusted.mp4").exists()


async def test_cancelled_voice_harness_stops_and_records_cancellation_without_export(monkeypatch, tmp_path):
    import asyncio
    import json

    from meetstream_agent_adapter.config import Settings

    entered, stopped = asyncio.Event(), asyncio.Event()
    closed = []

    class App:
        def websocket(self, path):
            return lambda handler: handler

    class Server:
        started = True

        def __init__(self, config):
            pass

        @property
        def should_exit(self):
            return stopped.is_set()

        @should_exit.setter
        def should_exit(self, value):
            if value:
                stopped.set()

        async def serve(self):
            await stopped.wait()

    class API:
        def __init__(self, **kwargs):
            pass

        async def aclose(self):
            closed.append("api")

    class Speech:
        async def close(self):
            closed.append("speech")

    class PublicClient:
        def __init__(self, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        async def get(self, path):
            entered.set()
            await asyncio.Event().wait()

    async def forbidden_export(*args):
        pytest.fail("A cancelled test must not wait for recording export")

    globals_ = harness["run"].__globals__
    monkeypatch.setitem(globals_, "load_settings", lambda path: Settings(
        profile="fake", output="voice_and_chat", transcript_source="deepgram", wake_words=["okay assistant"]))
    monkeypatch.setitem(globals_, "AsyncMeetStream", API)
    monkeypatch.setitem(globals_, "create_app", lambda *args, **kwargs: App())
    monkeypatch.setitem(globals_, "create_speech", lambda settings: Speech())
    monkeypatch.setitem(globals_, "save_demo_video", forbidden_export)
    monkeypatch.setattr(harness["uvicorn"], "Server", Server)
    monkeypatch.setattr(httpx, "AsyncClient", PublicClient)
    output = tmp_path / "cancelled.json"
    task = asyncio.create_task(harness["run"](SimpleNamespace(
        config="fixture", port=8097, interruptions=10, record_video=True, output=output)))
    await asyncio.wait_for(entered.wait(), 1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, 1)
    report = json.loads(output.read_text())
    assert not report["passed"] and report["stage"] == "cancelled"
    assert stopped.is_set() and sorted(closed) == ["api", "speech"]


def test_audio_observer_uses_channel_specific_ack_and_acceptance_needs_every_check():
    from meetstream_agent_adapter.meetstream.protocol import acknowledgement

    assert acknowledgement('owned') == {'command': 'ack', 'bot_id': 'owned'}
    assert acknowledgement('owned', audio=True) == {'type': 'ack', 'bot_id': 'owned'}
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
    try:
        checks = run_path(str(Path(__file__).resolve().parents[1] / 'scripts/voice_acceptance.py'))['acceptance_checks']
    finally:
        sys.path.pop(0)
    required = checks({})
    assert not any(required.values())
    evidence = dict.fromkeys(required, True)
    assert all(checks(evidence).values())
    assert 'unknown_switch_rejected' in checks({'mode': 'meeting'})
    assert 'unknown_switch_rejected' not in checks({'mode': 'mia'})
    for name in required:
        incomplete = {**evidence, name: False}
        assert not all(checks(incomplete).values())


def test_voice_confirmation_normalizes_spacing_without_accepting_other_titles():
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
    try:
        normalize = run_path(str(Path(__file__).resolve().parents[1] / 'scripts/voice_acceptance.py'))['canonical']
    finally:
        sys.path.pop(0)
    assert normalize('AlphaPilot') == normalize('alpha pilot')
    assert normalize('alpha pilot') != normalize('beta pilot')
    assert normalize('Riley') != normalize('Morgan')


def test_voice_stop_ack_requires_exact_assistant_reply_after_command():
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
    try:
        acknowledged = run_path(str(Path(__file__).resolve().parents[1] / 'scripts/voice_acceptance.py'))['stop_acknowledged']
    finally:
        sys.path.pop(0)
    heard = {'text': 'Stopped.', 'speaker': 'Adapter SDK Test', 'at': 10}
    assert acknowledged(heard, 9)
    assert acknowledged({**heard, 'text': 'Stop.'}, 9)
    assert not acknowledged(heard, 11)
    assert not acknowledged({**heard, 'speaker': 'SDK Finite Voice Test'}, 9)
    assert not acknowledged({**heard, 'text': 'I cannot stop.'}, 9)
    assert not acknowledged({**heard, 'text': 'Stop and cancel.'}, 9)
