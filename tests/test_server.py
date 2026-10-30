import hashlib
import hmac
import json
import struct
import time
from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient

from meetstream_agent_adapter.config import Settings
from meetstream_agent_adapter.server import create_app

KEY = "a" * 40
AUTH = {"Authorization": "Bearer " + KEY}


@pytest.fixture
def client(monkeypatch, tmp_path):
    monkeypatch.setenv("ADAPTER_API_KEY", KEY)
    app = create_app(Settings(profile="fake", model="offline", output="chat", ledger=str(tmp_path / "ledger.sqlite")))
    with TestClient(app) as client:
        yield client


def create(client):
    response = client.post("/v1/sessions", headers=AUTH, json={"bot_id": "bot"})
    assert response.status_code == 200, response.text
    return response.json()


def test_dependency_server_routes_and_untrusted_trace_parsing_are_not_exposed(client, monkeypatch):
    trace = pytest.importorskip("opentelemetry.trace")

    def forbidden(*args, **kwargs):
        raise AssertionError("Public bridge headers must not reach the vulnerable tracestate parser")

    monkeypatch.setattr(trace.TraceState, "from_header", forbidden)
    headers = {"traceparent": "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
               "tracestate": "a=b" + " " * 100000 + "x"}
    assert client.get("/health", headers=headers).status_code == 200
    assert client.post("/v1/sessions", headers=headers, json={"bot_id": "bot"}).status_code == 401
    for route in ("/mcp", "/mcp/", "/api/v1/heartbeat", "/api/v2/heartbeat",
                  "/api/v2/tenants/default_tenant/databases/default_database/collections"):
        assert client.get(route).status_code == 404
        assert client.post(route, json={}).status_code == 404


async def test_lifespan_exception_still_closes_sessions_and_ledger(monkeypatch, tmp_path):
    import sqlite3

    import httpx

    monkeypatch.setenv("ADAPTER_API_KEY", KEY)
    app = create_app(Settings(profile="fake", output="events", ledger=str(tmp_path / "ledger.sqlite")))
    with pytest.raises(RuntimeError, match="synthetic lifespan failure"):
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
                response = await client.post("/v1/sessions", headers=AUTH, json={"bot_id": "borrowed"})
                assert response.status_code == 200
            record = next(iter(app.state.records.values()))
            raise RuntimeError("synthetic lifespan failure")
    assert record.closed and record.host.closed and not app.state.records
    with pytest.raises(sqlite3.ProgrammingError, match="closed"):
        app.state.executor.db.execute("SELECT 1")


def test_management_auth_body_limit_and_scoped_socket(client):
    assert client.post("/v1/sessions", json={"bot_id": "bot"}).status_code == 401
    assert client.post("/v1/sessions", headers=[(b"authorization", b"Bearer\xff")], json={"bot_id": "bot"}).status_code == 401
    assert client.post("/v1/sessions", headers=AUTH, content="x" * 65537).status_code == 413
    session = create(client)
    token, sid = session["session_token"], session["session_id"]
    with client.websocket_connect(f"/bridge/{token}") as ws:
        ws.send_json({"type": "ready", "bot_id": "bot"})
        assert ws.receive_json()["command"] == "ack"
        assert client.get(f"/v1/sessions/{sid}", headers=AUTH).json()["control_connected"]
    from starlette.websockets import WebSocketDisconnect
    with pytest.raises(WebSocketDisconnect), client.websocket_connect(f"/other/bridge/{token}") as ws:
        ws.receive_json()
    assert client.post(f"/v1/sessions/{sid}/stop", headers=AUTH).status_code == 200
    assert client.get(f"/v1/sessions/{sid}", headers=AUTH).status_code == 404


def test_webhook_turns_binding_and_duplicates(client):
    session = create(client)
    sid, token = session["session_id"], session["session_token"]
    payload = {"bot_id": "bot", "speakerName": "Tester", "new_text": "hey assistant status?", "end_of_turn": True,
               "timestamp": "2026-10-08T08:00:00Z"}
    with client.websocket_connect(f"/bridge/{token}") as ws:
        ws.send_json({"type": "ready", "bot_id": "bot"})
        ws.receive_json()
        for path in [f"/transcription/{token}", f"/transcription/{token}/webhook"]:
            assert client.post(path, json=payload).status_code == 200
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline:
            events = client.get(f"/v1/sessions/{sid}/events", headers=AUTH).json()
            if any(e["type"] == "turn.completed" for e in events):
                break
            time.sleep(0.01)
        assert sum(e["type"] == "turn.started" for e in events) == 1
        assert events[-1]["type"] == "turn.completed", events
        stream = client.get(f"/v1/sessions/{sid}/events/stream?follow=false", headers=AUTH)
        assert "event: assistant.text.delta" in stream.text
        assert "event: turn.completed" in stream.text
        resume = client.get(f"/v1/sessions/{sid}/events/stream?follow=false",
                            headers={**AUTH, "Last-Event-ID": events[-1]["event_id"]})
        assert resume.status_code == 200 and resume.text == ""
        assert client.get(f"/v1/sessions/{sid}/events/stream?follow=false",
                          headers={**AUTH, "Last-Event-ID": "lost"}).status_code == 409
        payload["bot_id"] = "other"
        assert client.post(f"/transcription/{token}", json=payload).status_code == 400


def test_lost_audio_channel_expires_even_with_control_connected(monkeypatch, tmp_path):
    monkeypatch.setenv("ADAPTER_API_KEY", KEY)
    app = create_app(Settings(profile="fake", model="offline", output="chat", reconnect_grace=1,
                              ledger=str(tmp_path / "ledger.sqlite")))
    with TestClient(app) as client:
        session = create(client)
        sid, token = session["session_id"], session["session_token"]
        with client.websocket_connect(f"/bridge/{token}") as control:
            control.send_json({"type": "ready", "bot_id": "bot"})
            control.receive_json()
            with client.websocket_connect(f"/bridge/audio/{token}") as audio:
                audio.send_json({"type": "ready", "bot_id": "bot"})
                audio.receive_json()
            deadline = time.monotonic() + 2
            while time.monotonic() < deadline:
                response = client.get(f"/v1/sessions/{sid}", headers=AUTH)
                if response.status_code == 404:
                    break
                time.sleep(0.02)
            assert response.status_code == 404


def test_speech_onset_clears_unconfirmed_playback_after_worker_finishes(monkeypatch, tmp_path):
    class Speech:
        async def pcm(self, text):
            yield bytes(240)

        async def close(self):
            pass

    monkeypatch.setenv("ADAPTER_API_KEY", KEY)
    app = create_app(Settings(profile="fake", model="offline", output="voice_and_chat",
                              ledger=str(tmp_path / "ledger.sqlite")), speech_factory=Speech)
    with TestClient(app) as client:
        session = create(client)
        sid, token = session["session_id"], session["session_token"]
        with client.websocket_connect(f"/bridge/{token}") as control, \
                client.websocket_connect(f"/bridge/audio/{token}") as audio:
            control.send_json({"type": "ready", "bot_id": "bot"})
            control.receive_json()
            audio.send_json({"type": "ready", "bot_id": "bot"})
            audio.receive_json()
            assert client.post(f"/v1/sessions/{sid}/turns", headers=AUTH,
                               json={"event_id": "one", "text": "hey assistant status?", "timestamp": 1}).status_code == 200
            record = app.state.records[sid]
            deadline = time.monotonic() + 2
            while time.monotonic() < deadline:
                if record.host.run and record.host.run.terminal and not record.output.delivery_pending():
                    break
                time.sleep(0.01)
            assert record.host.run.terminal and not record.output.delivery_pending()
            assert record.output.delivery == "submitted_unconfirmed"
            sid_bytes, speaker = b"speaker", b"Tester"
            frame = (b"\x01" + struct.pack("<H", len(sid_bytes)) + sid_bytes
                     + struct.pack("<H", len(speaker)) + speaker + struct.pack("<h", 1800) * 6000)
            audio.send_bytes(frame)
            deadline = time.monotonic() + 2
            while time.monotonic() < deadline and record.host.metrics["speech_interruptions"] == 0:
                time.sleep(0.01)
            assert record.host.metrics["speech_interruptions"] == 1
            commands = []
            while "interrupt" not in commands:
                commands.append(control.receive_json()["command"])
            assert record.host.history[-1]["content"].startswith("[Previous output was interrupted")
            assert sum(e.type.value in {"turn.completed", "turn.cancelled", "turn.failed"}
                       for e in record.host.events) == 1


def test_signed_workspace_webhook_checks_signed_timestamp(client, monkeypatch):
    monkeypatch.setenv("MEETSTREAM_WEBHOOK_SECRET", "signing-secret")
    session = create(client)
    payload = {"bot_id": "bot", "event": "bot.stopped", "timestamp": datetime.now(timezone.utc).isoformat()}
    raw = json.dumps(payload).encode()
    signature = "sha256=" + hmac.new(b"signing-secret", raw, hashlib.sha256).hexdigest()
    assert client.post("/webhooks/workspace", content=raw).status_code == 401
    assert client.post("/webhooks/workspace", content=raw, headers={"x-meetstream-signature": signature}).status_code == 200
    assert client.get(f"/v1/sessions/{session['session_id']}", headers=AUTH).status_code == 404
    payload["timestamp"] = "2020-01-01T00:00:00Z"
    raw = json.dumps(payload).encode()
    signature = "sha256=" + hmac.new(b"signing-secret", raw, hashlib.sha256).hexdigest()
    assert client.post("/webhooks/workspace", content=raw, headers={"x-meetstream-signature": signature,
        "x-meetstream-timestamp": str(time.time())}).status_code == 401


def test_fail_closed_config(monkeypatch):
    monkeypatch.delenv("ADAPTER_API_KEY", raising=False)
    with pytest.raises(ValueError):
        create_app(Settings())
    monkeypatch.setenv("ADAPTER_API_KEY", "replace-with-at-least-32-random-characters")
    with pytest.raises(ValueError):
        create_app(Settings())
    with pytest.raises(ValueError):
        Settings(public_url="https://bridge.example/path?token=secret")
    with pytest.raises(ValueError):
        Settings(profile="adk", model="gemini/gemini-3.5-flash-lite")
    with pytest.raises(ValueError):
        Settings(waiting_room_timeout=30)


def test_peer_control_command_cannot_cancel_native_turn(monkeypatch, tmp_path):
    import asyncio

    from meetstream_agent_adapter.adapters.common import StatelessSession, delta

    class Session(StatelessSession):
        async def run_turn(self, turn, history):
            yield delta('Started.')
            await asyncio.Event().wait()

    class Adapter:
        async def open_session(self, context, tools):
            return Session()

    monkeypatch.setenv('ADAPTER_API_KEY', KEY)
    app = create_app(Settings(profile='fake', output='chat', ledger=str(tmp_path / 'ledger.sqlite')), adapter=Adapter())
    with TestClient(app) as client:
        session = create(client)
        sid, token = session['session_id'], session['session_token']
        with client.websocket_connect(f'/bridge/{token}') as ws:
            ws.send_json({'type': 'ready', 'bot_id': 'bot'})
            ws.receive_json()
            response = client.post(f'/v1/sessions/{sid}/turns', headers=AUTH,
                json={'event_id': 'one', 'text': 'hey assistant start', 'timestamp': 1})
            assert response.status_code == 200
            # Synchronize the owner-facing event journal before simulating a peer response.
            deadline = time.monotonic() + 2
            while time.monotonic() < deadline:
                events = client.get(f'/v1/sessions/{sid}/events', headers=AUTH).json()
                if any(e['type'] == 'assistant.text.delta' for e in events):
                    break
                time.sleep(0.01)
            assert any(e['type'] == 'assistant.text.delta' for e in events)
            ws.send_json({'command': 'interrupt', 'type': 'ack', 'bot_id': 'bot'})
            time.sleep(0.05)
            events = client.get(f'/v1/sessions/{sid}/events', headers=AUTH).json()
            assert not any(e['type'] == 'turn.cancelled' for e in events)
            outcome = client.post(f'/v1/sessions/{sid}/interrupt', headers=AUTH).json()
            assert outcome['output_fenced'] and outcome['runtime_stopped']


def test_events_mode_completes_without_owning_meeting_media(monkeypatch, tmp_path):
    monkeypatch.setenv('ADAPTER_API_KEY', KEY)
    app = create_app(Settings(profile='fake', output='events', transcript_source='deepgram', ledger=str(tmp_path / 'ledger.sqlite')))
    with TestClient(app) as client:
        assert client.post('/v1/sessions', headers=AUTH,
                           json={'meeting_link': 'https://meet.google.com/abc-defg-hij'}).status_code == 400
        session = create(client)
        assert session['output'] == 'events' and session['deployment'] == 'reasoning_service'
        sid = session['session_id']
        assert client.post(f'/v1/sessions/{sid}/turns', headers=AUTH,
            json={'event_id': 'one', 'text': 'Project status?', 'timestamp': 1}).status_code == 200
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline:
            events = client.get(f'/v1/sessions/{sid}/events', headers=AUTH).json()
            if any(e['type'] == 'turn.completed' for e in events):
                break
            time.sleep(0.01)
        assert events[-1]['type'] == 'turn.completed'
        assert any(e['type'] == 'assistant.text.delta' for e in events)
        assert client.get(f'/v1/sessions/{sid}', headers=AUTH).json()['control_connected'] is False


@pytest.mark.parametrize("already_absent", [False, True])
def test_owned_bot_cleanup_retains_pending_exit_across_restart(monkeypatch, tmp_path, already_absent):
    import sqlite3
    from contextlib import closing
    from types import SimpleNamespace

    from meetstream import NotFoundError, NotReadyError

    monkeypatch.setenv("ADAPTER_API_KEY", KEY)
    calls, terminal = [], False

    async def create_bot(payload, **kwargs):
        return {"bot_id": "owned-fixture"}

    async def status(bot_id):
        if already_absent:
            raise NotFoundError("Synthetic absent bot", status=404)
        return {"status": "Done" if terminal else "InMeeting"}

    async def remove(bot_id):
        calls.append(bot_id)
        raise NotReadyError("Synthetic pending exit", status=202)

    api = SimpleNamespace(bots=SimpleNamespace(create=create_bot, status=status, remove=remove))
    settings = Settings(profile="fake", output="chat", public_url="https://adapter.example",
                        ledger=str(tmp_path / "cleanup.sqlite"))

    def cleanup_rows():
        with closing(sqlite3.connect(settings.ledger)) as db:
            return db.execute("SELECT bot_id FROM bot_cleanup").fetchall()

    app = create_app(settings, api_client=api)
    with TestClient(app) as client:
        session = client.post("/v1/sessions", headers=AUTH,
                              json={"meeting_link": "https://meet.google.com/abc-defg-hij"}).json()
        response = client.post(f"/v1/sessions/{session['session_id']}/stop", headers=AUTH)
        assert response.status_code == (200 if already_absent else 202)
        assert client.get(f"/v1/sessions/{session['session_id']}", headers=AUTH).status_code == 404
        rows = cleanup_rows()
        assert [row[0] for row in rows] == ([] if already_absent else ["owned-fixture"])
    terminal = True
    restarted = create_app(settings, api_client=api)
    with TestClient(restarted) as client:
        deadline = time.monotonic() + 1
        while time.monotonic() < deadline:
            client.get("/health")
            if not cleanup_rows():
                break
            time.sleep(.01)
        assert not cleanup_rows()
    assert calls == ([] if already_absent else ["owned-fixture"])


@pytest.mark.parametrize('departed', [False, True])
@pytest.mark.parametrize('local_timeout', [False, True])
async def test_lost_remove_response_requires_independent_departure(departed, local_timeout):
    import sqlite3
    from types import SimpleNamespace

    from meetstream import TimeoutError as PlatformTimeout

    from meetstream_agent_adapter.meetstream.lifecycle import remove_owned_bot

    states = iter(['InMeeting', 'Done' if departed else 'InMeeting'])

    async def status(bot_id):
        assert bot_id == 'owned'
        return {'status': next(states)}

    async def remove(bot_id):
        error = TimeoutError if local_timeout else PlatformTimeout
        raise error('Synthetic lost removal response')

    api = SimpleNamespace(bots=SimpleNamespace(status=status, remove=remove))
    with sqlite3.connect(':memory:') as db:
        db.execute('CREATE TABLE bot_cleanup (bot_id TEXT PRIMARY KEY)')
        db.executemany('INSERT INTO bot_cleanup VALUES (?)', [('owned',), ('other',)])
        assert await remove_owned_bot(lambda: api, db, 'owned') == departed
        assert db.execute('SELECT bot_id FROM bot_cleanup ORDER BY bot_id').fetchall() == (
            [('other',)] if departed else [('other',), ('owned',)])


@pytest.mark.parametrize(('detail_id', 'detail_status', 'expected'), [
    ('owned', 'Done', True), ('other', 'Done', False), ('owned', 'InMeeting', False),
])
async def test_departure_uses_terminal_details_only_for_the_matching_bot(detail_id, detail_status, expected):
    from types import SimpleNamespace

    from meetstream_agent_adapter.meetstream.lifecycle import departure_status
    from meetstream_agent_adapter.meetstream.protocol import meeting_ended

    async def status(bot_id):
        return {'status': 'InMeeting'}  # Observed stale endpoint response.

    async def detail(bot_id):
        return {'bot_details': {'BotID': detail_id, 'Status': detail_status}}

    api = SimpleNamespace(bots=SimpleNamespace(status=status, detail=detail))
    result = await departure_status(api, 'owned')
    assert meeting_ended(result) == expected
    assert result.get('source') == ('bot_details' if expected else None)


def test_bridge_refuses_hosted_mia_config_and_removed_attestation():
    with pytest.raises(ValueError, match="Hosted MIA configurations cannot be combined"):
        Settings(mia_config_id="hosted-agent")
    with pytest.raises(ValueError, match="Extra inputs are not permitted"):
        Settings(mia_transport_only=True)


def test_private_sidecar_uses_owner_supplied_tools_and_trusted_context(monkeypatch, tmp_path):
    from meetstream_agent_adapter.tools import ToolSpec

    monkeypatch.setenv("ADAPTER_API_KEY", KEY)
    calls = []

    async def lookup(arguments, context, operation):
        calls.append((arguments, context.workspace, context.bot_id))
        return {"status": "owner-authorized"}

    spec = ToolSpec("lookup_project_status", "Owner-authorized lookup", {
        "type": "object", "properties": {"project_id": {"type": "string"}},
        "required": ["project_id"], "additionalProperties": False}, lookup)
    app = create_app(Settings(profile="fake", output="events", workspace="trusted-owner",
                              ledger=str(tmp_path / "tools.sqlite")), tool_specs=[spec])
    with TestClient(app) as client:
        sid = create(client)["session_id"]
        response = client.post(f"/v1/sessions/{sid}/turns", headers=AUTH,
                               json={"event_id": "finalized", "text": "Project status?", "timestamp": 1})
        assert response.status_code == 200
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline:
            events = client.get(f"/v1/sessions/{sid}/events", headers=AUTH).json()
            if events and events[-1]["type"] == "turn.completed":
                break
            time.sleep(.01)
        assert calls == [({"project_id": "adapter"}, "trusted-owner", "bot")]
        assert any(e["type"] == "assistant.message.completed" and "owner-authorized" in e["payload"]["text"]
                   for e in events)


@pytest.mark.parametrize("failure", ["ConnectionError", "TimeoutError", "ServerError"])
async def test_cleanup_retries_transient_confirmation_without_forgetting_owned_bot(failure):
    import sqlite3
    from types import SimpleNamespace

    import meetstream

    from meetstream_agent_adapter.meetstream.lifecycle import remove_owned_bot

    calls = 0
    async def status(bot_id):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise getattr(meetstream, failure)("Synthetic temporary failure")
        return {"status": "Done"}
    api = SimpleNamespace(bots=SimpleNamespace(status=status))
    with sqlite3.connect(":memory:") as db:
        db.execute("CREATE TABLE bot_cleanup (bot_id TEXT PRIMARY KEY)")
        db.execute("INSERT INTO bot_cleanup VALUES ('owned')")
        assert not await remove_owned_bot(lambda: api, db, "owned")
        assert db.execute("SELECT bot_id FROM bot_cleanup").fetchall() == [("owned",)]
        assert await remove_owned_bot(lambda: api, db, "owned")
        assert db.execute("SELECT bot_id FROM bot_cleanup").fetchall() == []


@pytest.mark.parametrize("remove_fails", [False, True])
def test_owned_departure_requested_before_transport_close(monkeypatch, tmp_path, remove_fails):
    from types import SimpleNamespace

    from meetstream import ServerError

    monkeypatch.setenv("ADAPTER_API_KEY", KEY)
    calls = []

    class Socket:
        async def send_json(self, message):
            pass

        async def close(self, **kwargs):
            calls.append("socket_closed")

    async def create_bot(payload, **kwargs):
        return {"bot_id": "owned-fixture"}

    async def status(bot_id):
        return {"status": "InMeeting" if remove_fails or "remove" not in calls else "Stopped"}

    async def remove(bot_id):
        assert "socket_closed" not in calls
        assert record.closed and record.host.closed
        calls.append("remove")
        if remove_fails:
            raise ServerError("Synthetic removal failure", status=503)

    api = SimpleNamespace(bots=SimpleNamespace(create=create_bot, status=status, remove=remove))
    app = create_app(Settings(profile="fake", output="chat", public_url="https://adapter.example",
                              ledger=str(tmp_path / "cleanup.sqlite")), api_client=api)
    with TestClient(app) as client:
        session = client.post("/v1/sessions", headers=AUTH,
                              json={"meeting_link": "https://meet.google.com/abc-defg-hij"}).json()
        record = app.state.records[session['session_id']]
        record.output.socket = Socket()
        response = client.post(f"/v1/sessions/{record.id}/stop", headers=AUTH)
        assert response.status_code == (202 if remove_fails else 200)
        assert calls == ["remove", "socket_closed"]
        import sqlite3
        from contextlib import closing
        with closing(sqlite3.connect(tmp_path / "cleanup.sqlite")) as db:
            pending = db.execute("SELECT COUNT(*) FROM bot_cleanup").fetchone()[0]
        assert pending == int(remove_fails)
