import asyncio
import time
from itertools import permutations

import pytest
from fastapi.testclient import TestClient
from framework_models import adk_model, crewai_model, langgraph_model, openai_model

from meetstream_agent_adapter.adapters import load_adapter
from meetstream_agent_adapter.adapters.common import StatelessSession, delta
from meetstream_agent_adapter.config import Settings
from meetstream_agent_adapter.contract import AdapterError, Capabilities, EventType, SessionContext, Turn
from meetstream_agent_adapter.host import MemoryOutput, SessionHost
from meetstream_agent_adapter.server import create_app
from meetstream_agent_adapter.tools import ToolExecutor, ToolSpec

KEY = "management-test-key-" + "a" * 32
AUTH = {"Authorization": "Bearer " + KEY}


class Runtime(StatelessSession):
    def __init__(self, profile, *, block=False):
        self.profile, self.block = profile, block
        self.started, self.cleaned = asyncio.Event(), asyncio.Event()
        self.closed = False
        self.histories = []

    async def run_turn(self, turn, history):
        self.histories.append(history)
        self.started.set()
        try:
            yield delta(self.profile + " ")
            if self.block:
                await asyncio.Event().wait()
            yield delta("ready.")
        finally:
            self.cleaned.set()

    async def close(self):
        self.closed = True


class Prepared:
    def __init__(self, runtime):
        self.runtime = runtime

    async def open_session(self, context, tools):
        return self.runtime


async def test_switch_fences_old_turn_preserves_history_tools_and_output():
    tools, output = ToolExecutor([], set()), MemoryOutput()
    old, new = Runtime("old", block=True), Runtime("new")
    host = SessionHost(SessionContext(session_id="switch", workspace="sandbox", bot_id="same-bot", profile="openai"), old, tools, output)
    host.history.append({"role": "user", "content": "Remember project blue."})
    try:
        await host.accept(Turn(event_id="one", text="Begin", timestamp=1))
        await old.started.wait()
        prior = host.run
        assert await host.switch(Prepared(new), "langgraph") == 1
        assert old.cleaned.is_set() and old.closed and not host.poisoned
        assert host.tools is tools and host.output is output and host.context.bot_id == "same-bot"
        await host.emit(prior, EventType.DELTA, {"text": "obsolete"})
        await host.accept(Turn(event_id="two", text="Continue", timestamp=2))
        await host.wait()
        assert new.histories[0][0]["content"] == "Remember project blue."
        assert host.history[-1]["content"] == "new ready."
        assert any(e.type == EventType.CANCELLED and e.turn_id == prior.turn.turn_id for e in host.events)
        assert any(e.type == EventType.SWITCHED for e in host.events)
        assert "obsolete" not in str(list(host.events))
    finally:
        await host.close()
        tools.close()


@pytest.mark.parametrize("capabilities", [Capabilities(incremental_text=False), None])
async def test_failed_switch_preflight_keeps_current_runtime_running(capabilities):
    tools, old = ToolExecutor([], set()), Runtime("old", block=True)
    candidate = Runtime("unsupported")
    candidate.capabilities = capabilities
    host = SessionHost(SessionContext(session_id="switch", workspace="test", bot_id="bot", profile="adk"), old, tools, MemoryOutput())
    try:
        await host.accept(Turn(event_id="one", text="Begin", timestamp=1))
        await old.started.wait()
        prior = host.run
        with pytest.raises(AdapterError):
            await host.switch(Prepared(candidate), "langgraph")
        assert candidate.closed and not old.closed and host.active(prior) and not host.poisoned
    finally:
        await host.close()
        tools.close()


async def test_switch_commit_cancellation_closes_candidate_and_blocks_unsafe_recovery():
    entered = asyncio.Event()
    class Closing(Runtime):
        async def close(self):
            entered.set()
            await asyncio.Event().wait()
    tools, old, candidate = ToolExecutor([], set()), Closing("old"), Runtime("new")
    host = SessionHost(SessionContext(session_id="switch", workspace="test", bot_id="bot", profile="adk"), old, tools, MemoryOutput())
    pending = asyncio.create_task(host.switch(Prepared(candidate), "langgraph"))
    await asyncio.wait_for(entered.wait(), 1)
    pending.cancel()
    with pytest.raises(asyncio.CancelledError):
        await pending
    assert candidate.closed and host.poisoned and host.runtime is old
    with pytest.raises(AdapterError):
        await host.accept(Turn(event_id="next", text="Next", timestamp=1))
    old.close = Runtime.close.__get__(old)
    await host.close()
    tools.close()


async def test_switch_does_not_activate_replacement_when_old_runtime_ignores_cancellation():
    from contextlib import suppress

    finish = asyncio.Event()
    class Stubborn(Runtime):
        async def run_turn(self, turn, history):
            self.started.set()
            with suppress(asyncio.CancelledError):
                await finish.wait()
            await finish.wait()
            yield delta("stale")
    tools, old, candidate = ToolExecutor([], set()), Stubborn("old"), Runtime("new")
    host = SessionHost(SessionContext(session_id="switch", workspace="test", bot_id="bot", profile="adk"), old, tools, MemoryOutput())
    try:
        await host.accept(Turn(event_id="old", text="Begin", timestamp=1))
        await old.started.wait()
        with pytest.raises(AdapterError):
            await host.switch(Prepared(candidate), "langgraph")
        assert candidate.closed and host.runtime is old and host.poisoned
        assert not any(e.type == EventType.SWITCHED for e in host.events)
    finally:
        finish.set()
        await host.wait()
        await host.close()
        tools.close()


@pytest.mark.parametrize("first,second", list(permutations(["openai", "langgraph", "adk", "crewai"], 2)))
async def test_actual_framework_switch_cancels_write_and_preserves_ledger(first, second):
    models = {"openai": openai_model, "langgraph": langgraph_model, "adk": adk_model, "crewai": crewai_model}
    def adapter(profile):
        model = models[profile]()
        return load_adapter(profile, **{"openai": {"model": model}, "langgraph": {"chat_model": model},
                                       "adk": {"llm": model}, "crewai": {"llm": model}}[profile])
    initial, replacement = adapter(first), adapter(second)
    entered, cancelled, writes = asyncio.Event(), asyncio.Event(), []

    async def save(args, context, operation):
        if context.profile == first:
            entered.set()
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()
        writes.append(context.profile)
        return {"saved": True}

    schema = {"type": "object", "properties": {"title": {"type": "string"}}, "required": ["title"], "additionalProperties": False}
    tools = ToolExecutor([ToolSpec("save", "Save", schema, save, writes=True)], {"save"})
    host = await SessionHost.create(SessionContext(session_id="native-switch", workspace="test", bot_id="same-bot", profile=first), initial, tools, MemoryOutput())
    try:
        await host.accept(Turn(event_id="first", text="Save an action", timestamp=1))
        await asyncio.wait_for(entered.wait(), 5)
        await host.switch(replacement, second)
        assert cancelled.is_set() and not writes
        assert tools.db.execute("SELECT status FROM operations").fetchone()[0] == "outcome_unknown"
        await host.accept(Turn(event_id="second", text="Save a new action", timestamp=2))
        await host.wait()
        assert host.events[-1].type == EventType.COMPLETED and writes == [second]
        assert tools.db.execute("SELECT COUNT(*) FROM operations").fetchone()[0] == 2
        assert sum(e.type == EventType.DELTA and e.turn_id == host.run.turn.turn_id for e in host.events) >= 2
    finally:
        await host.close()
        tools.close()


def test_http_switch_keeps_same_bot_transport_and_reports_per_session_runtime(monkeypatch, tmp_path):
    monkeypatch.setenv("ADAPTER_API_KEY", KEY)
    runtimes = []
    def factory(profile, **kwargs):
        runtime = Runtime(profile)
        runtimes.append(runtime)
        return Prepared(runtime)
    app = create_app(Settings(profile="fake", model="offline", output="chat", ledger=str(tmp_path / "ledger.sqlite")), adapter_factory=factory)
    with TestClient(app) as client:
        first = client.post("/v1/sessions", headers=AUTH, json={"bot_id": "same-bot"}).json()
        other = client.post("/v1/sessions", headers=AUTH, json={"bot_id": "other-bot"}).json()
        sid, token = first["session_id"], first["session_token"]
        base = "/v1/sessions/" + sid
        with client.websocket_connect("/bridge/" + token) as ws:
            ws.send_json({"type": "ready", "bot_id": "same-bot"})
            ws.receive_json()
            socket = app.state.records[sid].output.socket
            assert client.post(base + "/runtime", json={"profile": "adk"}).status_code == 401
            assert client.post(base + "/runtime", headers=AUTH, json={"profile": "unknown"}).status_code == 422
            assert client.post(base + "/runtime", headers=AUTH, json={"profile": "adk", "model": "gemini/wrong-prefix"}).status_code == 400
            for n, profile in enumerate(["adk", "langgraph", "crewai", "openai"], 1):
                response = client.post(base + "/runtime", headers=AUTH, json={"profile": profile, "provider": "google"})
                assert response.status_code == 200, response.json()
                status = client.get(base, headers=AUTH).json()
                assert status["profile"] == profile and status["runtime_revision"] == n
                assert status["bot_id"] == "same-bot" and app.state.records[sid].output.socket is socket
                client.post(base + "/turns", headers=AUTH, json={"event_id": str(n), "text": "hey assistant ready", "timestamp": n})
                deadline = time.monotonic() + 2
                while time.monotonic() < deadline:
                    events = client.get(base + "/events", headers=AUTH).json()
                    if events[-1]["type"] == "turn.completed":
                        break
                    time.sleep(.01)
                assert events[-1]["type"] == "turn.completed"
            assert client.get("/v1/sessions/" + other["session_id"], headers=AUTH).json()["profile"] == "fake"
            assert all(runtime.closed for runtime in [runtimes[0], *runtimes[2:-1]])


def test_switch_discards_input_already_dequeued_during_preparation(monkeypatch, tmp_path):
    import threading
    from concurrent.futures import ThreadPoolExecutor

    entered, release = threading.Event(), threading.Event()
    instances = []
    class Preparing(Prepared):
        async def open_session(self, context, tools):
            entered.set()
            assert await asyncio.to_thread(release.wait, 2)
            return self.runtime
    def factory(profile, **kwargs):
        runtime = Runtime(profile)
        instances.append(runtime)
        return Preparing(runtime) if profile == "adk" else Prepared(runtime)
    monkeypatch.setenv("ADAPTER_API_KEY", KEY)
    app = create_app(Settings(profile="fake", output="events", ledger=str(tmp_path / "ledger.sqlite")), adapter_factory=factory)
    with TestClient(app) as client, ThreadPoolExecutor(max_workers=1) as pool:
        sid = client.post("/v1/sessions", headers=AUTH, json={"bot_id": "bot"}).json()["session_id"]
        base = "/v1/sessions/" + sid
        pending = pool.submit(client.post, base + "/runtime", headers=AUTH, json={"profile": "adk"})
        assert entered.wait(2)
        client.post(base + "/turns", headers=AUTH, json={"event_id": "old-queued", "text": "obsolete question", "timestamp": 1})
        deadline = time.monotonic() + 1
        while not app.state.records[sid].inputs.empty() and time.monotonic() < deadline:
            time.sleep(.005)
        assert app.state.records[sid].inputs.empty()  # Already waiting at the switch lock.
        release.set()
        assert pending.result(timeout=2).status_code == 200
        client.post(base + "/turns", headers=AUTH, json={"event_id": "new", "text": "new question", "timestamp": 2})
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline:
            if client.get(base + "/events", headers=AUTH).json()[-1]["type"] == "turn.completed":
                break
            time.sleep(.01)
        history = list(app.state.records[sid].host.history)
        assert any("new question" in m["content"] for m in history)
        assert all("obsolete question" not in m["content"] for m in history)


async def test_switch_clears_pending_speech_after_reasoning_completed():
    from meetstream_agent_adapter.meetstream.output import MeetingOutput

    entered, cleaned, commands = asyncio.Event(), asyncio.Event(), []
    class Speech:
        async def pcm(self, text):
            try:
                yield bytes(240)
                entered.set()
                await asyncio.Event().wait()
            finally:
                cleaned.set()
        async def close(self):
            pass
    async def send(command):
        commands.append(command["command"])
    speech = Speech()
    output = MeetingOutput("same-bot", speech, sender=send)
    tools = ToolExecutor([], set())
    host = SessionHost(SessionContext(session_id="playback", workspace="test", bot_id="same-bot", profile="adk"), Runtime("old"), tools, output)
    try:
        await host.accept(Turn(event_id="old", text="Speak", timestamp=1))
        await host.wait()
        await asyncio.wait_for(entered.wait(), 1)
        assert host.run.terminal and output.delivery_pending()
        await host.switch(Prepared(Runtime("new")), "langgraph")
        assert cleaned.is_set() and not output.delivery_pending() and commands[-1] == "interrupt"
        assert output.speech is speech and output.bot_id == "same-bot"
    finally:
        await host.close()
        tools.close()


def test_missing_target_credentials_fail_before_interrupting_current_agent(monkeypatch, tmp_path):
    monkeypatch.setenv("ADAPTER_API_KEY", KEY)
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    with TestClient(create_app(Settings(profile="fake", output="events", ledger=str(tmp_path / "ledger.sqlite")))) as client:
        sid = client.post("/v1/sessions", headers=AUTH, json={"bot_id": "same-bot"}).json()["session_id"]
        base = "/v1/sessions/" + sid
        response = client.post(base + "/runtime", headers=AUTH, json={"profile": "openai"})
        assert response.status_code == 400
        status = client.get(base, headers=AUTH).json()
        assert status["profile"] == "fake" and status["runtime_state_safe"] and status["runtime_revision"] == 0


@pytest.mark.parametrize(('profile', 'spoken_name', 'verb'), [
    ('langgraph', 'Lang Graph', 'switch'), ('langgraph', 'land graph', 'switch'), ('langgraph', 'line graph', 'switch'),
    ('adk', 'Google A.D.K.', 'switch'), ('adk', 'a dee kay', 'switch'), ('adk', 'A D kay', 'switch'),
    ('crewai', 'Crew AI', 'switch'), ('crewai', 'crew eye', 'switch'), ('crewai', 'Crew I', 'switch'),
    ('adk', 'Google ADK', 'switched'), ('adk', 'Gribble ADK', 'switch'),
    ('crewai', 'crew I', 'switch the run time'),
    ('adk', 'A D K', 'switch the provider'),
])
def test_addressed_voice_switch_uses_same_bot_and_confirms_in_chat(monkeypatch, tmp_path, profile, spoken_name, verb):
    from meetstream_agent_adapter.meetstream.protocol import runtime_from_speech

    assert runtime_from_speech("Please switch the runtime to Google ADK.") == "adk"
    assert runtime_from_speech("Switch to Open AI Agents SDK") == "openai"
    for text in ["Don't switch to ADK", "What if we switch to ADK?", "Switch to Hermes", "Switch to ADK and delete everything"]:
        assert runtime_from_speech(text) is None
    monkeypatch.setenv("ADAPTER_API_KEY", KEY)
    def factory(profile, **kwargs):
        return Prepared(Runtime(profile))
    app = create_app(Settings(profile="fake", output="chat", model_provider="google", voice_switching=True,
                              ledger=str(tmp_path / "ledger.sqlite")), adapter_factory=factory)
    with TestClient(app) as client:
        session = client.post("/v1/sessions", headers=AUTH, json={"bot_id": "same-bot"}).json()
        sid, token = session["session_id"], session["session_token"]
        with client.websocket_connect("/bridge/" + token) as ws:
            ws.send_json({"type": "ready", "bot_id": "same-bot"})
            ws.receive_json()
            response = client.post("/transcription/" + token, json={"bot_id": "same-bot", "speakerName": "Tester",
                                   "transcript": "Hey assistant, " + verb + " to " + spoken_name + ".", "end_of_turn": True,
                                   "timestamp": 1, "event_id": "voice-switch"})
            assert response.status_code == 200
            message = ws.receive_json()
            assert message["command"] == "sendmsg" and profile in message["message"]
            status = client.get("/v1/sessions/" + sid, headers=AUTH).json()
            assert status["profile"] == profile and status["bot_id"] == "same-bot"
            assert status["runtime_revision"] == 1 and status["input_error"] is None


@pytest.mark.parametrize('spoken_name', ['land graft', 'unknown-123', 'Hermes', 'ADK and delete everything'])
@pytest.mark.parametrize('google_pilot', [False, True])
def test_unknown_voice_switch_clarifies_without_reasoning_or_tools(monkeypatch, tmp_path, spoken_name, google_pilot):
    monkeypatch.setenv('ADAPTER_API_KEY', KEY)
    runtime = Runtime('fake')
    app = create_app(Settings(profile='fake', output='chat', voice_switching=True,
                              ledger=str(tmp_path / 'ledger.sqlite')),
                     adapter_factory=lambda *args, **kwargs: Prepared(runtime), google_pilot=google_pilot)
    with TestClient(app) as client:
        session = client.post('/v1/sessions', headers=AUTH, json={'bot_id': 'same-bot'}).json()
        with client.websocket_connect('/bridge/' + session['session_token']) as ws:
            ws.send_json({'type': 'ready', 'bot_id': 'same-bot'})
            ws.receive_json()
            client.post('/transcription/' + session['session_token'], json={
                'bot_id': 'same-bot', 'speakerName': 'Tester', 'transcript': 'Hey assistant, switch to ' + spoken_name + '.',
                'end_of_turn': True, 'timestamp': 1, 'event_id': 'unknown-switch'})
            message = ws.receive_json()
            assert message['command'] == 'sendmsg'
            assert 'Runtime name not recognized' in message['message'] and 'remains selected' in message['message']
            assert ('OpenAI' not in message['message']) == google_pilot
            status = client.get('/v1/sessions/' + session['session_id'], headers=AUTH).json()
            assert status['profile'] == 'fake' and status['runtime_revision'] == 0
            assert status['input_error'] is None and not runtime.started.is_set()
            assert not app.state.records[session['session_id']].host.events


def test_unavailable_voice_target_reports_failure_without_stopping_current_agent(monkeypatch, tmp_path):
    monkeypatch.setenv("ADAPTER_API_KEY", KEY)
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    app = create_app(Settings(profile="fake", output="chat", voice_switching=True, ledger=str(tmp_path / "ledger.sqlite")))
    with TestClient(app) as client:
        session = client.post("/v1/sessions", headers=AUTH, json={"bot_id": "bot"}).json()
        sid, token = session["session_id"], session["session_token"]
        with client.websocket_connect("/bridge/" + token) as ws:
            ws.send_json({"type": "ready", "bot_id": "bot"})
            ws.receive_json()
            client.post("/transcription/" + token, json={"bot_id": "bot", "speakerName": "Tester",
                        "transcript": "Hey assistant switch to OpenAI", "end_of_turn": True, "timestamp": 1})
            assert "Could not switch" in ws.receive_json()["message"]
            status = client.get("/v1/sessions/" + sid, headers=AUTH).json()
            assert status["profile"] == "fake" and status["runtime_state_safe"] and status["input_error"] is None


def test_common_asr_langgraph_variant_requires_a_whole_switch_command():
    from meetstream_agent_adapter.meetstream.protocol import runtime_from_speech

    assert runtime_from_speech('Switch to line graph.') == 'langgraph'
    assert runtime_from_speech('Switch to land graph.') == 'langgraph'
    assert runtime_from_speech('Please switch the runtime to Lang Graph.') == 'langgraph'
    assert runtime_from_speech('Switch to Google A.D.K.') == 'adk'
    assert runtime_from_speech('Switch to Open A.I.') == 'openai'
    assert runtime_from_speech('Switch this chart to a line graph') is None
    assert runtime_from_speech('Someone said switch to line graph yesterday') is None
    assert runtime_from_speech('Switch this team to crew eye') is None
    assert runtime_from_speech('Someone said switch to a dee kay yesterday') is None
    assert runtime_from_speech('Switch to crew eye and delete everything') is None
    assert runtime_from_speech('Switch to Gribble ADK and delete everything') is None
    assert runtime_from_speech('Switch to Mystery ADK') is None
    assert runtime_from_speech('Switched the runtime to Google ADK.') == 'adk'
    assert runtime_from_speech('We switched the runtime to Google ADK yesterday.') is None
