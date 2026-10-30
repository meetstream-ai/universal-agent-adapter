import asyncio
import time
from concurrent.futures import ThreadPoolExecutor

from fastapi.testclient import TestClient

from meetstream_agent_adapter.adapters.common import StatelessSession, delta
from meetstream_agent_adapter.config import Settings
from meetstream_agent_adapter.server import create_app
from meetstream_agent_adapter.tools import ToolSpec

KEY = "management-fixture-" + "a" * 32
TOKEN = "mia-tool-fixture-" + "b" * 32
OWNER = {"Authorization": "Bearer " + KEY}
HEADERS = {"Authorization": "Bearer " + TOKEN, "X-Bot-ID": "bot-one"}


def payload(name, request_id, **args):
    return {"name": name, "bot": {"bot_id": "bot-one", "dynamic_vars": {}},
            "args": {"request_id": request_id, **args}}


def make_app(monkeypatch, path, calls, entered=None, *, google_pilot=False):
    monkeypatch.setenv("ADAPTER_API_KEY", KEY)
    monkeypatch.setenv("MIA_BRIDGE_TOKEN", TOKEN)
    class Adapter:
        def __init__(self, profile, **kwargs):
            self.profile = profile
        async def open_session(self, context, tools):
            profile = self.profile
            class Runtime(StatelessSession):
                async def run_turn(self, turn, history):
                    calls.append((profile, turn.text, list(history)))
                    if entered:
                        entered.set()
                    if turn.text == "block":
                        await asyncio.Event().wait()
                    if turn.text == "write":
                        await tools.call("save", {"title": "test"}, "stable-call")
                    yield delta(profile + " ")
                    yield delta("ready.")
            return Runtime()
    async def save(args, context, operation):
        calls.append(("write", context.bot_id, operation))
        return {"saved": True}
    spec = ToolSpec("save", "Save", {"type": "object", "properties": {"title": {"type": "string"}},
                                     "required": ["title"], "additionalProperties": False}, save, writes=True)
    return create_app(Settings(profile="adk", output="events", model_provider="google", public_url="https://bridge.example",
                               ledger=str(path)), adapter_factory=Adapter, tool_specs=[spec], google_pilot=google_pilot)


def test_google_pilot_blocks_openai_through_mia_and_management_routes(monkeypatch, tmp_path):
    calls = []
    with TestClient(make_app(monkeypatch, tmp_path / "ledger.sqlite", calls, google_pilot=True)) as client:
        functions = client.get("/v1/mia/functions", headers=OWNER).json()["agent"]["custom_functions"]
        assert functions[1]["parameters"]["properties"]["profile"]["enum"] == ["adk", "langgraph", "crewai"]
        assert "openai" not in {r["profile"] for r in client.get("/v1/runtimes", headers=OWNER).json()}
        response = client.post("/mia/switch_agent", headers=HEADERS,
                               json=payload("switch_agent", "blocked-openai", profile="openai"))
        assert response.status_code == 409
        sid = next(iter(client.app.state.records))
        for selection in [{"profile": "langgraph", "provider": "openai"},
                          {"profile": "crewai", "model": "openai/gpt-4.1-mini"}]:
            assert client.post(f"/v1/sessions/{sid}/runtime", headers=OWNER, json=selection).status_code == 409
        response = client.post(f"/v1/sessions/{sid}/runtime", headers=OWNER,
                               json={"profile": "langgraph", "provider": "google"})
        assert response.status_code == 200 and not calls


def test_mia_tools_auth_binding_switch_history_and_durable_retry_receipts(monkeypatch, tmp_path):
    calls = []
    ledger = tmp_path / "ledger.sqlite"
    with TestClient(make_app(monkeypatch, ledger, calls)) as client:
        functions = client.get("/v1/mia/functions", headers=OWNER).json()["agent"]["custom_functions"]
        assert {f["name"] for f in functions} == {"delegate_agent", "switch_agent", "interrupt_agent"}
        assert all(f["retries"] == 0 and f["headers"]["Authorization"] == "Bearer " + TOKEN for f in functions)
        body = payload("delegate_agent", "write-one", text="write")
        assert client.post("/mia/delegate_agent", json=body).status_code == 401
        assert client.post("/mia/delegate_agent", headers=[(b"authorization", b"Bearer\xff"), (b"x-bot-id", b"bot-one")], json=body).status_code == 401
        assert client.post("/mia/delegate_agent", headers={**HEADERS, "X-Bot-ID": "other"}, json=body).status_code == 403
        assert client.post("/mia/switch_agent", headers=HEADERS, json=body).status_code == 400
        first = client.post("/mia/delegate_agent", headers=HEADERS, json=body)
        assert first.status_code == 200 and first.json()["answer"] == "adk ready."
        assert client.post("/mia/delegate_agent", headers=HEADERS, json=body).json() == first.json()
        assert len([c for c in calls if c[0] == "write"]) == 1
        changed = payload("delegate_agent", "write-one", text="different")
        assert client.post("/mia/delegate_agent", headers=HEADERS, json=changed).status_code == 409
        for n, profile in enumerate(["langgraph", "crewai", "openai", "adk"]):
            switched = client.post("/mia/switch_agent", headers=HEADERS, json=payload("switch_agent", "switch-" + str(n), profile=profile))
            assert switched.status_code == 200 and switched.json()["profile"] == profile
            assert client.post("/mia/switch_agent", headers=HEADERS, json=payload("switch_agent", "switch-" + str(n), profile=profile)).json() == switched.json()
            answer = client.post("/mia/delegate_agent", headers=HEADERS, json=payload("delegate_agent", "ask-" + str(n), text="Remember the previous answer"))
            assert answer.json()["answer"] == profile + " ready."
            assert calls[-1][2]  # Shared history reaches the replacement runtime.
        assert len(client.app.state.records) == 1
        assert next(iter(client.app.state.records.values())).bot_id == "bot-one"
        assert len([c for c in calls if c[0] == "write"]) == 1
        assert client.get("/v1/mia/functions", headers=HEADERS).status_code == 401
        historical = payload("switch_agent", "switch-0", profile="langgraph")
        assert client.post("/mia/switch_agent", headers=HEADERS, json=historical).status_code == 409
    # The same MIA request after process restart returns its receipt, never a write.
    with TestClient(make_app(monkeypatch, ledger, calls)) as client:
        assert client.post("/mia/delegate_agent", headers=HEADERS, json=body).json() == first.json()
        assert not client.app.state.records
        assert len([c for c in calls if c[0] == "write"]) == 1
        latest = payload("switch_agent", "switch-3", profile="adk")
        assert client.post("/mia/switch_agent", headers=HEADERS, json=latest).status_code == 409


def test_switch_cancels_pending_mia_request_and_allows_next_turn(monkeypatch, tmp_path):
    calls = []
    with TestClient(make_app(monkeypatch, tmp_path / "ledger.sqlite", calls)) as client, ThreadPoolExecutor(max_workers=1) as pool:
        pending = pool.submit(client.post, "/mia/delegate_agent", headers=HEADERS,
                              json=payload("delegate_agent", "blocked", text="block"))
        deadline = time.monotonic() + 2
        while not calls and time.monotonic() < deadline:
            time.sleep(.01)
        assert calls
        response = client.post("/mia/switch_agent", headers=HEADERS,
                               json=payload("switch_agent", "switch-now", profile="langgraph"))
        assert response.status_code == 200
        assert pending.result(timeout=2).status_code == 409
        next_turn = client.post("/mia/delegate_agent", headers=HEADERS,
                                json=payload("delegate_agent", "after", text="Ready?"))
        assert next_turn.json()["answer"] == "langgraph ready."


def test_unknown_receipt_after_crash_is_not_replayed(monkeypatch, tmp_path):
    import hashlib
    import json
    import sqlite3

    calls = []
    with TestClient(make_app(monkeypatch, tmp_path / "ledger.sqlite", calls)) as client:
        db = sqlite3.connect(tmp_path / "ledger.sqlite")
        db.execute("CREATE TABLE mia_requests (bot TEXT, id TEXT, fingerprint TEXT, status INTEGER, result TEXT, PRIMARY KEY(bot,id))")
        body = payload("delegate_agent", "uncertain", text="write")
        fingerprint = hashlib.sha256(json.dumps([body["name"], body["args"]], sort_keys=True).encode()).hexdigest()
        db.execute("INSERT INTO mia_requests VALUES (?,?,?,?,?)", ("bot-one", "uncertain", fingerprint, 0, None))
        db.commit()
        db.close()
        response = client.post("/mia/delegate_agent", headers=HEADERS, json=body)
        assert response.status_code == 409 and not calls


def test_switch_receipt_cannot_match_a_new_session_with_same_runtime_revision(monkeypatch, tmp_path):
    with TestClient(make_app(monkeypatch, tmp_path / "ledger.sqlite", [])) as client:
        body = payload("switch_agent", "original-switch", profile="langgraph")
        original = client.post("/mia/switch_agent", headers=HEADERS, json=body).json()
        assert client.post(f"/v1/sessions/{original['session_id']}/stop", headers=OWNER).status_code == 200
        replacement = client.post("/mia/switch_agent", headers=HEADERS,
                                  json=payload("switch_agent", "replacement-switch", profile="langgraph")).json()
        assert replacement["session_id"] != original["session_id"]
        assert replacement["runtime_revision"] == original["runtime_revision"] == 1
        assert client.post("/mia/switch_agent", headers=HEADERS, json=body).status_code == 409


def test_switch_receipt_is_rejected_after_runtime_becomes_unsafe(monkeypatch, tmp_path):
    with TestClient(make_app(monkeypatch, tmp_path / "ledger.sqlite", [])) as client:
        body = payload("switch_agent", "switch-before-failed-cleanup", profile="langgraph")
        assert client.post("/mia/switch_agent", headers=HEADERS, json=body).status_code == 200
        next(iter(client.app.state.records.values())).host.poisoned = True
        response = client.post("/mia/switch_agent", headers=HEADERS, json=body)
        assert response.status_code == 409 and "requires restart" in response.json()["detail"]


async def test_cancelled_http_delegation_cleans_runtime_and_allows_followup(monkeypatch, tmp_path):
    import httpx

    calls = []
    entered = asyncio.Event()
    app = make_app(monkeypatch, tmp_path / "ledger.sqlite", calls, entered)
    async with app.router.lifespan_context(app), httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        pending = asyncio.create_task(client.post("/mia/delegate_agent", headers=HEADERS,
                                                  json=payload("delegate_agent", "cancelled-http", text="block")))
        await asyncio.wait_for(entered.wait(), 2)
        assert calls
        record = next(iter(app.state.records.values()))
        pending.cancel()
        try:
            await pending
        except asyncio.CancelledError:
            pass
        assert record.host.run.task.done() and not record.host.poisoned
        response = await client.post("/mia/delegate_agent", headers=HEADERS,
                                     json=payload("delegate_agent", "after-http-cancel", text="Ready?"))
        assert response.json()["answer"] == "adk ready."


async def test_interrupt_receipt_waits_for_cleanup_before_classifying_cancelled_turn(monkeypatch, tmp_path):
    import httpx

    entered, clearing, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
    calls = []
    app = make_app(monkeypatch, tmp_path / "ledger.sqlite", calls, entered)
    async with app.router.lifespan_context(app), httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        body = payload("delegate_agent", "pending-interrupt", text="block")
        pending = asyncio.create_task(client.post("/mia/delegate_agent", headers=HEADERS, json=body))
        await asyncio.wait_for(entered.wait(), 2)
        record = next(iter(app.state.records.values()))

        async def clear():
            clearing.set()
            await release.wait()

        record.output.interrupt = clear
        interrupt = asyncio.create_task(client.post("/mia/interrupt_agent", headers=HEADERS,
                                                   json=payload("interrupt_agent", "stop-pending")))
        try:
            await asyncio.wait_for(clearing.wait(), 2)
            await asyncio.sleep(.2)  # Delegation has observed the cancelled task; cleanup is still pending.
            assert record.host.run.task.done() and not pending.done()
        finally:
            release.set()
        stopped, cancelled = await asyncio.wait_for(asyncio.gather(interrupt, pending), 2)
        assert stopped.json()["state_safe"]
        assert cancelled.status_code == 409 and cancelled.json() == {"detail": {"code": "turn_cancelled"}}
        cached = await client.post("/mia/delegate_agent", headers=HEADERS, json=body)
        assert cached.json() == cancelled.json() and len(calls) == 1
        recovered = await client.post("/mia/delegate_agent", headers=HEADERS,
                                      json=payload("delegate_agent", "recover", text="Ready?"))
        assert recovered.json()["answer"] == "adk ready."
