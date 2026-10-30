import asyncio
import json
import os
import socket
import subprocess
import sys
import time
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest
from dotenv import dotenv_values

from meetstream_agent_adapter import quickstart
from meetstream_agent_adapter.config import Settings
from meetstream_agent_adapter.server import create_app


async def test_project_stop_authentication_isolation_and_graceful_cleanup(monkeypatch, tmp_path):
    from contextlib import asynccontextmanager

    from fastapi import FastAPI

    stopped = asyncio.Event()

    @asynccontextmanager
    async def lifespan(app):
        yield
        stopped.set()

    monkeypatch.setattr(quickstart, "create_app", lambda settings: FastAPI(lifespan=lifespan))
    state = tmp_path / "running.json"
    # Recover stale state without ever contacting its address.
    state.write_text('{"port": 1, "token": "stale"}')
    settings = Settings(profile="fake", output="events", ledger=str(tmp_path / "ledger.sqlite"))
    pending = asyncio.create_task(quickstart.run(settings, None, 0))
    try:
        async with asyncio.timeout(3):
            while not state.exists() or json.loads(state.read_text()).get("token") == "stale":  # noqa: ASYNC110 -- cross-process file
                await asyncio.sleep(.01)
        instance = json.loads(state.read_text())
        if sys.platform != "win32":
            assert state.stat().st_mode & 0o777 == 0o600
        async with httpx.AsyncClient(trust_env=False) as client:
            response = await client.post(f"http://127.0.0.1:{instance['port']}/_local/stop")
            assert response.status_code == 401
        await quickstart.stop(tmp_path / "another-checkout")
        assert not pending.done()
        await quickstart.stop(tmp_path)
        await asyncio.wait_for(pending, 3)
        assert stopped.is_set() and not state.exists()
        await quickstart.stop(tmp_path)
        # POSIX reuses TIME_WAIT sockets; Windows keeps exclusive port ownership.
        if os.name != "nt":
            pending = asyncio.create_task(quickstart.run(settings, None, instance["port"]))
            async with asyncio.timeout(3):
                while not state.exists():  # noqa: ASYNC110 -- cross-process state
                    await asyncio.sleep(.01)
            await quickstart.stop(tmp_path)
            await asyncio.wait_for(pending, 3)
    finally:
        if not pending.done():
            pending.cancel()
            with pytest.raises(asyncio.CancelledError):
                await pending


@pytest.mark.skipif(os.name == "nt", reason="POSIX service-manager termination; Windows uses console stop")
def test_sigterm_runs_graceful_project_cleanup(tmp_path):
    source = str(Path(quickstart.__file__).resolve().parents[1])
    code = '''
from pathlib import Path
from types import SimpleNamespace
from meetstream_agent_adapter import quickstart
from meetstream_agent_adapter.config import Settings
Path('.adapter').mkdir()
quickstart.setup = lambda args: ('service', Settings(profile='fake', output='events', ledger='.adapter/actions.sqlite'), None)
raise SystemExit(quickstart.start(SimpleNamespace(port=0)))
'''
    process = subprocess.Popen([sys.executable, "-u", "-c", code], cwd=tmp_path,
                               env={**os.environ, "PYTHONPATH": source, "ADAPTER_API_KEY": "test-termination-" + "a" * 32},
                               stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    try:
        deadline = time.monotonic() + 10
        while not (tmp_path / ".adapter/running.json").exists():
            assert process.poll() is None and time.monotonic() < deadline
            time.sleep(.05)
        process.terminate()
        output, _ = process.communicate(timeout=10)
        assert process.returncode == 0 and "Local assistant stopped." in output and "Traceback" not in output
        assert not (tmp_path / ".adapter/running.json").exists()
    finally:
        if process.poll() is None:
            process.kill()
            process.communicate()


def test_instance_lock_blocks_second_service_and_releases_after_exit(tmp_path):
    with quickstart.instance_lock(tmp_path), pytest.raises(quickstart.SetupError, match="already running"):
        with quickstart.instance_lock(tmp_path):
            pytest.fail("A second service must not use this ledger")
    with quickstart.instance_lock(tmp_path):
        pass


async def test_stalled_tunnel_startup_times_out_and_releases_project_lock(monkeypatch, tmp_path):
    cleaned = asyncio.Event()

    async def forward(*args, **kwargs):
        try:
            await asyncio.Event().wait()
        finally:
            cleaned.set()

    timeout = asyncio.timeout
    monkeypatch.setitem(sys.modules, "ngrok", SimpleNamespace(forward=forward))
    monkeypatch.setattr(quickstart.asyncio, "timeout", lambda seconds: timeout(.05 if seconds == 20 else seconds))
    settings = Settings(profile="fake", output="events", ledger=str(tmp_path / "ledger.sqlite"))
    with pytest.raises(TimeoutError):
        await quickstart.run(settings, None, 0, mia=True)
    assert cleaned.is_set() and not (tmp_path / "running.json").exists()
    with quickstart.instance_lock(tmp_path):
        pass


def test_start_reports_provider_failure_without_private_details(monkeypatch, tmp_path, capsys):
    async def broken(*args, **kwargs):
        raise RuntimeError("private credential or provider request URL")

    settings = Settings(profile="fake", ledger=str(tmp_path / "ledger.sqlite"))
    monkeypatch.setattr(quickstart, "setup", lambda args: ("meeting", settings, "https://meet.google.com/test"))
    monkeypatch.setattr(quickstart, "run", broken)
    assert quickstart.start(SimpleNamespace(port=0)) == 1
    output = capsys.readouterr().out
    assert "RuntimeError" in output and "private credential" not in output


def test_guided_setup_preserves_existing_values_and_hides_new_keys(monkeypatch, tmp_path, capsys):
    monkeypatch.chdir(tmp_path)
    for name in ["ADAPTER_API_KEY", "GOOGLE_API_KEY", "MEETSTREAM_API_KEY", "DEEPGRAM_API_KEY", "PUBLIC_BASE_URL"]:
        monkeypatch.delenv(name, raising=False)
    secret = "fixture-secret-with-'quotes'"
    (tmp_path / ".env").write_text("KEEP_THIS=existing\n")
    monkeypatch.setattr(sys, "stdin", SimpleNamespace(isatty=lambda: True))
    monkeypatch.setattr(quickstart.getpass, "getpass", lambda prompt: secret)
    monkeypatch.setattr(quickstart.importlib.metadata, "version", lambda name: "fixture")
    args = SimpleNamespace(env_file=None, mode="meeting", runtime="langgraph", public_url="https://adapter.example",
                           meeting="https://meet.google.com/abc-defg-hij", model=None)
    mode, settings, meeting = quickstart.setup(args)
    values = dotenv_values(".env")
    assert mode == "meeting" and settings.profile == "langgraph" and settings.model_provider == "google"
    assert meeting == args.meeting and values["KEEP_THIS"] == "existing"
    assert len(values["ADAPTER_API_KEY"]) >= 32 and values["GOOGLE_API_KEY"] == secret
    assert values["DEEPGRAM_API_KEY"] == secret and secret not in capsys.readouterr().out
    if sys.platform != "win32":
        assert (tmp_path / ".env").stat().st_mode & 0o777 == 0o600


def test_hosted_mia_setup_generates_separate_tool_token_without_media_credentials(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    for name in ["ADAPTER_API_KEY", "MIA_BRIDGE_TOKEN", "GOOGLE_API_KEY", "MEETSTREAM_API_KEY", "DEEPGRAM_API_KEY"]:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("OPENAI_API_KEY", "fixture-openai-key")
    monkeypatch.setenv("GOOGLE_API_KEY", "fixture-google-key")
    monkeypatch.setenv("MEETSTREAM_API_KEY", "fixture-meetstream-key")
    monkeypatch.setattr(sys, "stdin", SimpleNamespace(isatty=lambda: False))
    monkeypatch.setattr(quickstart.importlib.metadata, "version", lambda name: "fixture")
    args = SimpleNamespace(env_file=None, mode="mia", runtime="adk", public_url="https://bridge.example", meeting="https://meet.google.com/abc-defg-hij", model=None)
    mode, settings, meeting = quickstart.setup(args)
    assert mode == "mia" and meeting == args.meeting and settings.output == "events"
    values = dotenv_values(".env")
    assert len(values["MIA_BRIDGE_TOKEN"]) >= 32 and values["MIA_BRIDGE_TOKEN"] != values["ADAPTER_API_KEY"]
    assert "MEETSTREAM_API_KEY" not in values and "DEEPGRAM_API_KEY" not in values


def test_default_start_remembers_openai_and_explicit_opt_out_uses_adk(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".adapter").mkdir()
    (tmp_path / ".adapter/preferences.json").write_text('{"runtime":"openai"}')
    for name in ["ADAPTER_API_KEY", "MIA_BRIDGE_TOKEN", "GOOGLE_API_KEY", "OPENAI_API_KEY", "MEETSTREAM_API_KEY", "DEEPGRAM_API_KEY"]:
        monkeypatch.setenv(name, "fixture-" + name + "a" * 40)
    monkeypatch.setattr(sys, "stdin", SimpleNamespace(isatty=lambda: False))
    monkeypatch.setattr(quickstart.importlib.metadata, "version", lambda name: "fixture")
    args = SimpleNamespace(env_file=None, mode=None, runtime=None, public_url="https://bridge.example", meeting="https://meet.google.com/abc-defg-hij", model=None)
    mode, settings, _ = quickstart.setup(args)
    assert mode == "meeting" and settings.profile == "openai"
    args.allow_openai = False
    mode, settings, _ = quickstart.setup(args)
    assert settings.profile == "adk" and settings.model_provider == "google"
    args.runtime = "openai"
    with pytest.raises(quickstart.SetupError, match="cannot be combined"):
        quickstart.setup(args)
    args.allow_openai = True
    mode, settings, _ = quickstart.setup(args)
    assert mode == "meeting" and settings.profile == "openai" and settings.model_provider == "openai"
    assert settings.model == "gpt-4.1-mini"


@pytest.mark.parametrize("preflight_ok,stop_method", [(True, "cancel"), (True, "command"), (False, "cancel"), ("transient", "command")])
async def test_one_window_start_preflights_and_closes_single_owned_bot_and_tunnel(monkeypatch, tmp_path, preflight_ok, stop_method):
    monkeypatch.setenv("ADAPTER_API_KEY", "a" * 40)
    created, stopped = [], []
    bot_created = asyncio.Event()
    creation_finished = asyncio.Event()
    closed = []

    async def create(payload, **kwargs):
        created.append(payload)
        bot_created.set()
        await asyncio.sleep(.05)  # Cancellation arrives before the create response.
        creation_finished.set()
        return {"bot_id": "owned"}

    async def status(bot_id):
        return {"status": "Done" if stopped else "InMeeting"}

    async def remove(bot_id):
        assert creation_finished.is_set()
        stopped.append(bot_id)

    class Tunnel:
        def url(self):
            return "https://adapter.example"

        async def close(self):
            closed.append("tunnel")

    async def forward(address, **kwargs):
        assert address.startswith("127.0.0.1:") and kwargs == {"authtoken_from_env": True}
        return Tunnel()

    monkeypatch.setitem(sys.modules, "ngrok", SimpleNamespace(forward=forward))
    api = SimpleNamespace(bots=SimpleNamespace(create=create, status=status, remove=remove))
    monkeypatch.setattr(quickstart, "create_app", lambda settings, **kwargs: create_app(settings, api_client=api, **kwargs))
    client_type = httpx.AsyncClient
    bound_port = None
    health_attempts = 0

    def public_health(request):
        nonlocal health_attempts
        health_attempts += 1
        assert "authorization" not in request.headers and request.url.path == "/health"
        if preflight_ok == "transient" and health_attempts == 1:
            return httpx.Response(404)
        return httpx.Response(200 if preflight_ok else 502, json={"status": "ok", "version": "0.1.0"})

    def client(**kwargs):
        nonlocal bound_port
        if "base_url" not in kwargs:
            kwargs["transport"] = httpx.MockTransport(public_health)
        else:
            bound_port = httpx.URL(kwargs["base_url"]).port
        return client_type(**kwargs)

    monkeypatch.setattr(quickstart.httpx, "AsyncClient", client)
    settings = Settings(profile="fake", model="offline", output="chat", ledger=str(tmp_path / "ledger.sqlite"))
    task = asyncio.create_task(quickstart.run(settings, "https://meet.google.com/abc-defg-hij", 0))
    if preflight_ok:
        await asyncio.wait_for(bot_created.wait(), 3)
        if stop_method == "cancel":
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(task, 3)
        else:
            await quickstart.stop(tmp_path)
            await asyncio.wait_for(task, 3)
        assert len(created) == 1 and created[0]["bot_name"] == "Adapter SDK Test"
        assert stopped == ["owned"]
        with socket.socket() as probe:
            probe.settimeout(.5)
            assert probe.connect_ex(("127.0.0.1", bound_port)) != 0
    else:
        with pytest.raises(httpx.HTTPStatusError):
            await asyncio.wait_for(task, 3)
        assert not created and not stopped
    assert closed == ["tunnel"]
    assert health_attempts == (2 if preflight_ok == "transient" else 1 if preflight_ok else 4)


async def test_service_cancellation_runs_graceful_lifespan_cleanup(monkeypatch, tmp_path):
    from contextlib import asynccontextmanager

    from fastapi import FastAPI

    ready, stopped = asyncio.Event(), asyncio.Event()

    def observe(*values, **kwargs):
        if values and str(values[0]).startswith("Reasoning service ready"):
            ready.set()
        print(*values, **kwargs)

    @asynccontextmanager
    async def lifespan(app):
        yield
        stopped.set()

    monkeypatch.setattr(quickstart, "create_app", lambda settings: FastAPI(lifespan=lifespan))
    monkeypatch.setattr(quickstart, "print", observe, raising=False)
    settings = Settings(profile="fake", output="events", ledger=str(tmp_path / "ledger.sqlite"))
    task = asyncio.create_task(quickstart.run(settings, None, 0))
    await asyncio.wait_for(ready.wait(), 2)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, 2)
    assert stopped.is_set()


@pytest.mark.parametrize("mia", [False, True])
@pytest.mark.parametrize("response_status", [200, 502, "disconnect"])
async def test_stop_during_public_preflight_does_not_launch_or_export(monkeypatch, tmp_path, mia, response_status):
    from fastapi import FastAPI

    entered, release = asyncio.Event(), asyncio.Event()
    monkeypatch.setenv("ADAPTER_API_KEY", "a" * 40)
    monkeypatch.setattr(quickstart, "create_app", lambda settings, **kwargs: FastAPI())
    client = httpx.AsyncClient

    async def public(request):
        entered.set()
        await release.wait()
        if response_status == "disconnect":
            raise httpx.ConnectError("Synthetic preflight connection failure", request=request)
        return httpx.Response(response_status, json={"status": "ok", "version": "0.1.0"})

    def clients(**kwargs):
        if "base_url" not in kwargs:
            kwargs["transport"] = httpx.MockTransport(public)
        return client(**kwargs)

    monkeypatch.setattr(quickstart.httpx, "AsyncClient", clients)
    settings = Settings(profile="fake", output="events" if mia else "chat", public_url="https://bridge.example",
                        ledger=str(tmp_path / "ledger.sqlite"))
    pending = asyncio.create_task(quickstart.run(settings, None if mia else "https://meet.google.com/test", 0, mia=mia))
    try:
        await asyncio.wait_for(entered.wait(), 2)
        state = json.loads((tmp_path / "running.json").read_text())
        async with client(base_url=f"http://127.0.0.1:{state['port']}", trust_env=False) as local:
            response = await local.post("/_local/stop", headers={"Authorization": "Bearer " + state["token"]})
            assert response.status_code == 200
        release.set()
        await asyncio.wait_for(pending, 3)
        assert not (tmp_path / "mia-functions.json").exists() and not (tmp_path / "running.json").exists()
    finally:
        release.set()
        if not pending.done():
            pending.cancel()
            with pytest.raises(asyncio.CancelledError):
                await pending


@pytest.mark.parametrize('profile,allow_openai,google_only', [('fake', False, True), ('fake', None, False), ('openai', False, False)])
async def test_hosted_mia_start_exports_private_configuration_and_shuts_down(monkeypatch, tmp_path, profile, allow_openai, google_only):
    import json

    monkeypatch.setenv("ADAPTER_API_KEY", "a" * 40)
    monkeypatch.setenv("MIA_BRIDGE_TOKEN", "b" * 40)
    monkeypatch.setenv("MEETSTREAM_API_KEY", "fixture-meetstream-key")
    from test_pilot import Cloud
    cloud = Cloud()
    ready = asyncio.Event()
    def observe(*values, **kwargs):
        if values and str(values[0]).startswith("Hosted MIA pilot ready"):
            ready.set()
    monkeypatch.setattr(quickstart, "print", observe, raising=False)
    client = httpx.AsyncClient
    def public(request):
        assert "authorization" not in request.headers
        return httpx.Response(200, json={"status": "ok", "version": "0.1.0"})
    def clients(**kwargs):
        if kwargs.get("base_url") == "https://api.meetstream.ai":
            kwargs["transport"] = httpx.MockTransport(cloud.request)
        elif "base_url" not in kwargs:
            kwargs["transport"] = httpx.MockTransport(public)
        return client(**kwargs)
    monkeypatch.setattr(quickstart.httpx, "AsyncClient", clients)
    settings = Settings(profile=profile, output="events", public_url="https://bridge.example", ledger=str(tmp_path / "ledger.sqlite"))
    pending = asyncio.create_task(quickstart.run(settings, None, 0, mia=True, **({} if allow_openai is None else {"allow_openai": allow_openai})))
    await asyncio.wait_for(ready.wait(), 2)
    path = tmp_path / "mia-functions.json"
    config = json.loads(path.read_text())
    assert len(config["agent"]["custom_functions"]) == 3
    profiles = config["agent"]["custom_functions"][1]["parameters"]["properties"]["profile"]["enum"]
    assert ('openai' not in profiles) is google_only
    assert ('Never use OpenAI.' in cloud.creates[0]['model']['system_prompt']) is google_only
    assert all(f["url"].startswith("https://bridge.example/mia/") for f in config["agent"]["custom_functions"])
    if sys.platform != "win32":
        assert path.stat().st_mode & 0o777 == 0o600
    pending.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(pending, 2)
    assert list(cloud.configs) == ["unrelated"] and len(cloud.deletes) == 1
    assert not path.exists()


@pytest.mark.parametrize('link', ['http://meet.google.com/abc', 'https://meet.google.com.evil.test/abc', 'https://user@meet.google.com/abc', 'https://zoom.us:123/j/123', 'https://meet.google.com/'])
def test_invalid_meeting_link_rejected_before_cloud_setup(link):
    with pytest.raises(ValueError, match='valid HTTPS'):
        quickstart.validate_meeting_link(link)


def test_default_start_prompts_for_meeting_link(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    for key in ['MEETSTREAM_API_KEY', 'ADAPTER_API_KEY', 'MIA_BRIDGE_TOKEN', 'GOOGLE_API_KEY', 'DEEPGRAM_API_KEY', 'OPENAI_API_KEY']:
        monkeypatch.setenv(key, 'fixture-' + 'a' * 40)
    monkeypatch.setattr(sys, 'stdin', SimpleNamespace(isatty=lambda: True))
    monkeypatch.setattr(quickstart.importlib.metadata, 'version', lambda name: 'fixture')
    prompts = []
    def answer(prompt):
        prompts.append(prompt)
        return 'https://meet.google.com/abc-defg-hij'
    monkeypatch.setattr('builtins.input', answer)
    args = SimpleNamespace(env_file=None, mode=None, runtime=None, public_url='https://bridge.example', meeting=None, model=None)
    mode, _, meeting = quickstart.setup(args)
    assert mode == 'meeting' and meeting == 'https://meet.google.com/abc-defg-hij'
    assert prompts == ['Paste your meeting URL: ']
    monkeypatch.setattr(sys, 'stdin', SimpleNamespace(isatty=lambda: False))
    with pytest.raises(quickstart.SetupError, match='--meeting'):
        quickstart.setup(args)


def test_default_openai_switching_requires_its_own_credential(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv('OPENAI_API_KEY', raising=False)
    for name in ['ADAPTER_API_KEY', 'MIA_BRIDGE_TOKEN', 'GOOGLE_API_KEY', 'MEETSTREAM_API_KEY', 'DEEPGRAM_API_KEY']:
        monkeypatch.setenv(name, name + 'a' * 40)
    monkeypatch.setattr(sys, 'stdin', SimpleNamespace(isatty=lambda: False))
    monkeypatch.setattr(quickstart.importlib.metadata, 'version', lambda name: 'fixture')
    args = SimpleNamespace(env_file=None, mode=None, runtime='adk',
                           public_url='https://bridge.example', meeting='https://meet.google.com/abc-defg-hij', model=None)
    with pytest.raises(quickstart.SetupError, match='Missing OPENAI_API_KEY'):
        quickstart.setup(args)
    monkeypatch.setenv('OPENAI_API_KEY', 'fixture-openai')
    mode, settings, _ = quickstart.setup(args)
    assert mode == 'meeting' and settings.profile == 'adk' and settings.model_provider == 'google'


def test_start_enables_openai_by_default(monkeypatch, tmp_path):
    settings = Settings(profile='fake', output='events', ledger=str(tmp_path / 'ledger.sqlite'))
    monkeypatch.setattr(quickstart, 'setup', lambda args: ('mia', settings, 'https://meet.google.com/test'))
    seen = []
    async def run(config, meeting, port, **kwargs):
        seen.append(kwargs)
    monkeypatch.setattr(quickstart, 'run', run)
    assert quickstart.start(SimpleNamespace(port=0, env_file=None)) == 0
    assert seen == [{'mia': True, 'pilot_env': None, 'allow_openai': True}]


async def test_stop_after_streaming_pilot_crash_reconciles_only_ledger_owned_bots(monkeypatch, tmp_path):
    import sqlite3

    monkeypatch.delenv('MEETSTREAM_API_KEY', raising=False)
    env = tmp_path / 'private.env'
    env.write_text('MEETSTREAM_API_KEY=fixture-account\n')
    (tmp_path / 'preferences.json').write_text(json.dumps({'env_file': str(env)}))
    with sqlite3.connect(tmp_path / 'actions.sqlite') as db:
        db.execute('CREATE TABLE bot_cleanup(bot_id TEXT PRIMARY KEY, expires REAL)')
        db.execute('INSERT INTO bot_cleanup VALUES (?,?)', ('owned-bot', 0))
        db.execute('CREATE TABLE preserved(value TEXT)')
        db.execute("INSERT INTO preserved VALUES ('keep')")
    calls, states = [], iter(['InMeeting', 'InMeeting', 'Done'])
    async def status(bot_id):
        assert bot_id == 'owned-bot'
        return {'status': next(states)}
    async def remove(bot_id):
        calls.append(bot_id)
    async def close():
        calls.append('closed')
    def client(**kwargs):
        assert kwargs['api_key'] == 'fixture-account' and kwargs['max_retries'] == 0
        return SimpleNamespace(bots=SimpleNamespace(status=status, remove=remove), aclose=close)
    monkeypatch.setattr(quickstart, 'AsyncMeetStream', client)
    await quickstart.stop(tmp_path)
    with sqlite3.connect(tmp_path / 'actions.sqlite') as db:
        assert db.execute('SELECT COUNT(*) FROM bot_cleanup').fetchone()[0] == 0
        assert db.execute('SELECT value FROM preserved').fetchone()[0] == 'keep'
    assert calls == ['owned-bot', 'closed']
    await quickstart.stop(tmp_path)  # Confirmed cleanup is offline-safe.
    assert calls == ['owned-bot', 'closed']
