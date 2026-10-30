"""One-window setup and lifecycle for the shared meeting bridge."""

import asyncio
import getpass
import importlib.metadata
import json
import logging
import os
import secrets
import signal
import socket
import sqlite3
import sys
import time
from contextlib import contextmanager
from pathlib import Path

import httpx
import uvicorn
from dotenv import dotenv_values, load_dotenv, set_key
from fastapi import HTTPException, Request
from meetstream import AsyncMeetStream

from .config import RUNTIME_PACKAGES, Settings, default_model, model_credential, validate_meeting_link
from .meetstream.lifecycle import remove_owned_bot
from .meetstream.protocol import meeting_ended
from .pilot import MiaPilot, PilotError
from .server import create_app


class SetupError(ValueError):
    """Messages safe to display without provider responses or credentials."""


@contextmanager
def instance_lock(directory):
    """One guided service per ledger; kernel locks also recover after a crash."""
    descriptor = os.open(directory / "instance.lock", os.O_RDWR | os.O_CREAT, 0o600)
    try:
        try:
            if os.name == "nt":
                import msvcrt
                if os.fstat(descriptor).st_size == 0:
                    os.write(descriptor, b"0")
                os.lseek(descriptor, 0, os.SEEK_SET)
                msvcrt.locking(descriptor, msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            raise SetupError("This project's assistant is already running. Stop its window before opening another.") from exc
        yield
    finally:
        os.close(descriptor)


def credential(name, path, interactive):
    value = os.getenv(name, "")
    if value:
        if name in {"ADAPTER_API_KEY", "MIA_BRIDGE_TOKEN"} and (len(value) < 32 or value == "replace-with-at-least-32-random-characters"):
            raise SetupError(f"{name} must be a random value of at least 32 characters")
        return value
    if name in {"ADAPTER_API_KEY", "MIA_BRIDGE_TOKEN"}:
        value = secrets.token_urlsafe(32)
    elif interactive:
        value = getpass.getpass(f"Paste {name} (hidden): ").strip()
    else:
        raise SetupError(f"Missing {name}; run interactive setup or set it in your environment")
    if not value or len(value) > 4096 or any(c in value for c in "\r\n\x00"):
        raise SetupError(f"Invalid {name}")
    if not path.exists():
        with os.fdopen(os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "w"):
            pass
    set_key(str(path), name, value)
    path.chmod(0o600)
    os.environ[name] = value
    return value


def setup(args):
    interactive = sys.stdin.isatty()
    env = Path(args.env_file or ".env")
    if env.exists():
        load_dotenv(env, override=False)
    mode = args.mode or "meeting"
    if mode not in {"demo", "meeting", "mia", "service"}:
        raise SetupError("Choose demo, meeting, mia or service")
    if mode == "demo":
        return mode, None, None
    state = Path(".adapter")
    state.mkdir(mode=0o700, exist_ok=True)
    preferences = state / "preferences.json"
    previous = json.loads(preferences.read_text()).get("runtime", "adk") if preferences.exists() else "adk"
    if mode in {"mia", "meeting"} and previous == "openai" and not getattr(args, "allow_openai", True):
        previous = "adk"
    profile = args.runtime or (input(f"Runtime: adk, langgraph, openai, crewai [{previous}]: ").strip() or previous
                               if interactive and mode not in {"mia", "meeting"} else previous)
    if profile not in RUNTIME_PACKAGES:
        raise SetupError("Choose adk, langgraph, openai or crewai")
    if profile == "openai" and not getattr(args, "allow_openai", True):
        raise SetupError("--runtime openai cannot be combined with --no-openai")
    provider = "openai" if profile == "openai" else "google"
    model = default_model(profile, provider)
    key = model_credential(Settings(profile=profile, model=args.model or model, model_provider=provider))
    try:
        importlib.metadata.version(RUNTIME_PACKAGES[profile])
    except importlib.metadata.PackageNotFoundError as exc:
        raise SetupError(f"Install the {profile} runtime extra before starting") from exc
    for name in ["ADAPTER_API_KEY", key, *(["MEETSTREAM_API_KEY"] if mode in {"mia", "meeting"} else []),
                 *(["DEEPGRAM_API_KEY"] if mode == "meeting" else [])]:
        credential(name, env, interactive)
    if mode == "mia":
        credential("MIA_BRIDGE_TOKEN", env, interactive)
    if mode in {"mia", "meeting"} and getattr(args, "allow_openai", True) and key != "OPENAI_API_KEY":
        credential("OPENAI_API_KEY", env, interactive)
    public_url = args.public_url or os.getenv("PUBLIC_BASE_URL", "")
    if mode in {"meeting", "mia"} and not public_url:
        credential("NGROK_AUTHTOKEN", env, interactive)
    meeting = args.meeting
    if mode in {"meeting", "mia"} and not meeting:
        if not interactive:
            raise SetupError("Supply --meeting with your hosted meeting URL")
        meeting = input("Paste your meeting URL: ").strip()
    if meeting:
        try:
            validate_meeting_link(meeting)
        except ValueError as exc:
            raise SetupError(str(exc)) from exc
    settings = Settings(profile=profile, model=args.model or model, model_provider=provider,
                        output="events" if mode in {"service", "mia"} else "voice_and_chat",
                        voice_switching=mode == "meeting",
                        transcript_source="meetstream" if mode in {"service", "mia"} else "deepgram",
                        wake_words=["okay assistant"], speech_provider="deepgram", speech_model="aura-2-thalia-en",
                        public_url=public_url, ledger=str(state / "actions.sqlite"))
    for name, value in {"AGENT_RUNTIME": profile, "AGENT_MODEL": settings.model,
                        "MODEL_PROVIDER": settings.model_provider}.items():
        set_key(str(env), name, value)
    env.chmod(0o600)
    temporary = preferences.with_suffix(".tmp")
    temporary.write_text(json.dumps({"runtime": profile, "env_file": str(env.resolve())}) + "\n")
    temporary.replace(preferences)
    return mode, settings, meeting


async def run(settings, meeting, port, *, mia=False, pilot_env=None, allow_openai=True):
    if meeting:
        validate_meeting_link(meeting)
    tunnel = None
    serving = None
    server = None
    session = None
    pilot = None
    stop_requested = asyncio.Event()
    state = Path(settings.ledger).parent / "running.json"
    loop = asyncio.get_running_loop()
    owner_task = asyncio.current_task()
    # Service managers use SIGTERM; run the same cleanup as Control+C.
    previous_term = signal.getsignal(signal.SIGTERM)
    handles_term = False
    # Prebind the actual socket: never stop another application or race a free-port probe.
    with instance_lock(Path(settings.ledger).parent), socket.socket() as listener:
        state.unlink(missing_ok=True)
        if os.name == "nt":
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
        else:
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(("127.0.0.1", port))
        listener.listen(128)
        actual_port = listener.getsockname()[1]
        origin = f"http://127.0.0.1:{actual_port}"
        try:
            if os.name != "nt":
                loop.add_signal_handler(signal.SIGTERM, owner_task.cancel)
                handles_term = True
            if (meeting or mia) and not settings.public_url:
                import ngrok

                async with asyncio.timeout(20):
                    tunnel = await ngrok.forward(f"127.0.0.1:{actual_port}", authtoken_from_env=True)
                settings = Settings.model_validate({**settings.model_dump(), "public_url": tunnel.url()})
            app = create_app(settings, google_pilot=not (allow_openai or settings.profile == "openai")) if mia or meeting else create_app(settings)
            stop_token = secrets.token_urlsafe(32)

            @app.post("/_local/stop", include_in_schema=False)
            async def shutdown(request: Request):
                supplied = request.headers.get("authorization", "").encode()
                if not secrets.compare_digest(supplied, ("Bearer " + stop_token).encode()):
                    raise HTTPException(401, "Unauthorized")
                stop_requested.set()
                server.should_exit = True
                return {"status": "stopping"}

            server = uvicorn.Server(uvicorn.Config(app, log_level="warning", access_log=False,
                                                  ws_max_size=262144, timeout_graceful_shutdown=20))
            # asyncio.run owns Control+C; uvicorn must not replace its cancellation handler.
            server.capture_signals = _no_signals
            serving = asyncio.create_task(server.serve(sockets=[listener]))
            async with asyncio.timeout(20):
                while not server.started:
                    if serving.done():
                        await serving
                        raise RuntimeError("The local service could not start")
                    await asyncio.sleep(.05)
            temporary = state.with_suffix(".tmp")
            with os.fdopen(os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600), "w") as output:
                json.dump({"port": actual_port, "token": stop_token}, output)
            temporary.chmod(0o600)
            temporary.replace(state)
            if stop_requested.is_set():
                return
            if meeting or mia:
                # Public health is unauthenticated. Never send the management key to the tunnel preflight.
                async with httpx.AsyncClient(timeout=10) as public:
                    # A newly opened tunnel can briefly serve its previous/absent
                    # upstream. Retry only our own tunnel, before creating any bot.
                    for attempt in range(4 if tunnel else 1):
                        if stop_requested.is_set():
                            return
                        try:
                            response = await public.get(settings.public_url + "/health")
                        except httpx.TransportError:
                            if stop_requested.is_set():
                                return
                            if not tunnel or attempt == 3:
                                raise
                        else:
                            if response.status_code not in {404, 502, 503, 504} or not tunnel or attempt == 3:
                                break
                        await asyncio.sleep(.5)
                    if stop_requested.is_set():
                        return
                    response.raise_for_status()
                    if response.json() != {"status": "ok", "version": "0.1.0"}:
                        raise SetupError("The public address does not reach this application")
            if stop_requested.is_set():
                return
            if mia or not meeting:
                if mia:
                    async with httpx.AsyncClient(base_url=origin, timeout=10, trust_env=False,
                                                 headers={"Authorization": "Bearer " + os.environ["ADAPTER_API_KEY"]}) as owner:
                        response = await owner.get("/v1/mia/functions")
                        response.raise_for_status()
                    path = Path(settings.ledger).parent / "mia-functions.json"
                    with os.fdopen(os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600), "w") as export:
                        json.dump(response.json(), export, indent=2)
                    path.chmod(0o600)
                    if stop_requested.is_set():
                        return
                    pilot = MiaPilot(state.parent, env_file=pilot_env)
                    # Finish an in-flight create before cleanup when Ctrl+C arrives;
                    # never abandon a request that may have created a remote agent.
                    async def register():
                        async with asyncio.timeout(60):
                            return await pilot.register(response.json()["agent"]["custom_functions"])

                    registering = asyncio.create_task(register())
                    try:
                        config_id = await asyncio.shield(registering)
                    finally:
                        if not registering.done():
                            await asyncio.gather(registering, return_exceptions=True)
                    if stop_requested.is_set():
                        return
                    print(f"Hosted MIA pilot ready (Google voice; {settings.profile} runtime). Agent: {pilot.state['agent_name']}. Configuration ID: {config_id}.", flush=True)
                    if meeting:
                        async def join():
                            async with asyncio.timeout(30):
                                return await pilot.join(meeting, waiting_room_timeout=settings.waiting_room_timeout)

                        joining = asyncio.create_task(join())
                        try:
                            await asyncio.shield(joining)
                        finally:
                            if not joining.done():
                                await asyncio.gather(joining, return_exceptions=True)
                        if stop_requested.is_set():
                            return
                        print("MIA is joining. Admit Adapter MIA Pilot if your meeting asks. Keep this window open; npm stop or Control+C makes it leave.", flush=True)
                        deadline = time.monotonic() + settings.waiting_room_timeout + 30
                        connected = False
                        while not serving.done() and not stop_requested.is_set():
                            status = await pilot.status()
                            if meeting_ended(status):
                                print("The MIA meeting ended.", flush=True)
                                return
                            if status.get('status') == 'InMeeting' or status.get('bot_status') == 'InMeeting':
                                if not connected:
                                    print("MIA connected. You can speak now.", flush=True)
                                connected = True
                            elif not connected and time.monotonic() > deadline:
                                raise PilotError("MIA was not admitted in time. The pilot is being stopped.")
                            await asyncio.sleep(2)
                        await serving
                        return
                else:
                    print(f"Reasoning service ready at {origin}. Use MIA custom functions or the events client; Control+C stops it.", flush=True)
                await asyncio.shield(serving)
                return
            async with httpx.AsyncClient(base_url=origin, timeout=30,
                                         trust_env=False,
                                         headers={"Authorization": "Bearer " + os.environ["ADAPTER_API_KEY"]}) as client:
                async def create_session():
                    nonlocal session
                    async with asyncio.timeout(30):
                        response = await client.post("/v1/sessions", json={"meeting_link": meeting})
                        response.raise_for_status()
                        session = response.json()["session_id"]

                # Finish tracking an in-flight create before cancellation cleanup,
                # as in hosted mode; do not reset its HTTP connection mid-response.
                creating = asyncio.create_task(create_session())
                try:
                    await asyncio.shield(creating)
                finally:
                    if not creating.done():
                        await asyncio.gather(creating, return_exceptions=True)
                print("Admit Adapter SDK Test in your meeting. This starts one assistant, with no scripted speaker.", flush=True)
                wake = settings.wake_words[0]
                print(f'Say "{wake}" before a request. Control+C stops the assistant and closes the tunnel.')
                choices = "LangGraph, CrewAI, OpenAI" if allow_openai or settings.profile == 'openai' else "LangGraph, CrewAI"
                print(f'Say "{wake}, switch to ADK" (or {choices}). The meeting chat confirms the selection.')
                print(f"Switch from a terminal in this project folder: uv run --frozen --all-extras --no-extra dev meetstream-adapter --env-file .env switch {session} --profile langgraph --provider google --server {origin}", flush=True)
                deadline = time.monotonic() + settings.waiting_room_timeout + 30
                connected = False
                while not serving.done() and not stop_requested.is_set():
                    try:
                        response = await client.get(f"/v1/sessions/{session}")
                    except httpx.HTTPError:
                        if stop_requested.is_set():
                            break
                        raise
                    if response.status_code == 404:
                        print("The meeting session ended.")
                        return
                    response.raise_for_status()
                    status = response.json()
                    if not status["runtime_state_safe"] or status["delivery_error"] or status["input_error"]:
                        raise RuntimeError("The meeting connection failed; the session is being stopped")
                    if status["control_connected"] and status["audio_connected"]:
                        if not connected:
                            print("Assistant connected. You can speak now.", flush=True)
                        connected = True
                    elif not connected and time.monotonic() > deadline:
                        raise TimeoutError("The bot was not admitted in time")
                    await asyncio.sleep(2)
                await serving
        finally:
            # App shutdown fences output, closes every owned session, and keeps any
            # pending platform exits in the durable ledger for the next startup.
            if session and server and not serving.done():
                try:
                    async with httpx.AsyncClient(base_url=origin, timeout=20,
                                                 trust_env=False,
                                                 headers={"Authorization": "Bearer " + os.environ["ADAPTER_API_KEY"]}) as client:
                        response = await client.post(f"/v1/sessions/{session}/stop")
                        if response.status_code == 202:
                            print("Bot exit is pending on MeetStream; its cleanup entry is saved.")
                except httpx.HTTPError:
                    print("Bot cleanup will be reconciled from the saved ledger.")
            if server:
                server.should_exit = True
            try:
                try:
                    if serving:
                        await serving
                finally:
                    try:
                        if pilot:
                            try:
                                async with asyncio.timeout(60):
                                    await pilot.cleanup()
                            except Exception:
                                print("Hosted MIA cleanup is pending; the ownership record is saved. Run npm stop again when MeetStream is reachable.")
                            finally:
                                try:
                                    await pilot.close()
                                except Exception:
                                    print("MeetStream client close failed; local bridge and tunnel cleanup will continue.")
                        if session and not mia:
                            try:
                                await cleanup_owned_resources(state.parent)
                            except Exception:
                                print("Bot departure is pending; keep .adapter and run npm stop when MeetStream is reachable.")
                        if mia:
                            (state.parent / "mia-functions.json").unlink(missing_ok=True)
                    finally:
                        if tunnel:
                            await tunnel.close()
            finally:
                state.unlink(missing_ok=True)
                if handles_term:
                    loop.remove_signal_handler(signal.SIGTERM)
                    signal.signal(signal.SIGTERM, previous_term)
            print("Local MIA pilot stopped." if mia else "Local assistant stopped.")


async def stop(directory=Path(".adapter")):
    """Stop this checkout's guided instance without signals or process-name matching."""
    state = directory / "running.json"
    if not directory.exists():
        print("This project's bridge is not running.")
        return
    try:
        with instance_lock(directory):
            state.unlink(missing_ok=True)
            await cleanup_owned_resources(directory)
            print("This project's bridge is not running.")
            return
    except SetupError:
        pass
    # Startup may still be preparing the tunnel. Wait for its local control endpoint.
    async with asyncio.timeout(150):
        while not state.exists():
            try:
                with instance_lock(directory):
                    await cleanup_owned_resources(directory)
                    print("This project's bridge is not running.")
                    return
            except SetupError:
                pass
            await asyncio.sleep(.1)
        instance = json.loads(state.read_text())
        port = instance["port"]
        if type(port) is not int or not 1 <= port <= 65535:
            raise SetupError("Invalid local bridge state; stop the startup terminal with Control+C")
        async with httpx.AsyncClient(base_url=f"http://127.0.0.1:{port}", trust_env=False, timeout=10) as client:
            response = await client.post("/_local/stop",
                                         headers={"Authorization": "Bearer " + instance["token"]})
            response.raise_for_status()
        while True:
            try:
                with instance_lock(directory):
                    await cleanup_owned_resources(directory)
                    break
            except SetupError:
                await asyncio.sleep(.1)
    print("Bridge stopped.")


async def cleanup_owned_resources(directory):
    ledger = directory / "actions.sqlite"
    if ledger.exists():
        with sqlite3.connect(ledger) as db:
            if db.execute("SELECT 1 FROM sqlite_master WHERE name='bot_cleanup'").fetchone():
                bots = db.execute("SELECT bot_id FROM bot_cleanup").fetchall()
                if bots:
                    preferences = directory / 'preferences.json'
                    env = json.loads(preferences.read_text()).get('env_file', '.env') if preferences.exists() else '.env'
                    key = os.getenv('MEETSTREAM_API_KEY') or dotenv_values(env).get('MEETSTREAM_API_KEY')
                    if not key:
                        raise SetupError('Restore MEETSTREAM_API_KEY to make this project\'s pending bots leave.')
                    api = AsyncMeetStream(api_key=key, timeout=10, max_retries=0)
                    try:
                        async with asyncio.timeout(50):
                            for (bot_id,) in bots:
                                while not await remove_owned_bot(lambda: api, db, bot_id):  # noqa: ASYNC110 -- remote departure polling
                                    await asyncio.sleep(1)
                    finally:
                        await api.aclose()
    path = directory / "mia-pilot.json"
    if not path.exists() or json.loads(path.read_text()).get("pending") is False:
        (directory / "mia-functions.json").unlink(missing_ok=True)
        return
    pilot = MiaPilot(directory)
    try:
        async with asyncio.timeout(60):
            await pilot.cleanup()
    finally:
        try:
            await pilot.close()
        finally:
            (directory / "mia-functions.json").unlink(missing_ok=True)


def _no_signals():
    from contextlib import nullcontext
    return nullcontext()


def start(args):
    for name in ["google.adk", "google.genai", "agents", "crewai", "langchain", "httpx", "ngrok"]:
        logging.getLogger(name).setLevel(logging.CRITICAL)
    try:
        mode, settings, meeting = setup(args)
        if mode == "demo":
            from .cli import offline_demo
            asyncio.run(offline_demo(friendly=True))
        else:
            asyncio.run(run(settings, meeting, args.port, mia=mode == "mia", pilot_env=getattr(args, 'env_file', None),
                            allow_openai=getattr(args, "allow_openai", True)))
    except (SetupError, PilotError) as exc:
        print(f"Setup: {exc}")
        return 1
    except asyncio.CancelledError:
        print("Stopped.")
    except Exception as exc:
        # Provider/tunnel exception text can contain credentials or request URLs.
        print(f"Assistant stopped ({type(exc).__name__}). Check account access, quota and network settings.")
        return 1
    return 0
