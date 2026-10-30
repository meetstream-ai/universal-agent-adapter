"""Authenticated common bridge. Launches only public MeetStream bot operations."""

import asyncio
import hashlib
import hmac
import json
import os
import secrets
import time
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass, field
from datetime import datetime, timezone
from uuid import uuid4

from fastapi import Depends, FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import StreamingResponse
from meetstream import AsyncMeetStream
from pydantic import Field

from .adapters import load_adapter
from .config import (
    RUNTIME_PACKAGES,
    RuntimeSelection,
    Settings,
    adapter_config,
    model_credential,
    runtime_provider,
    validate_meeting_link,
)
from .contract import AdapterError, SessionContext, StrictModel, Turn
from .demo import DEMO_INSTRUCTIONS, demo_tools, initialize_demo
from .host import SessionHost
from .meetstream.lifecycle import maintain_sessions, remove_owned_bot
from .meetstream.output import MeetingOutput, create_speech
from .meetstream.protocol import (
    SpeechGate,
    TurnPolicy,
    acknowledgement,
    legacy_audio,
    meeting_ended,
    runtime_from_speech,
    runtime_switch_request,
    transcript_turn,
)
from .meetstream.transcription import DeepgramInput
from .mia import install_mia_tools, mia_functions
from .tools import ToolExecutor, execution


class SessionRequest(StrictModel):
    meeting_link: str | None = Field(default=None, max_length=2048)
    bot_id: str | None = Field(default=None, min_length=1, max_length=128)


@dataclass
class Record:
    id: str
    token: str
    expires: float
    created_bot: bool = False
    bot_id: str | None = None
    host: SessionHost | None = None
    output: MeetingOutput | None = None
    ready: asyncio.Event = field(default_factory=asyncio.Event)
    inputs: asyncio.Queue = field(default_factory=lambda: asyncio.Queue(maxsize=32))
    input_worker: asyncio.Task | None = None
    grace_task: asyncio.Task | None = None
    lost_channels: set[str] = field(default_factory=set)
    policy: TurnPolicy | None = None
    audio_socket: WebSocket | None = None
    closed: bool = False
    input_error: str | None = None
    lifecycle: str = "created"
    speech_gate: SpeechGate = field(default_factory=SpeechGate)
    seen_inputs: set[str] = field(default_factory=set)
    transcription: DeepgramInput | None = None
    runtime_settings: Settings | None = None
    operation_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    input_epoch: int = 0


def create_app(settings: Settings, *, api_client=None, adapter=None, adapter_factory=None, speech_factory=None, tool_specs=None, google_pilot=False):
    key = os.environ.get("ADAPTER_API_KEY", "")
    if len(key) < 32 or key == "replace-with-at-least-32-random-characters":
        raise ValueError("ADAPTER_API_KEY must be a random value of at least 32 characters, not the template")
    records: dict[str, Record] = {}
    token_index: dict[str, Record] = {}
    client = api_client
    owns_client = api_client is None
    executor = None
    closing = False
    creation_lock = asyncio.Lock()
    mia_session_lock = asyncio.Lock()
    mia_sessions = {}

    async def runtime_adapter(selected):
        if google_pilot and runtime_provider(selected) not in {"google", "offline"}:
            raise AdapterError("runtime_unavailable", "This local MIA pilot permits Google-backed runtimes only")
        if adapter_factory is None:
            credential = model_credential(selected)
            if credential and not os.getenv(credential):
                raise AdapterError("authentication_failed", "Model credentials are missing")
        config = adapter_config(selected, instructions=DEMO_INSTRUCTIONS if tool_specs is None else None)
        if adapter_factory:
            return adapter_factory(selected.profile, **config)
        # First imports can be slow; keep meeting/control I/O responsive.
        async with asyncio.timeout(10):
            return await asyncio.to_thread(load_adapter, selected.profile, **config)

    def api():
        nonlocal client
        if client is None:
            client = AsyncMeetStream(timeout=15, max_retries=0)
        return client

    async def send_chat(session_id, message):
        record = records[session_id]
        active = execution.get().active
        await record.output.send({"command": "sendmsg", "message": message, "msg": message}, active)
        if not active():
            raise asyncio.CancelledError
        record.output.chat_messages += 1

    specs = list(tool_specs) if tool_specs is not None else demo_tools(lambda: executor, send_chat)
    if tool_specs is None and settings.output == "events":
        # A caller-owned MIA pipeline consumes SSE and owns meeting delivery.
        specs = [s for s in specs if s.name != "send_meeting_chat"]

    async def close_record(record, remove=False):
        if record.closed:
            return
        removal_pending = False
        record.closed = True
        record.ready.set()
        current = asyncio.current_task()
        for task in [record.input_worker, record.grace_task]:
            if task and task is not current:
                task.cancel()
                with suppress(asyncio.CancelledError):
                    await task
        try:
            if record.transcription:
                await record.transcription.close()
            if record.host:
                await record.host.close()
            elif record.output:
                await record.output.close()
        finally:
            token_index.pop(record.token, None)
            try:
                # Request departure while the meeting transport is still alive;
                # local socket closure is not proof of remote departure.
                if remove and record.created_bot and record.bot_id:
                    executor.db.execute("UPDATE bot_cleanup SET expires=0 WHERE bot_id=?", (record.bot_id,))
                    executor.db.commit()
                    try:
                        removal_pending = not await remove_owned_bot(api, executor.db, record.bot_id)
                    except Exception as exc:
                        raise HTTPException(502, "Session closed; bot removal pending reconciliation") from exc
            finally:
                for socket in [record.audio_socket, record.output.socket if record.output else None]:
                    if socket:
                        with suppress(Exception):
                            await socket.close(code=1000)
                records.pop(record.id, None)
        return removal_pending

    @asynccontextmanager
    async def lifespan(app):
        nonlocal closing, executor
        executor = ToolExecutor(specs, {s.name for s in specs}, settings.ledger)
        if tool_specs is None:
            initialize_demo(executor)
        executor.db.execute("CREATE TABLE IF NOT EXISTS bot_cleanup (bot_id TEXT PRIMARY KEY, expires REAL)")
        executor.db.commit()
        app.state.executor = executor
        task = asyncio.create_task(maintain_sessions(records, executor.db, api, close_record))
        try:
            yield
        finally:
            closing = True
            task.cancel()
            try:
                with suppress(asyncio.CancelledError, Exception):
                    await task
                for record in list(records.values()):
                    with suppress(Exception):
                        await close_record(record, remove=True)
                if owns_client and client:
                    await client.aclose()
            finally:
                executor.close()

    app = FastAPI(title="MeetStream Agent Adapter", lifespan=lifespan, docs_url=None, redoc_url=None)
    app.state.records = records

    @app.middleware("http")
    async def body_limit(request, call_next):
        # Limit management input as well as webhook input before JSON decoding.
        from fastapi.responses import JSONResponse
        size = 0
        chunks = []
        async for chunk in request.stream():
            size += len(chunk)
            if size > 65536:
                return JSONResponse({"detail": "Payload too large"}, status_code=413)
            chunks.append(chunk)
        request._body = b"".join(chunks)
        return await call_next(request)

    async def authorize(request: Request):
        supplied = request.headers.get("authorization", "")
        if not hmac.compare_digest(supplied.encode(), ("Bearer " + key).encode()):
            raise HTTPException(401, "Invalid management authentication")

    async def bounded_json(request):
        # body_limit already bounded and cached the exact signed bytes.
        raw = await request.body()
        try:
            data = json.loads(raw)
        except ValueError as exc:
            raise HTTPException(400, "Invalid JSON") from exc
        if not isinstance(data, dict):
            raise HTTPException(400, "JSON object required")
        return data, raw

    def get_record(session_id):
        record = records.get(session_id)
        if not record or record.closed:
            raise HTTPException(404, "Session not found")
        return record

    async def authenticated_record(token):
        record = token_index.get(token)
        if not record or record.closed or time.time() > record.expires:
            raise HTTPException(401, "Invalid or expired session token")
        try:
            async with asyncio.timeout(10):
                await record.ready.wait()
        except TimeoutError as exc:
            raise HTTPException(503, "Bot binding is not ready") from exc
        if record.closed or not record.bot_id:
            raise HTTPException(410, "Session is closed")
        return record

    def snapshot(record):
        selected = record.runtime_settings
        return {"session_id": record.id, "bot_id": record.bot_id, "profile": record.host.context.profile,
                "model": selected.model, "model_provider": runtime_provider(selected),
                "runtime_revision": record.host.metrics["runtime_switches"],
                "lifecycle": record.lifecycle, "control_connected": bool(record.output.socket),
                "audio_connected": bool(record.audio_socket), "metrics": dict(record.host.metrics),
                "delivery": record.output.delivery, "delivery_error": record.output.error,
                "delivery_error_details": record.output.error_details,
                "audio_frames": record.output.audio_frames, "submitted_audio_seconds": record.output.submitted_audio_seconds,
                "chat_messages": record.output.chat_messages, "input_error": record.input_error,
                "runtime_state_safe": not record.host.poisoned}

    async def input_loop(record):
        while True:
            epoch, turn = await record.inputs.get()
            profile = None
            switch_requested = False
            try:
                if turn.event_id in record.seen_inputs:
                    continue
                if len(record.seen_inputs) >= 1024:
                    raise AdapterError("deadline_exceeded", "Session input limit reached")
                record.seen_inputs.add(turn.event_id)
                async with record.operation_lock:
                    if epoch != record.input_epoch:
                        record.host.metrics["switch_inputs_discarded"] += 1
                        continue
                    if turn.timestamp < record.host.last_timestamp:
                        continue
                    # MIA already owns acceptance for events/tool integration.
                    accepted = turn if settings.output == "events" else record.policy.apply(turn)
                    if accepted:
                        record.host.history.extend(record.policy.background)
                        record.policy.background.clear()
                        switch_requested = (settings.voice_switching and settings.output != "events"
                                            and runtime_switch_request(accepted.text) is not None)
                        profile = runtime_from_speech(accepted.text) if switch_requested else None
                        if not switch_requested:
                            await record.host.accept(accepted)
                if accepted and switch_requested:
                    try:
                        if profile:
                            await switch_record(record, RuntimeSelection(profile=profile))
                            message = "External runtime switched to " + profile + "."
                            record.host.history.append({"role": "user", "content": "[" + message + "]"})
                        else:
                            options = "Google ADK, LangGraph or CrewAI" if google_pilot else "Google ADK, LangGraph, CrewAI or OpenAI"
                            message = ("Runtime name not recognized. Say switch to " + options + ". "
                                       + record.host.context.profile + " remains selected.")
                    except HTTPException:
                        if record.host.poisoned:
                            raise AdapterError("runtime_unavailable", "Runtime switch requires session restart") from None
                        message = "Could not switch. " + record.host.context.profile + " remains selected; check its replacement's credentials and installation."
                    generation = record.host.generation
                    def active(generation=generation):
                        return not record.closed and record.host.generation == generation
                    await record.output.send({"command": "sendmsg", "message": message, "msg": message}, active)
                    if active():
                        record.output.chat_messages += 1
            except AdapterError as exc:
                record.input_error = exc.code
            except Exception:
                record.input_error = "input_processing_failed"
            finally:
                record.inputs.task_done()

    def enqueue(record, turn):
        if record.lost_channels:
            raise HTTPException(409, "Meeting transport is reconnecting")
        try:
            record.inputs.put_nowait((record.input_epoch, turn))
        except asyncio.QueueFull as exc:
            raise HTTPException(429, "Turn input queue is full") from exc

    @app.get("/health")
    async def health():
        return {"status": "ok", "version": "0.1.0"}

    @app.get("/ready")
    async def ready():
        if closing:
            raise HTTPException(503, "Server is draining")
        return {"ready": True, "profile": settings.profile,
                "deployment": "reasoning_service" if settings.output == "events" else "public_bridge"}

    @app.post("/v1/sessions", dependencies=[Depends(authorize)])
    async def create_session(body: SessionRequest):
        if bool(body.meeting_link) == bool(body.bot_id):
            raise HTTPException(400, "Supply exactly one of meeting_link or existing bot_id")
        if settings.output == "events" and not body.bot_id:
            raise HTTPException(400, "Native MIA must supply its existing bot_id; it owns meeting presence")
        if body.meeting_link:
            try:
                validate_meeting_link(body.meeting_link)
            except ValueError as exc:
                raise HTTPException(400, "Unsupported meeting link") from exc
            if not settings.public_url:
                raise HTTPException(400, "Set PUBLIC_BASE_URL before creating a meeting bot")
        async with creation_lock:
            if closing or sum(not r.closed for r in records.values()) >= settings.max_sessions:
                raise HTTPException(429, "Session capacity reached")
            sid, token = uuid4().hex, secrets.token_urlsafe(32)
            record = Record(sid, token, time.time() + settings.session_ttl,
                            policy=TurnPolicy(settings.wake_words, settings.followup_seconds), runtime_settings=settings)
            records[sid], token_index[token] = record, record
            try:
                selected_adapter = adapter or await runtime_adapter(settings)
                # Validate optional packages before a meeting bot is created.
                speech = None
                if settings.output == "voice_and_chat":
                    if speech_factory:
                        speech = speech_factory()
                    else:
                        speech = create_speech(settings)
                record.output = MeetingOutput(body.bot_id or "pending", speech, chat=settings.output != "events")
                context = SessionContext(session_id=sid, workspace=settings.workspace, bot_id=body.bot_id or "pending", profile=settings.profile)
                record.host = await SessionHost.create(context, selected_adapter, executor, record.output,
                                                      turn_timeout=settings.turn_timeout)
                if settings.output != "events" and settings.transcript_source == "deepgram":
                    stt_key = os.getenv("DEEPGRAM_API_KEY")
                    if not stt_key:
                        raise AdapterError("authentication_failed", "Deepgram credentials are missing")

                    def finalized(turn):
                        record.host.metrics["finalized_participant_turns"] += 1
                        enqueue(record, turn)

                    async def stt_failed():
                        record.input_error = "stt_disconnected"
                        # Reader cleanup cannot await itself.
                        async def terminate():
                            with suppress(Exception):
                                await close_record(record, remove=True)
                        asyncio.create_task(terminate())

                    record.transcription = DeepgramInput(stt_key, finalized, stt_failed)
                    await record.transcription.start()
                if body.meeting_link:
                    public, ws = settings.public_url, settings.public_url.replace("https:", "wss:", 1)
                    payload = {"meeting_link": body.meeting_link, "bot_name": "Adapter SDK Test", "video_required": False,
                               "socket_connection_url": {"websocket_url": f"{ws}/bridge/{token}"},
                               "live_audio_required": {"websocket_url": f"{ws}/bridge/audio/{token}"},
                               "live_transcription_required": {"webhook_url": f"{public}/transcription/{token}"},
                               "callback_url": f"{public}/lifecycle/{token}",
                               "custom_attributes": {"adapter_session_id": sid},
                               "deduplication_key": f"adapter-{sid}",
                               "recording_config": {"transcript": {"provider": {"deepgram_streaming": {
                                   "model": "nova-2", "language": "en", "transcription_mode": "sentence",
                                   "punctuate": True, "smart_format": True, "vad_events": True, "endpointing": 300}}}},
                               "automatic_leave": {"waiting_room_timeout": settings.waiting_room_timeout, "everyone_left_timeout": 60}}
                    if settings.transcript_source == "deepgram":
                        payload.pop("live_transcription_required")
                        payload.pop("recording_config")
                    response = await api().bots.create(payload, idempotency_key=sid)
                    record.bot_id = str(response["bot_id"])
                    record.created_bot = True
                    executor.db.execute("INSERT INTO bot_cleanup VALUES (?,?)", (record.bot_id, record.expires))
                    executor.db.commit()
                    if record.closed:
                        await remove_owned_bot(api, executor.db, record.bot_id)
                        raise AdapterError("transport_lost", "Input transport closed during bot creation")
                else:
                    record.bot_id = body.bot_id
                record.host.context = context.model_copy(update={"bot_id": record.bot_id})
                record.output.bot_id = record.bot_id
                record.input_worker = asyncio.create_task(input_loop(record))
                record.ready.set()
                # Token-bearing URLs are returned only to the authenticated owner.
                return {"session_id": sid, "bot_id": record.bot_id, "session_token": token,
                        "expires_at": record.expires, "output": settings.output,
                        "deployment": "reasoning_service" if settings.output == "events" else "public_bridge"}
            except Exception as exc:
                with suppress(Exception):
                    await close_record(record, remove=True)
                records.pop(sid, None)
                if isinstance(exc, AdapterError):
                    raise HTTPException(400, exc.message) from exc
                raise HTTPException(502, "Session creation failed; check credentials and configuration") from exc

    @app.get("/v1/sessions/{sid}", dependencies=[Depends(authorize)])
    async def status(sid: str):
        return snapshot(get_record(sid))

    async def switch_record(record, selection):
        async with record.operation_lock:
            try:
                selected = selection.settings(settings)
                revision = await record.host.switch(await runtime_adapter(selected), selected.profile)
            except AdapterError as exc:
                raise HTTPException(409 if exc.code == "runtime_unavailable" else 400, exc.public()) from exc
            except ValueError as exc:
                raise HTTPException(400, "Invalid runtime configuration") from exc
            except Exception as exc:
                raise HTTPException(502, "Runtime switch failed; check runtime availability and session status") from exc
            while not record.inputs.empty():
                record.inputs.get_nowait()
                record.inputs.task_done()
            record.input_epoch += 1
            record.runtime_settings = selected
            return {"session_id": record.id, "bot_id": record.bot_id, "profile": selected.profile,
                    "model": selected.model, "model_provider": runtime_provider(selected),
                    "runtime_revision": revision, "switched": True}

    @app.post("/v1/sessions/{sid}/runtime", dependencies=[Depends(authorize)])
    async def switch_runtime(sid: str, selection: RuntimeSelection):
        return await switch_record(get_record(sid), selection)

    @app.get("/v1/runtimes", dependencies=[Depends(authorize)])
    async def runtimes():
        import importlib.metadata

        result = []
        for profile, package in RUNTIME_PACKAGES.items():
            if google_pilot and profile == "openai":
                continue
            try:
                version = importlib.metadata.version(package)
            except importlib.metadata.PackageNotFoundError:
                version = None
            result.append({"profile": profile, "installed": version is not None, "version": version})
        return result

    async def session_for_bot(bot_id):
        async with mia_session_lock:
            sid = mia_sessions.get(bot_id)
            if sid in records and not records[sid].closed:
                return records[sid]
            session = await create_session(SessionRequest(bot_id=bot_id))
            # Keep this map bounded by the same live-session ownership limit.
            for bot, existing in list(mia_sessions.items()):
                if existing not in records:
                    mia_sessions.pop(bot)
            mia_sessions[bot_id] = session["session_id"]
            return get_record(session["session_id"])

    mia_token = install_mia_tools(app, settings, lambda: executor, session_for_bot,
                                 lambda bot: records.get(mia_sessions.get(bot)), switch_record)

    @app.get("/v1/mia/functions", dependencies=[Depends(authorize)])
    async def functions():
        token = mia_token
        if settings.output != "events" or not settings.public_url or not token:
            raise HTTPException(400, "Configure output=events, PUBLIC_BASE_URL and MIA_BRIDGE_TOKEN")
        return {"agent": {"custom_functions": mia_functions(settings.public_url, token, google_only=google_pilot)}}

    @app.get("/v1/sessions/{sid}/events", dependencies=[Depends(authorize)])
    async def events(sid: str):
        return [e.model_dump(mode="json") for e in get_record(sid).host.events]

    @app.get("/v1/sessions/{sid}/events/stream", dependencies=[Depends(authorize)])
    async def stream_events(sid: str, request: Request, follow: bool = True):
        record = get_record(sid)
        cursor = request.headers.get("last-event-id")
        if cursor and not any(e.event_id == cursor for e in record.host.events):
            raise HTTPException(409, "Event cursor expired; retrieve session status and retained events")

        async def stream():
            nonlocal cursor
            heartbeat = time.monotonic()
            while not record.closed:
                retained = list(record.host.events)
                if cursor:
                    positions = [n for n, e in enumerate(retained) if e.event_id == cursor]
                    if not positions:
                        yield 'event: stream.error\ndata: {"code":"cursor_expired"}\n\n'
                        return
                    retained = retained[positions[0] + 1:]
                for event in retained:
                    yield f"id: {event.event_id}\nevent: {event.type.value}\ndata: {event.model_dump_json()}\n\n"
                    cursor = event.event_id
                if not follow or await request.is_disconnected():
                    return
                if time.monotonic() - heartbeat >= 15:
                    yield ": keepalive\n\n"
                    heartbeat = time.monotonic()
                await asyncio.sleep(0.1)

        return StreamingResponse(stream(), media_type="text/event-stream",
                                 headers={"Cache-Control": "no-store", "X-Accel-Buffering": "no"})

    @app.post("/v1/sessions/{sid}/turns", dependencies=[Depends(authorize)])
    async def submit(sid: str, turn: Turn):
        enqueue(get_record(sid), turn)
        return {"queued": True}

    @app.post("/v1/sessions/{sid}/interrupt", dependencies=[Depends(authorize)])
    async def interrupt(sid: str):
        record = get_record(sid)
        # Interrupt is never queued behind ordinary turns.
        record.input_epoch += 1
        while not record.inputs.empty():
            record.inputs.get_nowait()
            record.inputs.task_done()
        return (await record.host.interrupt()).model_dump()

    @app.post("/v1/sessions/{sid}/stop", dependencies=[Depends(authorize)])
    async def stop(sid: str):
        pending = await close_record(get_record(sid), remove=True)
        if pending:
            from fastapi.responses import JSONResponse
            return JSONResponse({"closed": True, "bot_removal_pending": True}, status_code=202)
        return {"closed": True}

    @app.post("/transcription/{token}")
    @app.post("/transcription/{token}/webhook")
    async def transcription(token: str, request: Request):
        record = await authenticated_record(token)
        if settings.transcript_source != "meetstream":
            raise HTTPException(409, "MeetStream transcript ingress is disabled for this session")
        payload, _ = await bounded_json(request)
        record.host.metrics["transcript_callbacks"] += 1
        try:
            turn = transcript_turn(payload, record.bot_id)
        except (AdapterError, ValueError) as exc:
            record.input_error = "invalid_transcript"
            raise HTTPException(400, "Invalid transcript or bot binding") from exc
        if turn and turn.speaker != "Adapter SDK Test":
            record.host.metrics["finalized_participant_turns"] += 1
            enqueue(record, turn)
        return {"accepted": True}

    @app.post("/lifecycle/{token}")
    async def lifecycle(token: str, request: Request):
        record = await authenticated_record(token)
        payload, _ = await bounded_json(request)
        if payload.get("bot_id") != record.bot_id:
            raise HTTPException(403, "Bot binding mismatch")
        record.lifecycle = str(payload.get("event") or payload.get("bot_event") or "unknown")
        if meeting_ended(payload):
            executor.db.execute("DELETE FROM bot_cleanup WHERE bot_id=?", (record.bot_id,))
            executor.db.commit()
            await close_record(record)
        return {"accepted": True}

    @app.post("/webhooks/workspace")
    async def workspace_webhook(request: Request):
        secret = os.getenv("MEETSTREAM_WEBHOOK_SECRET")
        if not secret:
            raise HTTPException(503, "Workspace webhook signing is not configured")
        payload, raw = await bounded_json(request)
        expected = "sha256=" + hmac.new(secret.encode(), raw, hashlib.sha256).hexdigest()
        if not hmac.compare_digest(request.headers.get("x-meetstream-signature", "").encode(), expected.encode()):
            raise HTTPException(401, "Invalid signature")
        try:
            dt = datetime.fromisoformat(payload["timestamp"].replace("Z", "+00:00"))
            stamp = dt.replace(tzinfo=timezone.utc).timestamp() if dt.tzinfo is None else dt.timestamp()
            if abs(time.time() - stamp) > 300:
                raise ValueError
        except (KeyError, TypeError, ValueError) as exc:
            raise HTTPException(401, "Signed body requires a fresh timestamp") from exc
        record = next((r for r in records.values() if r.bot_id == payload.get("bot_id") and not r.closed), None)
        if record and meeting_ended(payload):
            executor.db.execute("DELETE FROM bot_cleanup WHERE bot_id=?", (record.bot_id,))
            executor.db.commit()
            await close_record(record)
        return {"accepted": True}

    async def socket_loop(ws: WebSocket, token, channel, path_bot=None):
        try:
            record = await authenticated_record(token)
        except HTTPException:
            await ws.close(code=1008)
            return
        if path_bot and path_bot != record.bot_id:
            await ws.close(code=1008)
            return
        await ws.accept()
        bound = False
        try:
            async with asyncio.timeout(5):
                initial = await ws.receive_json()
            if not isinstance(initial, dict) or initial.get("type") != "ready" or initial.get("bot_id") != record.bot_id:
                await ws.close(code=1008)
                return
            record.lost_channels.discard(channel)
            if channel == "control":
                old = record.output.socket
                record.output.socket = ws
            else:
                old = record.audio_socket
                record.audio_socket = ws
            if old and old is not ws:
                await old.close(code=1000)
            bound = True
            await ws.send_json(acknowledgement(record.bot_id, audio=channel == "audio"))
            while True:
                frame = await ws.receive()
                if frame["type"] == "websocket.disconnect":
                    break
                if record.closed:
                    continue  # Teardown retains transport only for remote departure.
                if frame.get("text"):
                    if len(frame["text"]) > (262144 if channel == "audio" else 65536):
                        await ws.close(code=1009)
                        break
                    data = json.loads(frame["text"])
                    if not isinstance(data, dict) or data.get("bot_id", record.bot_id) != record.bot_id:
                        await ws.close(code=1008)
                        break
                    if channel == 'audio' and (audio := legacy_audio(data)) is not None:
                        frame = {'bytes': audio}
                    # Control commands are outbound. A peer's echoed command or
                    # acknowledgment must not cancel a newly accepted turn.
                    # Inbound speech is owned by the selected transcript/audio
                    # stage; explicit owner interruption uses management HTTP.
                if frame.get("bytes"):
                    record.host.metrics["input_audio_frames"] += 1
                    if len(frame["bytes"]) > 262144:
                        await ws.close(code=1009)
                        break
                    if channel == "audio":
                        if record.speech_gate.onset(frame["bytes"]):
                            record.host.metrics["participant_speech_onsets"] += 1
                            if record.host.run and (not record.host.run.terminal or record.output.delivery_pending()
                                                    or (record.output.speech and record.output.delivery == "submitted_unconfirmed")):
                                # Local submission completion does not mean the
                                # platform has finished playing its audio queue.
                                record.host.metrics["speech_interruptions"] += 1
                                await record.host.interrupt("participant_speech")
                        if record.transcription:
                            try:
                                await record.transcription.audio(frame["bytes"], suppress_unknown=record.output.echo_possible())
                            except Exception:
                                record.input_error = "stt_transport_lost"
                                await close_record(record, remove=True)
                                break
                # Exactly one selected STT stage owns finalized transcript input.
        except (WebSocketDisconnect, TimeoutError, ValueError):
            pass
        finally:
            if bound:
                if channel == "control" and record.output.socket is ws:
                    record.output.socket = None
                    record.lost_channels.add(channel)
                    await record.host.interrupt("control_disconnected")
                elif channel == "audio" and record.audio_socket is ws:
                    record.audio_socket = None
                    record.lost_channels.add(channel)
                    await record.host.interrupt("audio_disconnected")
                if record.lost_channels and not record.closed:
                    if record.grace_task:
                        record.grace_task.cancel()
                    async def expire():
                        await asyncio.sleep(settings.reconnect_grace)
                        if record.lost_channels:
                            await close_record(record, remove=True)
                    record.grace_task = asyncio.create_task(expire())

    @app.websocket("/bridge/{token}")
    @app.websocket("/{path_bot}/bridge/{token}")
    async def control_socket(ws: WebSocket, token: str, path_bot: str | None = None):
        await socket_loop(ws, token, "control", path_bot)

    @app.websocket("/bridge/audio/{token}")
    @app.websocket("/{path_bot}/bridge/audio/{token}")
    async def audio_socket(ws: WebSocket, token: str, path_bot: str | None = None):
        await socket_loop(ws, token, "audio", path_bot)

    return app
