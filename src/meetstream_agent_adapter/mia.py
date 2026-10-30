"""Hosted MIA custom-function bridge; no private MIA hooks required."""

import asyncio
import hashlib
import hmac
import json
import os
import time
from typing import Any, Literal

from fastapi import HTTPException, Request
from pydantic import Field

from .config import RuntimeSelection
from .contract import EventType, StrictModel, Turn


class MiaBot(StrictModel):
    bot_id: str = Field(min_length=1, max_length=128)
    dynamic_vars: dict[str, Any] = Field(default_factory=dict)
    agent_id: str | None = None


class MiaCall(StrictModel):
    name: Literal["delegate_agent", "switch_agent", "interrupt_agent"]
    bot: MiaBot
    args: dict[str, Any]


class Delegation(StrictModel):
    request_id: str = Field(min_length=1, max_length=128)
    text: str = Field(min_length=1, max_length=16000)


class Switch(StrictModel):
    request_id: str = Field(min_length=1, max_length=128)
    profile: Literal["openai", "langgraph", "adk", "crewai"]


class Interrupt(StrictModel):
    request_id: str = Field(min_length=1, max_length=128)


def mia_functions(origin, token, *, google_only=False):
    """Return a mergeable custom_functions fragment, never a backend patch."""
    schemas = {"delegate_agent": Delegation, "switch_agent": Switch, "interrupt_agent": Interrupt}
    descriptions = {
        "delegate_agent": "Ask the selected external agent. Use a unique request_id for each new request; reuse it only for retries.",
        "switch_agent": "On an explicit request, switch the external runtime during this meeting. Never choose a runtime yourself.",
        "interrupt_agent": "Cancel pending external-agent work when explicitly requested."}
    functions = [{"name": name, "description": descriptions[name], "method": "POST", "url": origin + "/mia/" + name,
             "headers": {"Authorization": "Bearer " + token}, "parameters": model.model_json_schema(),
             "timeout_s": 120, "retries": 0, "response_cap_chars": 8000,
             "speak_during_execution": False, "speak_after_execution": True}
            for name, model in schemas.items()]
    if google_only:
        functions[1]["parameters"]["properties"]["profile"]["enum"] = ["adk", "langgraph", "crewai"]
    return functions


def install_mia_tools(app, settings, executor_ref, session_for_bot, current_session, switch_record):
    token = os.getenv("MIA_BRIDGE_TOKEN", "")
    table_ready = False
    if token and (len(token) < 32 or token == "replace-with-at-least-32-random-characters" or token == os.getenv("ADAPTER_API_KEY")):
        raise ValueError("MIA_BRIDGE_TOKEN must be a separate random token of at least 32 characters")

    @app.post("/mia/{function}")
    async def call(function: str, body: MiaCall, request: Request):
        nonlocal table_ready
        if settings.output != "events" or not token:
            raise HTTPException(503, "Start the events service and configure MIA_BRIDGE_TOKEN")
        if not hmac.compare_digest(request.headers.get("authorization", "").encode(), ("Bearer " + token).encode()):
            raise HTTPException(401, "Invalid MIA tool authentication")
        if request.headers.get("x-bot-id") != body.bot.bot_id:
            raise HTTPException(403, "MIA bot binding mismatch")
        if function != body.name:
            raise HTTPException(400, "MIA function binding mismatch")
        model = {"delegate_agent": Delegation, "switch_agent": Switch, "interrupt_agent": Interrupt}[function]
        try:
            args = model.model_validate(body.args)
        except ValueError as exc:
            raise HTTPException(400, "Invalid MIA function arguments") from exc
        db = executor_ref().db
        # This table belongs to the tool bridge, not the generic executor. A
        # durable receipt prevents replaying uncertain requests after restart.
        if not table_ready:
            db.execute("CREATE TABLE IF NOT EXISTS mia_requests (bot TEXT, id TEXT, fingerprint TEXT, status INTEGER, result TEXT, PRIMARY KEY(bot,id))")
            table_ready = True
        fingerprint = hashlib.sha256(json.dumps([function, body.args], sort_keys=True).encode()).hexdigest()
        row = db.execute("SELECT fingerprint,status,result FROM mia_requests WHERE bot=? AND id=?",
                         (body.bot.bot_id, args.request_id)).fetchone()
        if row:
            if row[0] != fingerprint:
                raise HTTPException(409, "Request ID reused with different arguments")
            if row[1] == 0:
                raise HTTPException(409, "Request in progress or outcome unknown; do not replay writes")
            result = json.loads(row[2])
            if row[1] != 200:
                raise HTTPException(row[1], result)
            if function == "switch_agent":
                current = current_session(body.bot.bot_id)
                if current and current.host.poisoned:
                    raise HTTPException(409, "Session requires restart; runtime state is unsafe")
                if (not current or current.closed or current.id != result["session_id"]
                        or current.host.context.profile != args.profile
                        or current.host.metrics["runtime_switches"] != result["runtime_revision"]):
                    raise HTTPException(409, "Switch receipt is historical; issue a new request ID to select the runtime again")
            return result
        db.execute("INSERT INTO mia_requests VALUES (?,?,?,?,?)",
                   (body.bot.bot_id, args.request_id, fingerprint, 0, None))
        db.commit()
        status, result, record, run = 409, {"code": "outcome_unknown"}, None, None
        try:
            record = await session_for_bot(body.bot.bot_id)
            # Authenticated activity renews the tool session's idle lease.
            record.expires = time.time() + settings.session_ttl
            if function == "switch_agent":
                result = await switch_record(record, RuntimeSelection(profile=args.profile))
            elif function == "interrupt_agent":
                result = (await record.host.interrupt("mia_tool_request")).model_dump()
            else:
                turn_id = hashlib.sha256((body.bot.bot_id + ":" + args.request_id).encode()).hexdigest()
                async with record.operation_lock:
                    accepted = await record.host.accept(Turn(turn_id=turn_id, event_id="mia:" + args.request_id,
                                                            text=args.text, timestamp=time.time()))
                    run = record.host.run
                    profile = record.host.context.profile
                if not accepted:
                    raise HTTPException(409, "Request was already accepted; its outcome is unknown")
                # Observe the captured task, never a later mutable host.run.
                while not run.task.done():
                    if await request.is_disconnected():
                        await record.host.interrupt("mia_client_disconnected", turn_id=turn_id)
                        raise HTTPException(409, "MIA client disconnected")
                    await asyncio.wait({run.task}, timeout=.1)
                # Interruption cancels the task before recording its terminal
                # event. Wait for that locked cleanup before classifying it.
                async with record.host.lock:
                    terminal = next((e for e in reversed(record.host.events)
                                     if e.turn_id == turn_id and e.type in {EventType.COMPLETED, EventType.CANCELLED, EventType.FAILED}), None)
                    if not record.host.active(run) or terminal is None or terminal.type != EventType.COMPLETED:
                        raise HTTPException(409, {"code": "turn_cancelled" if terminal and terminal.type == EventType.CANCELLED else "agent_failed"})
                result = {"status": "completed", "request_id": args.request_id,
                          "profile": profile, "answer": run.text[:6000],
                          "truncated": len(run.text) > 6000}
            status = 200
            return result
        except asyncio.CancelledError:
            if record and run:
                await record.host.interrupt("mia_request_cancelled", turn_id=run.turn.turn_id)
            raise
        except HTTPException as exc:
            status, result = exc.status_code, exc.detail
            raise
        except Exception as exc:
            status, result = 502, {"code": "agent_unavailable"}
            raise HTTPException(status, result) from exc
        finally:
            db.execute("UPDATE mia_requests SET status=?,result=? WHERE bot=? AND id=?",
                       (status, json.dumps(result), body.bot.bot_id, args.request_id))
            db.commit()
    return token
