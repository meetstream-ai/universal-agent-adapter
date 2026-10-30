"""Validated shared tools and a durable operation ledger."""

import asyncio
import contextvars
import hashlib
import json
import os
import sqlite3
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from uuid import uuid4

from jsonschema import Draft202012Validator

from .contract import AdapterError, EventType, SessionContext


@dataclass(frozen=True)
class ToolSpec:
    name: str
    description: str
    parameters: dict[str, Any]
    handler: Callable[[dict[str, Any], SessionContext, str], Awaitable[Any]]
    writes: bool = False
    timeout: float = 10


@dataclass
class Execution:
    context: SessionContext
    turn_id: str
    emit: Callable[[EventType, dict[str, Any]], Awaitable[None]]
    active: Callable[[], bool]
    calls: int = 0
    tasks: set[asyncio.Task] = field(default_factory=set)


execution: contextvars.ContextVar[Execution] = contextvars.ContextVar("adapter_execution")


class ToolExecutor:
    def __init__(self, specs: list[ToolSpec], allowed: set[str], ledger: Path | str = ":memory:"):
        self.specs = {s.name: s for s in specs}
        if len(self.specs) != len(specs) or not allowed <= self.specs.keys():
            raise ValueError("Duplicate tools or unknown allowed tool")
        self.allowed = frozenset(allowed)
        for spec in specs:
            Draft202012Validator.check_schema(spec.parameters)
            if spec.timeout <= 0:
                raise ValueError("Tool timeout must be positive")
        if str(ledger) != ":memory:":
            # New ledgers and their WAL inherit private permissions. Existing
            # operator-owned files retain their permissions.
            fd = os.open(ledger, os.O_WRONLY | os.O_CREAT, 0o600)
            os.close(fd)
        self.db = sqlite3.connect(str(ledger))
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("CREATE TABLE IF NOT EXISTS operations (id TEXT PRIMARY KEY, fingerprint TEXT, status TEXT, result TEXT, created REAL)")
        self.db.commit()

    def exposed(self) -> list[ToolSpec]:
        return [s for n, s in self.specs.items() if n in self.allowed]

    async def call(self, name: str, args: dict[str, Any], call_id: str | None = None) -> dict[str, Any]:
        ex = execution.get()
        if not ex.active():
            raise asyncio.CancelledError
        if name not in self.allowed:
            raise AdapterError("authentication_failed", "Tool is not authorized")
        spec = self.specs[name]
        if list(Draft202012Validator(spec.parameters).iter_errors(args)):
            raise AdapterError("invalid_input", "Tool arguments do not match the schema")
        ex.calls += 1
        if ex.calls > 16:
            raise AdapterError("deadline_exceeded", "Tool step limit reached")
        call_id = call_id or uuid4().hex
        # IDs are scoped by trusted context; model-supplied arguments cannot change scope.
        op = hashlib.sha256(f"{ex.context.workspace}:{ex.context.session_id}:{ex.turn_id}:{call_id}".encode()).hexdigest()
        fingerprint = hashlib.sha256(json.dumps([name, args], sort_keys=True).encode()).hexdigest()
        if spec.writes:
            row = self.db.execute("SELECT fingerprint,status,result FROM operations WHERE id=?", (op,)).fetchone()
            if row:
                if row[0] != fingerprint:
                    raise AdapterError("invalid_input", "Operation ID reused with different arguments")
                if row[1] == "completed":
                    return json.loads(row[2])
                return {"status": "outcome_unknown", "operation_id": op}
            self.db.execute("INSERT INTO operations VALUES (?,?,?,?,?)", (op, fingerprint, "started", None, time.time()))
            self.db.commit()
        task = asyncio.current_task()
        ex.tasks.add(task)
        try:
            await ex.emit(EventType.TOOL_STARTED, {"name": name, "call_id": call_id})
            async with asyncio.timeout(spec.timeout):
                value = await spec.handler(args, ex.context, op)
            if not ex.active():
                raise asyncio.CancelledError
            # Fail here if a handler returns unserializable data, before handing it to a model.
            serialized = json.dumps(value, allow_nan=False)
            if len(serialized) > 32000:
                raise AdapterError("tool_failed", "Tool result exceeded its size limit")
            result = {"status": "completed", "data": value, "operation_id": op}
            if spec.writes:
                self._finish(op, "completed", result)
            await ex.emit(EventType.TOOL_COMPLETED, {"name": name, "call_id": call_id, "status": "completed"})
            return result
        except asyncio.CancelledError:
            if spec.writes:
                self._finish(op, "outcome_unknown", {"operation_id": op})
            raise
        except Exception as exc:
            if spec.writes:
                self._finish(op, "outcome_unknown", {"operation_id": op})
            code = "deadline_exceeded" if isinstance(exc, TimeoutError) else "tool_failed"
            await ex.emit(EventType.TOOL_FAILED, {"name": name, "call_id": call_id, "code": code})
            return {"status": "outcome_unknown" if spec.writes else "failed", "code": code, "operation_id": op}
        finally:
            ex.tasks.discard(task)

    def _finish(self, operation: str, status: str, result: Any) -> None:
        self.db.execute("UPDATE operations SET status=?,result=? WHERE id=?", (status, json.dumps(result), operation))
        self.db.commit()

    def close(self) -> None:
        self.db.close()
