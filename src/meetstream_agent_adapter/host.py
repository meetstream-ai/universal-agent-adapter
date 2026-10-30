"""One reasoning owner per session, with output fencing independent of playback."""

import asyncio
from collections import Counter, OrderedDict, deque
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass
from typing import Any
from uuid import uuid4

from .contract import (
    TERMINAL,
    AdapterError,
    AgentAdapter,
    AgentEvent,
    CancelOutcome,
    EventType,
    SessionContext,
    Turn,
)
from .tools import Execution, ToolExecutor, execution


@dataclass
class Run:
    turn: Turn
    generation: int
    task: asyncio.Task | None = None
    terminal: bool = False
    sequence: int = 0
    text: str = ""
    execution: Execution | None = None


class SessionHost:
    def __init__(self, context: SessionContext, runtime, tools: ToolExecutor, output, *, turn_timeout=60.0):
        self.context, self.runtime, self.tools, self.output = context, runtime, tools, output
        self.turn_timeout = turn_timeout
        self.generation = 0
        self.run: Run | None = None
        self.lock = asyncio.Lock()
        self.closed = False
        self.poisoned = False
        self.history: deque[dict[str, str]] = deque(maxlen=40)
        self.events: deque[AgentEvent] = deque(maxlen=512)
        self.seen: OrderedDict[str, None] = OrderedDict()
        self.turn_ids: OrderedDict[str, None] = OrderedDict()
        self.last_timestamp = -1.0
        self.metrics: Counter = Counter()
        self.background: set[asyncio.Task] = set()

    @classmethod
    async def create(cls, context: SessionContext, adapter: AgentAdapter, tools: ToolExecutor, output, **kwargs):
        runtime = await cls._open_runtime(context, adapter, tools)
        return cls(context, runtime, tools, output, **kwargs)

    @staticmethod
    async def _open_runtime(context, adapter, tools):
        runtime = await adapter.open_session(context, tools)
        try:
            caps = runtime.capabilities
            if not caps or not caps.incremental_text or not caps.shared_tools or caps.cancellation not in {"native", "cooperative"}:
                raise AdapterError("capability_unsupported", "Runtime lacks required streaming/tool/cancellation capabilities")
        except BaseException:
            async with asyncio.timeout(2):
                await runtime.close()
            raise
        return runtime

    async def switch(self, adapter: AgentAdapter, profile: str) -> int:
        """Replace reasoning; retain transport, tools and bounded common history.

        Preparation leaves the current answer running. Commit fences it, awaits
        native/tool cleanup, clears playback and closes the old runtime before
        publishing the replacement. Interrupted work is never replayed.
        """
        candidate = None
        committing = False
        try:
            if self.closed or self.poisoned:
                raise AdapterError("runtime_unavailable", "Session requires restart")
            async with asyncio.timeout(10):
                candidate = await self._open_runtime(self.context.model_copy(update={"profile": profile}), adapter, self.tools)
            async with self.lock:
                if self.closed or self.poisoned:
                    raise AdapterError("runtime_unavailable", "Session requires restart")
                committing = True
                outcome = await self._interrupt("runtime_switch")
                if not outcome.state_safe:
                    raise AdapterError("runtime_unavailable", "Previous runtime did not stop safely")
                async with asyncio.timeout(2):
                    await self.runtime.close()
                previous = self.context.profile
                self.runtime, candidate = candidate, None
                self.context = self.context.model_copy(update={"profile": profile})
                self.run = None
                self.metrics["runtime_switches"] += 1
                self.metrics[EventType.SWITCHED.value] += 1
                self.events.append(AgentEvent(
                    session_id=self.context.session_id, turn_id="switch:" + uuid4().hex,
                    generation=self.generation, sequence=1, type=EventType.SWITCHED,
                    payload={"previous_profile": previous, "profile": profile,
                             "revision": self.metrics["runtime_switches"]}))
                return self.metrics["runtime_switches"]
        except BaseException:
            if committing:
                # Never reopen admission after unconfirmed runtime/playback cleanup.
                self.poisoned = True
            raise
        finally:
            if candidate is not None:
                async with asyncio.timeout(2):
                    await candidate.close()

    def active(self, run: Run) -> bool:
        return not self.closed and not self.poisoned and self.generation == run.generation

    async def accept(self, turn: Turn) -> bool:
        async with self.lock:
            if self.closed or self.poisoned:
                raise AdapterError("runtime_unavailable", "Session requires restart")
            if turn.event_id in self.seen or turn.turn_id in self.turn_ids or turn.timestamp < self.last_timestamp:
                self.metrics["input_rejected"] += 1
                return False
            self.seen[turn.event_id] = None
            self.turn_ids[turn.turn_id] = None
            if len(self.seen) > 512:
                self.seen.popitem(last=False)
                self.turn_ids.popitem(last=False)
            self.last_timestamp = turn.timestamp
            await self._interrupt("superseded")
            if self.poisoned:
                raise AdapterError("runtime_unavailable", "Previous runtime did not stop safely")
            self.generation += 1
            run = self.run = Run(turn, self.generation)
            run.task = asyncio.create_task(self._drive(run), name=f"turn:{turn.turn_id}")
            return True

    async def emit(self, run: Run, kind: EventType, payload: dict[str, Any]) -> None:
        if not self.active(run) or run.terminal:
            self.metrics["stale_output_dropped"] += 1
            return
        if kind == EventType.DELTA:
            delta = payload.get("text")
            if not isinstance(delta, str) or len(delta) > 16000:
                raise AdapterError("invalid_input", "Runtime emitted invalid text")
            run.text += delta
            if len(run.text) > 64000:
                raise AdapterError("deadline_exceeded", "Response size limit reached")
        run.sequence += 1
        event = AgentEvent(session_id=self.context.session_id, turn_id=run.turn.turn_id, generation=run.generation,
                           sequence=run.sequence, type=kind, payload=payload)
        self.events.append(event)
        self.metrics[kind.value] += 1
        if kind in TERMINAL:
            run.terminal = True
        await self.output.event(event, lambda: self.active(run))

    async def _drive(self, run: Run) -> None:
        run.execution = Execution(self.context, run.turn.turn_id,
                                  lambda kind, payload: self.emit(run, kind, payload), lambda: self.active(run))
        token = execution.set(run.execution)
        try:
            async with asyncio.timeout(self.turn_timeout):
                await self.emit(run, EventType.STARTED, {})
                while len(self.history) > 1 and sum(len(m["content"]) for m in self.history) > 64000:
                    self.history.popleft()
                stream = self.runtime.run_turn(run.turn, list(self.history))
                try:
                    async for event in stream:
                        if event.type != EventType.DELTA:
                            raise AdapterError("invalid_input", "Adapter emitted an unsupported event")
                        await self.emit(run, event.type, event.payload)
                finally:
                    if close_stream := getattr(stream, "aclose", None):
                        await close_stream()
                await self.emit(run, EventType.MESSAGE, {"message_id": run.turn.turn_id, "text": run.text})
                if self.active(run):
                    self.history.append({"role": "user", "content": f"{run.turn.speaker}: {run.turn.text}"})
                    self.history.append({"role": "assistant", "content": run.text})
                await self.emit(run, EventType.COMPLETED, {})
        except asyncio.CancelledError:
            # Cancellation terminal is published by _interrupt after synchronous fencing.
            raise
        except Exception as exc:
            self.metrics[f"runtime_error.{type(exc).__name__}"] += 1
            if isinstance(exc, AdapterError):
                error = exc
            elif isinstance(exc, TimeoutError):
                error = AdapterError("deadline_exceeded", "Turn timed out")
            elif getattr(exc, "code", None) in {"credit_balance_exhausted", "insufficient_quota", "RESOURCE_EXHAUSTED"}:
                error = AdapterError("provider_quota_exhausted", "Model provider quota is exhausted")
            elif getattr(exc, "status_code", None) in {401, 403} or getattr(exc, "code", None) in {401, 403}:
                error = AdapterError("authentication_failed", "Model provider authentication failed")
            elif getattr(exc, "status_code", None) == 429 or getattr(exc, "code", None) == 429:
                error = AdapterError("rate_limited", "Model provider rate limit reached", retryable=True)
            else:
                error = AdapterError("internal_error", "Agent turn failed; try again")
            if self.active(run):
                # A failed delivery callback must not prevent output fencing
                # or cleanup. The terminal remains in the local event journal.
                with suppress(Exception):
                    await self.emit(run, EventType.FAILED, error.public())
                # Obsolete cleanup must never clear a newer turn's output.
                self.generation += 1
                with suppress(Exception):
                    await self.output.interrupt()
        finally:
            if not await self._stop_tasks(run.execution.tasks):
                self.poisoned = True
            execution.reset(token)

    async def _stop_tasks(self, tasks) -> bool:
        tasks = {task for task in tasks if task is not asyncio.current_task() and not task.done()}
        if not tasks:
            return True
        for task in tasks:
            if not task.cancelling():
                task.cancel()
            if task not in self.background:
                self.background.add(task)
                task.add_done_callback(self._reap)
        _, pending = await asyncio.wait(tasks, timeout=1.0)
        return not pending

    async def interrupt(self, reason="interrupted", *, turn_id=None) -> CancelOutcome:
        async with self.lock:
            if turn_id is not None and (self.run is None or self.run.turn.turn_id != turn_id):
                return CancelOutcome(runtime_stopped=True, state_safe=not self.poisoned)
            return await self._interrupt(reason)

    async def _interrupt(self, reason: str) -> CancelOutcome:
        run = self.run
        self.generation += 1  # Fence BEFORE any await, including playback/network I/O.
        stopped = True
        if run:
            if not run.terminal or self.output.delivery_pending() or getattr(self.output, "delivery", None) == "submitted_unconfirmed":
                if not run.terminal:
                    self.history.append({"role": "user", "content": f"{run.turn.speaker}: {run.turn.text}"})
                self.history.append({"role": "user", "content": "[Previous output was interrupted; delivery may be incomplete.]"})
            task = run.task
            tasks = set(run.execution.tasks) if run.execution else set()
            if task and not task.done():
                tasks.add(task)
            if tasks:
                # Playback clearing and cooperative runtime cleanup are independent.
                clear = asyncio.create_task(self.output.interrupt())
                stopped = await self._stop_tasks(tasks)
                if not stopped:
                    self.poisoned = True
                try:
                    await clear
                except Exception:
                    self.poisoned = True
            else:
                try:
                    await self.output.interrupt()
                except Exception:
                    self.poisoned = True
            if not run.terminal:
                run.terminal = True
                run.sequence += 1
                event = AgentEvent(session_id=self.context.session_id, turn_id=run.turn.turn_id,
                                   generation=run.generation, sequence=run.sequence, type=EventType.CANCELLED,
                                   payload={"reason": reason, "runtime_stopped": stopped})
                self.events.append(event)
                self.metrics[EventType.CANCELLED.value] += 1
                # Observe terminal without permitting obsolete media output.
                await self.output.event(event, lambda: False)
        return CancelOutcome(runtime_stopped=stopped, state_safe=stopped and not self.poisoned)

    def _reap(self, task: asyncio.Task) -> None:
        self.background.discard(task)
        with suppress(asyncio.CancelledError, Exception):
            task.result()

    async def wait(self) -> None:
        if self.run and self.run.task:
            with suppress(asyncio.CancelledError):
                await self.run.task

    async def close(self) -> None:
        async with self.lock:
            if self.closed:
                return
            await self._interrupt("session_closed")
            self.closed = True
            try:
                async with asyncio.timeout(2):
                    await self.runtime.close()
            finally:
                async with asyncio.timeout(3):
                    await self.output.close()


class MemoryOutput:
    """Deterministic output for conformance checks and embedded applications."""

    def __init__(self):
        self.events: list[AgentEvent] = []
        self.interrupts = 0

    async def event(self, event: AgentEvent, active: Callable[[], bool]) -> None:
        self.events.append(event)

    async def interrupt(self) -> None:
        self.interrupts += 1

    async def close(self) -> None:
        pass

    def delivery_pending(self) -> bool:
        return False
