import asyncio
from contextlib import suppress

import pytest

from meetstream_agent_adapter.adapters.common import StatelessSession, delta
from meetstream_agent_adapter.contract import TERMINAL, AdapterError, EventType, SessionContext, Turn
from meetstream_agent_adapter.host import MemoryOutput, SessionHost
from meetstream_agent_adapter.tools import Execution, ToolExecutor, ToolSpec, execution


def test_generic_tool_executor_creates_no_demo_or_presence_tables():
    tools = ToolExecutor([], set())
    try:
        names = {row[0] for row in tools.db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        assert names == {"operations"}
    finally:
        tools.close()


def turn(n=1, text="hello"):
    return Turn(event_id=f"event-{n}", turn_id=f"turn-{n}", text=text, speaker="Tester", timestamp=float(n))


def context(sid="session"):
    return SessionContext(session_id=sid, workspace="test", bot_id="bot", profile="fake")


class Stream(StatelessSession):
    def __init__(self):
        self.started = asyncio.Event()
        self.finish = asyncio.Event()
        self.cleaned = asyncio.Event()

    async def run_turn(self, turn, history):
        self.started.set()
        try:
            yield delta("Hello ")
            await self.finish.wait()
            yield delta("world.")
        finally:
            self.cleaned.set()


@pytest.fixture
def tools():
    executor = ToolExecutor([], set())
    yield executor
    executor.close()


async def test_stream_and_single_terminal_and_history(tools):
    runtime, output = Stream(), MemoryOutput()
    host = SessionHost(context(), runtime, tools, output)
    await host.accept(turn())
    await runtime.started.wait()
    assert [e.payload.get("text") for e in host.events if e.type == EventType.DELTA] == ["Hello "]
    runtime.finish.set()
    await host.wait()
    assert [e.type for e in host.events] == [EventType.STARTED, EventType.DELTA, EventType.DELTA, EventType.MESSAGE, EventType.COMPLETED]
    assert [e.sequence for e in host.events] == list(range(1, 6))
    assert host.history[-1]["content"] == "Hello world."
    assert not await host.accept(turn())
    assert not await host.accept(turn(0))
    await host.close()
    assert sum(e.type in TERMINAL for e in host.events) == 1


async def test_interrupt_cleans_runtime_and_no_stale_delta(tools):
    runtime = Stream()
    host = SessionHost(context(), runtime, tools, MemoryOutput())
    await host.accept(turn())
    await runtime.started.wait()
    result = await host.interrupt()
    assert result.output_fenced and result.runtime_stopped and result.state_safe
    assert runtime.cleaned.is_set()
    old = host.run
    await host.emit(old, EventType.DELTA, {"text": "obsolete"})
    assert "obsolete" not in str(list(host.events))
    assert host.events[-1].type == EventType.CANCELLED
    runtime.finish.set()
    await host.accept(turn(2))
    await host.wait()
    assert host.events[-1].type == EventType.COMPLETED
    assert sum(e.type in TERMINAL for e in host.events if e.turn_id == old.turn.turn_id) == 1
    await host.close()


async def test_timeout_has_safe_error_and_cleans_stream(tools):
    runtime = Stream()
    host = SessionHost(context(), runtime, tools, MemoryOutput(), turn_timeout=0.02)
    await host.accept(turn())
    await host.wait()
    assert runtime.cleaned.is_set()
    assert host.events[-1].type == EventType.FAILED
    assert host.events[-1].payload["code"] == "deadline_exceeded"
    assert not host.poisoned
    await host.close()


async def test_delivery_failure_closes_native_stream_and_fences_output(tools):
    class BrokenOutput(MemoryOutput):
        async def event(self, event, active):
            if event.type != EventType.STARTED:
                raise RuntimeError("Private delivery detail")

    runtime, output = Stream(), BrokenOutput()
    host = SessionHost(context(), runtime, tools, output)
    await host.accept(turn())
    old = host.run
    await host.wait()
    assert runtime.cleaned.is_set()
    assert not host.active(old)
    assert output.interrupts == 1
    assert host.events[-1].type == EventType.FAILED
    assert "Private delivery detail" not in str(list(host.events))
    assert sum(e.type in TERMINAL for e in host.events) == 1
    await host.close()


async def test_runtime_that_ignores_cancellation_poisoned(tools):
    class Stubborn(Stream):
        async def run_turn(self, turn, history):
            self.started.set()
            with suppress(asyncio.CancelledError):
                await self.finish.wait()
            await self.finish.wait()
            yield delta("late")

    runtime = Stubborn()
    host = SessionHost(context(), runtime, tools, MemoryOutput())
    await host.accept(turn())
    await runtime.started.wait()
    outcome = await host.interrupt()
    assert not outcome.runtime_stopped and not outcome.state_safe
    with pytest.raises(AdapterError, match="restart"):
        await host.accept(turn(2))
    runtime.finish.set()
    await host.wait()
    assert not any(e.type == EventType.DELTA for e in host.events)
    await host.close()


async def test_old_error_does_not_clear_new_output(tools):
    class LateFailure(Stream):
        async def run_turn(self, turn, history):
            self.started.set()
            try:
                await self.finish.wait()
            except asyncio.CancelledError:
                raise RuntimeError("secret-provider-detail") from None
            yield delta("new")

    runtime, output = LateFailure(), MemoryOutput()
    host = SessionHost(context(), runtime, tools, output)
    await host.accept(turn())
    await runtime.started.wait()
    outcome = await host.interrupt()
    assert outcome.state_safe
    assert output.interrupts == 1
    assert host.events[-1].type == EventType.CANCELLED
    assert "secret-provider-detail" not in str(list(host.events))
    await host.close()


async def test_durable_tool_dedup_scope_and_schema(tmp_path):
    calls = []

    async def write(args, ctx, op):
        calls.append((ctx.session_id, op))
        return {"saved": args["title"]}

    spec = ToolSpec("save", "Save", {"type": "object", "properties": {"title": {"type": "string"}},
                                   "required": ["title"], "additionalProperties": False}, write, writes=True)
    journal = []

    async def emit(kind, payload):
        journal.append(kind)

    path = tmp_path / "ledger.sqlite"
    executor = ToolExecutor([spec], {"save"}, path)
    token = execution.set(Execution(context(), "turn", emit, lambda: True))
    try:
        result = await executor.call("save", {"title": "Task"}, "call")
        assert await executor.call("save", {"title": "Task"}, "call") == result
        executor.close()
        executor = ToolExecutor([spec], {"save"}, path)
        assert await executor.call("save", {"title": "Task"}, "call") == result
        assert len(calls) == 1
        with pytest.raises(AdapterError):
            await executor.call("save", {"title": "changed"}, "call")
        with pytest.raises(AdapterError):
            await executor.call("save", {"title": "Task", "session_id": "attacker"})
        with pytest.raises(AdapterError):
            await executor.call("private", {})
        execution.set(Execution(context("other"), "turn", emit, lambda: True))
        await executor.call("save", {"title": "Task"}, "call")
        assert len(calls) == 2
    finally:
        execution.reset(token)
        executor.close()


async def test_cancelled_write_is_unknown_and_never_replayed():
    started = asyncio.Event()
    count = 0

    async def write(args, ctx, op):
        nonlocal count
        count += 1
        started.set()
        await asyncio.Event().wait()

    async def emit(*args):
        pass

    executor = ToolExecutor([ToolSpec("write", "Write", {"type": "object"}, write, writes=True)], {"write"})
    token = execution.set(Execution(context(), "turn", emit, lambda: True))
    try:
        task = asyncio.create_task(executor.call("write", {}, "call"))
        await started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        result = await executor.call("write", {}, "call")
        assert result["status"] == "outcome_unknown" and count == 1
    finally:
        execution.reset(token)
        executor.close()


async def test_unconfirmed_completed_playback_is_reconciled_on_interrupt(tools):
    class Unconfirmed(MemoryOutput):
        delivery = 'submitted_unconfirmed'

    runtime = Stream()
    runtime.finish.set()
    host = SessionHost(context(), runtime, tools, Unconfirmed())
    await host.accept(turn())
    await host.wait()
    assert host.events[-1].type == EventType.COMPLETED
    await host.interrupt()
    assert host.history[-1]['content'].startswith('[Previous output was interrupted')
    assert sum(e.type in TERMINAL for e in host.events) == 1
    await host.close()
