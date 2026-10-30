"""Actual installed framework runners; only the model boundary is controlled."""

import asyncio

import pytest
from framework_models import SCHEMA, build

from meetstream_agent_adapter.contract import EventType, SessionContext, Turn
from meetstream_agent_adapter.host import MemoryOutput, SessionHost
from meetstream_agent_adapter.tools import ToolExecutor, ToolSpec


@pytest.mark.parametrize("profile", ["openai", "langgraph", "adk", "crewai"])
async def test_native_framework_stream_tools(profile):
    host, tools, model, calls = await build(profile)
    try:
        await host.accept(Turn(event_id="one", text="Save an action", timestamp=1))
        await host.wait()
        assert host.events[-1].type == EventType.COMPLETED, list(host.events)
        assert len(calls) == 1 and calls[0][1] == "framework-test"
        text = [e.payload["text"] for e in host.events if e.type == EventType.DELTA]
        assert len(text) >= 2 and "".join(text) == "Saved the action."
        assert "PRIVATE" not in "".join(text)
        assert [e.type for e in host.events].count(EventType.TOOL_COMPLETED) == 1
    finally:
        await host.close()
        tools.close()


@pytest.mark.parametrize("block", [False, True])
async def test_langgraph_native_google_tools_signature_privacy_and_owned_cleanup(monkeypatch, block):
    integration = pytest.importorskip("langchain_google_genai")
    from google.genai import types

    from meetstream_agent_adapter.adapters.langgraph import Adapter

    monkeypatch.setenv("GOOGLE_API_KEY", "fixture-key-not-a-credential")
    calls, requests, closed = [], [], []
    waiting, cleaned = asyncio.Event(), asyncio.Event()
    original_close = integration.ChatGoogleGenerativeAI.aclose

    async def close(model):
        closed.append(model)
        await original_close(model)

    monkeypatch.setattr(integration.ChatGoogleGenerativeAI, "aclose", close)

    async def save(args, ctx, op):
        calls.append(args)
        return {"saved": True}

    tools = ToolExecutor([ToolSpec("save", "Save one action", SCHEMA, save, writes=True)], {"save"})
    session = await Adapter(provider="google", model="gemini-3.5-flash-lite").open_session(
        SessionContext(session_id="native-google", workspace="test", bot_id="none", profile="langgraph"), tools)
    model = session.client
    assert model.include_thoughts is False

    async def responses(**kwargs):
        requests.append(kwargs)

        async def chunks():
            if len(requests) == 1:
                parts = [types.Part(function_call=types.FunctionCall(name="save", args={"title": "SDK test"}, id="call-1"),
                                    thought_signature=b"fixture-signature")]
                yield types.GenerateContentResponse(candidates=[types.Candidate(content=types.Content(role="model", parts=parts))])
            else:
                for part in [types.Part(text="PRIVATE", thought=True), types.Part(text="Saved "), types.Part(text="the action.")]:
                    yield types.GenerateContentResponse(candidates=[types.Candidate(content=types.Content(role="model", parts=[part]))])
                    await asyncio.sleep(0)
                    if block and len(requests) == 2 and part.text == "Saved ":
                        waiting.set()
                        try:
                            await asyncio.Event().wait()
                        finally:
                            cleaned.set()

        return chunks()

    monkeypatch.setattr(model.async_client.models, "generate_content_stream", responses)

    class Prepared:
        async def open_session(self, context, executor):
            return session

    host = await SessionHost.create(
        SessionContext(session_id="native-google", workspace="test", bot_id="none", profile="langgraph"),
        Prepared(), tools, MemoryOutput())
    try:
        await host.accept(Turn(event_id="one", text="Save an action", timestamp=1))
        if block:
            async with asyncio.timeout(5):
                await waiting.wait()
            outcome = await host.interrupt()
            assert outcome.output_fenced and outcome.runtime_stopped and outcome.state_safe
            assert cleaned.is_set() and host.events[-1].type == EventType.CANCELLED
            await host.accept(Turn(event_id="two", text="Confirm the saved action", timestamp=2))
        await host.wait()
        assert host.events[-1].type == EventType.COMPLETED, list(host.events)
        assert calls == [{"title": "SDK test"}]
        text = [e.payload["text"] for e in host.events if e.type == EventType.DELTA and e.turn_id == host.run.turn.turn_id]
        assert len(text) >= 2 and "".join(text) == "Saved the action."
        assert all("PRIVATE" not in e.payload.get("text", "") for e in host.events)
        assert len(requests) == (3 if block else 2)
        options = requests[0]["config"].http_options
        assert options.timeout == 60000 and options.retry_options.attempts == 1
        assert any(part.thought_signature == b"fixture-signature"
                   for content in requests[1]["contents"] for part in content.parts)
    finally:
        await host.close()
        tools.close()
    assert closed == [model]


async def test_crewai_native_no_tools_streams_direct_answer():
    pytest.importorskip("crewai")
    from crewai import BaseLLM

    from meetstream_agent_adapter.adapters.crewai import Adapter

    class DirectAnswer(BaseLLM):
        async def acall(self, *args, from_task=None, from_agent=None, **kwargs):
            for text in ["Ready", "."]:
                self._emit_stream_chunk_event(text, from_task=from_task, from_agent=from_agent)
                await asyncio.sleep(0)
            return "Ready."

        def call(self, *args, **kwargs):
            raise AssertionError("Async runner required")

    tools = ToolExecutor([], set())
    host = await SessionHost.create(
        SessionContext(session_id="no-tools", workspace="test", bot_id="none", profile="crewai"),
        Adapter(llm=DirectAnswer(model="controlled", stream=True)), tools, MemoryOutput())
    try:
        await host.accept(Turn(event_id="one", text="Say ready.", timestamp=1))
        await host.wait()
        assert host.events[-1].type == EventType.COMPLETED
        assert [e.payload["text"] for e in host.events if e.type == EventType.DELTA] == ["Ready", "."]
        assert host.metrics["tool.started"] == 0
    finally:
        await host.close()
        tools.close()


async def test_crewai_gemini_client_has_bounded_requests_and_no_thought_output(monkeypatch):
    pytest.importorskip("crewai")
    pytest.importorskip("google.genai")
    from meetstream_agent_adapter.adapters.crewai import Adapter

    monkeypatch.setenv("GOOGLE_API_KEY", "fixture-key-not-a-credential")
    tools = ToolExecutor([], set())
    session = await Adapter(model="gemini/gemini-3.5-flash-lite").open_session(
        SessionContext(session_id="provider-options", workspace="test", bot_id="none", profile="crewai"), tools)
    try:
        # Inspect the actual owned SDK client, not just the supplied options:
        # generic Crew timeout/max_retries were silently ignored by this provider.
        options = session.llm._client._api_client._http_options
        assert options.timeout == 60000
        assert options.retry_options.attempts == 1
        assert session.llm.thinking_config.include_thoughts is False
    finally:
        await session.close()
        tools.close()


async def test_crewai_native_gemini_receives_scoped_observation_stop_markers(monkeypatch):
    pytest.importorskip("crewai")
    pytest.importorskip("google.genai")
    from google.genai import types

    from meetstream_agent_adapter.adapters.crewai import Adapter

    monkeypatch.setenv("GOOGLE_API_KEY", "fixture-key-not-a-credential")
    calls, requests = [], []

    async def save(args, context, operation):
        calls.append(args)
        return {"saved": True}

    tools = ToolExecutor([ToolSpec("save", "Save one action", SCHEMA, save, writes=True)], {"save"})
    context = SessionContext(session_id="crewai-stop-scope", workspace="test", bot_id="none", profile="crewai")
    session = await Adapter(model="gemini/gemini-3.5-flash-lite").open_session(context, tools)
    original_stops = list(session.llm.stop_sequences)

    async def responses(**kwargs):
        requests.append(kwargs["config"])
        assert any("Observation:" in marker for marker in kwargs["config"].stop_sequences)
        pieces = (['Thought: PRIVATE\nAction: save\nAction Input: {"title":"SDK test"}']
                  if len(requests) == 1 else ['Thought: PRIVATE\nFinal Answer: Saved ', 'the action.'])

        async def chunks():
            for piece in pieces:
                yield types.GenerateContentResponse(candidates=[types.Candidate(
                    content=types.Content(role="model", parts=[types.Part(text=piece)]))])
                await asyncio.sleep(0)

        return chunks()

    monkeypatch.setattr(session.llm._get_async_client().aio.models, "generate_content_stream", responses)

    class Prepared:
        async def open_session(self, context, executor):
            return session

    host = await SessionHost.create(context, Prepared(), tools, MemoryOutput())
    try:
        await host.accept(Turn(event_id="one", text="Save an action", timestamp=1))
        await host.wait()
        assert host.events[-1].type == EventType.COMPLETED
        assert calls == [{"title": "SDK test"}] and len(requests) == 2
        assert session.llm.stop_sequences == original_stops
        assert host.run.text == "Saved the action." and "PRIVATE" not in host.run.text
    finally:
        await host.close()
        tools.close()


async def test_adk_native_http_retry_never_replays_an_established_stream(monkeypatch):
    pytest.importorskip("google.adk")
    from google.genai import errors

    from meetstream_agent_adapter.adapters.adk import Adapter

    monkeypatch.setenv("GOOGLE_API_KEY", "fixture-key-not-a-credential")
    tools = ToolExecutor([], set())
    session = await Adapter().open_session(
        SessionContext(session_id="retry-boundary", workspace="test", bot_id="none", profile="adk"), tools)
    attempts, midstream, preflight_status = [], False, None

    class Response:
        headers = {}

        async def __aiter__(self):
            yield {"candidates": [{"content": {"role": "model", "parts": [{"text": "Ready."}]}}]}
            if midstream:
                raise errors.ServerError(503, {"error": {"message": "Synthetic stream failure"}})

    async def request_once(request, stream=False):
        assert stream
        attempts.append(request)
        if preflight_status:
            raise errors.ClientError(preflight_status, {"error": {"message": "Synthetic quota rejection"}})
        if len(attempts) == 1 and not midstream:
            raise errors.ServerError(503, {"error": {"message": "Synthetic initial failure"}})
        return Response()

    # Keep the installed SDK's retry/stream decoding layers; control its one
    # HTTP attempt boundary only. This is deliberately version-sensitive.
    monkeypatch.setattr(session.client._api_client, "_async_request_once", request_once)
    try:
        stream = await session.client.aio.models.generate_content_stream(model="gemini-3.5-flash-lite", contents="Say ready.")
        assert [response.text async for response in stream] == ["Ready."]
        assert len(attempts) == 2
        attempts.clear()
        midstream = True
        stream = await session.client.aio.models.generate_content_stream(model="gemini-3.5-flash-lite", contents="Say ready.")
        received = []
        with pytest.raises(errors.ServerError):
            async for response in stream:
                received.append(response.text)
        assert received == ["Ready."] and len(attempts) == 1
        attempts.clear()
        preflight_status = 429
        with pytest.raises(errors.ClientError):
            stream = await session.client.aio.models.generate_content_stream(model="gemini-3.5-flash-lite", contents="Say ready.")
            async for _ in stream:
                raise AssertionError("Rejected requests must not emit a response")
        assert len(attempts) == 1
    finally:
        await session.close()
        tools.close()


@pytest.mark.parametrize("profile", ["openai", "langgraph", "adk", "crewai"])
async def test_native_slow_write_cancellation_does_not_replay(profile):
    from dataclasses import replace

    host, tools, model, writes = await build(profile)
    entered, cleaned = asyncio.Event(), asyncio.Event()
    original = tools.specs["save"]

    async def delayed(args, context, operation):
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            cleaned.set()

    tools.specs["save"] = replace(original, handler=delayed)
    try:
        await host.accept(Turn(event_id="slow-write", text="Save an action", timestamp=1))
        async with asyncio.timeout(5):
            await entered.wait()
        outcome = await host.interrupt()
        assert outcome.runtime_stopped and outcome.state_safe
        assert cleaned.is_set() and not writes
        assert tools.db.execute("SELECT status FROM operations").fetchone()[0] == "outcome_unknown"
        assert host.events[-1].type == EventType.CANCELLED
        await host.accept(Turn(event_id="recovery", text="Give the answer", timestamp=2))
        await host.wait()
        assert host.events[-1].type == EventType.COMPLETED
        assert not writes
        assert tools.db.execute("SELECT COUNT(*) FROM operations").fetchone()[0] == 1
    finally:
        await host.close()
        tools.close()


@pytest.mark.parametrize("profile", ["openai", "langgraph", "adk", "crewai"])
async def test_native_framework_cancellation_cleanup(profile):
    host, tools, model, calls = await build(profile, block=True)
    try:
        await host.accept(Turn(event_id="one", text="Save an action", timestamp=1))
        async with asyncio.timeout(5):
            await model.waiting.wait()
        outcome = await host.interrupt()
        assert outcome.output_fenced and outcome.runtime_stopped and outcome.state_safe
        async with asyncio.timeout(2):
            await model.cleaned.wait()
        assert host.events[-1].type == EventType.CANCELLED
        assert len(calls) == 1
        # The same native session can answer a subsequent turn without stale
        # invocation state or replaying its earlier write.
        model.blocking = False
        await host.accept(Turn(event_id="two", text="Confirm the saved action", timestamp=2))
        await host.wait()
        assert host.events[-1].type == EventType.COMPLETED
        assert not host.poisoned and len(calls) == 1
        assert host.history[-1]["content"] == "Saved the action."
    finally:
        await host.close()
        tools.close()
