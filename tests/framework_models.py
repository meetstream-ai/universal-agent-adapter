"""Shared controlled models for conformance tests and the stress benchmark.

Test-only: production adapters and native framework runners are unchanged.
"""

import asyncio

import pytest

from meetstream_agent_adapter.contract import SessionContext
from meetstream_agent_adapter.host import MemoryOutput, SessionHost
from meetstream_agent_adapter.tools import ToolExecutor, ToolSpec

SCHEMA = {"type": "object", "properties": {"title": {"type": "string"}},
          "required": ["title"], "additionalProperties": False}


def openai_model(block=False):
    pytest.importorskip("agents")
    from agents.models.interface import Model
    from openai.types.responses import (
        Response,
        ResponseCompletedEvent,
        ResponseFunctionToolCall,
        ResponseOutputMessage,
        ResponseOutputText,
        ResponseTextDeltaEvent,
    )

    class Controlled(Model):
        def __init__(self):
            self.calls = 0
            self.blocking = block
            self.waiting, self.cleaned = asyncio.Event(), asyncio.Event()

        async def get_response(self, *args, **kwargs):
            raise AssertionError("Native streaming required")

        async def stream_response(self, *args, **kwargs):
            self.calls += 1
            if self.calls == 1:
                output = [ResponseFunctionToolCall(type="function_call", name="save", arguments='{"title":"SDK test"}', call_id="native-call", id="fc")]
            else:
                try:
                    for n, text in enumerate(["Saved ", "the action."]):
                        yield ResponseTextDeltaEvent(type="response.output_text.delta", content_index=0, delta=text,
                            item_id="msg", logprobs=[], output_index=0, sequence_number=n)
                        await asyncio.sleep(0)
                    if self.blocking:
                        self.waiting.set()
                        await asyncio.Event().wait()
                    output = [ResponseOutputMessage(type="message", id="msg", role="assistant", status="completed",
                        content=[ResponseOutputText(type="output_text", text="Saved the action.", annotations=[], logprobs=[])])]
                finally:
                    self.cleaned.set()
            response = Response(id=f"r{self.calls}", created_at=0, model="controlled", object="response", output=output,
                                parallel_tool_calls=False, tool_choice="auto", tools=[], status="completed")
            yield ResponseCompletedEvent(type="response.completed", response=response, sequence_number=3)

    return Controlled()


def langgraph_model(block=False):
    pytest.importorskip("langgraph")
    from langchain_core.language_models.chat_models import BaseChatModel
    from langchain_core.messages import AIMessageChunk
    from langchain_core.outputs import ChatGenerationChunk
    from pydantic import PrivateAttr

    class Controlled(BaseChatModel):
        _calls: int = PrivateAttr(default=0)
        blocking: bool = block
        waiting: object = None
        cleaned: object = None

        @property
        def _llm_type(self):
            return "controlled"

        def _generate(self, *args, **kwargs):
            raise AssertionError("Native async streaming required")

        def bind_tools(self, tools, **kwargs):
            return self

        async def _astream(self, messages, stop=None, run_manager=None, **kwargs):
            self._calls += 1
            if self._calls == 1:
                yield ChatGenerationChunk(message=AIMessageChunk(content="", tool_call_chunks=[{
                    "name": "save", "args": '{"title":"SDK test"}', "id": "native-call", "index": 0}]))
            else:
                try:
                    for text in ["Saved ", "the action."]:
                        yield ChatGenerationChunk(message=AIMessageChunk(content=text))
                        await asyncio.sleep(0)
                    if self.blocking:
                        self.waiting.set()
                        await asyncio.Event().wait()
                finally:
                    self.cleaned.set()

    return Controlled(waiting=asyncio.Event(), cleaned=asyncio.Event())


def adk_model(block=False):
    pytest.importorskip("google.adk")
    from google.adk.models import BaseLlm, LlmResponse
    from google.genai import types
    from pydantic import PrivateAttr

    class Controlled(BaseLlm):
        _calls: int = PrivateAttr(default=0)
        blocking: bool = block
        waiting: object = None
        cleaned: object = None

        async def generate_content_async(self, llm_request, stream=False):
            assert stream
            assert llm_request.config.temperature == 0
            self._calls += 1
            if self._calls == 1:
                yield LlmResponse(content=types.Content(role="model", parts=[types.Part(function_call=types.FunctionCall(
                    id="native-call", name="save", args={"title": "SDK test"}))]))
            else:
                try:
                    for text in ["Saved ", "the action."]:
                        yield LlmResponse(content=types.Content(role="model", parts=[types.Part(text=text)]), partial=True)
                        await asyncio.sleep(0)
                    if self.blocking:
                        self.waiting.set()
                        await asyncio.Event().wait()
                    yield LlmResponse(content=types.Content(role="model", parts=[types.Part(text="Saved the action.")]), partial=False)
                finally:
                    self.cleaned.set()

    return Controlled(model="controlled", waiting=asyncio.Event(), cleaned=asyncio.Event())


def crewai_model(block=False):
    pytest.importorskip("crewai")
    from crewai import BaseLLM
    from pydantic import PrivateAttr

    class Controlled(BaseLLM):
        _calls: int = PrivateAttr(default=0)
        blocking: bool = block
        waiting: object = None
        cleaned: object = None

        def call(self, *args, **kwargs):
            raise AssertionError("Native async runner required")

        async def acall(self, *args, from_task=None, from_agent=None, **kwargs):
            self._calls += 1
            if self._calls == 1:
                response = 'Thought: PRIVATE REASONING\nAction: save\nAction Input: {"title":"SDK test"}'
                self._emit_stream_chunk_event(response, from_task=from_task, from_agent=from_agent)
                return response
            try:
                for text in ["Thought: PRIVATE\nFinal Ans", "wer: Saved ", "the action."]:
                    self._emit_stream_chunk_event(text, from_task=from_task, from_agent=from_agent)
                    await asyncio.sleep(0.005)
                if self.blocking:
                    self.waiting.set()
                    await asyncio.Event().wait()
                return "Thought: PRIVATE\nFinal Answer: Saved the action."
            finally:
                self.cleaned.set()

    return Controlled(model="controlled", stream=True, waiting=asyncio.Event(), cleaned=asyncio.Event())


async def build(profile, block=False):
    from meetstream_agent_adapter.adapters import load_adapter
    model = globals()[profile + "_model"](block)
    count = []

    async def save(args, ctx, op):
        count.append((args, ctx.session_id))
        return {"saved": True}

    tools = ToolExecutor([ToolSpec("save", "Save one action", SCHEMA, save, writes=True)], {"save"})
    kwargs = {"openai": {"model": model}, "langgraph": {"chat_model": model}, "adk": {"llm": model}, "crewai": {"llm": model}}[profile]
    host = await SessionHost.create(SessionContext(session_id="framework-test", workspace="test", bot_id="bot", profile=profile),
                                    load_adapter(profile, **kwargs), tools, MemoryOutput(), turn_timeout=10)
    return host, tools, model, count
