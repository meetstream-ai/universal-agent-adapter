import inspect
import json
import os
import re

# Disable SDK telemetry before importing it; no meeting data belongs in implicit traces.
os.environ.setdefault("CREWAI_DISABLE_TELEMETRY", "true")

from crewai import LLM, Agent, BaseLLM, Crew, Task
from crewai.llms.base_llm import call_stop_override
from crewai.tools import BaseTool
from crewai.types.streaming import StreamChunkType
from pydantic import PrivateAttr

from .common import INSTRUCTIONS, StatelessSession, args_model, delta, prompt


class SharedTool(BaseTool):
    _executor: object = PrivateAttr()

    def __init__(self, spec, executor):
        super().__init__(name=spec.name, description=spec.description, args_schema=args_model(spec.name, spec.parameters))
        self._executor = executor

    async def _run(self, **kwargs):
        # CrewStructuredTool builds its async callable from _run in this release.
        return json.dumps(await self._executor.call(self.name, kwargs))

    async def _arun(self, **kwargs):
        return await self._run(**kwargs)


class AnswerFilter:
    """Crew TEXT includes ReAct reasoning; expose only text after Final Answer:."""

    def __init__(self):
        self.pending = ""
        self.answer = False
        self.tail = ""
        self.closed = False

    def feed(self, text):
        if self.closed:
            return ""
        if not self.answer:
            self.pending = (self.pending + text)[-64000:]
            match = re.search(r"(?:^|\n)Final Answer:[ \t]*", self.pending)
            if not match:
                return ""
            self.answer = True
            text = self.pending[match.end():]
            self.pending = ""
        combined = self.tail + text
        fence = re.search(r"(?:^|\n)```[ \t]*(?:\n|$)", combined)
        if fence:
            self.closed, self.tail = True, ""
            return combined[:fence.start()]
        # Hold a small suffix so a split closing code fence is never spoken.
        result, self.tail = combined[:-8], combined[-8:]
        return result

    def finish(self):
        result, self.tail = self.tail, ""
        return result


class ReActLLM(BaseLLM):
    """Choose Crew's ReAct runner so its Final Answer boundary is explicit."""

    _delegate: object = PrivateAttr()

    def __init__(self, delegate):
        super().__init__(model=delegate.model, stream=True, stop=delegate.stop)
        self._delegate = delegate

    def supports_function_calling(self):
        return False

    def call(self, *args, **kwargs):
        raise RuntimeError("Use async meeting execution")

    async def acall(self, *args, **kwargs):
        # Crew scopes stop markers to this wrapper's identity. Forward that
        # scope so the provider cannot generate its own tool observations.
        with call_stop_override(self._delegate, self.stop_sequences):
            return await self._delegate.acall(*args, **kwargs)


class Adapter:
    def __init__(self, model="openai/gpt-4.1-mini", instructions=INSTRUCTIONS, llm=None):
        self.model, self.instructions, self.llm = model, instructions, llm

    async def open_session(self, context, tools):
        return Session(self.model, self.instructions, tools, self.llm)


class Session(StatelessSession):
    def __init__(self, model, instructions, tools, llm):
        self.model, self.instructions, self.tools = model, instructions, tools
        self.owns_llm = llm is None
        options = {"timeout": 60, "max_retries": 0}
        if model.startswith("gemini/"):
            from google.genai import types

            # GeminiCompletion forwards client_params, but ignores the generic
            # timeout/max_retries fields when creating its Google client.
            options["client_params"] = {"http_options": types.HttpOptions(
                timeout=60000, retry_options=types.HttpRetryOptions(attempts=1))}
            options["thinking_config"] = types.ThinkingConfig(include_thoughts=False)
        self.llm = llm or LLM(model=model, stream=True, **options)

    async def close(self):
        if not self.owns_llm:
            return
        self.owns_llm = False
        # These pinned providers expose no public close method. Close the exact
        # SDK-owned client fields, without touching a caller-supplied model.
        closed = set()
        for field in ["_async_client", "_client"]:
            client = getattr(self.llm, field, None)
            if not client or id(client) in closed:
                continue
            closed.add(id(client))
            if hasattr(client, "aio"):
                await client.aio.aclose()
            result = client.close()
            if inspect.isawaitable(result):
                await result

    async def run_turn(self, turn, history):
        specs = self.tools.exposed()
        agent = Agent(role="Meeting assistant", goal=self.instructions, backstory="Answer meeting participants concisely.",
                      llm=ReActLLM(self.llm),
                      tools=[SharedTool(s, self.tools) for s in specs],
                      verbose=False, allow_delegation=False, max_iter=16, cache=False,
                      # ponytail: retain the pinned async ReAct path; the replacement
                      # executor calls the synchronous model method in our probe.
                      executor_class="CrewAgentExecutor")
        task = Task(description=prompt(turn, history), expected_output="A participant-facing answer following the current request.", agent=agent)
        crew = Crew(agents=[agent], tasks=[task], stream=True, memory=False, cache=False, verbose=False, tracing=False)
        stream = await crew.akickoff()
        # The pinned executor uses a direct response path without tools; its
        # native no-tools prompt has no ReAct/Final Answer boundary.
        answer = AnswerFilter() if specs else None
        emitted = ""
        try:
            async for chunk in stream:
                if chunk.task_index == 0 and chunk.chunk_type == StreamChunkType.TEXT:
                    text = answer.feed(chunk.content) if answer else chunk.content
                    if text:
                        emitted += text
                        yield delta(text)
            final = stream.result.raw
            tail = answer.finish() if answer else ""
            if tail:
                emitted += tail
                yield delta(tail)
            if not emitted:
                raise ValueError("Crew omitted its participant-facing Final Answer boundary")
            elif final.strip() != emitted.strip():
                raise ValueError("Crew final answer differs from streamed answer")
        finally:
            await stream.aclose()
