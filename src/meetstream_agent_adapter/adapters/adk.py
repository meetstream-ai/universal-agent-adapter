from google import genai
from google.adk.agents import LlmAgent
from google.adk.agents.run_config import RunConfig, StreamingMode
from google.adk.models.google_llm import Gemini
from google.adk.runners import Runner
from google.adk.sessions import InMemorySessionService
from google.adk.tools import BaseTool
from google.genai import types

from .common import INSTRUCTIONS, StatelessSession, delta, prompt


class SharedTool(BaseTool):
    def __init__(self, spec, executor):
        super().__init__(name=spec.name, description=spec.description)
        self.spec, self.executor = spec, executor

    def _get_declaration(self):
        return types.FunctionDeclaration(name=self.name, description=self.description,
                                         parameters_json_schema=self.spec.parameters)

    async def run_async(self, *, args, tool_context):
        return await self.executor.call(self.name, args, tool_context.function_call_id)


class Adapter:
    def __init__(self, model="gemini-3.5-flash-lite", instructions=INSTRUCTIONS, llm=None):
        self.model, self.instructions, self.llm = model, instructions, llm

    async def open_session(self, context, tools):
        client = None if self.llm is not None else genai.Client(http_options=types.HttpOptions(
            timeout=60000, retry_options=types.HttpRetryOptions(
                attempts=2, initial_delay=0.25, max_delay=0.5, http_status_codes=[500, 502, 503, 504])))
        model = self.llm or Gemini(model=self.model, client=client)
        agent = LlmAgent(name="meeting_assistant", instruction=self.instructions, model=model,
                         generate_content_config=types.GenerateContentConfig(temperature=0),
                         tools=[SharedTool(s, tools) for s in tools.exposed()])
        return Session(agent, client)


class Session(StatelessSession):
    def __init__(self, agent, client=None):
        self.agent, self.client = agent, client

    async def close(self):
        if self.client:
            await self.client.aio.aclose()
            self.client.close()

    async def run_turn(self, turn, history):
        # A fresh native session avoids committing interrupted framework state.
        service = InMemorySessionService()
        session = await service.create_session(app_name="meeting_adapter", user_id="meeting", session_id=turn.turn_id)
        runner = Runner(app_name="meeting_adapter", agent=self.agent, session_service=service)
        stream = runner.run_async(user_id="meeting", session_id=session.id,
                                  new_message=types.Content(role="user", parts=[types.Part(text=prompt(turn, history))]),
                                  run_config=RunConfig(streaming_mode=StreamingMode.SSE, max_llm_calls=16))
        partial_text = ""
        try:
            async for event in stream:
                if event.author != self.agent.name or not event.content:
                    continue
                text = "".join(p.text or "" for p in event.content.parts or [] if not p.thought)
                if event.partial and text:
                    partial_text += text
                    yield delta(text)
                elif text and event.is_final_response():
                    if not partial_text:
                        yield delta(text)
                    elif text.startswith(partial_text) and len(text) > len(partial_text):
                        yield delta(text[len(partial_text):])
                    elif text != partial_text:
                        raise ValueError("ADK revised already streamed participant text")
                    partial_text = ""
        finally:
            await stream.aclose()
            await runner.close()
