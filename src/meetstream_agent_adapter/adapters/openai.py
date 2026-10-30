import json
import os

from agents import Agent, FunctionTool, ModelSettings, RunConfig, Runner
from agents.models.openai_chatcompletions import OpenAIChatCompletionsModel
from agents.models.openai_responses import OpenAIResponsesModel
from openai import AsyncOpenAI

from .common import INSTRUCTIONS, StatelessSession, delta


class Adapter:
    def __init__(self, model="gpt-4.1-mini", instructions=INSTRUCTIONS, agent=None,
                 base_url=None, key_env="OPENAI_API_KEY", model_api="responses"):
        self.model, self.instructions, self.agent = model, instructions, agent
        self.base_url, self.key_env, self.model_api = base_url, key_env, model_api

    async def open_session(self, context, tools):
        native = []
        for spec in tools.exposed():
            async def invoke(ctx, arguments, name=spec.name):
                return json.dumps(await tools.call(name, json.loads(arguments), ctx.tool_call_id))
            native.append(FunctionTool(name=spec.name, description=spec.description,
                                       params_json_schema=spec.parameters, on_invoke_tool=invoke,
                                       strict_json_schema=False))
        client, model = None, self.model
        if isinstance(model, str) and self.agent is None:
            client = AsyncOpenAI(api_key=os.environ.get(self.key_env), base_url=self.base_url, max_retries=0, timeout=60)
            model_cls = OpenAIChatCompletionsModel if self.model_api == "chat_completions" else OpenAIResponsesModel
            model = model_cls(model=model, openai_client=client)
        agent = self.agent or Agent(name="meeting_assistant", instructions=self.instructions,
                                   model=model, tools=native, model_settings=ModelSettings(parallel_tool_calls=False))
        return Session(agent, client)


class Session(StatelessSession):
    def __init__(self, agent, client=None):
        self.agent, self.client = agent, client

    async def close(self):
        if self.client:
            await self.client.close()

    async def run_turn(self, turn, history):
        stream = Runner.run_streamed(self.agent, [*history, {"role": "user", "content": f"{turn.speaker}: {turn.text}"}],
                                     max_turns=16, run_config=RunConfig(tracing_disabled=True))
        iterator = stream.stream_events()
        try:
            async for event in iterator:
                if event.type == "agent_updated_stream_event" and event.new_agent is not self.agent:
                    raise ValueError("Handoffs need an explicit participant answer selection policy")
                if event.type == "raw_response_event" and event.data.type == "response.output_text.delta":
                    if event.data.delta:
                        yield delta(event.data.delta)
        finally:
            stream.cancel(mode="immediate")
            await iterator.aclose()
