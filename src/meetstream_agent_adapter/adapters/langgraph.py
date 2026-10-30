import os

import httpx
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_core.tools import StructuredTool
from langchain_openai import ChatOpenAI
from langgraph.graph import END, START, MessagesState, StateGraph
from langgraph.prebuilt import ToolNode, tools_condition

from .common import INSTRUCTIONS, StatelessSession, delta


class Adapter:
    def __init__(self, model="gpt-4.1-mini", instructions=INSTRUCTIONS, chat_model=None, base_url=None, key_env="OPENAI_API_KEY", provider="openai"):
        if provider not in {"openai", "google"}:
            raise ValueError("Unsupported LangGraph model provider")
        self.model, self.instructions, self.chat_model = model, instructions, chat_model
        self.base_url, self.key_env = base_url, key_env
        self.provider = provider

    async def open_session(self, context, tools):
        native = []
        def wrap(name):
            async def invoke(**args):
                return await tools.call(name, args)
            return invoke

        for spec in tools.exposed():
            native.append(StructuredTool(name=spec.name, description=spec.description,
                                         args_schema=spec.parameters, coroutine=wrap(spec.name)))
        client = None
        try:
            if self.chat_model is not None:
                model = self.chat_model
            elif self.provider == "google":
                from langchain_google_genai import ChatGoogleGenerativeAI

                model = ChatGoogleGenerativeAI(model=self.model, include_thoughts=False, timeout=60, max_retries=1,
                                              api_key=os.environ.get("GOOGLE_API_KEY"))
                client = model  # Public aclose() owns both Google client transports.
            else:
                client = httpx.AsyncClient(timeout=60)
                model = ChatOpenAI(model=self.model, streaming=True, max_retries=0, timeout=60,
                                   base_url=self.base_url, api_key=os.environ.get(self.key_env), http_async_client=client)
            bound = model.bind_tools(native) if native else model

            async def answer(state):
                return {"messages": [await bound.ainvoke([SystemMessage(self.instructions), *state["messages"]])]}

            graph = StateGraph(MessagesState)
            graph.add_node("answer", answer)
            graph.add_node("tools", ToolNode(native))
            graph.add_edge(START, "answer")
            graph.add_conditional_edges("answer", tools_condition, {"tools": "tools", END: END})
            graph.add_edge("tools", "answer")
            return Session(graph.compile(), client)
        except BaseException:
            if client:
                await client.aclose()
            raise


class Session(StatelessSession):
    def __init__(self, graph, client=None):
        self.graph, self.client = graph, client

    async def close(self):
        if self.client:
            await self.client.aclose()

    async def run_turn(self, turn, history):
        messages = [*history, HumanMessage(f"{turn.speaker}: {turn.text}")]
        stream = self.graph.astream({"messages": messages}, config={"recursion_limit": 34}, stream_mode="messages")
        try:
            async for chunk, metadata in stream:
                if metadata.get("langgraph_node") != "answer":
                    continue
                if isinstance(chunk.content, str) and chunk.content:
                    yield delta(chunk.content)
                elif isinstance(chunk.content, list):
                    for block in chunk.content:
                        if isinstance(block, dict) and block.get("type") == "text" and block.get("text"):
                            yield delta(block["text"])
        finally:
            await stream.aclose()
