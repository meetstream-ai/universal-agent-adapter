"""Explicit offline demo runtime. Never selected as a provider fallback."""

import asyncio

from .common import StatelessSession, delta


class Adapter:
    def __init__(self, **kwargs):
        pass

    async def open_session(self, context, tools):
        return Session(tools)


class Session(StatelessSession):
    def __init__(self, tools):
        self.tools = tools

    async def run_turn(self, turn, history):
        result = await self.tools.call("lookup_project_status", {"project_id": "adapter"})
        for text in ["The adapter project is ", result["data"]["status"] + ".", " Tools share one interface."]:
            await asyncio.sleep(0.01)
            yield delta(text)
