"""In-process output boundary for a caller-owned MIA pipeline."""

from collections.abc import Awaitable, Callable

from .contract import AgentEvent


class NativeMIAOutput:
    """MIA supplies streaming text delivery and playback cancellation callbacks.

    Callbacks must recheck `active` immediately before submitting media, and
    `clear_playback` must resolve only after obsolete queued output is fenced.
    This is an integration boundary, not an undocumented hosted MIA endpoint.
    """

    def __init__(self, deliver: Callable[[AgentEvent, Callable[[], bool]], Awaitable[None]],
                 clear_playback: Callable[[], Awaitable[None]], pending: Callable[[], bool]):
        self.deliver, self.clear_playback, self.pending = deliver, clear_playback, pending

    async def event(self, event, active):
        await self.deliver(event, active)

    async def interrupt(self):
        await self.clear_playback()

    def delivery_pending(self):
        return self.pending()

    async def close(self):
        await self.clear_playback()
