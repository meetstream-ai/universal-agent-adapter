"""Illustrative wiring for a MIA owner with access to the actual speech pipeline.

No hosted endpoint is invented. The caller supplies authorized callbacks.
"""

from meetstream_agent_adapter.adapters import load_adapter
from meetstream_agent_adapter.contract import SessionContext
from meetstream_agent_adapter.host import SessionHost
from meetstream_agent_adapter.native import NativeMIAOutput


async def attach_mia(context: SessionContext, tools, deliver, clear_playback, pending, profile="openai", **config):
    output = NativeMIAOutput(deliver, clear_playback, pending)
    host = await SessionHost.create(context, load_adapter(profile, **config), tools, output)
    # MIA owner wires finalized turns -> host.accept(turn), speech onset ->
    # host.interrupt(), and session end -> host.close(). Only this host reasons.
    return host
