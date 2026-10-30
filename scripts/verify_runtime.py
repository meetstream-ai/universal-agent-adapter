"""Explicit paid-provider verification. No meetings are joined by this script."""

import argparse
import asyncio
import json
import logging
import time
from pathlib import Path

from meetstream_agent_adapter.adapters import load_adapter
from meetstream_agent_adapter.cli import load_environment
from meetstream_agent_adapter.config import adapter_config, load_settings
from meetstream_agent_adapter.contract import SessionContext, Turn
from meetstream_agent_adapter.demo import DEMO_INSTRUCTIONS, demo_tools, initialize_demo
from meetstream_agent_adapter.host import MemoryOutput, SessionHost
from meetstream_agent_adapter.tools import ToolExecutor


async def verify(settings):
    async def chat(session, text):
        pass

    executor = None
    specs = demo_tools(lambda: executor, chat)
    executor = ToolExecutor(specs, {s.name for s in specs})
    initialize_demo(executor)
    config = adapter_config(settings, instructions=DEMO_INSTRUCTIONS)
    host = await SessionHost.create(SessionContext(session_id="provider-verification", workspace="sandbox", bot_id="none", profile=settings.profile),
                                    load_adapter(settings.profile, **config), executor, MemoryOutput(), turn_timeout=settings.turn_timeout)
    started = time.monotonic()
    try:
        await host.accept(Turn(event_id="provider-tool-test", text="Look up project adapter and create one action item titled Validate SDK assigned to Test Team. Answer in one short sentence.", timestamp=time.time()))
        await host.wait()
        deltas = [e for e in host.events if e.type.value == "assistant.text.delta"]
        items = executor.db.execute("SELECT title, owner FROM action_items").fetchall()
        writes = len(items)
        report = {"profile": settings.profile, "model": settings.model, "terminal": host.events[-1].type.value,
                  "delta_count": len(deltas), "sandbox_action_items": writes,
                  "completed_tools": [e.payload["name"] for e in host.events if e.type.value == "tool.completed"],
                  "elapsed_seconds": round(time.monotonic() - started, 3),
                  "error_code": host.events[-1].payload.get("code"),
                  "kind": "real_provider_without_meeting"}
        report["sandbox_arguments_correct"] = len(items) == 1 and tuple(v.strip().lower() for v in items[0]) == ("validate sdk", "test team")
        report["passed"] = (report["terminal"] == "turn.completed" and writes == 1 and len(deltas) >= 2
                            and report["sandbox_arguments_correct"]
                            and {"lookup_project_status", "create_action_item"}.issubset(report["completed_tools"]))
        return report
    finally:
        await host.close()
        executor.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--env-file", default=".env")
    parser.add_argument("--config", default="config.toml")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    logging.disable(logging.CRITICAL)
    load_environment(args.env_file)
    report = asyncio.run(verify(load_settings(args.config)))
    text = json.dumps(report, indent=2) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(text)
    print(text)
    raise SystemExit(0 if report["passed"] else 1)
