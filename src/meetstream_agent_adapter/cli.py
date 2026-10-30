import argparse
import asyncio
import importlib.metadata
import json
import logging
import os
import time
from contextlib import suppress
from pathlib import Path

import httpx

from .adapters import load_adapter
from .config import RUNTIME_PACKAGES, load_settings, model_credential
from .contract import SessionContext, Turn
from .host import MemoryOutput, SessionHost
from .meetstream.output import SPEECH_KEYS
from .tools import ToolExecutor, ToolSpec


def load_environment(path):
    if path:
        if not Path(path).is_file():
            raise ValueError("The explicitly selected environment file does not exist")
        # dotenv is deliberately explicit; never search sibling projects for secrets.
        from dotenv import load_dotenv
        load_dotenv(path, override=False)


async def offline_demo(friendly=False):
    async def lookup(args, context, operation):
        return {"project": args["project_id"], "status": "in development"}

    tool = ToolSpec("lookup_project_status", "Lookup a demo project", {
        "type": "object", "properties": {"project_id": {"type": "string"}},
        "required": ["project_id"], "additionalProperties": False}, lookup)
    tools = ToolExecutor([tool], {tool.name})
    host = await SessionHost.create(SessionContext(session_id="offline", workspace="demo", bot_id="offline", profile="fake"),
                                    load_adapter("fake"), tools, MemoryOutput())
    try:
        await host.accept(Turn(event_id="offline-1", text="Project status?", speaker="Tester", timestamp=time.time()))
        await host.wait()
        if friendly:
            print("Offline demo: no meeting, credentials or paid AI service needed.")
            for event in host.events:
                if event.type.value == "tool.completed":
                    print("Project lookup: succeeded.")
                elif event.type.value == "assistant.message.completed":
                    print("Assistant: " + event.payload["text"])
                elif event.type.value == "turn.completed":
                    print("Finished successfully. The demo used the offline test runtime.")
        else:
            for event in host.events:
                print(event.model_dump_json())
    finally:
        await host.close()
        tools.close()


async def manage(args):
    key = os.environ.get("ADAPTER_API_KEY")
    if not key:
        raise ValueError("ADAPTER_API_KEY is required")
    async with httpx.AsyncClient(base_url=args.server, headers={"Authorization": f"Bearer {key}"}, timeout=30) as client:
        if args.command == "launch":
            # Reserve the destination before creating a remote bot. An existing
            # file must fail before any external side effect.
            path = Path(args.session_file) if args.session_file else None
            output = os.fdopen(os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "w") if path else None
            result = None
            try:
                response = await client.post("/v1/sessions", json={"meeting_link": args.meeting})
                response.raise_for_status()
                result = response.json()
                if output:
                    json.dump(result, output)
                    output.flush()
            except BaseException:
                if result and result.get("session_id"):
                    with suppress(Exception):
                        await client.post(f"/v1/sessions/{result['session_id']}/stop")
                if path:
                    path.unlink(missing_ok=True)
                raise
            finally:
                if output:
                    output.close()
            result.pop("session_token", None)
        else:
            route = f"/v1/sessions/{args.session}"
            if args.command == "switch":
                body = {"profile": args.profile}
                if args.model:
                    body["model"] = args.model
                if args.provider:
                    body["provider"] = args.provider
                response = await client.post(route + "/runtime", json=body)
            elif args.command in {"stop", "interrupt"}:
                response = await client.post(route + "/" + args.command)
            else:
                response = await client.get(route + ("/events" if args.command == "events" else ""))
            response.raise_for_status()
            result = response.json()
        print(json.dumps(result, indent=2))


def doctor(settings):
    installed = {}
    for profile, package in RUNTIME_PACKAGES.items():
        try:
            installed[profile] = importlib.metadata.version(package)
        except importlib.metadata.PackageNotFoundError:
            installed[profile] = None
    needed = ["ADAPTER_API_KEY"]
    if settings.output != "events":
        needed.append("MEETSTREAM_API_KEY")
    if credential := model_credential(settings):
        needed.append(credential)
    if settings.output == "voice_and_chat":
        needed.append("SPEECH_API_KEY" if os.getenv("SPEECH_API_KEY") else SPEECH_KEYS[settings.speech_provider])
    if settings.output != "events" and settings.transcript_source == "deepgram":
        needed.append("DEEPGRAM_API_KEY")
    needed = list(dict.fromkeys(needed))
    management_key = os.getenv("ADAPTER_API_KEY", "")
    checks = {"management_key_length": len(management_key) >= 32,
              "management_key_not_template": management_key != "replace-with-at-least-32-random-characters",
              "runtime_installed": settings.profile == "fake" or installed[settings.profile] is not None}
    if settings.output != "events":
        checks["public_https_origin"] = bool(settings.public_url)
    print(json.dumps({"profile": settings.profile, "packages": installed, "checks": checks,
                      "credential_present": {k: bool(os.getenv(k)) for k in needed}}, indent=2))
    return 0 if all(checks.values()) and all(os.getenv(k) for k in needed) else 1


def main():
    parser = argparse.ArgumentParser(description="Common external-agent meeting bridge")
    parser.add_argument("--env-file", help="Explicit local dotenv path (never printed)")
    parser.add_argument("--config", default="config.toml")
    sub = parser.add_subparsers(dest="command", required=True)
    serve = sub.add_parser("serve")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8080)
    sub.add_parser("doctor")
    demo = sub.add_parser("demo")
    demo.add_argument("--friendly", action="store_true", help="Print a plain-language offline result")
    start = sub.add_parser("start", help="Guided setup; one window for the service, tunnel and meeting bot")
    start.add_argument("--mode", choices=["demo", "meeting", "mia", "service"],
                       help="meeting (default streaming pilot), mia (hosted tool mode), demo or service")
    start.add_argument("--runtime", choices=["adk", "langgraph", "openai", "crewai"])
    start.add_argument("--allow-openai", action="store_true", default=True,
                       help="Allow OpenAI runtime switches (the default; requires OPENAI_API_KEY)")
    start.add_argument("--no-openai", dest="allow_openai", action="store_false",
                       help="Explicitly restrict this run to Google-backed runtimes")
    start.add_argument("--model")
    start.add_argument("--meeting")
    start.add_argument("--public-url")
    start.add_argument("--port", type=int, default=0, help="0 selects an unused local port")
    for command in ["launch", "status", "events", "interrupt", "stop", "switch"]:
        child = sub.add_parser(command)
        child.add_argument("--server", default="http://127.0.0.1:8080")
        if command == "launch":
            child.add_argument("--meeting", required=True)
            child.add_argument("--session-file", help="Create a new private file containing session callback token")
        else:
            child.add_argument("session", **({"nargs": "?"} if command == "stop" else {}))
        if command == "switch":
            child.add_argument("--profile", required=True, choices=["adk", "langgraph", "openai", "crewai"])
            child.add_argument("--model")
            child.add_argument("--provider", choices=["openai", "google"])
    args = parser.parse_args()
    if args.command == "start" and not 0 <= args.port <= 65535:
        parser.error("Choose a port from 0 to 65535 (0 selects an unused port)")
    try:
        load_environment(args.env_file)
        if args.command == "demo":
            asyncio.run(offline_demo(args.friendly))
        elif args.command == "start":
            from .quickstart import start
            raise SystemExit(start(args))
        elif args.command == "stop" and args.session is None:
            from .quickstart import stop
            asyncio.run(stop())
        elif args.command == "doctor":
            raise SystemExit(doctor(load_settings(args.config)))
        elif args.command == "serve":
            import uvicorn

            from .server import create_app
            for logger in ["google.adk", "google.genai", "agents", "crewai", "langchain", "httpx"]:
                logging.getLogger(logger).setLevel(logging.CRITICAL)
            # Disable access logs: callback URLs contain short-lived bearer tokens.
            uvicorn.run(create_app(load_settings(args.config)), host=args.host, port=args.port,
                        access_log=False, log_level="warning", ws_max_size=262144, timeout_graceful_shutdown=20)
        else:
            asyncio.run(manage(args))
    except (KeyboardInterrupt, EOFError):
        print("Stopped.")
    except (ValueError, OSError, RuntimeError, ImportError, httpx.HTTPError) as exc:
        # Never print provider response bodies, credential-bearing URLs or headers.
        parser.exit(1, f"Command failed ({type(exc).__name__}). Check configuration and authenticated server status.\n")


if __name__ == "__main__":
    main()
