"""Optional local ngrok tunnel; its authtoken stays in the environment."""

import argparse
import asyncio
import logging
import os

from meetstream_agent_adapter.cli import load_environment


async def run(port):
    import ngrok

    tunnel = await ngrok.forward(f"127.0.0.1:{port}", authtoken_from_env=True)
    try:
        print(f"HTTPS forwarding address: {tunnel.url()}", flush=True)
        print("Keep this window open. Control + C closes the tunnel.", flush=True)
        await asyncio.Event().wait()
    finally:
        await tunnel.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--env-file", default=".env")
    parser.add_argument("--port", type=int, default=8097)
    args = parser.parse_args()
    if not 1024 <= args.port <= 65535:
        parser.error("Choose a local port from 1024 to 65535")
    logging.disable(logging.CRITICAL)
    try:
        load_environment(args.env_file)
        if not os.getenv("NGROK_AUTHTOKEN"):
            raise ValueError("Missing local ngrok authtoken")
        asyncio.run(run(args.port))
    except KeyboardInterrupt:
        pass
    except Exception as exc:
        print(f"Tunnel failed ({type(exc).__name__}). Check the local token, account and network.")
        raise SystemExit(1) from None
