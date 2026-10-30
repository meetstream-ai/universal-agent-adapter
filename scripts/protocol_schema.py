"""Generate the public protocol artifact; --check detects contract drift."""

import argparse
import json
from pathlib import Path

from pydantic.json_schema import models_json_schema

from meetstream_agent_adapter.contract import AgentEvent, Capabilities, SessionContext, Turn


def protocol_schema():
    models = [AgentEvent, Turn, SessionContext, Capabilities]
    _, schema = models_json_schema([(model, "validation") for model in models])
    return {"$schema": "https://json-schema.org/draft/2020-12/schema",
            "$id": "urn:meetstream:agent-adapter:protocol:1", "title": "MeetStream agent adapter protocol v1",
            **schema, "oneOf": [{"$ref": f"#/$defs/{model.__name__}"} for model in models]}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    path = Path(__file__).resolve().parents[1] / "docs/protocol-v1.schema.json"
    schema = protocol_schema()
    if args.check:
        if json.loads(path.read_text()) != schema:
            parser.exit(1, "Protocol schema drifted; run scripts/protocol_schema.py to regenerate it.\n")
    else:
        path.write_text(json.dumps(schema, indent=2) + "\n")
