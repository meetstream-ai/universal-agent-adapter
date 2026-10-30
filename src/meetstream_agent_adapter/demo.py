"""Shared demo data/tools. Writes stay in a local sandbox ledger."""

from .adapters.common import INSTRUCTIONS
from .tools import ToolSpec

DEMO_INSTRUCTIONS = INSTRUCTIONS + (
    " Use the supplied tools for project status and action items. "
    "A lookup alone does not create an action item."
    " The bridge owns runtime selection. Never claim to change runtimes yourself. "
    "If a runtime-change request reaches you, ask for a whole 'switch to NAME' command "
    "instead of performing unrelated tools."
)


def initialize_demo(executor):
    executor.db.execute("CREATE TABLE IF NOT EXISTS action_items (id TEXT PRIMARY KEY, workspace TEXT, session TEXT, title TEXT, owner TEXT)")
    executor.db.commit()


def demo_tools(executor_ref, send_chat):
    async def lookup(args, context, operation):
        return {"project_id": args["project_id"], "status": "in development", "owner": "SDK team"}

    async def action(args, context, operation):
        db = executor_ref().db
        db.execute("INSERT OR IGNORE INTO action_items VALUES (?,?,?,?,?)",
                   (operation, context.workspace, context.session_id, args["title"], args["owner"]))
        db.commit()
        return {"action_id": operation, "title": args["title"], "owner": args["owner"]}

    async def chat(args, context, operation):
        await send_chat(context.session_id, args["message"])
        return {"submitted": True, "delivery": "unconfirmed"}

    def schema(properties):
        return {"type": "object", "properties": {n: {"type": "string", "minLength": 1, "maxLength": 2000} for n in properties},
                "required": list(properties), "additionalProperties": False}

    return [ToolSpec("lookup_project_status", "Look up a project's current status.", schema(["project_id"]), lookup),
            ToolSpec("create_action_item", "Create an action item in the local demo sandbox.", schema(["title", "owner"]), action, writes=True),
            ToolSpec("send_meeting_chat", "Post an explicitly requested message to this meeting's chat.", schema(["message"]), chat, writes=True)]
