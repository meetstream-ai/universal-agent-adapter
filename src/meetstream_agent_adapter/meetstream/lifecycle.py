"""Durable bot departure reconciliation and bridge session maintenance."""

import asyncio
import time
from contextlib import suppress

from meetstream import ConnectionError as PlatformConnectionError
from meetstream import MeetStreamError, NotFoundError, NotReadyError, ServerError
from meetstream import TimeoutError as PlatformTimeoutError

from .protocol import meeting_ended


async def departure_status(api, bot_id):
    """The status endpoint can stay cached at InMeeting after details say Done."""
    state = await api.bots.status(bot_id)
    detail = getattr(api.bots, 'detail', None)
    if meeting_ended(state) or detail is None:
        return state
    try:
        result = await detail(bot_id)
    except NotFoundError:
        raise
    except MeetStreamError:
        return state  # Unavailable details cannot establish departure.
    details = result.get('bot_details') if isinstance(result, dict) else None
    if isinstance(details, dict) and details.get('BotID') == bot_id:
        terminal = {'status': details.get('Status'), 'source': 'bot_details'}
        if meeting_ended(terminal):
            return terminal
    return state


async def remove_owned_bot(api, db, bot_id):
    try:
        if not meeting_ended(await departure_status(api(), bot_id)):
            try:
                async with asyncio.timeout(10):
                    await api().bots.remove(bot_id)
            except NotReadyError:
                # HTTP 202 acknowledges a pending exit; retain the durable
                # cleanup entry until independent status confirms departure.
                pass
            except (TimeoutError, MeetStreamError):
                # Removal can succeed with a lost/slow response. Check departure
                # independently and keep ownership when it is still uncertain.
                pass
            if not meeting_ended(await departure_status(api(), bot_id)):
                return False
    except NotFoundError:
        pass  # The owned bot is already absent.
    except (PlatformConnectionError, PlatformTimeoutError, ServerError):
        return False  # Retain ownership; the bounded cleanup loop retries confirmation.
    db.execute("DELETE FROM bot_cleanup WHERE bot_id=?", (bot_id,))
    db.commit()
    return True

async def maintain_sessions(records, db, api, close_record):
    while True:
        owned = {r.bot_id for r in records.values() if not r.closed}
        for bot_id, expires in db.execute("SELECT bot_id,expires FROM bot_cleanup").fetchall():
            if bot_id not in owned or expires < time.time():
                with suppress(Exception):
                    await remove_owned_bot(api, db, bot_id)
        await asyncio.sleep(30)
        for record in list(records.values()):
            if record.closed:
                continue
            if time.time() > record.expires:
                with suppress(Exception):
                    await close_record(record, remove=True)
            elif record.created_bot and record.bot_id:
                try:
                    status = await api().bots.status(record.bot_id)
                    record.lifecycle = str(status.get("event") or status.get("bot_event") or status.get("bot_status") or status.get("status") or record.lifecycle)
                    if meeting_ended(status):
                        db.execute("DELETE FROM bot_cleanup WHERE bot_id=?", (record.bot_id,))
                        db.commit()
                        await close_record(record)
                except Exception:
                    record.input_error = "status_reconciliation_failed"
