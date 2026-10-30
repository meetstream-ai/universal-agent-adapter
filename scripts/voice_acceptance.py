"""Finite Google-only meeting voice acceptance with independent listening.

Run against a disposable meeting. The host admits the two test bots. No OpenAI
key is loaded, and both owned bots/configuration are removed on completion.
"""

import argparse
import asyncio
import base64
import json
import logging
import os
import secrets
import time
from dataclasses import replace
from pathlib import Path

from dotenv import dotenv_values
from fastapi import WebSocket
from live_meeting_demo import stop_owned_bot, wait_for, wait_for_departure
from meetstream import AsyncMeetStream

from meetstream_agent_adapter import quickstart
from meetstream_agent_adapter.config import Settings, validate_meeting_link
from meetstream_agent_adapter.contract import EventType
from meetstream_agent_adapter.meetstream.output import DeepgramSpeech, pcm_48k
from meetstream_agent_adapter.meetstream.protocol import acknowledgement, decode_audio, legacy_audio
from meetstream_agent_adapter.meetstream.transcription import DeepgramInput
from meetstream_agent_adapter.pilot import private_json


def acceptance_checks(report):
    """Positive evidence for every requirement, never the absence of exceptions."""
    required = (
        'initial_tools_and_voice', 'target_tools_and_voice', 'same_bot_and_session',
        'runtime_switch_spoken', 'misheard_switch_spoken', 'unknown_switch_rejected', 'pending_tool_cancelled', 'speech_interrupted',
        'fresh_turn_recovered', 'owned_cleanup_confirmed')
    # Hosted MIA owns its voice-control recognition; this bridge's parser only
    # handles the default streaming path. Never claim that guard for hosted MIA.
    return {name: report.get(name) is True for name in required
            if report.get('mode') != 'mia' or name != 'unknown_switch_rejected'}


def canonical(text):
    return ''.join(char for char in text.casefold() if char.isalnum())


def stop_acknowledged(turn, since):
    # ASR can drop the final consonants in "Stopped". Require an exact
    # acknowledgement from the assistant, never the participant's stop command.
    return (turn['at'] >= since and turn['speaker'] in {'Adapter SDK Test', 'Adapter MIA Pilot'}
            and canonical(turn['text']) in {'stop', 'stopped'})


async def run(args):
    validate_meeting_link(args.meeting)
    values = dotenv_values(args.env_file)
    # Explicitly exclude every OpenAI credential from this test process.
    for name in list(os.environ):
        if name.startswith('OPENAI_'):
            os.environ.pop(name)
    for name in ('MEETSTREAM_API_KEY', 'GOOGLE_API_KEY', 'DEEPGRAM_API_KEY', *(() if args.public_url else ('NGROK_AUTHTOKEN',))):
        os.environ[name] = values.get(name) or os.environ.get(name, '')
        if not os.environ[name]:
            raise ValueError('Missing ' + name)
    os.environ['ADAPTER_API_KEY'] = secrets.token_urlsafe(40)
    os.environ['MIA_BRIDGE_TOKEN'] = secrets.token_urlsafe(40)
    directory = Path('.adapter') / ('voice-acceptance-' + secrets.token_hex(8))
    directory.mkdir(parents=True, mode=0o700)
    initial_profile = 'langgraph' if args.runtime == 'adk' else 'adk'
    names = {'adk': ('Google A D K', 'a dee kay'), 'langgraph': ('LangGraph', 'land graph'), 'crewai': ('Crew AI', 'crew eye')}
    report = {'kind': 'google_meeting_voice_acceptance', 'mode': args.mode, 'passed': False, 'stage': 'starting',
              'initial_profile': initial_profile, 'target_profile': args.runtime,
              'openai_calls': 0, 'heard': [], 'commands': [], 'inputs': [], 'chat': [], 'cleanup': [], 'directory': str(directory)}
    target = Path(args.report)
    target.parent.mkdir(parents=True, exist_ok=True)
    apps, speaker_id, speaker_ws = [], None, None
    bound = asyncio.Event()
    nonce = secrets.token_urlsafe(32)
    original = quickstart.create_app
    speaker_stream_error = None
    recent_words = []
    speaking = False

    def save():
        if apps and apps[0].state.records:
            records = list(apps[0].state.records.values())
            for record in records:
                if record.output and not getattr(record.output, '_acceptance_observed', False):
                    output = record.output
                    original_send = output.send
                    async def observe(command, active=lambda: True, send=original_send, current=record):
                        generating = bool(current.host and current.host.run and not current.host.run.task.done())
                        await send(command, active)
                        if command.get('command') == 'sendaudio' and active():
                            report['audio_chunks_delivered'] = report.get('audio_chunks_delivered', 0) + 1
                            report['audio_while_generating'] = report.get('audio_while_generating', False) or generating
                        if command.get('command') == 'interrupt':
                            report['playback_clears'] = report.get('playback_clears', 0) + 1
                        if command.get('command') == 'sendmsg' and active():
                            report['chat'].append(command.get('message') or command.get('msg'))
                    output.send = observe
                    output._acceptance_observed = True
                if record.transcription and not getattr(record.transcription, '_acceptance_observed', False):
                    forward = record.transcription.on_turn
                    def capture(turn, forward=forward):
                        report['inputs'].append({'text': turn.text, 'speaker': turn.speaker})
                        forward(turn)
                    record.transcription.on_turn = capture
                    receive = record.transcription.result
                    async def observe_stt(message, receive=receive, current=record.transcription):
                        if message.get('type') in {'Results', 'UtteranceEnd'}:
                            channel = message.get('channel', {})
                            alternatives = channel.get('alternatives', []) if isinstance(channel, dict) else []
                            alternative = alternatives[0] if alternatives else {}
                            trace = report.setdefault('input_boundaries', [])
                            trace.append({k: message.get(k) for k in ('type', 'start', 'duration', 'is_final', 'last_word_end')} | {
                                'text': alternative.get('transcript'), 'words': alternative.get('words', []),
                                'latest_word_end_before': current.latest_word_end})
                            del trace[:-128]
                        try:
                            await receive(message)
                        except Exception as exc:
                            report['transcription_exception'] = type(exc).__name__
                            if isinstance(exc, ValueError):
                                report['transcription_validation_error'] = str(exc)
                            save()
                            raise
                    record.transcription.result = observe_stt
                    on_error = record.transcription.on_error
                    async def observe_error(on_error=on_error, current=record.transcription):
                        reason = getattr(current.socket, 'close_reason', '') or ''
                        for value in values.values():
                            if value:
                                reason = reason.replace(value, '[redacted]')
                        report['transcription_failure'] = current.last_error
                        report['transcription_close'] = {'code': getattr(current.socket, 'close_code', None), 'reason': reason}
                        save()
                        await on_error()
                    record.transcription.on_error = observe_error
                    record.transcription._acceptance_observed = True
            report['sessions'] = [{'id': r.id, 'profile': r.host.context.profile} for r in records if r.host]
            report['events'] = [e.model_dump(mode='json') for r in records if r.host for e in r.host.events]
            report['history'] = [entry for r in records if r.host for entry in r.host.history]
        if apps:
            report['diagnostics'] = [{'closed': r.closed, 'input_error': r.input_error,
                'audio_connected': r.audio_socket is not None, 'control_connected': r.output.socket is not None if r.output else False,
                'stt_messages': r.transcription.messages_received if r.transcription else 0,
                'metrics': dict(r.host.metrics) if r.host else {}} for r in apps[0].state.records.values()]
        report['observer_stt_messages'] = stt.messages_received
        report['observer_pending_text'] = list(stt.parts)
        private_json(target, report)

    async def failed_audio():
        nonlocal speaker_stream_error
        speaker_stream_error = 'observer_stt_failed'

    def heard(turn):
        report['heard'].append({'text': turn.text, 'speaker': turn.speaker, 'at': time.monotonic()})
        save()

    class Observer(DeepgramInput):
        async def result(self, message):
            if message.get('type') == 'Results':
                alternatives = message.get('channel', {}).get('alternatives', [])
                words = alternatives[0].get('words', []) if alternatives else []
                recent_words.extend((w.get('word', ''), time.monotonic()) for w in words)
            await super().result(message)

    stt = Observer(os.environ['DEEPGRAM_API_KEY'], heard, failed_audio, bot_name='SDK Finite Voice Test')

    def application(settings, **kwargs):
        app = original(settings, **kwargs)
        apps.append(app)
        app.state.acceptance_origin = settings.public_url

        async def connection(ws, token, path_bot, audio):
            nonlocal speaker_ws, speaker_stream_error
            if not secrets.compare_digest(token, nonce):
                await ws.close(code=1008)
                return
            await ws.accept()
            try:
                first = await ws.receive_json()
                async with asyncio.timeout(20):
                    await bound.wait()
                if first.get('type') != 'ready' or first.get('bot_id') != speaker_id or (path_bot and path_bot != speaker_id):
                    await ws.close(code=1008)
                    return
                # Audio and control channels deliberately use different ACK keys.
                await ws.send_json(acknowledgement(speaker_id, audio=audio))
                if not audio:
                    speaker_ws = ws
                while True:
                    frame = await ws.receive()
                    if frame['type'] == 'websocket.disconnect':
                        break
                    if audio and frame.get('text'):
                        data = json.loads(frame['text'])
                        report.setdefault('audio_text_types', {})[data.get('type', 'unknown')] = list(data)
                        if (pcm := legacy_audio(data)) is not None:
                            frame = {'bytes': pcm}
                    if audio and frame.get('bytes'):
                        kind = str(frame['bytes'][0]) if frame['bytes'] else 'empty'
                        counts = report.setdefault('audio_binary_types', {})
                        counts[kind] = counts.get(kind, 0) + 1
                        report.setdefault('audio_binary_sizes', {})[kind] = len(frame['bytes'])
                        if frame['bytes'][0] == 4 and len(frame['bytes']) >= 6:
                            report['observed_audio_rate'] = int.from_bytes(frame['bytes'][1:5], 'little')
                            report['observed_audio_channels'] = frame['bytes'][5]
                    if audio and frame.get('bytes') and (decoded := decode_audio(frame['bytes'])):
                        report['received_audio_frames'] = report.get('received_audio_frames', 0) + 1
                        identities = report.setdefault('observed_speakers', {})
                        identities[decoded[1]] = identities.get(decoded[1], 0) + 1
                        if (decoded[0] != speaker_id and decoded[1] != 'SDK Finite Voice Test'
                                and not (speaking and decoded[1] in {'', 'NoSpeaker'})):
                            await stt.audio(frame['bytes'])
            except Exception as exc:
                speaker_stream_error = type(exc).__name__
            finally:
                if not audio:
                    speaker_ws = None

        @app.websocket('/voice-test-control/{token}')
        @app.websocket('/{path_bot}/voice-test-control/{token}')
        async def control(ws: WebSocket, token: str, path_bot: str | None = None):
            await connection(ws, token, path_bot, False)

        @app.websocket('/voice-test-audio/{token}')
        @app.websocket('/{path_bot}/voice-test-audio/{token}')
        async def audio(ws: WebSocket, token: str, path_bot: str | None = None):
            await connection(ws, token, path_bot, True)
        return app

    quickstart.create_app = application
    settings = Settings(public_url=args.public_url or '', profile=initial_profile, model='gemini-3.5-flash-lite', model_provider='google',
                        output='events' if args.mode == 'mia' else 'voice_and_chat',
                        transcript_source='deepgram', wake_words=['okay assistant'], voice_switching=True,
                        speech_provider='deepgram', speech_model='aura-2-thalia-en', ledger=str(directory / 'actions.sqlite'))
    task = asyncio.create_task(quickstart.run(settings, args.meeting, args.port, mia=args.mode == 'mia', pilot_env=args.env_file, allow_openai=False))
    api = AsyncMeetStream(timeout=15, max_retries=0)
    speech = DeepgramSpeech(os.environ['DEEPGRAM_API_KEY'])
    async def snapshots():
        while True:
            save()
            await asyncio.sleep(1)

    snapshotting = asyncio.create_task(snapshots())
    slow = False
    entered, cancelled = asyncio.Event(), asyncio.Event()

    async def say(text):
        nonlocal speaking
        report['commands'].append(text)
        save()
        pcm = b''.join([chunk async for chunk in speech.pcm(text)])
        # Submit the finite prerecorded participant utterance atomically. It is
        # still heard/transcribed over the real meeting; assistant output streams.
        chunks = [pcm]
        speaking = True
        try:
            for chunk in chunks:
                if not speaker_ws:
                    raise RuntimeError('Test speaker is disconnected')
                started = time.monotonic()
                await speaker_ws.send_json({'command': 'sendaudio', 'bot_id': speaker_id,
                    'audiochunk': base64.b64encode(pcm_48k(chunk)).decode(), 'sample_rate': 48000,
                    'encoding': 'pcm16', 'channels': 1, 'endianness': 'little'})
                await asyncio.sleep(max(0, len(chunk) / 48000 - (time.monotonic() - started)))
        finally:
            speaking = False

    def record():
        records = list(apps[0].state.records.values())
        return records[0] if len(records) == 1 and records[0].host else None

    async def wait_heard(fragment, since, seconds=70, *, primary_only=False):
        return await wait_for(lambda: any(canonical(fragment) in canonical(h['text']) and h['at'] >= since
                                          and (not primary_only or h['speaker'] in {'Adapter SDK Test', 'Adapter MIA Pilot'})
                                          for h in report['heard']), seconds, 'audible response: ' + fragment)

    async def tool_voice(title, owner):
        started = time.monotonic()
        previous = record().host.run.turn.turn_id if record() and record().host.run else None
        await say(f'Okay assistant, perform a fresh lookup of project adapter status using the lookup tool and create one action item titled {title} assigned to {owner}. Confirm the title and owner aloud.')
        await wait_for(lambda: record() and record().host.run and record().host.run.turn.turn_id != previous
                       and record().host.run.task.done(), 90, 'reasoning completion')
        await wait_heard(title, started, primary_only=True)
        rows = apps[0].state.executor.db.execute('SELECT title,owner FROM action_items WHERE session=?', (record().id,)).fetchall()
        assert sum(canonical(t) == canonical(title) and canonical(o) == canonical(owner) for t, o in rows) == 1
        events = [e for e in record().host.events if e.turn_id == record().host.run.turn.turn_id]
        assert {e.payload.get('name') for e in events if e.type == EventType.TOOL_COMPLETED} >= {'lookup_project_status', 'create_action_item'}
        assert sum(e.type == EventType.DELTA for e in events) >= 2

    async def spoken_switch(profile, name):
        revision = record().host.metrics['runtime_switches']
        started = time.monotonic()
        await say('Okay assistant, switch the runtime to ' + name + '.')
        await wait_for(lambda: record().host.context.profile == profile
                       and record().host.metrics['runtime_switches'] > revision, 70, 'spoken switch to ' + profile)
        if args.mode == 'mia':
            await wait_heard(profile, started)

    try:
        await wait_for(lambda: task.done() or apps and (directory / 'running.json').exists(), 60, 'service startup')
        if task.done():
            await task
            raise RuntimeError('Bridge stopped before startup completed')
        if args.mode == 'mia':
            await wait_for(lambda: (directory / 'mia-pilot.json').exists() and json.loads((directory / 'mia-pilot.json').read_text()).get('bot_id'), 90, 'hosted bot creation')
        else:
            await wait_for(lambda: record() and record().bot_id not in {None, 'pending'}, 90, 'streaming bot creation')
        executor = apps[0].state.executor
        original_lookup = executor.specs['lookup_project_status']

        async def lookup(*parameters):
            if slow:
                entered.set()
                try:
                    await asyncio.sleep(45)
                except asyncio.CancelledError:
                    cancelled.set()
                    raise
            return await original_lookup.handler(*parameters)
        executor.specs[original_lookup.name] = replace(original_lookup, handler=lookup, timeout=60)
        if args.mode == 'mia':
            state = json.loads((directory / 'mia-pilot.json').read_text())
            report['agent_config_id'] = state['agent_config_id']
            origin = json.loads((directory / 'mia-functions.json').read_text())['agent']['custom_functions'][0]['url'].split('/mia/')[0]
            primary_id = state['bot_id']
        else:
            origin, primary_id = apps[0].state.acceptance_origin, record().bot_id
        report['primary_bot_id'] = primary_id
        await stt.start()
        websocket = origin.replace('https:', 'wss:', 1)
        result = await api.bots.create({'meeting_link': args.meeting, 'bot_name': 'SDK Finite Voice Test',
            'video_required': False, 'socket_connection_url': {'websocket_url': f'{websocket}/voice-test-control/{nonce}'},
            'live_audio_required': {'websocket_url': f'{websocket}/voice-test-audio/{nonce}'},
            'automatic_leave': {'waiting_room_timeout': 180, 'everyone_left_timeout': 60}},
            idempotency_key=secrets.token_hex(16))
        speaker_id = result['bot_id']
        report.update(speaker_bot_id=speaker_id, stage='admit_bots')
        bound.set()
        save()
        print('Admit the assistant and SDK Finite Voice Test in the disposable meeting.', flush=True)
        await wait_for(lambda: speaker_ws is not None, 210, 'speaker admission')
        async with asyncio.timeout(210):
            while True:
                states = await asyncio.gather(api.bots.status(primary_id), api.bots.status(speaker_id))
                if all(s.get('status') in {'InMeeting', 'Recording'} for s in states):
                    break
                await asyncio.sleep(2)
        await asyncio.sleep(4)
        # Socket readiness can precede visible meeting admission. A harmless,
        # bounded voice roundtrip establishes real input AND audible output.
        for attempt in range(3):
            started = time.monotonic()
            await say('Okay assistant, just say ready.')
            try:
                await wait_heard('ready', started, 25)
                break
            except TimeoutError:
                if attempt == 2:
                    raise
        report['stage'] = 'initial_tools_and_voice'
        await tool_voice('review notes', 'Riley')
        report['initial_tools_and_voice'] = True
        sid = record().id
        report['stage'] = 'canonical_switch'
        await spoken_switch(args.runtime, names[args.runtime][0])
        report['runtime_switch_spoken'] = True
        report['stage'] = 'misheard_switch'
        await spoken_switch(initial_profile, names[initial_profile][0])
        await spoken_switch(args.runtime, names[args.runtime][1])
        report['misheard_switch_spoken'] = True
        if args.mode == 'meeting':
            report['stage'] = 'unknown_switch'
            prior_turns = record().host.metrics['turn.started']
            revision = record().host.metrics['runtime_switches']
            before_chat = len(report['chat'])
            await say('Okay assistant, switch the runtime to Mystery Runtime.')
            await wait_for(lambda: any('Runtime name not recognized' in message for message in report['chat'][before_chat:]),
                           40, 'unknown runtime clarification')
            report['unknown_switch_rejected'] = (record().host.metrics['turn.started'] == prior_turns
                and record().host.metrics['runtime_switches'] == revision and record().host.context.profile == args.runtime)
        report['stage'] = 'target_tools_and_voice'
        await tool_voice('check budget', 'Morgan')
        report['target_tools_and_voice'] = True
        report['same_bot_and_session'] = record().id == sid and record().bot_id == primary_id
        report['stage'] = 'pending_tool_cancel'
        slow = True
        await say('Okay assistant, ask the external agent to look up project adapter status.')
        await asyncio.wait_for(entered.wait(), 70)
        delayed_turn = record().host.run.turn.turn_id
        cancel_at = time.monotonic()
        await say('Okay assistant, stop and cancel the pending work. Say only Stopped.')
        await asyncio.wait_for(cancelled.wait(), 30)
        await wait_for(lambda: any(e.type == EventType.CANCELLED and e.turn_id == delayed_turn for e in record().host.events),
                       20, 'cancellation terminal')
        report['pending_tool_cancelled'] = not record().host.poisoned
        slow = False
        await wait_for(lambda: any(stop_acknowledged(h, cancel_at) for h in report['heard']),
                       30, 'audible stop acknowledgement')
        report['stage'] = 'speech_interrupt'
        baseline_interruptions = record().host.metrics['speech_interruptions']
        if args.mode == 'mia':
            baseline_receipts = executor.db.execute('SELECT COUNT(*) FROM mia_requests').fetchone()[0]
        before = len(recent_words)
        await say('Okay assistant, tell a short story in exactly six sentences, twelve words per sentence, about a spacecraft visiting Mercury, Venus, Earth, Mars, Jupiter and Saturn.')
        await wait_for(lambda: len(recent_words) > before + 12, 90, 'active audible answer')
        baseline_interruptions = record().host.metrics['speech_interruptions']
        baseline_clears = report.get('playback_clears', 0)
        report['speech_pending_before_stop'] = record().output.delivery_pending() if args.mode == 'meeting' else True
        await say('Okay assistant, stop and cancel. Say only Stopped, then stay silent.')
        stop_time = time.monotonic()
        await wait_for(lambda: any(stop_acknowledged(h, stop_time - 8) for h in report['heard']),
                       30, 'audible stop acknowledgement')
        acknowledged_at = time.monotonic()
        await asyncio.sleep(5)
        report['quiet_after_stop'] = not any(h['at'] > acknowledged_at + 2 for h in report['heard'])
        # Acknowledgement alone is insufficient: require an actual safe interrupt receipt.
        if args.mode == 'mia':
            receipts = executor.db.execute('SELECT result FROM mia_requests ORDER BY rowid LIMIT -1 OFFSET ?',
                                           (baseline_receipts,)).fetchall()
            report['speech_interrupted'] = any(json.loads(row[0] or '{}').get('state_safe') for row in receipts)
        else:
            report['speech_interrupted'] = (record().host.metrics['speech_interruptions'] > baseline_interruptions
                and report.get('playback_clears', 0) > baseline_clears
                and report['speech_pending_before_stop'] and report['quiet_after_stop'] and not record().host.poisoned)
        report['stage'] = 'fresh_recovery'
        await tool_voice('send summary', 'Casey')
        report['fresh_turn_recovered'] = True
        report['stage'] = 'completed'
    except asyncio.CancelledError:
        report['error_type'] = 'CancelledError'
        report['error_stage'] = report['stage']
        print('Voice acceptance cancelled; cleaning up owned bots.', flush=True)
    except Exception as exc:
        report['error_type'] = type(exc).__name__
        report['error_stage'] = report['stage']
        print('Voice acceptance failed at ' + report['stage'] + ': ' + type(exc).__name__, flush=True)
    finally:
        snapshotting.cancel()
        await asyncio.gather(snapshotting, return_exceptions=True)
        save()
        if speaker_id:
            try:
                report['cleanup'].append({'bot_id': speaker_id, **await stop_owned_bot(api, speaker_id)})
            except Exception as exc:
                report['cleanup'].append({'bot_id': speaker_id, 'error': type(exc).__name__})
        await stt.close()
        await speech.close()
        if not task.done():
            try:
                await quickstart.stop(directory)
            except Exception as exc:
                report['shutdown_error'] = type(exc).__name__
                task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            report['shutdown_error'] = 'CancelledError'
        except Exception as exc:
            report['shutdown_error'] = type(exc).__name__
        quickstart.create_app = original
        marker = directory / 'mia-pilot.json'
        if args.mode == 'mia':
            primary_clean = marker.exists() and not json.loads(marker.read_text())['pending']
        else:
            import sqlite3
            ledger = directory / 'actions.sqlite'
            primary_clean = not report.get('primary_bot_id')
            if ledger.exists():
                with sqlite3.connect(ledger) as db:
                    table = db.execute("SELECT 1 FROM sqlite_master WHERE name='bot_cleanup'").fetchone()
                    if table:
                        primary_clean = db.execute('SELECT COUNT(*) FROM bot_cleanup').fetchone()[0] == 0
        if report.get('primary_bot_id'):
            try:
                state = await wait_for_departure(api, report['primary_bot_id'])
                report['primary_departure_status'] = state.get('status')
                report['primary_departure_source'] = state.get('source', 'status')
            except Exception as exc:
                report['primary_departure_error'] = type(exc).__name__
                primary_clean = False
        await api.aclose()
        report['owned_cleanup_confirmed'] = (primary_clean and bool(report['cleanup'])
            and all('error' not in item for item in report['cleanup']))
        report['observer_error'] = speaker_stream_error
        report['checks'] = acceptance_checks(report)
        report['passed'] = (all(report['checks'].values()) and speaker_stream_error is None
                            and not report.get('error_type') and not report.get('shutdown_error'))
        private_json(target, report)
        print('Voice acceptance passed: ' + str(report['passed']), flush=True)
    return report['passed']


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--meeting', required=True)
    parser.add_argument('--mode', choices=['meeting', 'mia'], default='meeting')
    parser.add_argument('--runtime', choices=['adk', 'langgraph', 'crewai'], default='langgraph',
                        help='Runtime to switch to and test; Google only, never OpenAI')
    parser.add_argument('--public-url', help='Existing HTTPS tunnel to --port; otherwise create an ngrok tunnel')
    parser.add_argument('--port', type=int, default=0, help='Local port for an existing tunnel; 0 chooses a free port')
    parser.add_argument('--env-file', default='.env')
    parser.add_argument('--report', default='reports/streaming-voice-acceptance.json')
    arguments = parser.parse_args()
    logging.disable(logging.CRITICAL)
    raise SystemExit(0 if asyncio.run(run(arguments)) else 1)
