"""Owned Google MIA registration for the local pilot; no model inference here."""

import asyncio
import json
import os
import re
import secrets
from pathlib import Path
from urllib.parse import quote

import httpx
from dotenv import dotenv_values

from .config import validate_meeting_link
from .meetstream.protocol import meeting_ended


class PilotError(ValueError):
    """An actionable message that contains no remote responses or credentials."""


def private_json(path, value):
    temporary = path.with_suffix('.tmp')
    with os.fdopen(os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600), 'w') as target:
        json.dump(value, target, indent=2)
        target.flush()
        os.fsync(target.fileno())
    temporary.chmod(0o600)
    temporary.replace(path)


class MiaPilot:
    def __init__(self, directory, *, env_file=None, client=None):
        self.path = Path(directory) / 'mia-pilot.json'
        self.state = json.loads(self.path.read_text()) if self.path.exists() else {
            'agent_name': 'Adapter local pilot ' + secrets.token_hex(16),
            'pending': False,
        }
        if not re.fullmatch(r'Adapter local pilot [a-f0-9]{32}', self.state.get('agent_name', '')):
            raise PilotError('Invalid pilot ownership record. Restore .adapter/mia-pilot.json before cleanup.')
        self.state['env_file'] = str(Path(env_file or self.state.get('env_file') or '.env').resolve())
        key = os.getenv('MEETSTREAM_API_KEY') or dotenv_values(self.state['env_file']).get('MEETSTREAM_API_KEY')
        if not key:
            raise PilotError('MEETSTREAM_API_KEY is required to register or clean up the hosted MIA pilot.')
        # Persist the unpredictable ownership name BEFORE a remote create. A lost
        # response can then be reconciled by name on stop or the next start.
        private_json(self.path, self.state)
        self.client = client or httpx.AsyncClient(base_url='https://api.meetstream.ai', timeout=10,
                                                  headers={'Authorization': 'Token ' + key})

    async def owned(self):
        response = await self.client.get('/api/v1/mia')
        response.raise_for_status()
        return [row for row in response.json()['agent_configs']
                if row.get('AgentName') == self.state['agent_name']]

    async def cleanup(self, *, force=False):
        if not force and not self.state.get('pending', True):
            return
        await self.leave()
        for row in await self.owned():
            response = await self.client.delete('/api/v1/mia', params={'agent_config_id': row['AgentConfigID']})
            if response.status_code != 404:
                response.raise_for_status()
        if await self.owned():
            raise PilotError('Hosted pilot cleanup is pending. Run npm stop again when MeetStream is reachable.')
        self.state.pop('agent_config_id', None)
        self.state['pending'] = False
        private_json(self.path, self.state)

    async def join(self, meeting, *, waiting_room_timeout=180):
        validate_meeting_link(meeting)
        if self.state.get('bot_request'):
            raise PilotError('Previous meeting cleanup is pending. Run npm stop before joining again.')
        nonce = secrets.token_hex(16)
        body = {'meeting_link': meeting, 'bot_name': 'Adapter MIA Pilot',
                'agent_config_id': self.state['agent_config_id'], 'video_required': False,
                'custom_attributes': {'adapter_pilot': self.state['agent_name'], 'adapter_join': nonce},
                'automatic_leave': {'waiting_room_timeout': waiting_room_timeout, 'everyone_left_timeout': 60}}
        self.state['bot_request'] = {'key': nonce, 'body': body}
        private_json(self.path, self.state)
        # One durable key across transport retries: a replay is the original bot.
        for attempt in range(2):
            try:
                response = await self.client.post('/api/v1/bots/create_bot', json=body,
                                                  headers={'Idempotency-Key': nonce})
                break
            except httpx.TransportError:
                if attempt:
                    raise
        if response.status_code != 507:
            if 400 <= response.status_code < 500 and response.status_code != 408:
                self.state.pop('bot_request')
                private_json(self.path, self.state)
            response.raise_for_status()
        self.state['bot_id'] = response.json()['bot_id']
        private_json(self.path, self.state)
        return self.state['bot_id']

    async def status(self, bot_id=None):
        bot_id = bot_id or self.state['bot_id']
        response = await self.client.get(f'/api/v1/bots/{quote(bot_id, safe="")}/status')
        if response.status_code == 404:
            return {'status': 'Done'}
        response.raise_for_status()
        return response.json()

    async def leave(self):
        request = self.state.get('bot_request')
        bot_ids = {self.state['bot_id']} if self.state.get('bot_id') else set()
        if request and not bot_ids:
            # Reconcile a create whose response was lost; never create a bot on stop.
            response = await self.client.get('/api/v1/bots', params={
                'custom_attr[adapter_pilot]': self.state['agent_name'],
                'custom_attr[adapter_join]': request['key']})
            response.raise_for_status()
            data = response.json()
            bot_ids = {row['bot_id'] for row in data['bots']
                       if row.get('custom_attributes') == request['body']['custom_attributes']}
            if not bot_ids or data.get('hasNextPage'):
                raise PilotError('Meeting creation is unconfirmed. Keep .adapter and retry npm stop when MeetStream is reachable.')
        for bot_id in bot_ids:
            if meeting_ended(await self.status(bot_id)):
                continue
            response = await self.client.get(f'/api/v1/bots/{quote(bot_id, safe="")}/remove_bot')
            if response.status_code == 404:
                continue
            response.raise_for_status()
            # Accepted removal (202) is not evidence that the bot actually left.
            async with asyncio.timeout(40):
                while not meeting_ended(await self.status(bot_id)):  # noqa: ASYNC110 -- remote status polling
                    await asyncio.sleep(1)
        self.state.pop('bot_id', None)
        self.state.pop('bot_request', None)
        private_json(self.path, self.state)

    async def register(self, functions):
        await self.cleanup(force=True)  # Reconcile this project's interrupted/unknown prior create first.
        profiles = next(item for item in functions if item['name'] == 'switch_agent')['parameters']['properties']['profile']['enum']
        prompt = (
            'You are the local pilot meeting voice router. Speak English and wait silently for requests. '
            'For every participant task, call delegate_agent with the full request and relay its answer faithfully. '
            'Use a unique request_id for each new operation; reuse it only for an exact retry. '
            f'On an explicit switch request, execute switch_agent with one of {", ".join(profiles)}, then confirm. '
            + ('Never use OpenAI. ' if 'openai' not in profiles else '')
            + 'On stop or cancel, stop your own speech and execute interrupt_agent BEFORE '
            'acknowledging cancellation. Only confirm cancellation when its result has state_safe=true. '
            'Never repeat completed requests or resume cancelled output. Do not invent successful tool results.'
        )
        self.state['pending'] = True
        private_json(self.path, self.state)
        response = await self.client.post('/api/v1/mia', json={
            'agent_name': self.state['agent_name'], 'mode': 'realtime',
            'model': {'provider': 'google', 'model': 'gemini-2.5-flash-native-audio-preview-12-2025',
                      'voice': 'Puck', 'modalities': ['audio'], 'system_prompt': prompt},
            'agent': {'custom_functions': functions, 'enable_interruptions': True, 'interruption_mode': 'vad',
                      'interruptions': {'min_duration_seconds': 0.15, 'word_threshold': 1},
                      'resume_false_interruption': False, 'false_interruption_timeout': 0.5,
                      'response_modality': 'audio'},
        })
        if response.status_code == 400:
            raise PilotError('MIA rejected the Google pilot configuration. Connect Gemini under MeetStream Integrations → Models and check account/model access.')
        if response.status_code in {401, 403}:
            raise PilotError('The MeetStream key cannot manage MIA agents. Use a key from your own workspace with agent access.')
        response.raise_for_status()
        self.state['agent_config_id'] = response.json()['agent_config_id']
        private_json(self.path, self.state)
        response = await self.client.get('/api/v1/mia', params={'agent_config_id': self.state['agent_config_id']})
        response.raise_for_status()
        saved = response.json()['agent_config']
        model, agent = saved.get('Model', {}), saved.get('Agent', {})
        model = json.loads(model) if isinstance(model, str) else model
        agent = json.loads(agent) if isinstance(agent, str) else agent
        registered = {item['name']: item for item in agent.get('custom_functions', [])}
        if (saved.get('Mode') != 'realtime' or model.get('provider') != 'google'
                or any(registered.get(item['name'], {}).get(field) != item[field]
                       for item in functions for field in ('url', 'headers', 'parameters'))):
            raise PilotError('MeetStream did not save the expected Google pilot tools. Registration was stopped safely.')
        return self.state['agent_config_id']

    async def close(self):
        await self.client.aclose()
