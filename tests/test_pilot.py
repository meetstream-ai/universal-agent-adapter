import asyncio
import json
import sqlite3

import httpx
import pytest

from meetstream_agent_adapter import quickstart
from meetstream_agent_adapter.mia import mia_functions
from meetstream_agent_adapter.pilot import MiaPilot, PilotError


class Cloud:
    def __init__(self):
        self.configs = {'unrelated': {'AgentConfigID': 'unrelated', 'AgentName': 'User agent'}}
        self.creates = []
        self.deletes = []
        self.lost_response = False
        self.offline = False
        self.committed, self.release = asyncio.Event(), asyncio.Event()
        self.delay_create = False
        self.wrong_model = False
        self.on_delete = None

    async def request(self, request):
        assert request.url.host == 'api.meetstream.ai'
        if self.offline:
            raise httpx.ConnectError('fixture offline', request=request)
        if request.method == 'GET':
            cid = request.url.params.get('agent_config_id')
            return httpx.Response(200, json={'agent_config': self.configs[cid]} if cid else {'agent_configs': list(self.configs.values())})
        if request.method == 'DELETE':
            if self.on_delete:
                self.on_delete()
            cid = request.url.params['agent_config_id']
            self.deletes.append(cid)
            self.configs.pop(cid, None)
            return httpx.Response(200, json={})
        assert request.method == 'POST'
        body = json.loads(request.content)
        self.creates.append(body)
        cid = 'owned-' + str(len(self.creates))
        self.configs[cid] = {'AgentConfigID': cid, 'AgentName': body['agent_name'], 'Mode': body['mode'],
                             'Model': {**body['model'], 'provider': 'openai' if self.wrong_model else 'google'},
                             'Agent': body['agent']}
        self.committed.set()
        if self.delay_create:
            await self.release.wait()
        if self.lost_response:
            raise httpx.ReadTimeout('fixture lost create response', request=request)
        return httpx.Response(200, json={'agent_config_id': cid})


async def test_registration_refresh_recovery_and_scoped_cleanup(monkeypatch, tmp_path):
    monkeypatch.setenv('MEETSTREAM_API_KEY', 'fixture-meetstream-key')
    cloud = Cloud()
    async with httpx.AsyncClient(base_url='https://api.meetstream.ai', transport=httpx.MockTransport(cloud.request)) as client:
        pilot = MiaPilot(tmp_path, client=client)
        first = await pilot.register(mia_functions('https://first.example', 'tool-token', google_only=True))
        assert json.loads(pilot.path.read_text())['pending']
        assert cloud.creates[0]['model']['provider'] == 'google' and 'voice' not in cloud.creates[0]
        # A process crash leaves a durable owner marker. Restart replaces only that agent.
        restarted = MiaPilot(tmp_path, client=client)
        second = await restarted.register(mia_functions('https://second.example', 'new-token', google_only=True))
        assert first != second and cloud.deletes == [first]
        assert {row['url'] for row in cloud.configs[second]['Agent']['custom_functions']} == {
            'https://second.example/mia/' + name for name in ['delegate_agent', 'switch_agent', 'interrupt_agent']}
        await restarted.cleanup()
        await restarted.cleanup()  # Offline-safe repeated cleanup after confirmed deletion.
        assert list(cloud.configs) == ['unrelated']
        assert not json.loads(pilot.path.read_text())['pending']
        assert 'agent_config_id' not in json.loads(pilot.path.read_text())
        if __import__('os').name != 'nt':
            assert pilot.path.stat().st_mode & 0o777 == 0o600


async def test_lost_create_response_and_failed_cleanup_remain_recoverable(monkeypatch, tmp_path):
    monkeypatch.setenv('MEETSTREAM_API_KEY', 'fixture-meetstream-key')
    cloud = Cloud()
    async with httpx.AsyncClient(base_url='https://api.meetstream.ai', transport=httpx.MockTransport(cloud.request)) as client:
        pilot = MiaPilot(tmp_path, client=client)
        cloud.lost_response = True
        with pytest.raises(httpx.ReadTimeout):
            await pilot.register(mia_functions('https://bridge.example', 'token'))
        assert json.loads(pilot.path.read_text())['pending']
        cloud.offline = True
        with pytest.raises(httpx.ConnectError):
            await pilot.cleanup()
        assert json.loads(pilot.path.read_text())['pending']
        cloud.offline = False
        await pilot.cleanup()
        assert list(cloud.configs) == ['unrelated']


async def test_registration_rejects_unexpected_provider_and_recovers(monkeypatch, tmp_path):
    monkeypatch.setenv('MEETSTREAM_API_KEY', 'fixture-meetstream-key')
    cloud = Cloud()
    cloud.wrong_model = True
    async with httpx.AsyncClient(base_url='https://api.meetstream.ai', transport=httpx.MockTransport(cloud.request)) as client:
        pilot = MiaPilot(tmp_path, client=client)
        with pytest.raises(PilotError, match='expected Google'):
            await pilot.register(mia_functions('https://bridge.example', 'token'))
        await pilot.cleanup()
        assert list(cloud.configs) == ['unrelated']


@pytest.mark.parametrize('shutdown', ['command', 'cancel'])
@pytest.mark.parametrize('close_failure', [False, True])
async def test_launcher_registers_then_cleans_even_when_stopped_during_create(monkeypatch, tmp_path, shutdown, close_failure):
    monkeypatch.setenv('ADAPTER_API_KEY', 'a' * 40)
    monkeypatch.setenv('MIA_BRIDGE_TOKEN', 'b' * 40)
    monkeypatch.setenv('MEETSTREAM_API_KEY', 'fixture-meetstream-key')
    cloud = Cloud()
    cloud.delay_create = True
    client_type = httpx.AsyncClient

    def client(**kwargs):
        if kwargs.get('base_url') == 'https://api.meetstream.ai':
            kwargs['transport'] = httpx.MockTransport(cloud.request)
        elif 'base_url' not in kwargs:
            kwargs['transport'] = httpx.MockTransport(lambda request: httpx.Response(200, json={'status': 'ok', 'version': '0.1.0'}))
        return client_type(**kwargs)

    monkeypatch.setattr(quickstart.httpx, 'AsyncClient', client)
    original_app = quickstart.create_app
    apps = []

    def observe_app(settings, **kwargs):
        app = original_app(settings, **kwargs)
        apps.append(app)
        return app

    def assert_local_service_drained():
        with pytest.raises(sqlite3.ProgrammingError):
            apps[0].state.executor.db.execute('SELECT 1')

    monkeypatch.setattr(quickstart, 'create_app', observe_app)
    cloud.on_delete = assert_local_service_drained
    if close_failure:
        original_close = MiaPilot.close

        async def broken_close(self):
            await original_close(self)
            raise RuntimeError('fixture client close failure')

        monkeypatch.setattr(MiaPilot, 'close', broken_close)
    settings = quickstart.Settings(profile='fake', output='events', public_url='https://bridge.example', ledger=str(tmp_path / 'ledger.sqlite'))
    running = asyncio.create_task(quickstart.run(settings, None, 0, mia=True))
    stop_task = None
    try:
        await asyncio.wait_for(cloud.committed.wait(), 3)
        if shutdown == 'cancel':
            running.cancel()
        else:
            stop_task = asyncio.create_task(quickstart.stop(tmp_path))
        await asyncio.sleep(.1)
        assert not running.done()  # The in-flight create must finish before its owned registration is deleted.
        cloud.release.set()
        if shutdown == 'cancel':
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(running, 3)
        else:
            await asyncio.wait_for(asyncio.gather(stop_task, running), 3)
        assert list(cloud.configs) == ['unrelated'] and len(cloud.deletes) == 1
        assert not (tmp_path / 'running.json').exists() and not (tmp_path / 'mia-functions.json').exists()
    finally:
        cloud.release.set()
        if not running.done():
            running.cancel()
        await asyncio.gather(running, *([stop_task] if stop_task else []), return_exceptions=True)


async def test_stop_after_process_crash_reconciles_owned_registration(monkeypatch, tmp_path):
    monkeypatch.setenv('MEETSTREAM_API_KEY', 'fixture-meetstream-key')
    cloud = Cloud()
    client_type = httpx.AsyncClient
    def clients(**kwargs):
        assert kwargs['base_url'] == 'https://api.meetstream.ai'
        kwargs['transport'] = httpx.MockTransport(cloud.request)
        return client_type(**kwargs)
    monkeypatch.setattr(quickstart.httpx, 'AsyncClient', clients)
    pilot = MiaPilot(tmp_path)
    await pilot.register(mia_functions('https://old.example', 'token'))
    await pilot.close()  # Simulate a dead process without its finally block.
    (tmp_path / 'running.json').write_text('{"port":1,"token":"stale"}')
    (tmp_path / 'mia-functions.json').write_text('private stale export')
    await quickstart.stop(tmp_path)
    assert list(cloud.configs) == ['unrelated'] and not (tmp_path / 'running.json').exists()
    assert not (tmp_path / 'mia-functions.json').exists()
    cloud.offline = True
    await quickstart.stop(tmp_path)  # A confirmed-clean repeated stop does not need the network.


async def test_cleanup_credentials_are_scoped_and_invalid_ownership_is_rejected(monkeypatch, tmp_path):
    monkeypatch.delenv('MEETSTREAM_API_KEY', raising=False)
    monkeypatch.delenv('PILOT_UNRELATED_SECRET', raising=False)
    env = tmp_path / 'custom.env'
    env.write_text('MEETSTREAM_API_KEY=fixture-account\nPILOT_UNRELATED_SECRET=must-stay-private\n')
    client = httpx.AsyncClient(base_url='https://api.meetstream.ai')
    pilot = MiaPilot(tmp_path, env_file=env, client=client)
    assert 'PILOT_UNRELATED_SECRET' not in __import__('os').environ
    assert 'MEETSTREAM_API_KEY' not in __import__('os').environ
    state = json.loads(pilot.path.read_text())
    state['agent_name'] = 'User agent'
    pilot.path.write_text(json.dumps(state))
    with pytest.raises(PilotError, match='ownership record'):
        MiaPilot(tmp_path, client=client)
    await pilot.close()


class MeetingCloud(Cloud):
    def __init__(self):
        super().__init__()
        self.bots = {'unrelated-bot': {'bot_id': 'unrelated-bot', 'custom_attributes': {}, 'status': 'InMeeting'}}
        self.bot_creates = []
        self.removals = []
        self.lost_bot_response = False
        self.leave_pending = False
        self.join_committed, self.join_release = asyncio.Event(), asyncio.Event()
        self.delay_join = False

    async def request(self, request):
        path = request.url.path
        if not path.startswith('/api/v1/bots'):
            return await super().request(request)
        if self.offline:
            raise httpx.ConnectError('fixture offline', request=request)
        if path.endswith('/create_bot'):
            body = json.loads(request.content)
            self.bot_creates.append((request.headers['Idempotency-Key'], body))
            bot_id = 'bot-' + request.headers['Idempotency-Key']
            replay = bot_id in self.bots
            self.bots[bot_id] = {'bot_id': bot_id, 'custom_attributes': body['custom_attributes'], 'status': 'InMeeting'}
            self.join_committed.set()
            if self.delay_join:
                await self.join_release.wait()
            if self.lost_bot_response:
                raise httpx.ReadTimeout('lost bot response', request=request)
            return httpx.Response(507 if replay else 201, json={'bot_id': bot_id})
        if path == '/api/v1/bots':
            return httpx.Response(200, json={'bots': list(self.bots.values()), 'hasNextPage': False})
        bot_id = path.split('/')[-2]
        if path.endswith('/remove_bot'):
            self.removals.append(bot_id)
            if not self.leave_pending:
                self.bots[bot_id]['status'] = 'Stopped'
            return httpx.Response(202, json={})
        assert path.endswith('/status')
        return httpx.Response(200, json=self.bots[bot_id])


@pytest.mark.parametrize('lost_response', [False, True])
async def test_owned_meeting_join_crash_recovery_and_leave_before_config_deletion(monkeypatch, tmp_path, lost_response):
    monkeypatch.setenv('MEETSTREAM_API_KEY', 'fixture-account')
    cloud = MeetingCloud()
    async with httpx.AsyncClient(base_url='https://api.meetstream.ai', transport=httpx.MockTransport(cloud.request)) as client:
        pilot = MiaPilot(tmp_path, client=client)
        cid = await pilot.register(mia_functions('https://bridge.example', 'token', google_only=True))
        cloud.lost_bot_response = lost_response
        if lost_response:
            with pytest.raises(httpx.ReadTimeout):
                await pilot.join('https://meet.google.com/abc-defg-hij')
            assert cloud.bot_creates[0][0] == cloud.bot_creates[1][0]
        else:
            await pilot.join('https://meet.google.com/abc-defg-hij')
        payload = cloud.bot_creates[0][1]
        assert payload['agent_config_id'] == cid and payload['video_required'] is False
        assert 'socket_connection_url' not in payload and 'live_audio_required' not in payload
        cloud.leave_pending = True
        timeout = asyncio.timeout
        monkeypatch.setattr(quickstart.asyncio, 'timeout', lambda seconds: timeout(.02 if seconds == 40 else seconds))
        restarted = MiaPilot(tmp_path, client=client)
        with pytest.raises(TimeoutError):
            await restarted.cleanup()
        assert cid in cloud.configs and json.loads(pilot.path.read_text())['pending']
        cloud.leave_pending = False
        await restarted.cleanup()
        assert list(cloud.configs) == ['unrelated']
        assert cloud.bots['unrelated-bot']['status'] == 'InMeeting'
        assert all(row['status'] == 'Stopped' for key, row in cloud.bots.items() if key != 'unrelated-bot')
        state = json.loads(pilot.path.read_text())
        assert not state['pending'] and 'bot_request' not in state and 'bot_id' not in state


@pytest.mark.parametrize('shutdown', ['command', 'cancel'])
async def test_launcher_joins_and_stop_waits_for_inflight_bot_then_leaves(monkeypatch, tmp_path, shutdown):
    for key in ['MEETSTREAM_API_KEY', 'ADAPTER_API_KEY', 'MIA_BRIDGE_TOKEN']:
        monkeypatch.setenv(key, key + '-' + 'a' * 40)
    cloud = MeetingCloud()
    cloud.delay_join = True
    client_type = httpx.AsyncClient
    def clients(**kwargs):
        if kwargs.get('base_url') == 'https://api.meetstream.ai':
            kwargs['transport'] = httpx.MockTransport(cloud.request)
        elif 'base_url' not in kwargs:
            kwargs['transport'] = httpx.MockTransport(lambda request: httpx.Response(200, json={'status': 'ok', 'version': '0.1.0'}))
        return client_type(**kwargs)
    monkeypatch.setattr(quickstart.httpx, 'AsyncClient', clients)
    settings = quickstart.Settings(profile='fake', output='events', public_url='https://bridge.example', ledger=str(tmp_path / 'ledger.sqlite'))
    running = asyncio.create_task(quickstart.run(settings, 'https://meet.google.com/abc-defg-hij', 0, mia=True))
    stopping = None
    try:
        await asyncio.wait_for(cloud.join_committed.wait(), 3)
        if shutdown == 'cancel':
            running.cancel()
        else:
            stopping = asyncio.create_task(quickstart.stop(tmp_path))
        await asyncio.sleep(.1)
        assert not running.done() and not cloud.removals
        cloud.join_release.set()
        if shutdown == 'cancel':
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(running, 3)
        else:
            await asyncio.wait_for(asyncio.gather(running, stopping), 3)
        assert len(cloud.bot_creates) == 1 and len(cloud.removals) == 1
        assert list(cloud.configs) == ['unrelated'] and not (tmp_path / 'running.json').exists()
    finally:
        cloud.join_release.set()
        if not running.done():
            running.cancel()
        await asyncio.gather(running, *([stopping] if stopping else []), return_exceptions=True)


@pytest.mark.parametrize('creation_result', ['unknown', 'rejected', 'replay'])
async def test_join_rejection_replay_and_unconfirmed_cleanup_are_safe(monkeypatch, tmp_path, creation_result):
    monkeypatch.setenv('MEETSTREAM_API_KEY', 'fixture-account')
    cloud = MeetingCloud()
    attempts = []
    async def handle(request):
        if request.url.path.endswith('/create_bot'):
            attempts.append(request.headers['Idempotency-Key'])
            if creation_result == 'unknown':
                raise httpx.ReadTimeout('unconfirmed', request=request)
            if creation_result == 'rejected':
                return httpx.Response(429)
            if len(attempts) == 1:
                await cloud.request(request)
                raise httpx.ReadTimeout('response lost', request=request)
        return await cloud.request(request)
    async with httpx.AsyncClient(base_url='https://api.meetstream.ai', transport=httpx.MockTransport(handle)) as client:
        pilot = MiaPilot(tmp_path, client=client)
        cid = await pilot.register(mia_functions('https://bridge.example', 'token'))
        if creation_result == 'replay':
            bot_id = await pilot.join('https://meet.google.com/abc-defg-hij')
            assert attempts[0] == attempts[1] and pilot.state['bot_id'] == bot_id
        elif creation_result == 'rejected':
            with pytest.raises(httpx.HTTPStatusError):
                await pilot.join('https://meet.google.com/abc-defg-hij')
        else:
            with pytest.raises(httpx.ReadTimeout):
                await pilot.join('https://meet.google.com/abc-defg-hij')
            with pytest.raises(PilotError, match='unconfirmed'):
                await pilot.cleanup()
            assert cid in cloud.configs and pilot.state['pending'] and len(attempts) == 2
            assert not cloud.removals  # Stop neither creates a bot nor touches unrelated ones.
            return
        await pilot.cleanup()
        assert list(cloud.configs) == ['unrelated']


@pytest.mark.parametrize('google_only', [False, True])
async def test_registration_prompt_matches_enabled_runtime_schema(monkeypatch, tmp_path, google_only):
    monkeypatch.setenv('MEETSTREAM_API_KEY', 'fixture-account')
    cloud = Cloud()
    async with httpx.AsyncClient(base_url='https://api.meetstream.ai', transport=httpx.MockTransport(cloud.request)) as client:
        pilot = MiaPilot(tmp_path, client=client)
        await pilot.register(mia_functions('https://bridge.example', 'token', google_only=google_only))
        prompt = cloud.creates[0]['model']['system_prompt']
        assert ('Never use OpenAI.' in prompt) is google_only
        assert ('adk, langgraph, crewai' in prompt) if google_only else ('openai' in prompt)
        await pilot.cleanup()
