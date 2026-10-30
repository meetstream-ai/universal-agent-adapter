"""Real OpenAI clients, Responses SSE parser and Agents runner; no external requests."""

import asyncio
import json

import httpx
import pytest

from meetstream_agent_adapter.contract import EventType, SessionContext, Turn
from meetstream_agent_adapter.host import MemoryOutput, SessionHost
from meetstream_agent_adapter.tools import ToolExecutor, ToolSpec


def sse(kind, **data):
    return ('event: ' + kind + '\ndata: ' + json.dumps({'type': kind, **data}) + '\n\n').encode()


@pytest.mark.parametrize('scenario', ['success', 'quota_recovery', 'cancel_recovery'])
async def test_native_openai_http_stream_tools_and_recovery(monkeypatch, scenario):
    pytest.importorskip('agents')
    from openai import AsyncOpenAI

    from meetstream_agent_adapter.adapters import openai as adapter_module

    monkeypatch.setenv('OPENAI_API_KEY', 'fixture-openai-key')
    requests, writes, clients = [], [], []
    waiting, closed = asyncio.Event(), asyncio.Event()
    tool_sent = False
    quota_sent = False
    blocked = False

    class Blocked(httpx.AsyncByteStream):
        async def __aiter__(self):
            try:
                yield sse('response.output_text.delta', delta='Old answer ', content_index=0,
                          output_index=0, item_id='msg', logprobs=[], sequence_number=0)
                waiting.set()
                await asyncio.Event().wait()
            finally:
                closed.set()

    async def respond(request):
        nonlocal tool_sent, quota_sent, blocked
        assert str(request.url) == 'https://api.openai.com/v1/responses'
        assert request.headers['Authorization'] == 'Bearer fixture-openai-key'
        body = json.loads(request.content)
        assert body['stream'] is True and body['model'] == 'gpt-4.1-mini'
        assert body['parallel_tool_calls'] is False
        assert body['tools'][0]['name'] == 'save'
        requests.append(body)
        if scenario == 'quota_recovery' and not quota_sent:
            quota_sent = True
            return httpx.Response(429, json={'error': {'type': 'insufficient_quota',
                                  'code': 'insufficient_quota', 'message': 'Fixture exhausted quota'}})
        if not tool_sent:
            tool_sent = True
            output = [{'type': 'function_call', 'name': 'save', 'arguments': '{"title":"HTTP SDK test"}',
                       'call_id': 'transport-call', 'id': 'fc'}]
            deltas = b''
        else:
            if scenario == 'cancel_recovery' and not blocked:
                blocked = True
                return httpx.Response(200, headers={'Content-Type': 'text/event-stream'}, stream=Blocked())
            output = [{'type': 'message', 'role': 'assistant', 'id': 'msg', 'status': 'completed',
                       'content': [{'type': 'output_text', 'text': 'Saved the action.', 'annotations': [], 'logprobs': []}]}]
            deltas = b''.join(sse('response.output_text.delta', delta=text, content_index=0,
                                  output_index=0, item_id='msg', logprobs=[], sequence_number=n)
                              for n, text in enumerate(['Saved ', 'the action.']))
        response = {'id': 'r' + str(len(requests)), 'created_at': 0, 'model': body['model'], 'object': 'response',
                    'status': 'completed', 'output': output, 'parallel_tool_calls': False,
                    'tool_choice': 'auto', 'tools': []}
        return httpx.Response(200, headers={'Content-Type': 'text/event-stream'},
                              content=deltas + sse('response.completed', response=response, sequence_number=3))

    def client(**kwargs):
        assert kwargs['max_retries'] == 0
        result = AsyncOpenAI(**kwargs, http_client=httpx.AsyncClient(transport=httpx.MockTransport(respond)))
        clients.append(result)
        return result

    monkeypatch.setattr(adapter_module, 'AsyncOpenAI', client)
    async def save(args, context, operation):
        writes.append(args)
        return {'saved': True}
    tools = ToolExecutor([ToolSpec('save', 'Save an action', {'type': 'object',
        'properties': {'title': {'type': 'string'}}, 'required': ['title'], 'additionalProperties': False},
        save, writes=True)], {'save'})
    host = await SessionHost.create(SessionContext(session_id='openai-http', workspace='test', bot_id='owned', profile='openai'),
                                   adapter_module.Adapter(), tools, MemoryOutput())
    try:
        await host.accept(Turn(event_id='one', text='Save an action', timestamp=1))
        if scenario == 'cancel_recovery':
            await asyncio.wait_for(waiting.wait(), 5)
            outcome = await host.interrupt()
            assert outcome.state_safe and outcome.runtime_stopped
            assert closed.is_set() and host.events[-1].type == EventType.CANCELLED
        else:
            await host.wait()
            if scenario == 'quota_recovery':
                assert host.events[-1].type == EventType.FAILED
                assert host.events[-1].payload['code'] == 'provider_quota_exhausted'
                assert not writes and len(requests) == 1  # No retry storm on billing failures.
        if scenario != 'success':
            await host.accept(Turn(event_id='two', text='Confirm the action', timestamp=2))
            await host.wait()
        assert host.events[-1].type == EventType.COMPLETED, list(host.events)
        assert writes == [{'title': 'HTTP SDK test'}]
        text = [event.payload['text'] for event in host.events
                if event.type == EventType.DELTA and event.turn_id == host.run.turn.turn_id]
        assert ''.join(text) == 'Saved the action.' and len(text) >= 2
        assert any(item['type'] == 'function_call_output' for body in requests for item in body['input'] if 'type' in item)
    finally:
        await host.close()
        tools.close()
    assert len(clients) == 1 and clients[0].is_closed()
