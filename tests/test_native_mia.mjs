import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { attachMia } from '../examples/native_mia.mjs';

const pause = () => new Promise(resolve => setImmediate(resolve));

test('Native MIA sidecar fences previous delivery and validates output ownership', async () => {
  let stream, mode = 'events';
  const delivered = [], calls = [];
  let clearCount = 0;
  const server = createServer(async (request, response) => {
    assert.equal(request.headers.authorization, 'Bearer test-only-key');
    calls.push(request.url);
    if (request.url.endsWith('/events/stream')) {
      stream = response;
      response.writeHead(200, { 'Content-Type': 'text/event-stream' });
      response.flushHeaders();
      return;
    }
    for await (const chunk of request) void chunk;
    response.writeHead(200, { 'Content-Type': 'application/json' });
    response.end(JSON.stringify(request.url === '/v1/sessions' ? {session_id:'s', output:mode} : request.url.endsWith('/interrupt') ? {state_safe:true} : {queued:true}));
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  const origin = `http://127.0.0.1:${server.address().port}`;
  const callbacks = { server:origin, key:'test-only-key', botId:'bot',
    deliver: async (event, active) => delivered.push({event, active}),
    clearPlayback: async () => { clearCount++; } };
  let client;
  try {
    client = await attachMia(callbacks);
    await client.turn({turn_id:'first', event_id:'one', text:'hey assistant test', timestamp:1});
    while (!stream) await pause();
    function emit(id, text) {
      stream.write(`data: ${JSON.stringify({version:1, session_id:'s', turn_id:id, type:'assistant.text.delta', payload:{text}})}\n\n`);
    }
    emit('first', 'First response');
    while (delivered.length < 1) await pause();
    assert.equal(delivered[0].active(), true);
    assert.equal((await client.turn({event_id:'one', text:'duplicate', timestamp:1})).duplicate_or_stale, true);
    assert.equal(delivered[0].active(), true);
    await client.interrupt();
    assert.equal(delivered[0].active(), false);
    await client.turn({turn_id:'second', event_id:'two', text:'hey assistant next', timestamp:2});
    emit('first', 'Obsolete');
    emit('second', 'Next response');
    while (delivered.length < 2) await pause();
    assert.equal(delivered.length, 2);
    assert.equal(delivered[1].active(), true);
    await client.close();
    assert.equal(delivered[1].active(), false);
    assert.ok(clearCount >= 4);
    assert.ok(calls.includes('/v1/sessions/s/interrupt'));
    mode = 'voice_and_chat';
    await assert.rejects(attachMia(callbacks), /requires output="events"/);
  } finally {
    if (client) await client.close();
    server.closeAllConnections();
    await new Promise(resolve => server.close(resolve));
  }
});
