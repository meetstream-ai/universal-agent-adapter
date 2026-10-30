// Generic Node.js 22+ reasoning-sidecar client. No framework-specific bridge.
// Configure the Python service with output="events". The MIA owner supplies
// finalized transcript, streaming delivery and playback-clear hooks.
import { randomUUID } from 'node:crypto';

export async function attachMia({ server, key, botId, deliver, clearPlayback }) {
  const origin = new URL(server);
  if (origin.protocol !== 'https:' && !(origin.protocol === 'http:' && ['localhost', '127.0.0.1'].includes(origin.hostname))) {
    throw new Error('Use HTTPS or a loopback reasoning service');
  }
  const headers = { Authorization: `Bearer ${key}`, 'Content-Type': 'application/json' };
  async function post(path, body) {
    const response = await fetch(new URL(path, origin), { method: 'POST', headers, signal: AbortSignal.timeout(15000), body: body === undefined ? undefined : JSON.stringify(body) });
    if (!response.ok) throw new Error(`Reasoning service returned HTTP ${response.status}`);
    return response.json();
  }
  const session = await post('/v1/sessions', { bot_id: botId });
  const base = `/v1/sessions/${session.session_id}`;
  if (session.output !== 'events') {
    await post(`${base}/stop`);
    throw new Error('Native MIA requires output="events" and a single reasoning owner');
  }
  const controller = new AbortController();
  let epoch = 0, current = null, closed = false, unavailable = false;
  const seen = new Set();
  let timestamp = -1;
  const events = (async () => {
    try {
      const response = await fetch(new URL(`${base}/events/stream`, origin), { headers, signal: controller.signal });
      if (!response.ok) throw new Error(`Event stream returned HTTP ${response.status}`);
      const decoder = new TextDecoder();
      let buffer = '';
      for await (const chunk of response.body) {
        buffer += decoder.decode(chunk, { stream: true });
        if (buffer.length > 262144) throw new Error('Event frame exceeded its limit');
        let boundary;
        while ((boundary = buffer.indexOf('\n\n')) >= 0) {
          const frame = buffer.slice(0, boundary);
          buffer = buffer.slice(boundary + 2);
          const line = frame.split('\n').find(value => value.startsWith('data: '));
          if (!line) continue;
          const event = JSON.parse(line.slice(6));
          if (event.version !== 1 || event.session_id !== session.session_id) throw new Error('Invalid event binding/version');
          if (event.turn_id !== current) continue;
          const accepted = epoch;
          // MIA must check this predicate immediately before every media send.
          await deliver(event, () => !closed && epoch === accepted && current === event.turn_id);
        }
      }
      if (!closed) throw new Error('Reasoning event stream disconnected');
    } catch (error) {
      if (!closed) {
        unavailable = true;
        epoch++; current = null;
        await Promise.allSettled([clearPlayback(), post(`${base}/interrupt`)]);
        throw error;
      }
    }
  })();
  return {
    sessionId: session.session_id,
    events, // The MIA supervisor must monitor this promise; no silent reconnect.
    async turn(turn) {
      if (closed || unavailable) throw new Error('Reasoning session requires restart');
      if (seen.has(turn.event_id) || turn.timestamp < timestamp) return {queued:false, duplicate_or_stale:true};
      const id = turn.turn_id ?? randomUUID();
      epoch++; current = id;
      const [, outcome] = await Promise.all([clearPlayback(), post(`${base}/interrupt`)]);
      if (outcome.state_safe !== true) throw new Error('Runtime cancellation requires restart');
      const result = await post(`${base}/turns`, { ...turn, turn_id: id });
      seen.add(turn.event_id); timestamp = turn.timestamp;
      if (seen.size > 512) seen.delete(seen.values().next().value);
      return result;
    },
    async interrupt() {
      epoch++; current = null; // Fence locally before network or queue cleanup.
      return Promise.all([clearPlayback(), post(`${base}/interrupt`)]);
    },
    async close() {
      if (closed) return;
      closed = true; epoch++; current = null;
      controller.abort();
      await Promise.all([clearPlayback(), post(`${base}/stop`)]);
      await events;
    },
  };
}
