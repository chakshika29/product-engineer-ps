import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import { createServer } from '../server/index.mjs';

function parseSSEChunk(rawChunk) {
  const events = [];
  const blocks = rawChunk.split('\n\n');
  for (const block of blocks) {
    if (!block.trim()) continue;
    const lines = block.split('\n');
    let id = null;
    let event = null;
    let data = null;
    for (const line of lines) {
      if (line.startsWith('id: ')) id = parseInt(line.slice(4), 10);
      else if (line.startsWith('event: ')) event = line.slice(7);
      else if (line.startsWith('data: ')) data = JSON.parse(line.slice(6));
    }
    if (event && data) {
      events.push({ id, event, data });
    }
  }
  return events;
}

function fetchJson(url, options = {}) {
  return new Promise((resolve, reject) => {
    const req = http.request(url, options, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        try {
          resolve({ status: res.statusCode, headers: res.headers, body: data ? JSON.parse(data) : null });
        } catch (e) {
          resolve({ status: res.statusCode, headers: res.headers, raw: data });
        }
      });
    });
    req.on('error', reject);
    if (options.body) {
      req.write(typeof options.body === 'string' ? options.body : JSON.stringify(options.body));
    }
    req.end();
  });
}

function listenSSE(url, { onEvent, stopAtSeq = null, cursor = null }) {
  return new Promise((resolve, reject) => {
    const parsedUrl = new URL(url);
    if (cursor !== null && cursor !== undefined) {
      parsedUrl.searchParams.set('cursor', cursor);
    }

    const req = http.request(parsedUrl, (res) => {
      if (res.statusCode !== 200) {
        let errBody = '';
        res.on('data', c => errBody += c);
        res.on('end', () => resolve({ errorStatus: res.statusCode, errorBody: errBody }));
        return;
      }

      let buffer = '';
      const collected = [];

      res.on('data', (chunk) => {
        buffer += chunk.toString();
        const parts = buffer.split('\n\n');
        buffer = parts.pop(); // keep remainder

        for (const part of parts) {
          if (!part.trim()) continue;
          const parsed = parseSSEChunk(part);
          for (const ev of parsed) {
            collected.push(ev);
            if (onEvent) onEvent(ev);

            if (stopAtSeq && ev.id >= stopAtSeq) {
              req.destroy();
              return resolve({ events: collected, aborted: true });
            }

            if (ev.data.is_terminal) {
              return resolve({ events: collected, completed: true });
            }
          }
        }
      });

      res.on('end', () => {
        resolve({ events: collected, closed: true });
      });
    });

    req.on('error', (err) => {
      // If manually destroyed, do not reject
      if (req.destroyed) return;
      reject(err);
    });

    req.end();
  });
}

test('Protocol and State Machine Verification Suite', async (t) => {
  const app = createServer({ dbPath: ':memory:' });
  await new Promise(r => app.listen(0, r));
  const port = app.server.address().port;
  const baseUrl = `http://localhost:${port}`;

  t.after(() => {
    return new Promise(r => app.close(r));
  });

  await t.test('AC1: Ordered live stream delivers events once in monotonic order to completion', async () => {
    const convRes = await fetchJson(`${baseUrl}/api/conversations`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: { id: 'conv_ac1' }
    });
    assert.equal(convRes.status, 201);

    const runRes = await fetchJson(`${baseUrl}/api/conversations/conv_ac1/runs`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: {
        prompt: 'Hello AI',
        words: ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon'],
        token_delay_ms: 10
      }
    });
    assert.equal(runRes.status, 201);
    const runId = runRes.body.run_id;

    const streamResult = await listenSSE(`${baseUrl}/api/conversations/conv_ac1/runs/${runId}/stream`, {
      cursor: 0
    });

    assert.equal(streamResult.completed || streamResult.closed, true);
    assert.equal(streamResult.events.length, 6); // 5 tokens + 1 terminal completed event

    // Verify ordering and uniqueness
    for (let i = 0; i < streamResult.events.length; i++) {
      const ev = streamResult.events[i];
      assert.equal(ev.id, i + 1, `Event at index ${i} must have sequence ${i + 1}`);
      if (i < 5) {
        assert.equal(ev.event, 'chunk');
        assert.equal(ev.data.is_terminal, false);
      } else {
        assert.equal(ev.event, 'run_completed');
        assert.equal(ev.data.is_terminal, true);
      }
    }

    // Verify DB state
    const runInDb = app.db.getRun(runId);
    assert.equal(runInDb.status, 'completed');
    assert.equal(runInDb.error_message, null);
  });

  await t.test('AC2: Missed-event recovery replays exactly from cursor without gaps or duplicates', async () => {
    const convId = 'conv_ac2';
    const runRes = await fetchJson(`${baseUrl}/api/conversations/${convId}/runs`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: {
        prompt: 'Count to 10',
        words: ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10'],
        token_delay_ms: 10
      }
    });
    const runId = runRes.body.run_id;

    // First connection: disconnect abruptly after event seq 4
    const firstClient = await listenSSE(`${baseUrl}/api/conversations/${convId}/runs/${runId}/stream`, {
      cursor: 0,
      stopAtSeq: 4
    });
    assert.equal(firstClient.events.length, 4);
    assert.deepEqual(firstClient.events.map(e => e.id), [1, 2, 3, 4]);

    // Allow generator to complete in background
    await new Promise(r => setTimeout(r, 150));

    // Second connection: reconnect with cursor=4
    const secondClient = await listenSSE(`${baseUrl}/api/conversations/${convId}/runs/${runId}/stream`, {
      cursor: 4
    });

    assert.equal(secondClient.completed || secondClient.closed, true);
    // Events should be 5, 6, 7, 8, 9, 10, 11 (run_completed)
    const secondSeqIds = secondClient.events.map(e => e.id);
    assert.deepEqual(secondSeqIds, [5, 6, 7, 8, 9, 10, 11]);

    // Verify complete combined stream has 0 duplicates and 0 gaps
    const allReceived = [...firstClient.events, ...secondClient.events];
    assert.equal(allReceived.length, 11);
    const allSeqIds = allReceived.map(e => e.id);
    assert.deepEqual(allSeqIds, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]);
  });

  await t.test('AC3: Replay and live stream overlap is merged deterministically with 0 duplicates', async () => {
    const convId = 'conv_ac3';
    // 20 words with 25ms delay = 500ms total run time
    const words = Array.from({ length: 20 }, (_, i) => `W${i + 1}`);
    const runRes = await fetchJson(`${baseUrl}/api/conversations/${convId}/runs`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: {
        prompt: 'Stream words',
        words,
        token_delay_ms: 25
      }
    });
    const runId = runRes.body.run_id;

    // Client 1 disconnects after seq 5
    const client1 = await listenSSE(`${baseUrl}/api/conversations/${convId}/runs/${runId}/stream`, {
      cursor: 0,
      stopAtSeq: 5
    });
    assert.equal(client1.events.length, 5);

    // Wait 75ms (generator reaches ~seq 8-9 and is actively generating)
    await new Promise(r => setTimeout(r, 75));

    // Verify generator is still running
    const runStatusMidway = app.db.getRun(runId);
    assert.equal(runStatusMidway.status, 'running');

    // Client 2 connects with cursor=5 WHILE generation is active
    const client2 = await listenSSE(`${baseUrl}/api/conversations/${convId}/runs/${runId}/stream`, {
      cursor: 5
    });

    assert.equal(client2.completed || client2.closed, true);
    const client2Seqs = client2.events.map(e => e.id);

    // Verify client 2 received strictly sequences 6 through 21 in monotonic order
    const expectedClient2Seqs = Array.from({ length: 16 }, (_, i) => i + 6);
    assert.deepEqual(client2Seqs, expectedClient2Seqs);

    // Combined stream has zero duplicates
    const fullStream = [...client1.events, ...client2.events];
    const fullSeqs = fullStream.map(e => e.id);
    const expectedFullSeqs = Array.from({ length: 21 }, (_, i) => i + 1);
    assert.deepEqual(fullSeqs, expectedFullSeqs);
  });

  await t.test('AC5: Generator failure transitions run to failed, preserves history, and never completes', async () => {
    const convId = 'conv_ac5';
    const runRes = await fetchJson(`${baseUrl}/api/conversations/${convId}/runs`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: {
        prompt: 'Fail midway',
        words: ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H'],
        token_delay_ms: 10,
        fail_at_seq: 5 // fails at sequence 5
      }
    });
    const runId = runRes.body.run_id;

    const streamResult = await listenSSE(`${baseUrl}/api/conversations/${convId}/runs/${runId}/stream`, {
      cursor: 0
    });

    assert.equal(streamResult.events.length, 5); // 4 chunks + 1 run_failed
    assert.equal(streamResult.events[3].event, 'chunk');
    assert.equal(streamResult.events[3].id, 4);

    const failEvent = streamResult.events[4];
    assert.equal(failEvent.event, 'run_failed');
    assert.equal(failEvent.id, 5);
    assert.equal(failEvent.data.is_terminal, true);
    assert.match(failEvent.data.error, /Simulated generator failure/);

    // Verify DB state
    const runInDb = app.db.getRun(runId);
    assert.equal(runInDb.status, 'failed');
    assert.match(runInDb.error_message, /Simulated generator failure/);

    // Reconnecting after failure from cursor=2 still gets events 3, 4, 5 (run_failed)
    const reconnected = await listenSSE(`${baseUrl}/api/conversations/${convId}/runs/${runId}/stream`, {
      cursor: 2
    });
    assert.deepEqual(reconnected.events.map(e => e.id), [3, 4, 5]);
    assert.equal(reconnected.events[2].event, 'run_failed');
  });

  await t.test('AC6: Unknown or stale cursor returns an explicit recoverable error response', async () => {
    const convId = 'conv_ac6';
    const runRes = await fetchJson(`${baseUrl}/api/conversations/${convId}/runs`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: {
        words: ['One', 'Two', 'Three'],
        token_delay_ms: 5
      }
    });
    const runId = runRes.body.run_id;

    // Wait for completion (4 events total)
    await new Promise(r => setTimeout(r, 60));

    // Request cursor 999 (far beyond known latest_seq = 4)
    const invalidRes = await fetchJson(`${baseUrl}/api/conversations/${convId}/runs/${runId}/stream?cursor=999`);
    assert.equal(invalidRes.status, 400);
    assert.equal(invalidRes.body.error, 'INVALID_CURSOR');
    assert.equal(invalidRes.body.latest_seq, 4);
    assert.equal(invalidRes.body.suggested_action, 'resync_from_zero_or_earliest');

    // Request negative cursor
    const negRes = await fetchJson(`${baseUrl}/api/conversations/${convId}/runs/${runId}/stream?cursor=-5`);
    assert.equal(negRes.status, 400);
    assert.equal(negRes.body.error, 'INVALID_CURSOR');
  });
});
