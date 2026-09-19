import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import fs from 'node:fs';
import path from 'node:path';
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

function listenSSE(url, { cursor = null, stopAtSeq = null } = {}) {
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
        buffer = parts.pop();

        for (const part of parts) {
          if (!part.trim()) continue;
          const parsed = parseSSEChunk(part);
          for (const ev of parsed) {
            collected.push(ev);
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
      if (req.destroyed) return;
      reject(err);
    });

    req.end();
  });
}

test('AC4: Service Restart and Crash Recovery Suite', async (t) => {
  const testDbDir = path.resolve('data');
  const testDbPath = path.resolve(testDbDir, `test_restart_${Date.now()}_${Math.random().toString(36).slice(2, 6)}.db`);

  t.after(() => {
    try {
      if (fs.existsSync(testDbPath)) fs.unlinkSync(testDbPath);
      if (fs.existsSync(testDbPath + '-wal')) fs.unlinkSync(testDbPath + '-wal');
      if (fs.existsSync(testDbPath + '-shm')) fs.unlinkSync(testDbPath + '-shm');
    } catch (e) {}
  });

  await t.test('Interrupted run during service crash is reconciled on startup and resumable from cursor', async () => {
    // 1. Start Server Instance 1
    const server1 = createServer({ dbPath: testDbPath });
    await new Promise(r => server1.listen(0, r));
    const port1 = server1.server.address().port;
    const baseUrl1 = `http://localhost:${port1}`;
    const convId = `conv_crash_${Date.now()}`;

    const convRes = await fetchJson(`${baseUrl1}/api/conversations`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: { id: convId }
    });
    assert.equal(convRes.status, 201);

    // Start a 20-token run with 30ms delay
    const runRes = await fetchJson(`${baseUrl1}/api/conversations/${convId}/runs`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: {
        words: Array.from({ length: 20 }, (_, i) => `Part${i + 1}`),
        token_delay_ms: 30
      }
    });
    const runId = runRes.body.run_id;

    // Client receives events 1..5
    const initialStream = await listenSSE(`${baseUrl1}/api/conversations/${convId}/runs/${runId}/stream`, {
      cursor: 0,
      stopAtSeq: 5
    });
    assert.equal(initialStream.events.length, 5);

    // Wait a brief moment so generator persists a few more tokens (e.g. up to seq 8)
    await new Promise(r => setTimeout(r, 90));

    // Forcibly kill Server 1 while generation is in flight
    await new Promise(r => server1.close(r));

    // 2. Start Server Instance 2 pointing to the exact same SQLite database
    const server2 = createServer({ dbPath: testDbPath });
    await new Promise(r => server2.listen(0, r));
    const port2 = server2.server.address().port;
    const baseUrl2 = `http://localhost:${port2}`;

    // Verify DB state in Server 2: run was transitioned from 'running' to 'failed' with clear restart reason
    const runInDb = server2.db.getRun(runId);
    assert.equal(runInDb.status, 'failed');
    assert.match(runInDb.error_message, /restarted/);

    // Inspect event bounds
    const bounds = server2.db.getEventBounds(runId);
    assert.ok(bounds.latest_seq >= 6, `Latest seq should be >= 6, got ${bounds.latest_seq}`);

    // Client reconnects to Server 2 with cursor=5
    const recoveredStream = await listenSSE(`${baseUrl2}/api/conversations/${convId}/runs/${runId}/stream`, {
      cursor: 5
    });

      assert.ok(recoveredStream.events.length >= 1, 'Should receive remaining persisted events plus terminal event');
      
      // First recovered event must be seq 6
      assert.equal(recoveredStream.events[0].id, 6);

      // Terminal event must be run_failed with reason 'process_restart'
      const lastEvent = recoveredStream.events[recoveredStream.events.length - 1];
      assert.equal(lastEvent.event, 'run_failed');
      assert.equal(lastEvent.data.is_terminal, true);
      assert.equal(lastEvent.data.reason, 'process_restart');

      // All sequences from initial stream (1..5) + recovered stream (6..terminal) must form a contiguous sequence
      const fullSeqIds = [...initialStream.events.map(e => e.id), ...recoveredStream.events.map(e => e.id)];
      for (let i = 0; i < fullSeqIds.length; i++) {
        assert.equal(fullSeqIds[i], i + 1, `Seq at ${i} should be ${i + 1}`);
      }

    await new Promise(r => server2.close(r));
  });

  await t.test('Completed run survives service restart and can be fully replayed', async () => {
    // 1. Start Server Instance 1
    const server1 = createServer({ dbPath: testDbPath });
    await new Promise(r => server1.listen(0, r));
    const port1 = server1.server.address().port;
    const baseUrl1 = `http://localhost:${port1}`;

    const runRes = await fetchJson(`${baseUrl1}/api/conversations/conv_restart_test/runs`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: {
        words: ['Done1', 'Done2', 'Done3'],
        token_delay_ms: 10
      }
    });
    const runId = runRes.body.run_id;

    // Stream until completed
    const client = await listenSSE(`${baseUrl1}/api/conversations/conv_restart_test/runs/${runId}/stream`, { cursor: 0 });
    assert.equal(client.completed, true);
    assert.equal(client.events.length, 4); // 3 chunks + 1 run_completed

    // Stop Server 1
    await new Promise(r => server1.close(r));

    // 2. Start Server Instance 2
    const server2 = createServer({ dbPath: testDbPath });
    await new Promise(r => server2.listen(0, r));
    const port2 = server2.server.address().port;
    const baseUrl2 = `http://localhost:${port2}`;

    // Replay full stream on Server 2
    const replayStream = await listenSSE(`${baseUrl2}/api/conversations/conv_restart_test/runs/${runId}/stream`, { cursor: 0 });
    assert.equal(replayStream.events.length, 4);
    assert.equal(replayStream.events[3].event, 'run_completed');

    // Replay from cursor=2
    const partialStream = await listenSSE(`${baseUrl2}/api/conversations/conv_restart_test/runs/${runId}/stream`, { cursor: 2 });
    assert.deepEqual(partialStream.events.map(e => e.id), [3, 4]);

    await new Promise(r => server2.close(r));
  });
});
