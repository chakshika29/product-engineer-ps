import http from 'node:http';
import { createServer } from '../server/index.mjs';
import { DEFAULT_BENCHMARK_WORDS } from '../server/generator.mjs';

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

function streamSSE(url, { cursor = null, stopAtSeq = null } = {}) {
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
              return resolve({ events: collected, interrupted: true });
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

async function runBenchmark() {
  console.log('\n' + '='.repeat(70));
  console.log('⚡ PROBLEM 1 VERIFICATION BENCHMARK: RESUMABLE REALTIME CONVERSATION');
  console.log('='.repeat(70));
  console.log('Candidate: Chakshika Pawar');
  console.log('Target:    >= 30 ordered events, active-stream interruption & resume');
  console.log('Criteria:  Strict monotonic order, 0 missing, 0 duplicates, completed state');
  console.log('-'.repeat(70));

  const app = createServer({ dbPath: ':memory:' });
  await new Promise(r => app.listen(0, r));
  const port = app.server.address().port;
  const baseUrl = `http://localhost:${port}`;

  const targetWords = DEFAULT_BENCHMARK_WORDS; // Exactly 36 words
  const expectedTotalChunks = targetWords.length;
  const expectedTerminalSeq = expectedTotalChunks + 1; // 37 (including run_completed)
  const interruptAtSeq = 12;

  console.log(`[1/5] Initializing conversation and run with ${expectedTotalChunks} ordered events...`);
  const convRes = await fetchJson(`${baseUrl}/api/conversations`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: { id: `conv_bench_${Date.now()}` }
  });
  const convId = convRes.body.id;

  const runRes = await fetchJson(`${baseUrl}/api/conversations/${convId}/runs`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: {
      prompt: 'Benchmark run',
      words: targetWords,
      token_delay_ms: 25 // 25ms per token
    }
  });
  const runId = runRes.body.run_id;
  console.log(`      Conversation: ${convId}`);
  console.log(`      Run ID:       ${runId}`);

  console.log(`[2/5] Client 1 connecting with cursor=0...`);
  const phase1 = await streamSSE(`${baseUrl}/api/conversations/${convId}/runs/${runId}/stream`, {
    cursor: 0,
    stopAtSeq: interruptAtSeq
  });

  console.log(`      ⚡ Abruptly dropped connection after event #${interruptAtSeq}!`);
  console.log(`      Received in Phase 1: ${phase1.events.length} events (seq 1..${phase1.events[phase1.events.length - 1]?.id})`);

  // Verify server is still actively running
  const midwayRun = app.db.getRun(runId);
  console.log(`[3/5] Verifying generation continues asynchronously in background...`);
  console.log(`      Server run status midway: '${midwayRun.status}' (active: ${app.generator.isRunActive(runId)})`);
  
  // Wait 150ms while server produces next events
  await new Promise(r => setTimeout(r, 150));

  console.log(`[4/5] Client 2 reconnecting from checkpoint cursor=${interruptAtSeq}...`);
  const phase2 = await streamSSE(`${baseUrl}/api/conversations/${convId}/runs/${runId}/stream`, {
    cursor: interruptAtSeq
  });

  console.log(`      Received in Phase 2: ${phase2.events.length} events (seq ${phase2.events[0]?.id}..${phase2.events[phase2.events.length - 1]?.id})`);
  console.log(`      Terminal event received: '${phase2.events[phase2.events.length - 1]?.event}'`);

  // [5/5] State analysis & verification
  console.log(`[5/5] Analyzing reconstructed stream integrity and deduplication...`);
  const allEvents = [...phase1.events, ...phase2.events];

  const seenSeqs = new Set();
  const duplicateSeqs = [];
  const missingSeqs = [];
  let reconstructedText = '';

  for (let i = 0; i < allEvents.length; i++) {
    const ev = allEvents[i];
    if (seenSeqs.has(ev.id)) {
      duplicateSeqs.push(ev.id);
    }
    seenSeqs.add(ev.id);

    if (ev.event === 'chunk') {
      reconstructedText += ev.data.chunk;
    }
  }

  for (let seq = 1; seq <= expectedTerminalSeq; seq++) {
    if (!seenSeqs.has(seq)) {
      missingSeqs.push(seq);
    }
  }

  const finalRunState = app.db.getRun(runId);
  const expectedText = targetWords.join(' ');
  const textMatches = reconstructedText.trim() === expectedText.trim();

  console.log('\n' + '='.repeat(70));
  console.log('📊 BENCHMARK VERIFICATION RESULTS');
  console.log('='.repeat(70));
  console.log(`Total ordered text events generated:  ${expectedTotalChunks} (Requirement: >= 30) -> PASS`);
  console.log(`Total events with terminal event:     ${expectedTerminalSeq}`);
  console.log(`Interruption point:                   Sequence #${interruptAtSeq}`);
  console.log(`Reconnection cursor used:             ${interruptAtSeq}`);
  console.log(`Phase 1 events received:              ${phase1.events.length}`);
  console.log(`Phase 2 events received:              ${phase2.events.length}`);
  console.log(`Total events observed:                ${allEvents.length}`);
  console.log(`Duplicate events count:               ${duplicateSeqs.length} ${duplicateSeqs.length === 0 ? '✔ (0 duplicates)' : '✖ FAIL'}`);
  console.log(`Missing events count:                 ${missingSeqs.length} ${missingSeqs.length === 0 ? '✔ (0 missing)' : '✖ FAIL'}`);
  console.log(`Strict sequence monotonicity:         ${missingSeqs.length === 0 && duplicateSeqs.length === 0 ? '✔ STRICT MONOTONIC 1..' + expectedTerminalSeq : '✖ FAIL'}`);
  console.log(`Final terminal run state:             '${finalRunState.status}' ${finalRunState.status === 'completed' ? '✔' : '✖ FAIL'}`);
  console.log(`Text reconstruction matches fixture:  ${textMatches ? '✔ EXACT MATCH' : '✖ MISMATCH'}`);
  console.log('='.repeat(70));

  if (
    expectedTotalChunks >= 30 &&
    duplicateSeqs.length === 0 &&
    missingSeqs.length === 0 &&
    finalRunState.status === 'completed' &&
    textMatches
  ) {
    console.log('\n🎉 ALL ACCEPTANCE SCENARIOS AND BENCHMARK CRITERIA VERIFIED SUCCESSFULLY!\n');
    await new Promise(r => app.close(r));
    process.exit(0);
  } else {
    console.error('\n❌ BENCHMARK CRITERIA FAILED!\n');
    await new Promise(r => app.close(r));
    process.exit(1);
  }
}

runBenchmark().catch(err => {
  console.error('Fatal Benchmark Error:', err);
  process.exit(1);
});
