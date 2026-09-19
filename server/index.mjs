import http from 'node:http';
import url from 'node:url';
import path from 'node:path';
import fs from 'node:fs';
import { DatabaseStore } from './db.mjs';
import { RunPubSub } from './pubsub.mjs';
import { ResponseGenerator, DEFAULT_BENCHMARK_WORDS } from './generator.mjs';

export function createServer({
  dbPath = process.env.DB_PATH || './data/conversations.db',
  clientDir = path.resolve('client')
} = {}) {
  const db = new DatabaseStore(dbPath);
  const pubsub = new RunPubSub();
  const generator = new ResponseGenerator({ db, pubsub });

  // AC4: Reconcile any runs that were left in 'running' state during a previous server crash/restart
  const reconciledRuns = db.reconcileDanglingRunsOnStartup();
  if (reconciledRuns.length > 0) {
    console.log(`[Startup] Reconciled ${reconciledRuns.length} dangling runs from previous process:`, reconciledRuns);
  }

  function sendJson(res, statusCode, data) {
    res.writeHead(statusCode, {
      'Content-Type': 'application/json',
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type, Last-Event-ID, Cursor'
    });
    res.end(JSON.stringify(data));
  }

  function sendSSE(res, event) {
    // Standard Server-Sent Events format
    // id: <seq_id>
    // event: <event_type>
    // data: <json_string>
    res.write(`id: ${event.seq_id}\n`);
    res.write(`event: ${event.event_type}\n`);
    res.write(`data: ${JSON.stringify(event.payload)}\n\n`);
  }

  async function parseBody(req) {
    return new Promise((resolve, reject) => {
      let body = '';
      req.on('data', chunk => {
        body += chunk;
        if (body.length > 1e6) {
          req.destroy();
          reject(new Error('Request body too large'));
        }
      });
      req.on('end', () => {
        try {
          resolve(body ? JSON.parse(body) : {});
        } catch (err) {
          reject(new Error('Invalid JSON'));
        }
      });
      req.on('error', reject);
    });
  }

  const server = http.createServer(async (req, res) => {
    const parsedUrl = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
    const pathname = parsedUrl.pathname;
    const method = req.method;

    // CORS preflight
    if (method === 'OPTIONS') {
      res.writeHead(204, {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type, Last-Event-ID, Cursor'
      });
      return res.end();
    }

    try {
      // 1. Health check
      if (pathname === '/health' && method === 'GET') {
        return sendJson(res, 200, { status: 'ok', uptime: process.uptime() });
      }

      // 2. POST /api/conversations
      if (pathname === '/api/conversations' && method === 'POST') {
        const body = await parseBody(req);
        const conv = db.createConversation(body.id);
        return sendJson(res, 201, conv);
      }

      // 3. GET /api/conversations/:id
      const convMatch = pathname.match(/^\/api\/conversations\/([^/]+)$/);
      if (convMatch && method === 'GET') {
        const convId = convMatch[1];
        const conv = db.getConversation(convId);
        if (!conv) {
          return sendJson(res, 404, { error: 'Conversation not found' });
        }
        const messages = db.getMessages(convId);
        return sendJson(res, 200, { ...conv, messages });
      }

      // 4. POST /api/conversations/:id/runs - start a generation turn
      const runsMatch = pathname.match(/^\/api\/conversations\/([^/]+)\/runs$/);
      if (runsMatch && method === 'POST') {
        const convId = runsMatch[1];
        let conv = db.getConversation(convId);
        if (!conv) {
          conv = db.createConversation(convId);
        }

        const body = await parseBody(req);
        const prompt = body.prompt || 'Hello';
        const userMsg = db.createMessage({
          id: body.message_id,
          conversationId: convId,
          role: 'user',
          content: prompt
        });

        const run = db.createRun({
          conversationId: convId,
          userMessageId: userMsg.id
        });

        // Trigger generator
        generator.startRun({
          runId: run.id,
          conversationId: convId,
          prompt,
          words: body.words,
          totalTokens: body.total_tokens,
          tokenDelayMs: body.token_delay_ms !== undefined ? body.token_delay_ms : 30,
          failAtSeq: body.fail_at_seq !== undefined ? body.fail_at_seq : null
        });

        return sendJson(res, 201, {
          conversation_id: convId,
          message_id: userMsg.id,
          run_id: run.id,
          status: 'running'
        });
      }

      // 5. GET /api/conversations/:id/runs/:run_id - get run metadata
      const runMetaMatch = pathname.match(/^\/api\/conversations\/([^/]+)\/runs\/([^/]+)$/);
      if (runMetaMatch && method === 'GET') {
        const runId = runMetaMatch[2];
        const run = db.getRun(runId);
        if (!run) {
          return sendJson(res, 404, { error: 'Run not found' });
        }
        const bounds = db.getEventBounds(runId);
        return sendJson(res, 200, {
          ...run,
          ...bounds,
          is_active_in_memory: generator.isRunActive(runId)
        });
      }

      // 6. GET /api/conversations/:id/runs/:run_id/events - inspect all durable events
      const runEventsMatch = pathname.match(/^\/api\/conversations\/([^/]+)\/runs\/([^/]+)\/events$/);
      if (runEventsMatch && method === 'GET') {
        const runId = runEventsMatch[2];
        const run = db.getRun(runId);
        if (!run) {
          return sendJson(res, 404, { error: 'Run not found' });
        }
        const events = db.getAllEvents(runId);
        return sendJson(res, 200, { run_id: runId, count: events.length, events });
      }

      // 7. GET /api/conversations/:id/runs/:run_id/stream - Resumable SSE Stream!
      const streamMatch = pathname.match(/^\/api\/conversations\/([^/]+)\/runs\/([^/]+)\/stream$/);
      if (streamMatch && method === 'GET') {
        const runId = streamMatch[2];
        const run = db.getRun(runId);
        if (!run) {
          return sendJson(res, 404, { error: 'Run not found' });
        }

        // Determine client cursor from query parameter or Last-Event-ID header
        const rawCursor = parsedUrl.searchParams.get('cursor') ?? req.headers['last-event-id'] ?? req.headers['cursor'];
        let cursor = 0;
        if (rawCursor !== undefined && rawCursor !== null && rawCursor !== '') {
          cursor = parseInt(rawCursor, 10);
        }

        // AC6: Unknown or stale cursor handling
        const bounds = db.getEventBounds(runId);
        const isCursorInvalid = isNaN(cursor) || cursor < 0;
        const isCursorBeyondKnown = cursor > bounds.latest_seq;
        const isCursorExpired = bounds.earliest_seq > 1 && cursor < bounds.earliest_seq - 1;

        if (isCursorInvalid || isCursorBeyondKnown || isCursorExpired) {
          // Explicit recoverable error response
          const reason = isCursorExpired ? 'CURSOR_EXPIRED' : 'INVALID_CURSOR';
          return sendJson(res, 400, {
            error: reason,
            message: `Requested cursor (${rawCursor}) is invalid, expired, or out of available event bounds`,
            requested_cursor: rawCursor,
            earliest_seq: bounds.earliest_seq,
            latest_seq: bounds.latest_seq,
            total_events: bounds.total_events,
            suggested_action: 'resync_from_zero_or_earliest'
          });
        }

        // Initialize SSE Connection
        res.writeHead(200, {
          'Content-Type': 'text/event-stream; charset=utf-8',
          'Cache-Control': 'no-cache, no-transform',
          'Connection': 'keep-alive',
          'X-Accel-Buffering': 'no',
          'Access-Control-Allow-Origin': '*'
        });

        // AC3: Safe Replay & Live Overlap Handling
        // Register buffer listener on live pubsub BEFORE querying SQLite
        const liveBuffer = [];
        let isReplaying = true;
        let highestSentSeq = cursor;
        let isClientClosed = false;

        const handleLiveEvent = (event) => {
          if (isClientClosed) return;
          if (isReplaying) {
            liveBuffer.push(event);
          } else {
            // Deduplicate at delivery boundary
            if (event.seq_id > highestSentSeq) {
              highestSentSeq = event.seq_id;
              sendSSE(res, event);
              if (event.payload.is_terminal) {
                cleanup();
                res.end();
              }
            }
          }
        };

        const unsubscribe = pubsub.subscribe(runId, handleLiveEvent);

        const cleanup = () => {
          if (!isClientClosed) {
            isClientClosed = true;
            unsubscribe();
          }
        };

        req.on('close', cleanup);
        req.on('error', cleanup);

        // Fetch and stream replayed events from SQLite
        const replayedEvents = db.getEventsAfterCursor(runId, cursor);
        for (const event of replayedEvents) {
          if (isClientClosed) break;
          if (event.seq_id > highestSentSeq) {
            highestSentSeq = event.seq_id;
            sendSSE(res, event);
            if (event.payload.is_terminal) {
              cleanup();
              return res.end();
            }
          }
        }

        // Transition from replay to live: drain buffer
        isReplaying = false;
        while (liveBuffer.length > 0) {
          if (isClientClosed) break;
          const bufferedEvent = liveBuffer.shift();
          // Filter out duplicates that were already in SQLite replay
          if (bufferedEvent.seq_id > highestSentSeq) {
            highestSentSeq = bufferedEvent.seq_id;
            sendSSE(res, bufferedEvent);
            if (bufferedEvent.payload.is_terminal) {
              cleanup();
              return res.end();
            }
          }
        }

        // If run is already completed or failed in DB and all events were sent, end stream
        const currentRun = db.getRun(runId);
        if (currentRun.status !== 'running') {
          const latestBounds = db.getEventBounds(runId);
          if (highestSentSeq >= latestBounds.latest_seq) {
            cleanup();
            return res.end();
          }
        }

        // Otherwise, connection remains open awaiting live events from pubsub
        return;
      }

      // 8. Serve static frontend files
      if (method === 'GET') {
        let filePath = pathname === '/' ? '/index.html' : pathname;
        // sanitize path
        const safePath = path.normalize(filePath).replace(/^(\.\.[/\\])+/, '');
        const targetPath = path.join(clientDir, safePath);

        if (fs.existsSync(targetPath) && fs.statSync(targetPath).isFile()) {
          const ext = path.extname(targetPath).toLowerCase();
          const mimeTypes = {
            '.html': 'text/html; charset=utf-8',
            '.css': 'text/css; charset=utf-8',
            '.js': 'application/javascript; charset=utf-8',
            '.mjs': 'application/javascript; charset=utf-8',
            '.json': 'application/json',
            '.png': 'image/png',
            '.svg': 'image/svg+xml'
          };
          const contentType = mimeTypes[ext] || 'application/octet-stream';
          res.writeHead(200, { 'Content-Type': contentType });
          return fs.createReadStream(targetPath).pipe(res);
        }
      }

      // Fallback 404
      return sendJson(res, 404, { error: 'Not Found' });
    } catch (err) {
      console.error('[Server Error]', err);
      if (!res.headersSent) {
        return sendJson(res, 500, { error: 'Internal Server Error', message: err.message });
      }
    }
  });

  return {
    server,
    db,
    pubsub,
    generator,
    listen: (port, cb) => server.listen(port, cb),
    close: (cb) => {
      for (const controller of generator.activeRuns.values()) {
        controller.abort();
      }
      server.close(() => {
        db.close();
        if (cb) cb();
      });
    }
  };
}

// Standalone CLI entrypoint
if (process.argv[1] && path.resolve(process.argv[1]) === path.resolve('server/index.mjs')) {
  const PORT = process.env.PORT || 3000;
  const app = createServer();
  app.listen(PORT, () => {
    console.log(`\n======================================================`);
    console.log(`🚀 Resumable Realtime Conversation Service Running!`);
    console.log(`👉 Web Client:   http://localhost:${PORT}`);
    console.log(`👉 Health Check: http://localhost:${PORT}/health`);
    console.log(`======================================================\n`);
  });
}
