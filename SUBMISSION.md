# Product Engineering Challenge Submission

## Candidate

- **Name:** Chakshika Pawar
- **Email:** chakshikapawar@gmail.com
- **GitHub:** [https://github.com/chakshika29](https://github.com/chakshika29)
- **Selected problem:** Problem 1: Resumable Realtime Conversation
- **Demo video:** [Loom / YouTube Video Demo Link](https://www.loom.com/share/placeholder-demo-video-chakshika-caygnus) *(See instructions below to record or run live in browser)*

---

## Run the project

### Prerequisites
- **Node.js**: v24.x recommended (Node 24 includes zero-external-dependency native `node:sqlite`).
- **Operating System**: Cross-platform (Windows, macOS, Linux).
- **External Dependencies**: Zero! Uses native Node.js ESM modules, native SQLite, and standard Web APIs. No `npm install` build step or C++ compiler required.

### Setup and Start Commands

```bash
# 1. Clone repository and navigate to root
cd product-engineer-ps

# 2. Start the realtime conversation service
npm start
```

The service will start on port `3000`:
- **Web UI & Interactive Client:** `http://localhost:3000`
- **Health Check Endpoint:** `http://localhost:3000/health`

### Triggering Acceptance Scenarios in the Web UI

1. **Successful Ordered Stream (AC1):**
   - Type a prompt in the message bar and click **Stream Response** (or select a quick prompt).
   - Observe live token streaming with monotonically increasing sequence IDs (`#1, #2, ...`) and status transitioning to `COMPLETED`.

2. **Network Interruption & Resumption from Cursor (AC2 & AC3):**
   - Click **Run Benchmark Live** or start streaming a message.
   - Click the **⚡ Drop Connection (2.5s)** button while tokens are actively streaming.
   - The connection badge transitions to `DISCONNECTED` / `RECONNECTING`.
   - The server continues generating tokens in the background to SQLite.
   - After 2.5 seconds, the client automatically reconnects with `?cursor=<last_seen_seq>`, replays missed tokens, seamlessly merges live tokens without gaps or duplicates, and finishes at `COMPLETED`.

3. **Generator Failure Recovery (AC5):**
   - Click **💥 Inject Generator Error (AC5)**.
   - The generator streams tokens up to sequence 4 and deliberately fails at sequence 5.
   - The run transitions to `FAILED`, the failure is preserved in SQLite, and subsequent reconnects remain bounded in `FAILED` state (never falsely completes).

4. **Stale / Out-of-Bounds Cursor (AC6):**
   - Click **⚠️ Stale Cursor (AC6)**.
   - The client requests `cursor=99999`. The server immediately returns an explicit, recoverable `HTTP 400 INVALID_CURSOR` response specifying available bounds `[earliest_seq, latest_seq]`.

5. **Durable SQLite Event Log:**
   - Click **📊 Inspect SQLite Log** to inspect the durable event log stored in SQLite.

---

## Run the tests

Run the complete automated test suite covering all acceptance criteria (AC1, AC2, AC3, AC4, AC5, AC6):

```bash
npm test
```

### Problem-Specific Verification Benchmark

Run the deterministic 30+ event verification benchmark with active interruption and resume:

```bash
npm run benchmark
```

---

## Acceptance scenarios and verification

### Completed Acceptance Scenarios

- [x] **AC1: Ordered live stream:** Verified in `tests/protocol.test.mjs`. Server streams chunks with strictly monotonic integer sequence IDs `1..N` followed by terminal event `run_completed`.
- [x] **AC2: Missed-event recovery:** Verified in `tests/protocol.test.mjs`. Client disconnects after event 4, generator completes in background, client reconnects with `cursor=4`, receiving exactly events 5..11 without duplicate or omitted chunks.
- [x] **AC3: Replay/live overlap:** Verified in `tests/protocol.test.mjs`. Client reconnects with `cursor=5` while the server is actively generating tokens 8..20. The server buffers live events before reading historical SQLite records, drops any buffered events `<= max_replayed_seq`, and seamlessly transitions to live delivery with 0 duplicates and 0 gaps.
- [x] **AC4: Service restart:** Verified in `tests/restart.test.mjs`. Server process is forcibly terminated mid-stream. On restart with the same SQLite database file, `db.reconcileDanglingRunsOnStartup()` detects in-flight runs, marks them `failed` with `'Service process restarted while generation was in progress'`, and appends a terminal `run_failed` event. Reconnecting clients recover all persisted events up to the crash point followed by the terminal state.
- [x] **AC5: Generation failure:** Verified in `tests/protocol.test.mjs`. When generator fails at sequence 5, durable history for tokens 1..4 is preserved in SQLite, the run is marked `failed`, and terminal event `run_failed` is emitted. Subsequent reconnects return the failed state and cannot transition to completed.
- [x] **AC6: Unknown or stale cursor:** Verified in `tests/protocol.test.mjs`. Requests with `cursor < 0` or `cursor > latest_known_seq` return HTTP 400 with `INVALID_CURSOR`, known bounds, and suggested recovery action.

### Problem-Specific Verification Benchmark Command and Observed Output

Command:
```bash
npm run benchmark
```

Observed Output:
```text
======================================================================
⚡ PROBLEM 1 VERIFICATION BENCHMARK: RESUMABLE REALTIME CONVERSATION
======================================================================
Candidate: Chakshika Pawar
Target:    >= 30 ordered events, active-stream interruption & resume
Criteria:  Strict monotonic order, 0 missing, 0 duplicates, completed state
----------------------------------------------------------------------
[1/5] Initializing conversation and run with 36 ordered events...
      Conversation: conv_bench_1789815936094
      Run ID:       run_1789815936746_72jlhv
[2/5] Client 1 connecting with cursor=0...
      ⚡ Abruptly dropped connection after event #12!
      Received in Phase 1: 12 events (seq 1..12)
[3/5] Verifying generation continues asynchronously in background...
      Server run status midway: 'running' (active: true)
[4/5] Client 2 reconnecting from checkpoint cursor=12...
      Received in Phase 2: 25 events (seq 13..37)
      Terminal event received: 'run_completed'
[5/5] Analyzing reconstructed stream integrity and deduplication...

======================================================================
📊 BENCHMARK VERIFICATION RESULTS
======================================================================
Total ordered text events generated:  36 (Requirement: >= 30) -> PASS
Total events with terminal event:     37
Interruption point:                   Sequence #12
Reconnection cursor used:             12
Phase 1 events received:              12
Phase 2 events received:              25
Total events observed:                37
Duplicate events count:               0 ✔ (0 duplicates)
Missing events count:                 0 ✔ (0 missing)
Strict sequence monotonicity:         ✔ STRICT MONOTONIC 1..37
Final terminal run state:             'completed' ✔
Text reconstruction matches fixture:  ✔ EXACT MATCH
======================================================================

🎉 ALL ACCEPTANCE SCENARIOS AND BENCHMARK CRITERIA VERIFIED SUCCESSFULLY!
```

---

## Architecture and data flow

### High-Level Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                             BROWSER CLIENT                                  │
│  ┌─────────────────────────┐                ┌────────────────────────────┐  │
│  │   Chat UI & Presets     │                │   State Machine & Dedup    │  │
│  │ (Interruption Controls) │                │  (Cursor: highestSeenSeq)  │  │
│  └───────────┬─────────────┘                └─────────────▲──────────────┘  │
└──────────────┼────────────────────────────────────────────┼─────────────────┘
               │ POST /api/conversations/:id/runs           │ GET .../stream?cursor=X
               │ (prompt, token_delay_ms, fail_at_seq)      │ SSE (id: seq_id, event, data)
               ▼                                            │
┌───────────────────────────────────────────────────────────┼─────────────────┐
│                           HTTP & SSE SERVER               │                 │
│  ┌─────────────────────────┐                ┌─────────────┴──────────────┐  │
│  │      REST Endpoints     │                │     SSE Stream Handler     │  │
│  │  (Runs, Events, Health) │                │ (Pre-Replay PubSub Buffer) │  │
│  └───────────┬─────────────┘                └───────▲────────────▲───────┘  │
│              ▼                                      │            │          │
│  ┌─────────────────────────┐                        │            │          │
│  │   Response Generator    │─── Publish Live ───────┘            │          │
│  │ (Deterministic Tokens)  │                                     │          │
│  └───────────┬─────────────┘                                     │          │
│              │ Append Event                                      │ Replay   │
│              ▼                                                   │          │
│  ┌───────────────────────────────────────────────────────────────┴───────┐  │
│  │                     DURABLE SQLITE STORE                              │  │
│  │   Tables: conversations, messages, runs, events (PK: run_id, seq_id)  │  │
│  │   Startup Reconciliation: Reconciles dangling runs after crash        │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Component Responsibilities

1. **`DatabaseStore` (`server/db.mjs`)**:
   - Manages SQLite tables: `conversations`, `messages`, `runs`, and `events`.
   - Assigns strictly monotonic sequence numbers (`seq_id`) scoped to each `run_id`.
   - Guarantees durability using SQLite WAL mode (`PRAGMA journal_mode = WAL`).
   - Reconciles dangling runs on startup (`reconcileDanglingRunsOnStartup()`): identifies runs stuck in `'running'` from previous crashes and transitions them to `'failed'` with `'process_restart'` terminal events.

2. **`RunPubSub` (`server/pubsub.mjs`)**:
   - In-memory event broker providing decoupled per-run pub/sub channels.
   - Allows multiple SSE connections to subscribe to live generation events.

3. **`ResponseGenerator` (`server/generator.mjs`)**:
   - Deterministic token generator producing ordered chunks.
   - Accepts failure injection parameters (`failAtSeq`) and custom token delays for testing.
   - Writes events to SQLite before publishing to `RunPubSub`.
   - Updates terminal state to `completed` or `failed`.

4. **`HTTPServer & SSE Streamer` (`server/index.mjs`)**:
   - Manages client connections, parses `cursor` from query parameters or `Last-Event-ID` header.
   - Validates cursor against known sequence bounds (`AC6`).
   - Implements the pre-replay buffering algorithm (`AC3`): subscribes to live events before reading historical records, streams replayed events, and drains the buffer discarding any `seq_id <= max_replayed_seq`.

5. **`RealtimeChatClient` (`client/app.js`)**:
   - Implements frontend state machine: `DISCONNECTED`, `CONNECTED`, `RECONNECTING`, `COMPLETED`, `FAILED`.
   - Tracks `clientCursor` (highest contiguous sequence rendered).
   - Discards duplicate events (`seq_id <= clientCursor`) and tracks sequence gaps.
   - Implements exponential backoff with jitter on network disconnects.

---

## Technology choices

- **Runtime & Language:** Node.js (v24.x ESM)
  - *Why?* Node 24 provides built-in native SQLite (`node:sqlite`), built-in test runner (`node:test`), and native WHATWG Streams.
  - *Zero Setup Friction:* Requires zero compilation tools (`node-gyp`, Visual Studio C++, Python build tools) and zero `npm install` dependencies. It works immediately out of the box on Windows, macOS, and Linux.
- **Transport:** Server-Sent Events (SSE) over HTTP
  - *Why SSE over WebSockets?* SSE is standard unidirectional HTTP with native browser reconnection support (`Last-Event-ID`). It integrates naturally with HTTP caching, load balancers, and corporate proxies without the stateful connection handshake overhead of WebSockets.
- **Storage:** SQLite with WAL Mode
  - *Why SQLite?* File-backed durability without requiring external service dependencies (Redis, Postgres). Embedded atomicity ensures zero risk of database network disconnects during evaluation.

---

## Important decisions

1. **Pre-Replay Buffering for Live/Replay Overlap (AC3):**
   - *Problem:* When a client reconnects during active generation, a race condition exists between reading historical events from SQLite and subscribing to ongoing live events. Live events could be missed or duplicated.
   - *Decision:* The SSE handler registers an in-memory buffer listener on `RunPubSub` *before* querying SQLite. It streams all events where `seq_id > cursor` from SQLite, records `max_replayed_seq`, and then flushes the buffer, dropping any buffered events where `seq_id <= max_replayed_seq`. This guarantees zero drops, zero duplicates, and strict monotonicity.

2. **Durable Startup Reconciler for Honest Restart Semantics (AC4):**
   - *Problem:* If the server process crashes or restarts while a run is in progress, the previous generator process is terminated. A naive server might leave the run in `'running'` indefinitely or silently wipe it.
   - *Decision:* On server startup, `db.reconcileDanglingRunsOnStartup()` queries for runs where `status = 'running'`. It updates their status to `'failed'` and appends a terminal `run_failed` event with reason `'process_restart'`. When a client reconnects with its cursor, it receives all historical tokens up to the crash point followed by the explicit restart failure event.

3. **Client-Side Deduplication & Gap Detection Boundary:**
   - *Problem:* Even with server-side filtering, transient transport retries or browser wakeups could re-deliver events.
   - *Decision:* The frontend client maintains `clientCursor = max(clientCursor, event.seq_id)`. Any event with `seq_id <= clientCursor` is silently dropped at the presentation boundary, updating a visible duplicate counter. Any non-contiguous sequence (`seq_id > clientCursor + 1`) increments the gap detector.

---

## Assumptions and limitations

- **Single Server Process:** The prototype assumes a single server instance managing SQLite and local in-memory pub/sub. In a distributed multi-node cluster, a shared message broker (Redis Streams or Kafka) and a shared database (Postgres) would be used.
- **Generator Process Lifetime on Crash:** When the server crashes, active generation stops. The system reconciles the run to `failed` rather than attempting prompt re-execution from the middle of generation, which avoids paying for model token re-generation or producing incoherent completions.
- **Fixed Retention:** The prototype currently retains full event history per run in SQLite. In a high-volume production system, historical events would be archived or compacted once the run reaches a terminal state.

---

## Production and scale

If scaling this system to millions of concurrent conversational turns in production:
1. **Distributed Event Streaming:** Replace the in-memory `RunPubSub` with Redis Streams or Apache Pulsar. Redis Streams naturally supports cursor-based reading (`XREAD` / `XRANGE`) and consumer groups.
2. **Database Partitioning & Compaction:** Partition the `events` table by `conversation_id` or timestamp. Once a run reaches `completed`, store the concatenated final message in `messages` and set a 7-day TTL or compaction policy on fine-grained chunk events.
3. **Sticky Sessions vs Distributed SSE Gateways:** Deploy stateless SSE edge gateways (using Cloudflare Workers or Envoy) that subscribe to PubSub channels and stream to clients, while backend worker pools execute model generation.
4. **Heartbeats & Keep-Alives:** Add periodic SSE comment pings (`: heartbeat\n\n`) every 15 seconds to prevent intermediate cloud proxies or mobile carriers from terminating idle connections.

---

## AI usage

- **AI Tools Used:** Antigravity AI assistant.
- **Contribution:** Assisted in scaffolding test fixtures, optimizing SQL schema statements, and designing the CSS layout.
- **Review & Verification:** Every line of code, test case, SQL query, and protocol transition was reviewed, tested, and validated locally via deterministic automated test suites (`node --test`) and manual verification.

---

## Credibility note

### Project: Real-Time Fleet Telemetry & Dispatch System (Vehicle Rental & Tracking)
- **Problem Solved:** Engineered a high-reliability full-stack web application and fleet management platform tracking hundreds of concurrent vehicle states, reservation lifecycles, and asynchronous status updates across intermittent mobile network connections.
- **Personal Contribution:** Designed and implemented the backend REST APIs, WebSocket/SSE update pipelines, and frontend reactive state management. Built the client-side optimistic UI updates and reconnection synchronization logic to handle poor mobile network coverage.
- **Operational Complexity & Scale:** Handled continuous state transitions across multiple concurrent users and live status streams. Prevented race conditions between booking dispatch events and vehicle availability updates using database row-level locking and idempotent transaction boundaries.
- **Difficult Engineering Decision:** Faced with network drops where client reservations could be double-submitted or stale vehicle states displayed, chose to replace optimistic fire-and-forget requests with an idempotent client-generated UUID request key and cursor-based event replay log. This eliminated duplicate bookings and ensured 100% consistent state across browser refreshes and mobile reconnections.
- **Public Evidence:** Available on GitHub: [chakshika29/Vechile-Rental-System](https://github.com/chakshika29/Vechile-Rental-System) and [chakshika29](https://github.com/chakshika29).
