import { DatabaseSync } from 'node:sqlite';
import fs from 'node:fs';
import path from 'node:path';

export class DatabaseStore {
  constructor(dbPath = './data/conversations.db') {
    this.dbPath = dbPath;
    if (dbPath !== ':memory:') {
      const dir = path.dirname(path.resolve(dbPath));
      if (!fs.existsSync(dir)) {
        fs.mkdirSync(dir, { recursive: true });
      }
    }
    this.db = new DatabaseSync(dbPath);
    this.initSchema();
  }

  initSchema() {
    this.db.exec(`
      PRAGMA journal_mode = WAL;
      PRAGMA synchronous = NORMAL;

      CREATE TABLE IF NOT EXISTS conversations (
        id TEXT PRIMARY KEY,
        created_at INTEGER NOT NULL
      );

      CREATE TABLE IF NOT EXISTS messages (
        id TEXT PRIMARY KEY,
        conversation_id TEXT NOT NULL,
        role TEXT NOT NULL,
        content TEXT NOT NULL,
        created_at INTEGER NOT NULL,
        FOREIGN KEY (conversation_id) REFERENCES conversations(id)
      );

      CREATE TABLE IF NOT EXISTS runs (
        id TEXT PRIMARY KEY,
        conversation_id TEXT NOT NULL,
        user_message_id TEXT NOT NULL,
        status TEXT NOT NULL,
        error_message TEXT,
        created_at INTEGER NOT NULL,
        updated_at INTEGER NOT NULL,
        FOREIGN KEY (conversation_id) REFERENCES conversations(id)
      );

      CREATE TABLE IF NOT EXISTS events (
        run_id TEXT NOT NULL,
        seq_id INTEGER NOT NULL,
        event_type TEXT NOT NULL,
        payload TEXT NOT NULL,
        created_at INTEGER NOT NULL,
        PRIMARY KEY (run_id, seq_id),
        FOREIGN KEY (run_id) REFERENCES runs(id)
      );

      CREATE INDEX IF NOT EXISTS idx_events_run_seq ON events(run_id, seq_id);
      CREATE INDEX IF NOT EXISTS idx_runs_conversation ON runs(conversation_id);
    `);
  }

  createConversation(id = `conv_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`) {
    const existing = this.getConversation(id);
    if (existing) return existing;
    const now = Date.now();
    const stmt = this.db.prepare('INSERT INTO conversations (id, created_at) VALUES (?, ?)');
    stmt.run(id, now);
    return { id, created_at: now };
  }

  getConversation(id) {
    const stmt = this.db.prepare('SELECT * FROM conversations WHERE id = ?');
    return stmt.get(id);
  }

  createMessage({ id = `msg_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`, conversationId, role, content }) {
    const now = Date.now();
    const stmt = this.db.prepare('INSERT INTO messages (id, conversation_id, role, content, created_at) VALUES (?, ?, ?, ?, ?)');
    stmt.run(id, conversationId, role, content, now);
    return { id, conversation_id: conversationId, role, content, created_at: now };
  }

  getMessages(conversationId) {
    const stmt = this.db.prepare('SELECT * FROM messages WHERE conversation_id = ? ORDER BY created_at ASC');
    return stmt.all(conversationId);
  }

  createRun({ id = `run_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`, conversationId, userMessageId, status = 'running' }) {
    const now = Date.now();
    const stmt = this.db.prepare('INSERT INTO runs (id, conversation_id, user_message_id, status, error_message, created_at, updated_at) VALUES (?, ?, ?, ?, NULL, ?, ?)');
    stmt.run(id, conversationId, userMessageId, status, now, now);
    return { id, conversation_id: conversationId, user_message_id: userMessageId, status, error_message: null, created_at: now, updated_at: now };
  }

  getRun(id) {
    const stmt = this.db.prepare('SELECT * FROM runs WHERE id = ?');
    return stmt.get(id);
  }

  updateRunStatus(id, status, errorMessage = null) {
    const now = Date.now();
    const stmt = this.db.prepare('UPDATE runs SET status = ?, error_message = ?, updated_at = ? WHERE id = ?');
    stmt.run(status, errorMessage, now, id);
    return this.getRun(id);
  }

  appendEvent({ runId, seqId, eventType, payload }) {
    const now = Date.now();
    const payloadStr = typeof payload === 'string' ? payload : JSON.stringify(payload);
    const stmt = this.db.prepare('INSERT INTO events (run_id, seq_id, event_type, payload, created_at) VALUES (?, ?, ?, ?, ?)');
    stmt.run(runId, seqId, eventType, payloadStr, now);
    return {
      run_id: runId,
      seq_id: seqId,
      event_type: eventType,
      payload: typeof payload === 'string' ? JSON.parse(payload) : payload,
      created_at: now
    };
  }

  getEventsAfterCursor(runId, cursor = 0) {
    const stmt = this.db.prepare('SELECT * FROM events WHERE run_id = ? AND seq_id > ? ORDER BY seq_id ASC');
    const rows = stmt.all(runId, cursor);
    return rows.map(r => ({
      run_id: r.run_id,
      seq_id: r.seq_id,
      event_type: r.event_type,
      payload: JSON.parse(r.payload),
      created_at: r.created_at
    }));
  }

  getAllEvents(runId) {
    return this.getEventsAfterCursor(runId, 0);
  }

  getEventBounds(runId) {
    const stmt = this.db.prepare(`
      SELECT 
        MIN(seq_id) as earliest_seq,
        MAX(seq_id) as latest_seq,
        COUNT(seq_id) as total_events
      FROM events 
      WHERE run_id = ?
    `);
    const res = stmt.get(runId);
    return {
      earliest_seq: res.earliest_seq || 0,
      latest_seq: res.latest_seq || 0,
      total_events: res.total_events || 0
    };
  }

  reconcileDanglingRunsOnStartup() {
    // AC4: Preserving completed or resumable state across a service restart
    // Any run left in 'running' when the service starts was interrupted by process termination.
    const stmt = this.db.prepare("SELECT * FROM runs WHERE status = 'running'");
    const danglingRuns = stmt.all();
    const reconciled = [];

    for (const run of danglingRuns) {
      const bounds = this.getEventBounds(run.id);
      const nextSeq = bounds.latest_seq + 1;
      const errorMsg = 'Service process restarted while generation was in progress';

      this.updateRunStatus(run.id, 'failed', errorMsg);
      this.appendEvent({
        runId: run.id,
        seqId: nextSeq,
        eventType: 'run_failed',
        payload: {
          run_id: run.id,
          seq_id: nextSeq,
          error: errorMsg,
          reason: 'process_restart',
          interrupted_at_seq: bounds.latest_seq,
          is_terminal: true
        }
      });

      reconciled.push({
        run_id: run.id,
        interrupted_at_seq: bounds.latest_seq,
        terminal_seq: nextSeq
      });
    }

    return reconciled;
  }

  close() {
    this.db.close();
  }
}
