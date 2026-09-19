import { setTimeout as sleep } from 'node:timers/promises';

// Default deterministic token corpora for benchmarks and demonstrations
export const DEFAULT_BENCHMARK_WORDS = [
  'Persistent', 'conversational', 'companions', 'require', 'thoughtful',
  'client', 'state,', 'realtime', 'streaming', 'protocols,',
  'durable', 'event', 'logs,', 'and', 'resilient',
  'reconnection', 'mechanisms', 'across', 'unreliable', 'network',
  'boundaries.', 'When', 'connections', 'drop', 'or',
  'processes', 'restart,', 'every', 'streamed', 'token',
  'must', 'survive', 'without', 'loss', 'or', 'duplication.'
]; // Exactly 36 words!

export class ResponseGenerator {
  constructor({ db, pubsub }) {
    this.db = db;
    this.pubsub = pubsub;
    this.activeRuns = new Map(); // runId -> AbortController
  }

  async startRun({
    runId,
    conversationId,
    prompt,
    words = null,
    totalTokens = null,
    tokenDelayMs = 40,
    failAtSeq = null
  }) {
    const abortController = new AbortController();
    this.activeRuns.set(runId, abortController);

    // Run in background asynchronously
    (async () => {
      let currentSeq = 0;
      let accumulatedText = '';

      try {
        let tokenList = [];
        if (words && Array.isArray(words)) {
          tokenList = words;
        } else if (totalTokens) {
          tokenList = Array.from({ length: totalTokens }, (_, i) => `[Token-${i + 1}] `);
        } else if (prompt && prompt.toLowerCase().includes('benchmark')) {
          tokenList = DEFAULT_BENCHMARK_WORDS;
        } else {
          // Default response tokens based on prompt
          tokenList = [
            'Hello! ', 'I ', 'am ', 'your ', 'resilient ', 'AI ', 'companion. ',
            'I ', 'can ', 'stream ', 'responses ', 'smoothly, ', 'recover ', 'from ',
            'network ', 'disconnects, ', 'and ', 'guarantee ', 'strict ', 'monotonic ',
            'event ', 'ordering ', 'using ', 'durable ', 'SQLite ', 'storage.'
          ];
        }

        for (let i = 0; i < tokenList.length; i++) {
          if (abortController.signal.aborted) {
            // Process abruptly killed mid-stream; do not update DB or emit terminal events
            return;
          }

          currentSeq = i + 1;

          // Simulated failure at specific sequence
          if (failAtSeq !== null && currentSeq >= failAtSeq) {
            throw new Error(`Simulated generator failure triggered at sequence ${currentSeq}`);
          }

          const chunk = tokenList[i] + (tokenList[i].endsWith(' ') || tokenList[i].endsWith('\n') ? '' : ' ');
          accumulatedText += chunk;

          const event = this.db.appendEvent({
            runId,
            seqId: currentSeq,
            eventType: 'chunk',
            payload: {
              seq_id: currentSeq,
              run_id: runId,
              chunk: chunk,
              text_so_far: accumulatedText,
              is_terminal: false
            }
          });

          this.pubsub.publish(runId, event);

          if (tokenDelayMs > 0) {
            try {
              await sleep(tokenDelayMs, null, { signal: abortController.signal });
            } catch (err) {
              if (abortController.signal.aborted) return;
              throw err;
            }
            if (abortController.signal.aborted) return;
          }
        }

        // Terminal success event
        const terminalSeq = currentSeq + 1;
        this.db.updateRunStatus(runId, 'completed', null);

        // Store assistant message in messages table
        this.db.createMessage({
          conversationId,
          role: 'assistant',
          content: accumulatedText.trim()
        });

        const terminalEvent = this.db.appendEvent({
          runId,
          seqId: terminalSeq,
          eventType: 'run_completed',
          payload: {
            seq_id: terminalSeq,
            run_id: runId,
            total_events: terminalSeq,
            final_text: accumulatedText.trim(),
            is_terminal: true
          }
        });

        this.pubsub.publish(runId, terminalEvent);
      } catch (err) {
        // AC5: Generation failure - terminal state is failed, durable history is preserved, never becomes completed
        const bounds = this.db.getEventBounds(runId);
        const terminalSeq = bounds.latest_seq + 1;
        const errorMsg = err.message || 'Generator error';

        this.db.updateRunStatus(runId, 'failed', errorMsg);

        const failureEvent = this.db.appendEvent({
          runId,
          seqId: terminalSeq,
          eventType: 'run_failed',
          payload: {
            seq_id: terminalSeq,
            run_id: runId,
            error: errorMsg,
            interrupted_at_seq: bounds.latest_seq,
            is_terminal: true
          }
        });

        this.pubsub.publish(runId, failureEvent);
      } finally {
        this.activeRuns.delete(runId);
      }
    })();

    return { runId, status: 'running' };
  }

  cancelRun(runId) {
    const controller = this.activeRuns.get(runId);
    if (controller) {
      controller.abort();
      return true;
    }
    return false;
  }

  isRunActive(runId) {
    return this.activeRuns.has(runId);
  }
}
