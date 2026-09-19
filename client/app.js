// Resumable Realtime Conversation - Client State Machine & Protocol Driver
// Candidate: Chakshika Pawar (https://github.com/chakshika29)

class RealtimeChatClient {
  constructor() {
    this.conversationId = this.getOrCreateConversationId();
    this.activeRunId = null;
    this.clientCursor = 0; // Highest contiguous sequence ID rendered
    this.eventsReceived = 0;
    this.duplicatesFiltered = 0;
    this.gapsDetected = 0;
    this.connectionState = 'DISCONNECTED';
    this.reconnectAttempts = 0;
    this.maxReconnectAttempts = 5;
    this.reconnectTimer = null;
    this.abortController = null;
    this.currentAssistantBubble = null;
    this.isDeliberateDisconnect = false;

    this.initElements();
    this.bindEvents();
    this.log('info', `Initialized conversation ${this.conversationId}`);
  }

  getOrCreateConversationId() {
    let id = sessionStorage.getItem('caygnus_conv_id');
    if (!id) {
      id = `conv_${Date.now()}_${Math.random().toString(36).slice(2, 7)}`;
      sessionStorage.setItem('caygnus_conv_id', id);
    }
    return id;
  }

  initElements() {
    this.elStatusBadge = document.getElementById('connection-badge');
    this.elStatusText = document.getElementById('connection-status-text');
    this.elRunStatus = document.getElementById('metric-run-status');
    this.elCursor = document.getElementById('metric-cursor');
    this.elEvents = document.getElementById('metric-events');
    this.elDuplicates = document.getElementById('metric-duplicates');
    this.elGaps = document.getElementById('metric-gaps');
    this.elRunId = document.getElementById('metric-run-id');
    this.elPhaseTag = document.getElementById('stream-phase-indicator');
    this.elChatMessages = document.getElementById('chat-messages');
    this.elProtocolLogs = document.getElementById('protocol-logs');
    this.elMessageInput = document.getElementById('message-input');
    this.elChatForm = document.getElementById('chat-form');
    this.elDbModal = document.getElementById('db-modal');
    this.elDbJsonView = document.getElementById('db-json-view');
  }

  bindEvents() {
    this.elChatForm.addEventListener('submit', (e) => {
      e.preventDefault();
      const text = this.elMessageInput.value.trim();
      if (!text) return;
      this.elMessageInput.value = '';
      this.startNewTurn({ prompt: text });
    });

    document.getElementById('btn-drop-network').addEventListener('click', () => {
      this.simulateNetworkDrop(2500);
    });

    document.getElementById('btn-reconnect').addEventListener('click', () => {
      if (this.activeRunId) {
        this.log('reconnect', `Manual reconnect requested with cursor=${this.clientCursor}`);
        this.connectStream(this.activeRunId, this.clientCursor);
      } else {
        alert('Please start a turn or click "Run Benchmark Live" first before reconnecting.');
      }
    });

    document.getElementById('btn-fail-generator').addEventListener('click', () => {
      this.startNewTurn({
        prompt: 'Simulate generator failure midway (AC5 test)',
        words: ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon', 'Zeta', 'Eta', 'Theta'],
        tokenDelayMs: 60,
        failAtSeq: 5
      });
    });

    document.getElementById('btn-stale-cursor').addEventListener('click', () => {
      if (!this.activeRunId) {
        alert('Please start a turn or benchmark first to generate an active run ID.');
        return;
      }
      this.log('error', `Attempting stream with out-of-bounds cursor=99999 (AC6 test)`);
      this.connectStream(this.activeRunId, 99999);
    });

    document.getElementById('btn-run-benchmark').addEventListener('click', () => {
      this.runInteractiveBenchmark();
    });

    document.getElementById('btn-inspect-db').addEventListener('click', () => {
      this.inspectSQLiteRecords();
    });

    document.getElementById('btn-close-modal').addEventListener('click', () => {
      this.elDbModal.classList.add('hidden');
    });

    document.getElementById('btn-clear-logs').addEventListener('click', () => {
      this.elProtocolLogs.innerHTML = '';
    });
  }

  updateConnectionState(state, detail = '') {
    this.connectionState = state;
    this.elStatusBadge.className = 'badge';
    
    switch (state) {
      case 'CONNECTED':
        this.elStatusBadge.classList.add('badge-connected');
        this.elStatusText.textContent = 'CONNECTED';
        break;
      case 'RECONNECTING':
        this.elStatusBadge.classList.add('badge-reconnecting');
        this.elStatusText.textContent = detail || 'RECONNECTING';
        break;
      case 'DISCONNECTED':
        this.elStatusBadge.classList.add('badge-disconnected');
        this.elStatusText.textContent = 'DISCONNECTED';
        break;
      case 'COMPLETED':
        this.elStatusBadge.classList.add('badge-connected');
        this.elStatusText.textContent = 'STREAM COMPLETED';
        break;
      case 'FAILED':
        this.elStatusBadge.classList.add('badge-failed');
        this.elStatusText.textContent = 'RUN FAILED';
        break;
    }
  }

  updateMetrics() {
    this.elCursor.textContent = this.clientCursor;
    this.elEvents.textContent = this.eventsReceived;
    this.elDuplicates.textContent = this.duplicatesFiltered;
    this.elGaps.textContent = this.gapsDetected;
    this.elRunId.textContent = this.activeRunId ? this.activeRunId.slice(-10) : 'none';
  }

  log(type, msg, seq = null) {
    const time = new Date().toLocaleTimeString();
    const entry = document.createElement('div');
    entry.className = `log-entry log-${type}`;
    
    let seqBadge = seq !== null ? `<span class="log-seq">#${seq}</span>` : '';
    entry.innerHTML = `<span class="log-time">[${time}]</span>${seqBadge}<span class="log-msg">${msg}</span>`;
    
    this.elProtocolLogs.appendChild(entry);
    this.elProtocolLogs.scrollTop = this.elProtocolLogs.scrollHeight;
  }

  appendUserMessage(text) {
    const msg = document.createElement('div');
    msg.className = 'message message-user';
    msg.innerHTML = `
      <div class="message-bubble">${this.escapeHtml(text)}</div>
      <div class="message-meta">${new Date().toLocaleTimeString()}</div>
    `;
    this.elChatMessages.appendChild(msg);
    this.elChatMessages.scrollTop = this.elChatMessages.scrollHeight;
  }

  createAssistantBubble() {
    const msg = document.createElement('div');
    msg.className = 'message message-assistant';
    const bubble = document.createElement('div');
    bubble.className = 'message-bubble';
    bubble.innerHTML = '<span class="bubble-text"></span><span class="typing-cursor"></span>';
    
    const meta = document.createElement('div');
    meta.className = 'message-meta';
    meta.textContent = `${new Date().toLocaleTimeString()} • Streaming`;

    msg.appendChild(bubble);
    msg.appendChild(meta);
    this.elChatMessages.appendChild(msg);
    this.elChatMessages.scrollTop = this.elChatMessages.scrollHeight;

    this.currentAssistantBubble = {
      container: msg,
      bubbleText: bubble.querySelector('.bubble-text'),
      cursor: bubble.querySelector('.typing-cursor'),
      meta
    };
  }

  escapeHtml(str) {
    const p = document.createElement('p');
    p.textContent = str;
    return p.innerHTML;
  }

  async startNewTurn({ prompt, words = null, tokenDelayMs = 35, failAtSeq = null }) {
    if (this.abortController) {
      this.abortController.abort();
    }
    clearTimeout(this.reconnectTimer);

    // Reset client counters for new turn
    this.clientCursor = 0;
    this.eventsReceived = 0;
    this.duplicatesFiltered = 0;
    this.gapsDetected = 0;
    this.reconnectAttempts = 0;
    this.updateMetrics();

    this.appendUserMessage(prompt);
    this.createAssistantBubble();

    this.elRunStatus.textContent = 'RUNNING';
    this.elRunStatus.className = 'metric-value status-running';
    this.elPhaseTag.textContent = 'Starting Run';

    try {
      this.log('info', `Submitting run for conversation ${this.conversationId}...`);
      const res = await fetch(`/api/conversations/${this.conversationId}/runs`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          prompt,
          words,
          token_delay_ms: tokenDelayMs,
          fail_at_seq: failAtSeq
        })
      });

      if (!res.ok) {
        throw new Error(`Server returned ${res.status}: ${res.statusText}`);
      }

      const data = await res.json();
      this.activeRunId = data.run_id;
      this.updateMetrics();
      this.log('info', `Run started successfully: ${this.activeRunId}`);

      // Begin SSE Stream from cursor=0
      this.connectStream(this.activeRunId, 0);
    } catch (err) {
      this.log('error', `Failed to start run: ${err.message}`);
      this.updateConnectionState('FAILED');
      this.currentAssistantBubble.cursor.remove();
      this.currentAssistantBubble.meta.textContent += ` • Failed to start`;
    }
  }

  async connectStream(runId, fromCursor = 0) {
    if (this.abortController) {
      this.isDeliberateDisconnect = true;
      this.abortController.abort();
    }
    this.isDeliberateDisconnect = false;
    this.abortController = new AbortController();

    this.updateConnectionState('RECONNECTING', `CONNECTING (cursor=${fromCursor})`);
    this.elPhaseTag.textContent = `Connecting (seq ${fromCursor})`;

    const url = `/api/conversations/${this.conversationId}/runs/${runId}/stream?cursor=${fromCursor}`;
    this.log('reconnect', `Opening SSE stream to ${url}`);

    try {
      const response = await fetch(url, {
        headers: {
          'Last-Event-ID': fromCursor.toString()
        },
        signal: this.abortController.signal
      });

      // AC6 Check: Explicit recoverable error
      if (!response.ok) {
        const errorJson = await response.json().catch(() => ({}));
        this.log('error', `Stream rejected with HTTP ${response.status}: ${errorJson.error || 'Error'} (${errorJson.message || ''})`);
        this.updateConnectionState('FAILED');
        this.elRunStatus.textContent = 'INVALID CURSOR';
        this.elRunStatus.className = 'metric-value status-failed';
        if (this.currentAssistantBubble) {
          this.currentAssistantBubble.cursor.remove();
          this.currentAssistantBubble.meta.textContent = `Rejected: ${errorJson.message || 'Cursor error'}`;
        }
        return;
      }

      this.updateConnectionState('CONNECTED');
      this.elPhaseTag.textContent = fromCursor > 0 ? 'Resumed Stream' : 'Live Stream';
      this.reconnectAttempts = 0;

      const reader = response.body.getReader();
      const decoder = new TextDecoder();
      let streamBuffer = '';

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        streamBuffer += decoder.decode(value, { stream: true });
        const parts = streamBuffer.split('\n\n');
        streamBuffer = parts.pop();

        for (const part of parts) {
          if (!part.trim()) continue;
          this.parseAndProcessSSE(part);
        }
      }

      if (this.connectionState === 'CONNECTED') {
        this.updateConnectionState('COMPLETED');
      }
    } catch (err) {
      if (err.name === 'AbortError' || this.isDeliberateDisconnect) {
        this.log('info', 'Active connection closed deliberately.');
        return;
      }
      this.log('error', `Connection error: ${err.message}`);
      this.handleStreamDisconnect(runId);
    }
  }

  parseAndProcessSSE(rawBlock) {
    const lines = rawBlock.split('\n');
    let id = null;
    let eventType = null;
    let data = null;

    for (const line of lines) {
      const cleanLine = line.replace(/\r$/, '');
      if (cleanLine.startsWith('id: ')) {
        id = parseInt(cleanLine.slice(4).trim(), 10);
      } else if (cleanLine.startsWith('event: ')) {
        eventType = cleanLine.slice(7).trim();
      } else if (cleanLine.startsWith('data: ')) {
        try {
          data = JSON.parse(cleanLine.slice(6));
        } catch (e) {
          data = cleanLine.slice(6);
        }
      }
    }

    if (id === null || !eventType || !data) return;

    this.eventsReceived++;

    // Crucial Invariant: Client-side deduplication check
    if (id <= this.clientCursor) {
      this.duplicatesFiltered++;
      this.updateMetrics();
      this.log('replay', `[DEDUP] Discarded duplicate event #${id} (<= cursor ${this.clientCursor})`, id);
      return;
    }

    // Gap detection check
    if (this.clientCursor > 0 && id > this.clientCursor + 1) {
      const gap = id - (this.clientCursor + 1);
      this.gapsDetected += gap;
      this.log('error', `[GAP] Detected sequence gap! Expected ${this.clientCursor + 1}, got ${id}`, id);
    }

    this.clientCursor = id;
    this.updateMetrics();

    if (eventType === 'chunk') {
      this.log('chunk', `Chunk: "${data.chunk.trim()}"`, id);
      if (this.currentAssistantBubble) {
        this.currentAssistantBubble.bubbleText.textContent += data.chunk;
        this.elChatMessages.scrollTop = this.elChatMessages.scrollHeight;
      }
    } else if (eventType === 'run_completed') {
      this.log('completed', `Run completed successfully (${data.total_events} events total)`, id);
      this.updateConnectionState('COMPLETED');
      this.elRunStatus.textContent = 'COMPLETED';
      this.elRunStatus.className = 'metric-value status-completed';
      this.elPhaseTag.textContent = 'Finished';
      if (this.currentAssistantBubble) {
        this.currentAssistantBubble.cursor.remove();
        this.currentAssistantBubble.meta.textContent = `${new Date().toLocaleTimeString()} • Completed (Zero Duplicates)`;
      }
    } else if (eventType === 'run_failed') {
      this.log('failed', `Run failed: ${data.error} (reason: ${data.reason || 'runtime_error'})`, id);
      this.updateConnectionState('FAILED');
      this.elRunStatus.textContent = 'FAILED';
      this.elRunStatus.className = 'metric-value status-failed';
      this.elPhaseTag.textContent = 'Failed';
      if (this.currentAssistantBubble) {
        this.currentAssistantBubble.cursor.remove();
        this.currentAssistantBubble.meta.textContent = `${new Date().toLocaleTimeString()} • Run Failed: ${data.error}`;
      }
    }
  }

  handleStreamDisconnect(runId) {
    if (this.connectionState === 'COMPLETED' || this.connectionState === 'FAILED') {
      return;
    }

    this.reconnectAttempts++;
    if (this.reconnectAttempts > this.maxReconnectAttempts) {
      this.log('failed', `Max reconnect attempts (${this.maxReconnectAttempts}) reached. Stream halted.`);
      this.updateConnectionState('FAILED');
      return;
    }

    // Exponential backoff with jitter
    const baseDelay = 600;
    const backoff = Math.min(baseDelay * Math.pow(1.6, this.reconnectAttempts - 1), 4000);
    const jitter = Math.floor(Math.random() * 200);
    const delay = backoff + jitter;

    this.updateConnectionState('RECONNECTING', `RECONNECTING (${this.reconnectAttempts}/${this.maxReconnectAttempts}) in ${delay}ms`);
    this.log('reconnect', `Connection lost. Reconnecting from cursor=${this.clientCursor} in ${delay}ms...`);

    this.reconnectTimer = setTimeout(() => {
      this.connectStream(runId, this.clientCursor);
    }, delay);
  }

  simulateNetworkDrop(durationMs = 2500) {
    if (!this.activeRunId || !this.abortController) {
      alert('No active stream to drop. Click "Stream Response" or "Run Benchmark Live" first.');
      return;
    }

    clearTimeout(this.reconnectTimer);
    clearTimeout(this.networkDropTimer);

    this.log('warning', `⚡ Simulating abrupt network failure for ${durationMs}ms... (cursor checkpoint: ${this.clientCursor})`);
    this.isDeliberateDisconnect = true;
    this.abortController.abort();
    this.updateConnectionState('DISCONNECTED');
    this.elPhaseTag.textContent = 'Network Disconnected';

    this.networkDropTimer = setTimeout(() => {
      this.log('reconnect', `Network restored. Resuming stream from checkpoint cursor=${this.clientCursor}...`);
      this.connectStream(this.activeRunId, this.clientCursor);
    }, durationMs);
  }

  async runInteractiveBenchmark() {
    this.log('info', '🚀 Starting interactive 36-token benchmark with in-flight interruption and recovery...');
    
    // Default benchmark fixture
    const benchmarkWords = [
      'Persistent', 'conversational', 'companions', 'require', 'thoughtful',
      'client', 'state,', 'realtime', 'streaming', 'protocols,',
      'durable', 'event', 'logs,', 'and', 'resilient',
      'reconnection', 'mechanisms', 'across', 'unreliable', 'network',
      'boundaries.', 'When', 'connections', 'drop', 'or',
      'processes', 'restart,', 'every', 'streamed', 'token',
      'must', 'survive', 'without', 'loss', 'or', 'duplication.'
    ];

    await this.startNewTurn({
      prompt: 'Benchmark: Demonstrate 36-token ordered stream with active mid-flight interruption and resumption.',
      words: benchmarkWords,
      tokenDelayMs: 65 // 65ms per token allows visible interruption
    });

    // Automatically drop connection after sequence 12 arrives
    const checkInterval = setInterval(() => {
      if (this.clientCursor >= 12 && this.connectionState === 'CONNECTED') {
        clearInterval(checkInterval);
        this.log('warning', `⚡ Benchmark trigger: Client cursor reached #${this.clientCursor}. Forcibly dropping connection for 1.8s!`);
        this.simulateNetworkDrop(1800);
      } else if (this.connectionState === 'COMPLETED' || this.connectionState === 'FAILED') {
        clearInterval(checkInterval);
      }
    }, 50);
  }

  async inspectSQLiteRecords() {
    if (!this.activeRunId) {
      alert('No run has been executed yet.');
      return;
    }

    this.elDbModal.classList.remove('hidden');
    this.elDbJsonView.textContent = 'Fetching SQLite event log from server...';

    try {
      const res = await fetch(`/api/conversations/${this.conversationId}/runs/${this.activeRunId}/events`);
      const data = await res.json();
      this.elDbJsonView.textContent = JSON.stringify(data, null, 2);
    } catch (err) {
      this.elDbJsonView.textContent = `Error fetching records: ${err.message}`;
    }
  }
}

// Instantiate on load
window.addEventListener('DOMContentLoaded', () => {
  window.chatClient = new RealtimeChatClient();
});
