import { EventEmitter } from 'node:events';

export class RunPubSub {
  constructor() {
    this.emitter = new EventEmitter();
    this.emitter.setMaxListeners(100);
  }

  getChannelName(runId) {
    return `run:${runId}`;
  }

  publish(runId, event) {
    this.emitter.emit(this.getChannelName(runId), event);
  }

  subscribe(runId, listener) {
    const channel = this.getChannelName(runId);
    this.emitter.on(channel, listener);
    return () => {
      this.emitter.off(channel, listener);
    };
  }

  subscriberCount(runId) {
    return this.emitter.listenerCount(this.getChannelName(runId));
  }
}
