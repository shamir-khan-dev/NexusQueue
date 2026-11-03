import { EventEmitter } from 'node:events';
import { IBroker } from '../brokers/interface.js';
import { Task } from './types.js';
import { globalMetrics } from '../telemetry/metrics.js';

export interface SupervisorOptions {
  checkIntervalMs?: number;
  heartbeatTimeoutMs?: number;
}

export class ZombieSupervisor extends EventEmitter {
  private broker: IBroker;
  private checkIntervalMs: number;
  private heartbeatTimeoutMs: number;
  private intervalTimer: NodeJS.Timeout | null = null;
  private isRunning: boolean = false;

  constructor(broker: IBroker, options: SupervisorOptions = {}) {
    super();
    this.broker = broker;
    this.checkIntervalMs = options.checkIntervalMs ?? 1000;
    this.heartbeatTimeoutMs = options.heartbeatTimeoutMs ?? 4000;
  }

  start(): void {
    if (this.isRunning) return;
    this.isRunning = true;

    this.intervalTimer = setInterval(async () => {
      await this.scanAndReclaim();
    }, this.checkIntervalMs);
    this.intervalTimer.unref();

    this.emit('started', {
      interval: this.checkIntervalMs,
      timeout: this.heartbeatTimeoutMs,
    });
  }

  async scanAndReclaim(): Promise<Task[]> {
    try {
      const reclaimed = await this.broker.reclaimZombies(this.heartbeatTimeoutMs);
      if (reclaimed.length > 0) {
        for (const _ of reclaimed) {
          globalMetrics.recordZombieReclaimed();
        }
        this.emit('zombies:reclaimed', reclaimed);
      }
      return reclaimed;
    } catch (err) {
      this.emit('error', err);
      return [];
    }
  }

  stop(): void {
    this.isRunning = false;
    if (this.intervalTimer) {
      clearInterval(this.intervalTimer);
      this.intervalTimer = null;
    }
    this.emit('stopped');
  }
}

