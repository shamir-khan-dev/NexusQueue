import { randomUUID } from 'node:crypto';
import { EventEmitter } from 'node:events';
import { IBroker } from '../brokers/interface.js';
import { Task, TaskHandler, WorkerInfo } from './types.js';
import { calculateBackoff } from './backoff.js';
import { globalMetrics } from '../telemetry/metrics.js';
import { TokenBucketRateLimiter, SlidingWindowRateLimiter } from './rate-limiter.js';

export interface WorkerOptions {
  id?: string;
  queue: string;
  concurrency?: number;
  pollIntervalMs?: number;
  heartbeatIntervalMs?: number;
  rateLimiter?: TokenBucketRateLimiter | SlidingWindowRateLimiter;
}

export class Worker extends EventEmitter {
  public readonly id: string;
  public readonly queue: string;
  public readonly concurrency: number;
  private broker: IBroker;
  private handler: TaskHandler;
  private pollIntervalMs: number;
  private heartbeatIntervalMs: number;
  private rateLimiter?: TokenBucketRateLimiter | SlidingWindowRateLimiter;

  private isRunning: boolean = false;
  private isPaused: boolean = false;
  private isSimulatingCrash: boolean = false;
  private activeTasks: Map<string, { task: Task; abortController: AbortController }> = new Map();
  private heartbeatTimer: NodeJS.Timeout | null = null;
  private pollTimer: NodeJS.Timeout | null = null;

  constructor(
    broker: IBroker,
    handler: TaskHandler,
    options: WorkerOptions
  ) {
    super();
    this.broker = broker;
    this.handler = handler;
    this.id = options.id ?? `worker-${randomUUID().slice(0, 8)}`;
    this.queue = options.queue;
    this.concurrency = options.concurrency ?? 5;
    this.pollIntervalMs = options.pollIntervalMs ?? 50;
    this.heartbeatIntervalMs = options.heartbeatIntervalMs ?? 1500;
    this.rateLimiter = options.rateLimiter;
  }

  get activeCount(): number {
    return this.activeTasks.size;
  }

  get status(): 'active' | 'crashed' | 'stopping' | 'stopped' {
    if (this.isSimulatingCrash) return 'crashed';
    if (this.isRunning) return 'active';
    return 'stopped';
  }

  async start(): Promise<void> {
    if (this.isRunning) return;
    this.isRunning = true;
    this.isSimulatingCrash = false;

    // Start heartbeat
    this.startHeartbeat();

    // Start poll loop
    this.scheduleNextPoll(0);
    this.emit('started', { id: this.id, queue: this.queue });
  }

  private startHeartbeat(): void {
    const sendPulse = async () => {
      if (!this.isRunning || this.isSimulatingCrash) return;
      try {
        const activeIds = Array.from(this.activeTasks.keys());
        await this.broker.heartbeat(this.id, this.queue, activeIds, this.concurrency);
      } catch (err) {
        this.emit('error', err);
      }
    };

    sendPulse();
    this.heartbeatTimer = setInterval(sendPulse, this.heartbeatIntervalMs);
  }

  private scheduleNextPoll(delayMs: number = this.pollIntervalMs): void {
    if (!this.isRunning || this.isSimulatingCrash) return;

    if (this.pollTimer) clearTimeout(this.pollTimer);
    this.pollTimer = setTimeout(async () => {
      await this.poll();
    }, delayMs);
  }

  private async poll(): Promise<void> {
    if (!this.isRunning || this.isPaused || this.isSimulatingCrash) return;

    // Check available concurrency slots
    const availableSlots = this.concurrency - this.activeTasks.size;
    if (availableSlots <= 0) {
      this.scheduleNextPoll(this.pollIntervalMs);
      return;
    }

    let dispatched = 0;
    while (
      this.isRunning &&
      !this.isPaused &&
      !this.isSimulatingCrash &&
      this.concurrency - this.activeTasks.size > 0
    ) {
      if (this.rateLimiter) {
        const limitResult = this.rateLimiter.tryConsume(1);
        if (!limitResult.allowed) {
          this.scheduleNextPoll(Math.max(50, limitResult.retryAfterMs));
          return;
        }
      }

      try {
        const task = await this.broker.dequeue(this.queue, this.id, 0);
        if (!task) {
          break; // Queue is currently empty
        }
        dispatched++;
        this.processTask(task);
      } catch (err) {
        this.emit('error', err);
        break;
      }
    }

    if (dispatched === 0) {
      this.scheduleNextPoll(this.pollIntervalMs);
    }
  }

  private async processTask(task: Task): Promise<void> {
    const abortController = new AbortController();
    this.activeTasks.set(task.id, { task, abortController });

    const startTime = Date.now();
    let timeoutTimer: NodeJS.Timeout | null = null;

    if (task.timeoutMs && task.timeoutMs > 0) {
      timeoutTimer = setTimeout(() => {
        abortController.abort(new Error(`Task execution timed out after ${task.timeoutMs}ms`));
      }, task.timeoutMs);
      timeoutTimer.unref();
    }

    try {
      this.emit('task:start', task);
      const result = await this.handler(task, abortController.signal);

      if (timeoutTimer) clearTimeout(timeoutTimer);

      if (this.isSimulatingCrash) {
        // If worker crashed mid-execution, do not ACK so supervisor can reclaim!
        return;
      }

      const latencyMs = Date.now() - startTime;
      await this.broker.ack(task, result);
      globalMetrics.recordComplete(latencyMs);

      this.emit('task:completed', { task, result, latencyMs });
    } catch (rawError: any) {
      if (timeoutTimer) clearTimeout(timeoutTimer);

      if (this.isSimulatingCrash) return;

      const error = rawError instanceof Error ? rawError : new Error(String(rawError));
      const willRetry = task.attempts < task.maxRetries;

      if (willRetry) {
        const delayMs = calculateBackoff(task.backoff, task.attempts);
        globalMetrics.recordRetry();
        await this.broker.nack(task, error, true, delayMs);
        this.emit('task:failed', { task, error, willRetry: true, delayMs });
      } else {
        globalMetrics.recordFailure(true);
        await this.broker.nack(task, error, false);
        this.emit('task:dlq', { task, error });
      }
    } finally {
      this.activeTasks.delete(task.id);
      // As a slot freed up, poll for next task immediately
      if (this.isRunning && !this.isSimulatingCrash) {
        this.scheduleNextPoll(0);
      }
    }
  }

  pause(): void {
    this.isPaused = true;
    this.emit('paused');
  }

  resume(): void {
    this.isPaused = false;
    this.scheduleNextPoll(0);
    this.emit('resumed');
  }

  /**
   * Simulates a hard crash (e.g. SIGKILL, out-of-memory, machine shutdown).
   * Freezes heartbeats and leaves active tasks orphaned for the Zombie Supervisor.
   */
  simulateCrash(): void {
    this.isSimulatingCrash = true;
    if (this.heartbeatTimer) {
      clearInterval(this.heartbeatTimer);
      this.heartbeatTimer = null;
    }
    if (this.pollTimer) {
      clearTimeout(this.pollTimer);
      this.pollTimer = null;
    }
    this.emit('crashed', { id: this.id });
  }

  async stop(drainTimeoutMs: number = 5000): Promise<void> {
    this.isRunning = false;

    if (this.pollTimer) {
      clearTimeout(this.pollTimer);
      this.pollTimer = null;
    }

    if (this.heartbeatTimer) {
      clearInterval(this.heartbeatTimer);
      this.heartbeatTimer = null;
    }

    // Wait for in-flight active tasks to drain (unless worker was crashed)
    if (this.activeTasks.size > 0) {
      if (!this.isSimulatingCrash) {
        const startDrain = Date.now();
        while (this.activeTasks.size > 0 && Date.now() - startDrain < drainTimeoutMs) {
          await new Promise((resolve) => setTimeout(resolve, 50));
        }
      }

      // Force-abort any remaining tasks
      for (const { abortController } of this.activeTasks.values()) {
        abortController.abort(new Error('Worker shutdown forced'));
      }
      this.activeTasks.clear();
    }

    await this.broker.unregisterWorker(this.id);
    this.emit('stopped', { id: this.id });
  }
}
