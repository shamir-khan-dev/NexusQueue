import { EventEmitter } from 'node:events';
import { IBroker } from '../brokers/interface.js';
import { MemoryBroker } from '../brokers/memory-broker.js';
import {
  Task,
  TaskOptions,
  TaskHandler,
  QueueStats,
  createTask,
} from './types.js';
import { Worker, WorkerOptions } from './worker.js';
import { globalMetrics } from '../telemetry/metrics.js';

export interface NexusQueueOptions {
  broker?: IBroker;
}

export class NexusQueue extends EventEmitter {
  public readonly name: string;
  public readonly broker: IBroker;
  private ownsBroker: boolean;
  private workers: Worker[] = [];

  constructor(name: string, options: NexusQueueOptions = {}) {
    super();
    this.name = name;
    this.ownsBroker = !options.broker;
    this.broker = options.broker ?? new MemoryBroker();

    // Bubble broker events
    if ('on' in this.broker) {
      (this.broker as any).on('task:enqueued', (task: Task) => {
        if (task.queue === this.name) this.emit('task:enqueued', task);
      });
      (this.broker as any).on('task:completed', (task: Task) => {
        if (task.queue === this.name) this.emit('task:completed', task);
      });
      (this.broker as any).on('task:dlq', (task: Task) => {
        if (task.queue === this.name) this.emit('task:dlq', task);
      });
    }
  }

  /**
   * Adds a new task to the queue with priority, retry, or delay options.
   */
  async add<T = any>(
    name: string,
    payload: T,
    options: TaskOptions = {}
  ): Promise<Task<T>> {
    const task = createTask<T>(this.name, name, payload, options);
    await this.broker.enqueue(task);
    globalMetrics.recordEnqueue();
    return task;
  }

  /**
   * Bulk enqueues multiple tasks in an atomic or high-throughput batch.
   */
  async addBulk<T = any>(
    tasks: Array<{ name: string; payload: T; options?: TaskOptions }>
  ): Promise<Task<T>[]> {
    const createdTasks = tasks.map((t) =>
      createTask<T>(this.name, t.name, t.payload, t.options)
    );

    for (const task of createdTasks) {
      await this.broker.enqueue(task);
      globalMetrics.recordEnqueue();
    }

    return createdTasks;
  }

  /**
   * Attaches a worker handler to process jobs from this queue.
   */
  process<T = any, R = any>(
    handler: TaskHandler<T, R>,
    options: Omit<WorkerOptions, 'queue'> = {}
  ): Worker {
    const worker = new Worker(this.broker, handler, {
      ...options,
      queue: this.name,
    });
    this.workers.push(worker);
    worker.start();
    return worker;
  }

  /**
   * Retrieves live metrics and statistics for this queue.
   */
  async getStats(): Promise<QueueStats> {
    return this.broker.getQueueStats(this.name);
  }

  /**
   * Fetches poisoned or failed tasks currently quarantined in the DLQ.
   */
  async getDLQ(limit: number = 50): Promise<Task[]> {
    return this.broker.getDLQ(this.name, limit);
  }

  /**
   * Replays a dead-letter task back into the pending execution stream.
   */
  async replayDLQ(taskId: string): Promise<Task | null> {
    return this.broker.replayDLQ(this.name, taskId);
  }

  /**
   * Purges all dead-letter tasks from this queue.
   */
  async purgeDLQ(): Promise<number> {
    return this.broker.purgeDLQ(this.name);
  }

  /**
   * Retrieves a task by its UUID.
   */
  async getTask(taskId: string): Promise<Task | null> {
    return this.broker.getTask(taskId);
  }

  /**
   * Gracefully shuts down all workers associated with this queue and releases broker resources.
   */
  async close(closeBroker?: boolean): Promise<void> {
    await Promise.all(this.workers.map((w) => w.stop()));
    this.workers = [];
    if (closeBroker || (closeBroker === undefined && this.ownsBroker)) {
      await this.broker.close();
    }
    this.removeAllListeners();
  }
}
