import { EventEmitter } from 'node:events';
import { IBroker } from './interface.js';
import {
  Task,
  TaskStatus,
  WorkerInfo,
  QueueStats,
  TaskPriority,
} from '../core/types.js';
import { PriorityHeap } from '../core/priority-heap.js';

interface InternalQueue {
  pendingHeap: PriorityHeap<Task>;
  delayedTasks: Map<string, Task>;
  processingTasks: Map<string, Task>;
  completedTasks: Map<string, Task>;
  failedTasks: Map<string, Task>;
  dlqTasks: Map<string, Task>;
  totalThroughput: number;
}

export class MemoryBroker extends EventEmitter implements IBroker {
  private queues: Map<string, InternalQueue> = new Map();
  private allTasks: Map<string, Task> = new Map();
  private workers: Map<string, WorkerInfo> = new Map();
  private delayedCheckInterval: NodeJS.Timeout | null = null;
  private isClosed: boolean = false;

  constructor() {
    super();
    // Background polling for delayed tasks becoming eligible
    this.delayedCheckInterval = setInterval(() => {
      this.promoteDelayedTasks();
    }, 100);
  }

  private getOrCreateQueue(queueName: string): InternalQueue {
    let q = this.queues.get(queueName);
    if (!q) {
      q = {
        pendingHeap: new PriorityHeap<Task>((a, b) => {
          // Priority first (lower number = higher priority)
          if (a.priority !== b.priority) {
            return a.priority - b.priority;
          }
          // FIFO tie-break on same priority
          return a.enqueuedAt - b.enqueuedAt;
        }),
        delayedTasks: new Map(),
        processingTasks: new Map(),
        completedTasks: new Map(),
        failedTasks: new Map(),
        dlqTasks: new Map(),
        totalThroughput: 0,
      };
      this.queues.set(queueName, q);
    }
    return q;
  }

  async enqueue(task: Task): Promise<void> {
    if (this.isClosed) throw new Error('Broker is closed');

    const q = this.getOrCreateQueue(task.queue);
    this.allTasks.set(task.id, task);

    const now = Date.now();
    if (task.scheduledFor && task.scheduledFor > now) {
      task.status = 'delayed';
      q.delayedTasks.set(task.id, task);
      this.emit('task:delayed', task);
    } else {
      task.status = 'pending';
      q.pendingHeap.push(task);
      this.emit('task:enqueued', task);
    }
  }

  private promoteDelayedTasks(): void {
    const now = Date.now();
    for (const [queueName, q] of this.queues.entries()) {
      for (const [taskId, task] of q.delayedTasks.entries()) {
        if (!task.scheduledFor || task.scheduledFor <= now) {
          q.delayedTasks.delete(taskId);
          task.status = 'pending';
          task.enqueuedAt = now;
          q.pendingHeap.push(task);
          this.emit('task:promoted', task);
          this.emit('task:enqueued', task);
        }
      }
    }
  }

  async dequeue(
    queueName: string,
    workerId: string,
    timeoutMs: number = 0
  ): Promise<Task | null> {
    if (this.isClosed) return null;

    this.promoteDelayedTasks();
    const q = this.getOrCreateQueue(queueName);

    let task = q.pendingHeap.pop();

    if (!task && timeoutMs > 0) {
      // Wait for next task if timeout provided
      const startTime = Date.now();
      while (!task && Date.now() - startTime < timeoutMs && !this.isClosed) {
        await new Promise((resolve) => setTimeout(resolve, 50));
        this.promoteDelayedTasks();
        task = q.pendingHeap.pop();
      }
    }

    if (!task) return null;

    const now = Date.now();
    task.status = 'processing';
    task.workerId = workerId;
    task.processedAt = now;
    task.heartbeatAt = now;

    q.processingTasks.set(task.id, task);

    // Update worker state
    const worker = this.workers.get(workerId);
    if (worker && !worker.activeTasks.includes(task.id)) {
      worker.activeTasks.push(task.id);
      worker.activeCount = worker.activeTasks.length;
    }

    this.emit('task:processing', task);
    return task;
  }

  async ack(task: Task, result?: any): Promise<void> {
    const q = this.getOrCreateQueue(task.queue);
    const now = Date.now();

    q.processingTasks.delete(task.id);

    task.status = 'completed';
    task.result = result;
    task.completedAt = now;

    // Prune completed memory cache if exceeding 2,000 tasks
    if (q.completedTasks.size > 2000) {
      const oldestId = q.completedTasks.keys().next().value;
      if (oldestId) {
        q.completedTasks.delete(oldestId);
        this.allTasks.delete(oldestId);
      }
    }
    q.completedTasks.set(task.id, task);
    q.totalThroughput++;

    // Unbind from worker
    if (task.workerId) {
      const worker = this.workers.get(task.workerId);
      if (worker) {
        worker.activeTasks = worker.activeTasks.filter((id) => id !== task.id);
        worker.activeCount = worker.activeTasks.length;
      }
    }

    this.emit('task:completed', task);
  }

  async nack(
    task: Task,
    error: Error,
    requeue: boolean = true,
    delayMs: number = 0
  ): Promise<void> {
    const q = this.getOrCreateQueue(task.queue);
    const now = Date.now();

    q.processingTasks.delete(task.id);

    task.attempts++;
    const errorDetails = {
      message: error.message,
      stack: error.stack,
      timestamp: now,
      workerId: task.workerId,
    };
    task.error = errorDetails;

    // Unbind worker
    if (task.workerId) {
      const worker = this.workers.get(task.workerId);
      if (worker) {
        worker.activeTasks = worker.activeTasks.filter((id) => id !== task.id);
        worker.activeCount = worker.activeTasks.length;
      }
    }
    task.workerId = undefined;

    if (requeue && task.attempts <= task.maxRetries) {
      const nextRunAt = now + Math.max(0, delayMs);
      task.retryHistory.push({
        attempt: task.attempts,
        error: error.message,
        timestamp: now,
        nextRunAt,
      });

      if (delayMs > 0) {
        task.status = 'delayed';
        task.scheduledFor = nextRunAt;
        q.delayedTasks.set(task.id, task);
        this.emit('task:retry_delayed', task);
      } else {
        task.status = 'pending';
        task.enqueuedAt = now;
        q.pendingHeap.push(task);
        this.emit('task:retrying', task);
      }
    } else {
      await this.moveToDLQ(task, error);
    }
  }

  async moveToDLQ(task: Task, error: Error): Promise<void> {
    const q = this.getOrCreateQueue(task.queue);
    const now = Date.now();

    q.processingTasks.delete(task.id);
    q.delayedTasks.delete(task.id);

    task.status = 'dlq';
    task.failedAt = now;
    task.error = {
      message: error.message,
      stack: error.stack,
      timestamp: now,
      workerId: task.workerId,
    };

    q.dlqTasks.set(task.id, task);
    this.emit('task:dlq', task);
  }

  async getDLQ(queueName: string, limit: number = 50): Promise<Task[]> {
    const q = this.getOrCreateQueue(queueName);
    const list = Array.from(q.dlqTasks.values());
    return list.slice(0, limit);
  }

  async replayDLQ(queueName: string, taskId: string): Promise<Task | null> {
    const q = this.getOrCreateQueue(queueName);
    const task = q.dlqTasks.get(taskId);
    if (!task) return null;

    q.dlqTasks.delete(taskId);
    task.status = 'pending';
    task.attempts = 0; // reset retry budget
    task.enqueuedAt = Date.now();
    task.error = undefined;

    q.pendingHeap.push(task);
    this.emit('task:replayed', task);
    this.emit('task:enqueued', task);
    return task;
  }

  async purgeDLQ(queueName: string): Promise<number> {
    const q = this.getOrCreateQueue(queueName);
    const count = q.dlqTasks.size;
    for (const taskId of q.dlqTasks.keys()) {
      this.allTasks.delete(taskId);
    }
    q.dlqTasks.clear();
    return count;
  }

  async heartbeat(
    workerId: string,
    queueName: string,
    activeTaskIds: string[],
    concurrency: number = 5
  ): Promise<void> {
    const now = Date.now();
    let worker = this.workers.get(workerId);
    if (!worker) {
      worker = {
        id: workerId,
        queue: queueName,
        concurrency,
        activeCount: activeTaskIds.length,
        activeTasks: [...activeTaskIds],
        registeredAt: now,
        lastHeartbeat: now,
        status: 'active',
      };
      this.workers.set(workerId, worker);
    } else {
      worker.lastHeartbeat = now;
      worker.status = 'active';
      worker.activeTasks = [...activeTaskIds];
      worker.activeCount = activeTaskIds.length;
    }

    // Refresh heartbeat on processing tasks
    const q = this.getOrCreateQueue(queueName);
    for (const taskId of activeTaskIds) {
      const task = q.processingTasks.get(taskId);
      if (task) {
        task.heartbeatAt = now;
      }
    }
  }

  async unregisterWorker(workerId: string): Promise<void> {
    const worker = this.workers.get(workerId);
    if (worker) {
      worker.status = 'stopped';
      this.workers.delete(workerId);
    }
  }

  async getWorkers(): Promise<WorkerInfo[]> {
    return Array.from(this.workers.values());
  }

  async reclaimZombies(heartbeatTimeoutMs: number): Promise<Task[]> {
    const now = Date.now();
    const reclaimedTasks: Task[] = [];

    for (const [workerId, worker] of this.workers.entries()) {
      if (worker.status === 'active' && now - worker.lastHeartbeat > heartbeatTimeoutMs) {
        worker.status = 'crashed';
        this.emit('worker:crashed', worker);

        const q = this.getOrCreateQueue(worker.queue);

        // Find orphaned tasks
        for (const taskId of worker.activeTasks) {
          const task = q.processingTasks.get(taskId);
          if (task) {
            q.processingTasks.delete(taskId);
            task.attempts++;

            const error = new Error(`Worker ${workerId} crashed or missed heartbeat`);
            task.error = {
              message: error.message,
              timestamp: now,
              workerId,
            };

            if (task.attempts <= task.maxRetries) {
              task.status = 'pending';
              task.enqueuedAt = now;
              task.workerId = undefined;
              task.priority = Math.max(0, task.priority - 1); // Boost priority on reclaim
              q.pendingHeap.push(task);
              this.emit('task:reclaimed', task);
            } else {
              await this.moveToDLQ(task, error);
            }
            reclaimedTasks.push(task);
          }
        }

        worker.activeTasks = [];
        worker.activeCount = 0;
      }
    }

    return reclaimedTasks;
  }

  async getQueueStats(queueName: string): Promise<QueueStats> {
    const q = this.getOrCreateQueue(queueName);
    const activeWorkers = Array.from(this.workers.values()).filter(
      (w) => w.queue === queueName && w.status === 'active'
    ).length;

    return {
      queue: queueName,
      pending: q.pendingHeap.size,
      delayed: q.delayedTasks.size,
      processing: q.processingTasks.size,
      completed: q.completedTasks.size,
      failed: q.failedTasks.size,
      dlq: q.dlqTasks.size,
      totalThroughput: q.totalThroughput,
      activeWorkers,
    };
  }

  async getAllStats(): Promise<Record<string, QueueStats>> {
    const stats: Record<string, QueueStats> = {};
    for (const queueName of this.queues.keys()) {
      stats[queueName] = await this.getQueueStats(queueName);
    }
    return stats;
  }

  async getTask(taskId: string): Promise<Task | null> {
    return this.allTasks.get(taskId) ?? null;
  }

  async listTasks(filter?: {
    queue?: string;
    status?: TaskStatus;
    limit?: number;
  }): Promise<Task[]> {
    let tasks = Array.from(this.allTasks.values());

    if (filter?.queue) {
      tasks = tasks.filter((t) => t.queue === filter.queue);
    }
    if (filter?.status) {
      tasks = tasks.filter((t) => t.status === filter.status);
    }

    tasks.sort((a, b) => b.createdAt - a.createdAt);

    if (filter?.limit) {
      tasks = tasks.slice(0, filter.limit);
    }

    return tasks;
  }

  async close(): Promise<void> {
    this.isClosed = true;
    if (this.delayedCheckInterval) {
      clearInterval(this.delayedCheckInterval);
      this.delayedCheckInterval = null;
    }
    this.removeAllListeners();
  }
}
