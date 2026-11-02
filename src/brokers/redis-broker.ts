import { EventEmitter } from 'node:events';
import { Redis, RedisOptions } from 'ioredis';
import { IBroker } from './interface.js';
import {
  Task,
  TaskStatus,
  WorkerInfo,
  QueueStats,
} from '../core/types.js';
import { REDIS_TOKEN_BUCKET_LUA } from '../core/rate-limiter.js';

export interface RedisBrokerOptions {
  connection?: RedisOptions | string;
  consumerGroup?: string;
  keyPrefix?: string;
}

export class RedisBroker extends EventEmitter implements IBroker {
  private client: Redis;
  private subClient: Redis;
  private consumerGroup: string;
  private prefix: string;
  private isClosed: boolean = false;
  private delayedPollInterval: NodeJS.Timeout | null = null;

  constructor(options: RedisBrokerOptions = {}) {
    super();
    this.consumerGroup = options.consumerGroup ?? 'nexus_workers';
    this.prefix = options.keyPrefix ?? 'nexus:';

    const conn = options.connection ?? {
      host: process.env.REDIS_HOST || '127.0.0.1',
      port: Number(process.env.REDIS_PORT || 6379),
      lazyConnect: true,
      maxRetriesPerRequest: 3,
    };

    this.client = typeof conn === 'string' ? new Redis(conn) : new Redis(conn);
    this.subClient = typeof conn === 'string' ? new Redis(conn) : new Redis(conn);

    // Register custom Lua commands
    this.client.defineCommand('consumeTokenBucket', {
      numberOfKeys: 1,
      lua: REDIS_TOKEN_BUCKET_LUA,
    });

    this.startDelayedPoller();
  }

  private k(name: string): string {
    return `${this.prefix}${name}`;
  }

  private startDelayedPoller(): void {
    this.delayedPollInterval = setInterval(async () => {
      try {
        await this.promoteDelayedTasks();
      } catch (err) {
        // Silently handle poll errors if Redis reconnecting
      }
    }, 200);
    this.delayedPollInterval.unref();
  }

  private async promoteDelayedTasks(): Promise<void> {
    if (this.isClosed) return;
    const now = Date.now();

    // Scan for all delayed keys
    const delayedKeys = await this.client.keys(this.k('delayed:*'));
    for (const key of delayedKeys) {
      const queueName = key.replace(this.k('delayed:'), '');
      // Range by score up to now
      const taskIds = await this.client.zrangebyscore(key, 0, now, 'LIMIT', 0, 50);

      for (const taskId of taskIds) {
        const taskJson = await this.client.hget(this.k(`task:${taskId}`), 'data');
        if (taskJson) {
          const task: Task = JSON.parse(taskJson);
          task.status = 'pending';
          task.enqueuedAt = now;
          await this.client.hset(this.k(`task:${taskId}`), 'data', JSON.stringify(task));
          await this.client.xadd(this.k(`stream:${queueName}`), '*', 'taskId', taskId);
          await this.client.zrem(key, taskId);
          this.emit('task:enqueued', task);
        } else {
          await this.client.zrem(key, taskId);
        }
      }
    }
  }

  private async ensureConsumerGroup(queueName: string): Promise<void> {
    const streamKey = this.k(`stream:${queueName}`);
    try {
      await this.client.xgroup('CREATE', streamKey, this.consumerGroup, '0', 'MKSTREAM');
    } catch (err: any) {
      if (!err.message.includes('BUSYGROUP')) {
        throw err;
      }
    }
  }

  async enqueue(task: Task): Promise<void> {
    if (this.isClosed) throw new Error('Broker is closed');

    const taskKey = this.k(`task:${task.id}`);
    const queueKey = this.k(`stream:${task.queue}`);
    const delayedKey = this.k(`delayed:${task.queue}`);
    const now = Date.now();

    await this.client.hset(taskKey, {
      data: JSON.stringify(task),
      status: task.status,
      queue: task.queue,
      priority: task.priority,
    });

    if (task.scheduledFor && task.scheduledFor > now) {
      task.status = 'delayed';
      await this.client.zadd(delayedKey, task.scheduledFor, task.id);
      this.emit('task:delayed', task);
    } else {
      task.status = 'pending';
      await this.client.xadd(queueKey, '*', 'taskId', task.id);
      this.emit('task:enqueued', task);
    }
  }

  async dequeue(
    queueName: string,
    workerId: string,
    timeoutMs: number = 2000
  ): Promise<Task | null> {
    if (this.isClosed) return null;
    await this.ensureConsumerGroup(queueName);

    const streamKey = this.k(`stream:${queueName}`);
    const blockMs = Math.max(100, timeoutMs);

    // Read new pending messages
    const response: any = await (this.client as any).xreadgroup(
      'GROUP',
      this.consumerGroup,
      workerId,
      'COUNT',
      1,
      'BLOCK',
      blockMs,
      'STREAMS',
      streamKey,
      '>'
    );

    if (!response || !response.length || !response[0][1].length) {
      return null;
    }

    const message = response[0][1][0];
    const messageId = message[0];
    const fields = message[1];
    let taskId: string | undefined;

    for (let i = 0; i < fields.length; i += 2) {
      if (fields[i] === 'taskId') {
        taskId = fields[i + 1];
        break;
      }
    }

    if (!taskId) {
      await this.client.xack(streamKey, this.consumerGroup, messageId);
      return null;
    }

    const taskJson = await this.client.hget(this.k(`task:${taskId}`), 'data');
    if (!taskJson) {
      await this.client.xack(streamKey, this.consumerGroup, messageId);
      return null;
    }

    const task: Task = JSON.parse(taskJson);
    const now = Date.now();
    task.status = 'processing';
    task.workerId = workerId;
    task.processedAt = now;
    task.heartbeatAt = now;
    task.metadata = { ...task.metadata, streamMessageId: messageId };

    await this.client.hset(this.k(`task:${taskId}`), {
      data: JSON.stringify(task),
      status: 'processing',
      workerId,
    });

    this.emit('task:processing', task);
    return task;
  }

  async ack(task: Task, result?: any): Promise<void> {
    const streamKey = this.k(`stream:${task.queue}`);
    const messageId = task.metadata?.streamMessageId;

    if (messageId) {
      await this.client.xack(streamKey, this.consumerGroup, messageId);
      await this.client.xdel(streamKey, messageId);
    }

    const now = Date.now();
    task.status = 'completed';
    task.result = result;
    task.completedAt = now;

    await this.client.hset(this.k(`task:${task.id}`), {
      data: JSON.stringify(task),
      status: 'completed',
    });
    // Set 24h expiration on completed tasks in Redis
    await this.client.expire(this.k(`task:${task.id}`), 86400);

    this.emit('task:completed', task);
  }

  async nack(
    task: Task,
    error: Error,
    requeue: boolean = true,
    delayMs: number = 0
  ): Promise<void> {
    const streamKey = this.k(`stream:${task.queue}`);
    const messageId = task.metadata?.streamMessageId;

    if (messageId) {
      await this.client.xack(streamKey, this.consumerGroup, messageId);
      await this.client.xdel(streamKey, messageId);
    }

    const now = Date.now();
    task.attempts++;
    task.error = {
      message: error.message,
      stack: error.stack,
      timestamp: now,
      workerId: task.workerId,
    };
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
        await this.client.hset(this.k(`task:${task.id}`), 'data', JSON.stringify(task));
        await this.client.zadd(this.k(`delayed:${task.queue}`), nextRunAt, task.id);
        this.emit('task:retry_delayed', task);
      } else {
        task.status = 'pending';
        task.enqueuedAt = now;
        await this.client.hset(this.k(`task:${task.id}`), 'data', JSON.stringify(task));
        await this.client.xadd(streamKey, '*', 'taskId', task.id);
        this.emit('task:retrying', task);
      }
    } else {
      await this.moveToDLQ(task, error);
    }
  }

  async moveToDLQ(task: Task, error: Error): Promise<void> {
    const dlqKey = this.k(`dlq:${task.queue}`);
    const now = Date.now();

    task.status = 'dlq';
    task.failedAt = now;
    task.error = {
      message: error.message,
      stack: error.stack,
      timestamp: now,
      workerId: task.workerId,
    };

    await this.client.hset(this.k(`task:${task.id}`), {
      data: JSON.stringify(task),
      status: 'dlq',
    });
    await this.client.lpush(dlqKey, task.id);

    this.emit('task:dlq', task);
  }

  async getDLQ(queueName: string, limit: number = 50): Promise<Task[]> {
    const dlqKey = this.k(`dlq:${queueName}`);
    const taskIds = await this.client.lrange(dlqKey, 0, limit - 1);
    const tasks: Task[] = [];

    for (const taskId of taskIds) {
      const data = await this.client.hget(this.k(`task:${taskId}`), 'data');
      if (data) {
        tasks.push(JSON.parse(data));
      }
    }
    return tasks;
  }

  async replayDLQ(queueName: string, taskId: string): Promise<Task | null> {
    const dlqKey = this.k(`dlq:${queueName}`);
    await this.client.lrem(dlqKey, 0, taskId);

    const taskJson = await this.client.hget(this.k(`task:${taskId}`), 'data');
    if (!taskJson) return null;

    const task: Task = JSON.parse(taskJson);
    task.status = 'pending';
    task.attempts = 0;
    task.enqueuedAt = Date.now();
    task.error = undefined;

    await this.client.hset(this.k(`task:${taskId}`), 'data', JSON.stringify(task));
    await this.client.xadd(this.k(`stream:${queueName}`), '*', 'taskId', task.id);

    this.emit('task:replayed', task);
    return task;
  }

  async purgeDLQ(queueName: string): Promise<number> {
    const dlqKey = this.k(`dlq:${queueName}`);
    const count = await this.client.llen(dlqKey);
    await this.client.del(dlqKey);
    return count;
  }

  async heartbeat(
    workerId: string,
    queueName: string,
    activeTaskIds: string[],
    concurrency: number = 5
  ): Promise<void> {
    const workerKey = this.k(`worker:${workerId}`);
    const now = Date.now();

    const info: WorkerInfo = {
      id: workerId,
      queue: queueName,
      concurrency,
      activeCount: activeTaskIds.length,
      activeTasks: activeTaskIds,
      registeredAt: now,
      lastHeartbeat: now,
      status: 'active',
    };

    await this.client.set(workerKey, JSON.stringify(info), 'EX', 30);
    await this.client.sadd(this.k('workers:active'), workerId);
  }

  async unregisterWorker(workerId: string): Promise<void> {
    await this.client.del(this.k(`worker:${workerId}`));
    await this.client.srem(this.k('workers:active'), workerId);
  }

  async getWorkers(): Promise<WorkerInfo[]> {
    const workerIds = await this.client.smembers(this.k('workers:active'));
    const workers: WorkerInfo[] = [];

    for (const workerId of workerIds) {
      const json = await this.client.get(this.k(`worker:${workerId}`));
      if (json) {
        workers.push(JSON.parse(json));
      } else {
        await this.client.srem(this.k('workers:active'), workerId);
      }
    }
    return workers;
  }

  async reclaimZombies(heartbeatTimeoutMs: number): Promise<Task[]> {
    // Reclaims unacknowledged tasks in consumer group whose idle time exceeds timeout
    const reclaimedTasks: Task[] = [];
    const streamKeys = await this.client.keys(this.k('stream:*'));

    for (const streamKey of streamKeys) {
      const queueName = streamKey.replace(this.k('stream:'), '');
      try {
        const pending = (await this.client.xpending(
          streamKey,
          this.consumerGroup,
          '-',
          '+',
          50
        )) as any[];

        if (!pending || !pending.length) continue;

        for (const item of pending) {
          const [messageId, consumer, idleTimeMs, deliveryCount] = item;
          if (idleTimeMs > heartbeatTimeoutMs) {
            // Claim message back into group
            const claimed: any = await this.client.xclaim(
              streamKey,
              this.consumerGroup,
              'reclaimer',
              idleTimeMs,
              messageId
            );

            if (claimed && claimed.length) {
              const fields = claimed[0][1];
              let taskId: string | undefined;
              for (let i = 0; i < fields.length; i += 2) {
                if (fields[i] === 'taskId') taskId = fields[i + 1];
              }

              if (taskId) {
                const taskJson = await this.client.hget(this.k(`task:${taskId}`), 'data');
                if (taskJson) {
                  const task: Task = JSON.parse(taskJson);
                  task.attempts++;
                  if (task.attempts <= task.maxRetries) {
                    task.status = 'pending';
                    task.workerId = undefined;
                    await this.client.hset(this.k(`task:${taskId}`), 'data', JSON.stringify(task));
                    this.emit('task:reclaimed', task);
                  } else {
                    await this.moveToDLQ(task, new Error('Task orphaned by crashed worker'));
                  }
                  reclaimedTasks.push(task);
                }
              }
            }
          }
        }
      } catch (err) {
        // Stream may not have group yet
      }
    }

    return reclaimedTasks;
  }

  async getQueueStats(queueName: string): Promise<QueueStats> {
    const streamKey = this.k(`stream:${queueName}`);
    const delayedKey = this.k(`delayed:${queueName}`);
    const dlqKey = this.k(`dlq:${queueName}`);

    const [pendingCount, delayedCount, dlqCount] = await Promise.all([
      this.client.xlen(streamKey).catch(() => 0),
      this.client.zcard(delayedKey).catch(() => 0),
      this.client.llen(dlqKey).catch(() => 0),
    ]);

    const workers = await this.getWorkers();
    const activeWorkers = workers.filter((w) => w.queue === queueName).length;

    return {
      queue: queueName,
      pending: pendingCount,
      delayed: delayedCount,
      processing: 0,
      completed: 0,
      failed: 0,
      dlq: dlqCount,
      totalThroughput: pendingCount,
      activeWorkers,
    };
  }

  async getAllStats(): Promise<Record<string, QueueStats>> {
    const streamKeys = await this.client.keys(this.k('stream:*'));
    const stats: Record<string, QueueStats> = {};

    for (const key of streamKeys) {
      const q = key.replace(this.k('stream:'), '');
      stats[q] = await this.getQueueStats(q);
    }
    return stats;
  }

  async getTask(taskId: string): Promise<Task | null> {
    const data = await this.client.hget(this.k(`task:${taskId}`), 'data');
    return data ? JSON.parse(data) : null;
  }

  async listTasks(filter?: {
    queue?: string;
    status?: TaskStatus;
    limit?: number;
  }): Promise<Task[]> {
    const keys = await this.client.keys(this.k('task:*'));
    const tasks: Task[] = [];
    const max = filter?.limit ?? 50;

    for (const key of keys) {
      if (tasks.length >= max) break;
      const data = await this.client.hget(key, 'data');
      if (data) {
        const task: Task = JSON.parse(data);
        if (filter?.queue && task.queue !== filter.queue) continue;
        if (filter?.status && task.status !== filter.status) continue;
        tasks.push(task);
      }
    }

    return tasks.sort((a, b) => b.createdAt - a.createdAt);
  }

  async close(): Promise<void> {
    this.isClosed = true;
    if (this.delayedPollInterval) {
      clearInterval(this.delayedPollInterval);
      this.delayedPollInterval = null;
    }
    await Promise.all([this.client.quit(), this.subClient.quit()]);
    this.removeAllListeners();
  }
}
