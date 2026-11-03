import { Task, TaskStatus, WorkerInfo, QueueStats } from '../core/types.js';

export interface IBroker {
  /**
   * Enqueues a new or delayed task.
   */
  enqueue(task: Task): Promise<void>;

  /**
   * Dequeues the highest priority available task for a given worker.
   */
  dequeue(queue: string, workerId: string, timeoutMs?: number): Promise<Task | null>;

  /**
   * Acknowledges successful task completion and persists result.
   */
  ack(task: Task, result?: any): Promise<void>;

  /**
   * Rejects a task due to an error, scheduling a retry or failing it.
   */
  nack(task: Task, error: Error, requeue?: boolean, delayMs?: number): Promise<void>;

  /**
   * Moves a poisoned or retry-exhausted task to the Dead-Letter Queue.
   */
  moveToDLQ(task: Task, error: Error): Promise<void>;

  /**
   * Retrieves tasks stored in the Dead-Letter Queue.
   */
  getDLQ(queue: string, limit?: number): Promise<Task[]>;

  /**
   * Re-queues a task from the DLQ back to the primary pending queue.
   */
  replayDLQ(queue: string, taskId: string): Promise<Task | null>;

  /**
   * Purges all tasks from the DLQ for a given queue.
   */
  purgeDLQ(queue: string): Promise<number>;

  /**
   * Emits worker heartbeat with active task IDs.
   */
  heartbeat(
    workerId: string,
    queue: string,
    activeTaskIds: string[],
    concurrency?: number
  ): Promise<void>;

  /**
   * Unregisters a worker on graceful shutdown.
   */
  unregisterWorker(workerId: string): Promise<void>;

  /**
   * Returns current active and crashed workers.
   */
  getWorkers(): Promise<WorkerInfo[]>;

  /**
   * Detects crashed workers and reclaims their active tasks.
   */
  reclaimZombies(heartbeatTimeoutMs: number): Promise<Task[]>;

  /**
   * Retrieves statistics for a specific queue.
   */
  getQueueStats(queue: string): Promise<QueueStats>;

  /**
   * Retrieves aggregated statistics for all active queues.
   */
  getAllStats(): Promise<Record<string, QueueStats>>;

  /**
   * Fetches a specific task by its ID.
   */
  getTask(taskId: string): Promise<Task | null>;

  /**
   * Lists tasks filtered by queue, status, and count.
   */
  listTasks(filter?: {
    queue?: string;
    status?: TaskStatus;
    limit?: number;
  }): Promise<Task[]>;

  /**
   * Gracefully closes the broker connections and resources.
   */
  close(): Promise<void>;
}

