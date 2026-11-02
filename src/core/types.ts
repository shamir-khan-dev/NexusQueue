import { randomUUID } from 'node:crypto';

export type TaskStatus =
  | 'pending'
  | 'delayed'
  | 'processing'
  | 'completed'
  | 'failed'
  | 'dlq'
  | 'cancelled';

export enum TaskPriority {
  CRITICAL = 0,
  HIGH = 1,
  NORMAL = 2,
  LOW = 3,
}

export type BackoffStrategy = 'exponential' | 'linear' | 'fixed';

export interface BackoffConfig {
  type: BackoffStrategy;
  baseMs: number;
  maxMs: number;
  jitter?: boolean;
}

export interface TaskOptions {
  priority?: TaskPriority | number;
  delayMs?: number;
  runAt?: Date | number;
  maxRetries?: number;
  backoff?: BackoffConfig;
  timeoutMs?: number;
  concurrencyKey?: string;
  dependencies?: string[];
  metadata?: Record<string, any>;
}

export interface TaskError {
  message: string;
  stack?: string;
  code?: string;
  timestamp: number;
  workerId?: string;
}

export interface RetryAttempt {
  attempt: number;
  error: string;
  timestamp: number;
  nextRunAt: number;
  workerId?: string;
}

export interface Task<T = any, R = any> {
  id: string;
  queue: string;
  name: string;
  payload: T;
  result?: R;
  error?: TaskError;
  status: TaskStatus;
  attempts: number;
  maxRetries: number;
  priority: number;
  createdAt: number;
  enqueuedAt: number;
  scheduledFor?: number;
  processedAt?: number;
  completedAt?: number;
  failedAt?: number;
  workerId?: string;
  heartbeatAt?: number;
  backoff: BackoffConfig;
  timeoutMs?: number;
  concurrencyKey?: string;
  dependencies: string[];
  retryHistory: RetryAttempt[];
  metadata: Record<string, any>;
}

export interface WorkerInfo {
  id: string;
  queue: string;
  concurrency: number;
  activeCount: number;
  activeTasks: string[];
  registeredAt: number;
  lastHeartbeat: number;
  status: 'active' | 'crashed' | 'stopping' | 'stopped';
}

export interface QueueStats {
  queue: string;
  pending: number;
  delayed: number;
  processing: number;
  completed: number;
  failed: number;
  dlq: number;
  totalThroughput: number;
  activeWorkers: number;
}

export interface SystemMetrics {
  uptimeSeconds: number;
  throughputPerSecond: number;
  totalProcessed: number;
  totalCompleted: number;
  totalFailed: number;
  totalRetried: number;
  totalDLQ: number;
  totalZombiesReclaimed: number;
  latencyPercentiles: {
    p50: number;
    p90: number;
    p95: number;
    p99: number;
  };
  queues: Record<string, QueueStats>;
  workers: WorkerInfo[];
}

export type TaskHandler<T = any, R = any> = (
  task: Task<T, R>,
  signal: AbortSignal
) => Promise<R>;

export function createTask<T = any>(
  queue: string,
  name: string,
  payload: T,
  options: TaskOptions = {}
): Task<T> {
  const now = Date.now();
  const scheduledFor = options.runAt
    ? typeof options.runAt === 'number'
      ? options.runAt
      : options.runAt.getTime()
    : options.delayMs && options.delayMs > 0
    ? now + options.delayMs
    : undefined;

  return {
    id: randomUUID(),
    queue,
    name,
    payload,
    status: scheduledFor && scheduledFor > now ? 'delayed' : 'pending',
    attempts: 0,
    maxRetries: options.maxRetries ?? 3,
    priority: options.priority ?? TaskPriority.NORMAL,
    createdAt: now,
    enqueuedAt: now,
    scheduledFor,
    backoff: options.backoff ?? {
      type: 'exponential',
      baseMs: 1000,
      maxMs: 30000,
      jitter: true,
    },
    timeoutMs: options.timeoutMs ?? 30000,
    concurrencyKey: options.concurrencyKey,
    dependencies: options.dependencies ?? [],
    retryHistory: [],
    metadata: options.metadata ?? {},
  };
}
