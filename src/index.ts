import { NexusQueue } from './core/queue.js';
import { Worker } from './core/worker.js';
import { MemoryBroker } from './brokers/memory-broker.js';
import { RedisBroker } from './brokers/redis-broker.js';
import { IBroker } from './brokers/interface.js';
import { PriorityHeap } from './core/priority-heap.js';
import { calculateBackoff, calculateDecorrelatedJitter } from './core/backoff.js';
import {
  TokenBucketRateLimiter,
  SlidingWindowRateLimiter,
  RateLimiterResult,
} from './core/rate-limiter.js';
import { MetricsCollector, globalMetrics } from './telemetry/metrics.js';
import { ZombieSupervisor } from './core/supervisor.js';
import { WorkflowEngine } from './scheduler/workflow.js';
import { CronScheduler } from './scheduler/cron.js';
import { createDashboardServer } from './dashboard/server.js';
import {
  Task,
  TaskOptions,
  TaskPriority,
  TaskStatus,
  createTask,
} from './core/types.js';

// Export public API
export {
  NexusQueue,
  Worker,
  MemoryBroker,
  RedisBroker,
  IBroker,
  PriorityHeap,
  calculateBackoff,
  calculateDecorrelatedJitter,
  TokenBucketRateLimiter,
  SlidingWindowRateLimiter,
  RateLimiterResult,
  MetricsCollector,
  globalMetrics,
  ZombieSupervisor,
  WorkflowEngine,
  CronScheduler,
  createDashboardServer,
  Task,
  TaskOptions,
  TaskPriority,
  TaskStatus,
  createTask,
};

// If run directly via `npm start` / `tsx src/index.ts`, launch standalone cluster & dashboard
if (process.argv[1]?.endsWith('src/index.ts') || process.argv[1]?.endsWith('dist/index.js')) {
  console.log('⚡ Starting NexusQueue Enterprise Engine & Telemetry Dashboard...');

  const broker = new MemoryBroker();
  const queue = new NexusQueue('default', { broker });
  const supervisor = new ZombieSupervisor(broker, {
    checkIntervalMs: 1000,
    heartbeatTimeoutMs: 3500, // 3.5s crash threshold
  });
  supervisor.start();

  const workflowEngine = new WorkflowEngine(broker);
  const cronScheduler = new CronScheduler(broker);
  cronScheduler.start();

  // Track worker crash simulation callbacks
  const activeWorkers: Array<{ id: string; simulateCrash: () => void; restart: () => void }> = [];

  // Spawn Worker 1 (Fast Transaction Worker)
  const worker1 = queue.process(
    async (task, signal) => {
      // Simulate realistic execution time
      const delay = task.payload?.load === 'light' ? 15 : 40;
      await new Promise((r) => setTimeout(r, delay));

      // Handle poison pill demo
      if (task.payload?.fatal) {
        throw new Error('Fatal poison pill: unrecoverable schema validation error');
      }

      // Handle flaky job demo
      if (task.name === 'flaky-external-api' && task.attempts < 2) {
        throw new Error(`503 Service Unavailable: upstream payment gateway timeout (attempt ${task.attempts + 1})`);
      }

      return { processed: true, processedAt: new Date().toISOString() };
    },
    { id: 'worker-node-alpha', concurrency: 5 }
  );
  activeWorkers.push({
    id: worker1.id,
    simulateCrash: () => worker1.simulateCrash(),
    restart: () => worker1.restart(),
  });

  // Spawn Worker 2 (Heavy Compute & ML Worker)
  const worker2 = queue.process(
    async (task) => {
      const workTime = task.payload?.model ? 60 : 30;
      await new Promise((r) => setTimeout(r, workTime));
      return { outputRecords: 1500, score: 0.985 };
    },
    { id: 'worker-node-beta', concurrency: 3 }
  );
  activeWorkers.push({
    id: worker2.id,
    simulateCrash: () => worker2.simulateCrash(),
    restart: () => worker2.restart(),
  });

  // Start HTTP & WebSocket Telemetry Dashboard
  const PORT = Number(process.env.PORT || 3000);
  const { server } = createDashboardServer({
    port: PORT,
    broker,
    workflowEngine,
    workers: activeWorkers,
  });

  server.listen(PORT, async () => {
    console.log(`\n======================================================`);
    console.log(`🚀 NexusQueue Dashboard is LIVE at: http://localhost:${PORT}`);
    console.log(`📊 Prometheus Metrics available at: http://localhost:${PORT}/metrics`);
    console.log(`======================================================\n`);

    // Enqueue initial sample jobs to immediately demonstrate functionality
    console.log('⚡ Enqueuing bootstrap demo tasks...');
    await queue.add('auth-token-refresh', { userId: 'usr_8921a' }, { priority: TaskPriority.NORMAL });
    await queue.add('stripe-settlement', { amount: 45000, currency: 'USD' }, { priority: TaskPriority.HIGH });
    await queue.add('database-vacuum', { tables: ['transactions', 'audit_logs'] }, { priority: TaskPriority.LOW });

    console.log('✔ Cluster operational. Ready for tasks.\n');
  });
}

