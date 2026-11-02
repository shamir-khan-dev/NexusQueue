import { describe, it } from 'node:test';
import assert from 'node:assert';
import { MemoryBroker } from '../../src/brokers/memory-broker.js';
import { Worker } from '../../src/core/worker.js';
import { ZombieSupervisor } from '../../src/core/supervisor.js';
import { createTask, TaskPriority } from '../../src/core/types.js';

describe('Integration: Zombie Task Recovery & Crash Resilience', () => {
  it('should automatically reclaim orphaned tasks when a worker crashes', async () => {
    const broker = new MemoryBroker();
    const queueName = 'zombie-failover-queue';

    // Supervisor with very short thresholds for test speed (300ms heartbeat timeout)
    const supervisor = new ZombieSupervisor(broker, {
      checkIntervalMs: 100,
      heartbeatTimeoutMs: 300,
    });
    supervisor.start();

    let taskPickedUpByWorker1 = false;
    let taskCompletedByWorker2 = false;

    // Worker 1: picks up task and immediately crashes
    const worker1 = new Worker(
      broker,
      async (task, signal) => {
        taskPickedUpByWorker1 = true;
        // Simulate immediate worker crash
        worker1.simulateCrash();
        // Wait until aborted or timeout
        await new Promise((resolve) => {
          signal.addEventListener('abort', () => resolve(true));
          setTimeout(resolve, 5000);
        });
        return { ok: false };
      },
      { id: 'fragile-worker-1', queue: queueName, heartbeatIntervalMs: 50 }
    );
    await worker1.start();

    // Enqueue the task
    const task = createTask(queueName, 'critical-financial-transfer', { amount: 50000 }, {
      priority: TaskPriority.HIGH,
      maxRetries: 3,
    });
    await broker.enqueue(task);

    // Wait until Worker 1 picks up the task
    while (!taskPickedUpByWorker1) {
      await new Promise((r) => setTimeout(r, 20));
    }

    // Now wait for the supervisor to detect the missed heartbeat and reclaim it
    const reclaimed = await new Promise<boolean>((resolve) => {
      supervisor.on('zombies:reclaimed', (tasks) => {
        if (tasks.some((t: any) => t.id === task.id)) {
          resolve(true);
        }
      });
    });
    assert.strictEqual(reclaimed, true);

    // Spawn Worker 2 (surviving worker) to process reclaimed task
    const worker2 = new Worker(
      broker,
      async (t) => {
        taskCompletedByWorker2 = true;
        return { recovered: true };
      },
      { id: 'resilient-worker-2', queue: queueName }
    );
    await worker2.start();

    // Wait for task completion by Worker 2
    await new Promise<void>((resolve) => {
      broker.on('task:completed', (t) => {
        if (t.id === task.id) resolve();
      });
    });

    assert.strictEqual(taskCompletedByWorker2, true);

    const finalTask = await broker.getTask(task.id);
    assert.strictEqual(finalTask?.status, 'completed');
    assert.strictEqual(finalTask?.attempts, 1); // 1 failed crash attempt + completed

    supervisor.stop();
    await worker1.stop();
    await worker2.stop();
    await broker.close();
  });
});
