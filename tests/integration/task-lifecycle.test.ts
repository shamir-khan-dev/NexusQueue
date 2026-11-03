import { describe, it } from 'node:test';
import assert from 'node:assert';
import { NexusQueue } from '../../src/core/queue.js';
import { MemoryBroker } from '../../src/brokers/memory-broker.js';
import { TaskPriority } from '../../src/core/types.js';

describe('Integration: Task Lifecycle & Retries', () => {
  it('should enqueue, process, and complete a task with return value', async () => {
    const broker = new MemoryBroker();
    const queue = new NexusQueue('lifecycle-queue', { broker });

    let processedTaskPayload: any = null;

    const worker = queue.process(async (task) => {
      processedTaskPayload = task.payload;
      return { status: 'ok', processedValue: task.payload.val * 2 };
    });

    const task = await queue.add('math-double', { val: 21 }, { priority: TaskPriority.HIGH });

    // Wait for task completion
    await new Promise<void>((resolve) => {
      queue.on('task:completed', (completedTask) => {
        if (completedTask.id === task.id) resolve();
      });
    });

    const fetched = await queue.getTask(task.id);
    assert.strictEqual(fetched?.status, 'completed');
    assert.strictEqual(fetched?.result?.processedValue, 42);
    assert.deepStrictEqual(processedTaskPayload, { val: 21 });

    await worker.stop();
    await queue.close();
    await broker.close();
  });

  it('should not process a delayed task before scheduled time', async () => {
    const broker = new MemoryBroker();
    const queue = new NexusQueue('delay-queue', { broker });

    let processed = false;
    const worker = queue.process(async () => {
      processed = true;
    });

    // Delay by 300ms
    const task = await queue.add('delayed-job', {}, { delayMs: 300 });

    // Check after 100ms - should still be delayed
    await new Promise((r) => setTimeout(r, 100));
    assert.strictEqual(processed, false);
    const midTask = await queue.getTask(task.id);
    assert.strictEqual(midTask?.status, 'delayed');

    // Wait past delay (total ~400ms)
    await new Promise((r) => setTimeout(r, 300));
    assert.strictEqual(processed, true);

    await worker.stop();
    await queue.close();
    await broker.close();
  });

  it('should retry a failing task with backoff until success', async () => {
    const broker = new MemoryBroker();
    const queue = new NexusQueue('retry-queue', { broker });

    let attempts = 0;
    const worker = queue.process(async () => {
      attempts++;
      if (attempts < 3) {
        throw new Error(`Transient network glitch (attempt ${attempts})`);
      }
      return { success: true };
    });

    const task = await queue.add(
      'flaky-service',
      {},
      {
        maxRetries: 3,
        backoff: { type: 'fixed', baseMs: 50, maxMs: 100, jitter: false },
      }
    );

    await new Promise<void>((resolve) => {
      queue.on('task:completed', (completedTask) => {
        if (completedTask.id === task.id) resolve();
      });
    });

    assert.strictEqual(attempts, 3);
    const finalTask = await queue.getTask(task.id);
    assert.strictEqual(finalTask?.status, 'completed');
    assert.strictEqual(finalTask?.retryHistory.length, 2);

    await worker.stop();
    await queue.close();
    await broker.close();
  });
});

