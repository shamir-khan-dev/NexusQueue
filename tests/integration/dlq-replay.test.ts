import { describe, it } from 'node:test';
import assert from 'node:assert';
import { NexusQueue } from '../../src/core/queue.js';
import { MemoryBroker } from '../../src/brokers/memory-broker.js';

describe('Integration: Dead Letter Queue (DLQ) & Manual Replay', () => {
  it('should quarantine exhausted tasks to DLQ and allow manual replay', async () => {
    const broker = new MemoryBroker();
    const queue = new NexusQueue('dlq-test-queue', { broker });

    let shouldSucceed = false;

    const worker = queue.process(async (task) => {
      if (!shouldSucceed) {
        throw new Error('Unrecoverable database corruption');
      }
      return { healed: true };
    });

    const task = await queue.add(
      'corruptible-job',
      { data: 'payload' },
      {
        maxRetries: 2,
        backoff: { type: 'fixed', baseMs: 10, maxMs: 10, jitter: false },
      }
    );

    // Wait until task lands in DLQ
    await new Promise<void>((resolve) => {
      queue.on('task:dlq', (dlqTask) => {
        if (dlqTask.id === task.id) resolve();
      });
    });

    const dlqTasks = await queue.getDLQ();
    assert.strictEqual(dlqTasks.length, 1);
    assert.strictEqual(dlqTasks[0].id, task.id);
    assert.strictEqual(dlqTasks[0].status, 'dlq');
    assert.strictEqual(dlqTasks[0].error?.message, 'Unrecoverable database corruption');

    // Fix underlying issue
    shouldSucceed = true;

    // Trigger manual replay
    const replayed = await queue.replayDLQ(task.id);
    assert.ok(replayed);
    assert.strictEqual(replayed.status, 'pending');

    // Wait for successful completion
    await new Promise<void>((resolve) => {
      queue.on('task:completed', (completedTask) => {
        if (completedTask.id === task.id) resolve();
      });
    });

    const finalTask = await queue.getTask(task.id);
    assert.strictEqual(finalTask?.status, 'completed');
    assert.strictEqual(finalTask?.result?.healed, true);

    // DLQ should now be empty
    const remainingDLQ = await queue.getDLQ();
    assert.strictEqual(remainingDLQ.length, 0);

    await worker.stop();
    await queue.close();
    await broker.close();
  });
});
