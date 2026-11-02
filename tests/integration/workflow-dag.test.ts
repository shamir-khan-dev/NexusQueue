import { describe, it } from 'node:test';
import assert from 'node:assert';
import { MemoryBroker } from '../../src/brokers/memory-broker.js';
import { NexusQueue } from '../../src/core/queue.js';
import { WorkflowEngine } from '../../src/scheduler/workflow.js';

describe('Integration: DAG Workflow Orchestration', () => {
  it('should execute tasks in strict topological dependency order', async () => {
    const broker = new MemoryBroker();
    const queue = new NexusQueue('workflow-queue', { broker });
    const engine = new WorkflowEngine(broker);

    const executionLog: string[] = [];

    // Worker handler
    queue.process(async (task) => {
      executionLog.push(task.name);
      await new Promise((r) => setTimeout(r, 20));
      return { from: task.name, done: true };
    });

    const workflow = await engine.submit({
      name: 'ETL-Pipeline',
      nodes: [
        {
          id: 'step-extract',
          name: 'Extract',
          queue: 'workflow-queue',
          payload: {},
        },
        {
          id: 'step-transform-1',
          name: 'Transform-Part-1',
          queue: 'workflow-queue',
          payload: {},
          dependencies: ['step-extract'],
        },
        {
          id: 'step-transform-2',
          name: 'Transform-Part-2',
          queue: 'workflow-queue',
          payload: {},
          dependencies: ['step-extract'],
        },
        {
          id: 'step-load',
          name: 'Load',
          queue: 'workflow-queue',
          payload: {},
          dependencies: ['step-transform-1', 'step-transform-2'],
        },
      ],
    });

    // Wait for workflow completion
    await new Promise<void>((resolve) => {
      engine.on('workflow:completed', (e) => {
        if (e.workflowId === workflow.id) resolve();
      });
    });

    // Validate execution order
    assert.strictEqual(executionLog[0], 'ETL-Pipeline::Extract');
    assert.ok(
      executionLog[1] === 'ETL-Pipeline::Transform-Part-1' ||
        executionLog[1] === 'ETL-Pipeline::Transform-Part-2'
    );
    assert.ok(
      executionLog[2] === 'ETL-Pipeline::Transform-Part-1' ||
        executionLog[2] === 'ETL-Pipeline::Transform-Part-2'
    );
    assert.strictEqual(executionLog[3], 'ETL-Pipeline::Load');

    const state = engine.getWorkflow(workflow.id);
    assert.strictEqual(state?.status, 'completed');
    assert.strictEqual(Object.keys(state.results).length, 4);

    await queue.close();
    await broker.close();
  });

  it('should reject cyclic workflow graphs with validation error', async () => {
    const broker = new MemoryBroker();
    const engine = new WorkflowEngine(broker);

    await assert.rejects(
      async () => {
        await engine.submit({
          name: 'Cyclic-Graph',
          nodes: [
            { id: 'node-a', name: 'A', queue: 'q', payload: {}, dependencies: ['node-b'] },
            { id: 'node-b', name: 'B', queue: 'q', payload: {}, dependencies: ['node-a'] },
          ],
        });
      },
      /Cyclic dependency detected/
    );

    await broker.close();
  });
});
