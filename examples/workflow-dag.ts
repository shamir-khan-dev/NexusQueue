import { MemoryBroker } from '../src/brokers/memory-broker.js';
import { NexusQueue } from '../src/core/queue.js';
import { WorkflowEngine } from '../src/scheduler/workflow.js';

async function main() {
  console.log('\n======================================================');
  console.log('🧬 NexusQueue Distributed DAG Workflow Engine');
  console.log('======================================================\n');

  const broker = new MemoryBroker();
  const queue = new NexusQueue('data-pipeline', { broker });
  const engine = new WorkflowEngine(broker);

  // Setup worker handlers
  queue.process(async (task) => {
    console.log(`[Task Node Executing] -> ${task.name}`);
    await new Promise((r) => setTimeout(r, 200));

    if (task.name.includes('Extract')) {
      return { rowCount: 50000, rawDataUri: 's3://bucket/raw_sales.csv' };
    }
    if (task.name.includes('Sanitize')) {
      return { cleanRows: 49820, invalidRowsPruned: 180 };
    }
    if (task.name.includes('Feature-Engineering')) {
      return { featuresGenerated: 14, featureSetUri: 's3://bucket/features.parquet' };
    }
    if (task.name.includes('Train-Model')) {
      return { modelType: 'XGBoost', aucRoc: 0.942, savedModelPath: 'models/v2.bin' };
    }
    return { ok: true };
  });

  // Define DAG:
  //      Extract
  //     /       \
  // Sanitize   Feature-Engineering
  //     \       /
  //    Train-Model
  console.log('Submitting DAG Workflow:');
  console.log('          [Extract Raw Sales Data]');
  console.log('                 /         \\');
  console.log('   [Sanitize & Clean]     [Feature Engineering]');
  console.log('                 \\         /');
  console.log('            [Train XGBoost Model]\n');

  engine.on('workflow:started', (e) => console.log(`[Event] Workflow Started: ${e.workflowId}`));
  engine.on('node:completed', (e) => console.log(`[Event] Node "${e.nodeId}" Completed with:`, e.result));
  engine.on('workflow:completed', async (e) => {
    console.log(`\n🎉 Workflow Successfully Completed in ${e.durationMs}ms!`);
    console.log('Final Aggregated Results:', JSON.stringify(e.results, null, 2));
    await queue.close();
    await broker.close();
    process.exit(0);
  });

  await engine.submit({
    name: 'Customer-Churn-ML-Pipeline',
    nodes: [
      {
        id: 'node-extract',
        name: 'Extract Raw Sales Data',
        queue: 'data-pipeline',
        payload: { database: 'production_warehouse' },
      },
      {
        id: 'node-clean',
        name: 'Sanitize & Clean Missing Values',
        queue: 'data-pipeline',
        payload: { removeNulls: true },
        dependencies: ['node-extract'],
      },
      {
        id: 'node-features',
        name: 'Feature Engineering & Vectorization',
        queue: 'data-pipeline',
        payload: { windowDays: 30 },
        dependencies: ['node-extract'],
      },
      {
        id: 'node-train',
        name: 'Train XGBoost Model',
        queue: 'data-pipeline',
        payload: { learningRate: 0.05, maxDepth: 6 },
        dependencies: ['node-clean', 'node-features'],
      },
    ],
  });
}

main().catch(console.error);
