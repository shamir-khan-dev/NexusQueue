import { NexusQueue } from '../src/core/queue.js';
import { MemoryBroker } from '../src/brokers/memory-broker.js';
import { TaskPriority } from '../src/core/types.js';

async function runBenchmark() {
  console.log('\n======================================================');
  console.log('⚡ NexusQueue High-Throughput Engine Benchmark Suite');
  console.log('======================================================\n');

  const TOTAL_JOBS = 5000;
  const CONCURRENCY = 50;

  const broker = new MemoryBroker();
  const queue = new NexusQueue('benchmark-queue', { broker });

  let processedCount = 0;
  const latencies: number[] = [];

  // Register worker pool
  queue.process(
    async (task) => {
      const start = task.enqueuedAt;
      // Simulate real asynchronous I/O micro-tick
      await new Promise((r) => setImmediate(r));
      const end = Date.now();
      latencies.push(end - start);
      processedCount++;
    },
    { concurrency: CONCURRENCY }
  );

  console.log(`🚀 Enqueuing ${TOTAL_JOBS.toLocaleString()} jobs across priority levels...`);
  const enqueueStartTime = Date.now();

  const batchSize = 1000;
  for (let i = 0; i < TOTAL_JOBS; i += batchSize) {
    const batch = [];
    for (let j = 0; j < batchSize; j++) {
      const id = i + j;
      const priority = id % 4 === 0 ? TaskPriority.CRITICAL : TaskPriority.NORMAL;
      batch.push({
        name: `benchmark-job-${id}`,
        payload: { index: id, data: 'high-throughput-payload-bytes' },
        options: { priority },
      });
    }
    await queue.addBulk(batch);
  }

  const enqueueDurationSec = (Date.now() - enqueueStartTime) / 1000;
  const enqueueThroughput = Math.round(TOTAL_JOBS / enqueueDurationSec);
  console.log(`✔ Enqueued ${TOTAL_JOBS.toLocaleString()} jobs in ${enqueueDurationSec.toFixed(2)}s (${enqueueThroughput.toLocaleString()} jobs/sec)`);

  console.log(`⚡ Processing jobs across ${CONCURRENCY} concurrent workers...`);
  const processStartTime = Date.now();

  while (processedCount < TOTAL_JOBS) {
    await new Promise((r) => setTimeout(r, 20));
  }

  const processDurationSec = (Date.now() - processStartTime) / 1000;
  const processThroughput = Math.round(TOTAL_JOBS / processDurationSec);

  // Compute Latency Percentiles
  latencies.sort((a, b) => a - b);
  const getP = (p: number) => latencies[Math.ceil((p / 100) * latencies.length) - 1] || 0;

  console.log('\n======================================================');
  console.log('📊 BENCHMARK RESULTS');
  console.log('======================================================');
  console.log(`Total Tasks Processed : ${TOTAL_JOBS.toLocaleString()}`);
  console.log(`Worker Concurrency    : ${CONCURRENCY} parallel pipelines`);
  console.log(`Enqueue Throughput    : ${enqueueThroughput.toLocaleString()} tasks/sec`);
  console.log(`Processing Throughput : ${processThroughput.toLocaleString()} tasks/sec`);
  console.log(`------------------------------------------------------`);
  console.log(`p50 Latency (Median)  : ${getP(50)} ms`);
  console.log(`p90 Latency           : ${getP(90)} ms`);
  console.log(`p95 Latency           : ${getP(95)} ms`);
  console.log(`p99 Latency           : ${getP(99)} ms`);
  console.log('======================================================\n');

  // Save persistent benchmark report
  try {
    const fs = await import('node:fs');
    const path = await import('node:path');
    const outDir = path.resolve('benchmarks');
    if (!fs.existsSync(outDir)) {
      fs.mkdirSync(outDir, { recursive: true });
    }
    const report = {
      totalJobs: TOTAL_JOBS,
      concurrency: CONCURRENCY,
      enqueueThroughputJobsPerSec: enqueueThroughput,
      processingThroughputJobsPerSec: processThroughput,
      latencyMs: {
        p50: getP(50),
        p90: getP(90),
        p95: getP(95),
        p99: getP(99),
      },
    };
    fs.writeFileSync(path.join(outDir, 'benchmark_results.json'), JSON.stringify(report, null, 2));
    console.log(`[OK] Benchmark report saved to benchmarks/benchmark_results.json`);
  } catch (err) {
    console.warn('Failed to save benchmark report:', err);
  }

  await queue.close();
  await broker.close();
  process.exit(0);
}

runBenchmark().catch(console.error);
