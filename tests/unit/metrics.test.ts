import { describe, it } from 'node:test';
import assert from 'node:assert';
import { MetricsCollector } from '../../src/telemetry/metrics.js';

describe('MetricsCollector & Latency Percentiles', () => {
  it('should accurately calculate p50, p90, p95, p99 percentiles', () => {
    const collector = new MetricsCollector(1000);

    // Feed latencies from 1ms to 100ms
    for (let i = 1; i <= 100; i++) {
      collector.recordComplete(i);
    }

    const p = collector.getPercentiles();
    assert.strictEqual(p.p50, 50);
    assert.strictEqual(p.p90, 90);
    assert.strictEqual(p.p95, 95);
    assert.strictEqual(p.p99, 99);
  });

  it('should increment counters and generate Prometheus format', () => {
    const collector = new MetricsCollector();
    collector.recordEnqueue();
    collector.recordEnqueue();
    collector.recordComplete(25);
    collector.recordFailure(false);
    collector.recordRetry();
    collector.recordDLQ();
    collector.recordZombieReclaimed();

    const snapshot = collector.getSnapshot();
    assert.strictEqual(snapshot.totalEnqueued, 2);
    assert.strictEqual(snapshot.totalCompleted, 1);
    assert.strictEqual(snapshot.totalFailed, 1);
    assert.strictEqual(snapshot.totalRetried, 1);
    assert.strictEqual(snapshot.totalDLQ, 1);
    assert.strictEqual(snapshot.totalZombiesReclaimed, 1);

    const prom = collector.toPrometheusFormat();
    assert.ok(prom.includes('nexusqueue_tasks_enqueued_total 2'));
    assert.ok(prom.includes('nexusqueue_tasks_completed_total 1'));
    assert.ok(prom.includes('nexusqueue_tasks_dlq_total 1'));
    assert.ok(prom.includes('nexusqueue_zombies_reclaimed_total 1'));
  });
});

