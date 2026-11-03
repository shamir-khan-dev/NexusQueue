export class MetricsCollector {
  private startTime: number = Date.now();
  private latencySamples: number[] = [];
  private maxSamples: number;

  // Global counters
  private enqueuedCount: number = 0;
  private completedCount: number = 0;
  private failedCount: number = 0;
  private retriedCount: number = 0;
  private dlqCount: number = 0;
  private zombiesReclaimedCount: number = 0;

  // Moving throughput tracking (last 10 seconds)
  private throughputHistory: Array<{ timestamp: number; count: number }> = [];

  constructor(maxSamples: number = 5000) {
    this.maxSamples = maxSamples;
  }

  recordEnqueue(): void {
    this.enqueuedCount++;
  }

  recordComplete(latencyMs: number): void {
    this.completedCount++;
    this.recordLatency(latencyMs);
    this.recordThroughput();
  }

  recordFailure(isFatalOrDLQ: boolean = false): void {
    this.failedCount++;
    if (isFatalOrDLQ) {
      this.dlqCount++;
    }
  }

  recordRetry(): void {
    this.retriedCount++;
  }

  recordDLQ(): void {
    this.dlqCount++;
  }

  recordZombieReclaimed(): void {
    this.zombiesReclaimedCount++;
  }

  private recordLatency(latencyMs: number): void {
    if (this.latencySamples.length >= this.maxSamples) {
      this.latencySamples.shift();
    }
    this.latencySamples.push(latencyMs);
  }

  private recordThroughput(): void {
    const now = Date.now();
    this.throughputHistory.push({ timestamp: now, count: 1 });
    this.pruneThroughputHistory(now);
  }

  private pruneThroughputHistory(now: number): void {
    const cutoff = now - 10000; // 10 seconds sliding window
    while (
      this.throughputHistory.length > 0 &&
      this.throughputHistory[0].timestamp < cutoff
    ) {
      this.throughputHistory.shift();
    }
  }

  getThroughputPerSec(): number {
    const now = Date.now();
    this.pruneThroughputHistory(now);
    if (this.throughputHistory.length === 0) return 0;

    const windowDurationSec = Math.max(
      1,
      (now - this.throughputHistory[0].timestamp) / 1000
    );
    return Number((this.throughputHistory.length / windowDurationSec).toFixed(2));
  }

  getPercentiles(): { p50: number; p90: number; p95: number; p99: number } {
    if (this.latencySamples.length === 0) {
      return { p50: 0, p90: 0, p95: 0, p99: 0 };
    }

    const sorted = [...this.latencySamples].sort((a, b) => a - b);
    const getPercentile = (p: number) => {
      const index = Math.ceil((p / 100) * sorted.length) - 1;
      return Number(sorted[Math.max(0, index)].toFixed(2));
    };

    return {
      p50: getPercentile(50),
      p90: getPercentile(90),
      p95: getPercentile(95),
      p99: getPercentile(99),
    };
  }

  getSnapshot() {
    const percentiles = this.getPercentiles();
    const throughput = this.getThroughputPerSec();
    const uptimeSec = Math.floor((Date.now() - this.startTime) / 1000);

    return {
      uptimeSeconds: uptimeSec,
      throughputPerSecond: throughput,
      totalEnqueued: this.enqueuedCount,
      totalCompleted: this.completedCount,
      totalFailed: this.failedCount,
      totalRetried: this.retriedCount,
      totalDLQ: this.dlqCount,
      totalZombiesReclaimed: this.zombiesReclaimedCount,
      latencyPercentiles: percentiles,
    };
  }

  toPrometheusFormat(): string {
    const snapshot = this.getSnapshot();
    return [
      '# HELP nexusqueue_tasks_enqueued_total Total tasks enqueued',
      '# TYPE nexusqueue_tasks_enqueued_total counter',
      `nexusqueue_tasks_enqueued_total ${snapshot.totalEnqueued}`,
      '',
      '# HELP nexusqueue_tasks_completed_total Total tasks successfully processed',
      '# TYPE nexusqueue_tasks_completed_total counter',
      `nexusqueue_tasks_completed_total ${snapshot.totalCompleted}`,
      '',
      '# HELP nexusqueue_tasks_failed_total Total tasks that failed execution',
      '# TYPE nexusqueue_tasks_failed_total counter',
      `nexusqueue_tasks_failed_total ${snapshot.totalFailed}`,
      '',
      '# HELP nexusqueue_tasks_retried_total Total task retry attempts initiated',
      '# TYPE nexusqueue_tasks_retried_total counter',
      `nexusqueue_tasks_retried_total ${snapshot.totalRetried}`,
      '',
      '# HELP nexusqueue_tasks_dlq_total Total tasks moved to Dead Letter Queue',
      '# TYPE nexusqueue_tasks_dlq_total counter',
      `nexusqueue_tasks_dlq_total ${snapshot.totalDLQ}`,
      '',
      '# HELP nexusqueue_zombies_reclaimed_total Total crashed worker tasks reclaimed',
      '# TYPE nexusqueue_zombies_reclaimed_total counter',
      `nexusqueue_zombies_reclaimed_total ${snapshot.totalZombiesReclaimed}`,
      '',
      '# HELP nexusqueue_throughput_per_sec Instantaneous task throughput per second',
      '# TYPE nexusqueue_throughput_per_sec gauge',
      `nexusqueue_throughput_per_sec ${snapshot.throughputPerSecond}`,
      '',
      '# HELP nexusqueue_latency_ms Task execution latency percentiles in ms',
      '# TYPE nexusqueue_latency_ms summary',
      `nexusqueue_latency_ms{quantile="0.5"} ${snapshot.latencyPercentiles.p50}`,
      `nexusqueue_latency_ms{quantile="0.9"} ${snapshot.latencyPercentiles.p90}`,
      `nexusqueue_latency_ms{quantile="0.95"} ${snapshot.latencyPercentiles.p95}`,
      `nexusqueue_latency_ms{quantile="0.99"} ${snapshot.latencyPercentiles.p99}`,
    ].join('\n');
  }

  reset(): void {
    this.startTime = Date.now();
    this.latencySamples = [];
    this.throughputHistory = [];
    this.enqueuedCount = 0;
    this.completedCount = 0;
    this.failedCount = 0;
    this.retriedCount = 0;
    this.dlqCount = 0;
    this.zombiesReclaimedCount = 0;
  }
}

export const globalMetrics = new MetricsCollector();

