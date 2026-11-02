import { EventEmitter } from 'node:events';
import cronParser from 'cron-parser';
import { IBroker } from '../brokers/interface.js';
import { TaskOptions, createTask } from '../core/types.js';

export interface CronJobConfig {
  id: string;
  name: string;
  queue: string;
  cronExpression: string;
  payload?: any;
  options?: TaskOptions;
}

export class CronScheduler extends EventEmitter {
  private broker: IBroker;
  private jobs: Map<string, { config: CronJobConfig; timer: NodeJS.Timeout | null; nextRun: Date }> =
    new Map();
  private isRunning: boolean = false;

  constructor(broker: IBroker) {
    super();
    this.broker = broker;
  }

  register(config: CronJobConfig): void {
    // Validate cron expression
    try {
      const interval = cronParser.parseExpression(config.cronExpression);
      const nextRun = interval.next().toDate();

      this.jobs.set(config.id, {
        config,
        timer: null,
        nextRun,
      });

      if (this.isRunning) {
        this.scheduleJob(config.id);
      }
    } catch (err: any) {
      throw new Error(`Invalid cron expression "${config.cronExpression}": ${err.message}`);
    }
  }

  start(): void {
    if (this.isRunning) return;
    this.isRunning = true;

    for (const id of this.jobs.keys()) {
      this.scheduleJob(id);
    }
  }

  private scheduleJob(id: string): void {
    const job = this.jobs.get(id);
    if (!job || !this.isRunning) return;

    const now = Date.now();
    const interval = cronParser.parseExpression(job.config.cronExpression);
    const nextRunDate = interval.next().toDate();
    job.nextRun = nextRunDate;

    const delayMs = Math.max(0, nextRunDate.getTime() - now);

    job.timer = setTimeout(async () => {
      try {
        const task = createTask(
          job.config.queue,
          job.config.name,
          job.config.payload ?? {},
          {
            ...job.config.options,
            metadata: {
              cronJobId: job.config.id,
              cronExpression: job.config.cronExpression,
            },
          }
        );
        await this.broker.enqueue(task);
        this.emit('cron:triggered', { jobId: id, taskId: task.id, scheduledTime: nextRunDate });
      } catch (err) {
        this.emit('error', err);
      } finally {
        if (this.isRunning) {
          this.scheduleJob(id);
        }
      }
    }, delayMs);
    job.timer.unref();
  }

  stop(): void {
    this.isRunning = false;
    for (const job of this.jobs.values()) {
      if (job.timer) {
        clearTimeout(job.timer);
        job.timer = null;
      }
    }
  }

  listJobs() {
    return Array.from(this.jobs.values()).map((j) => ({
      id: j.config.id,
      name: j.config.name,
      queue: j.config.queue,
      cronExpression: j.config.cronExpression,
      nextRun: j.nextRun,
    }));
  }
}
