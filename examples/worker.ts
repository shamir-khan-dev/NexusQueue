import { NexusQueue } from '../src/core/queue.js';
import { TokenBucketRateLimiter } from '../src/core/rate-limiter.js';

async function main() {
  const queue = new NexusQueue('email-notifications');

  // Rate limiter: 10 emails/sec capacity with 5/sec steady refill
  const rateLimiter = new TokenBucketRateLimiter(10, 5);

  console.log('⚡ Starting Worker with Concurrency = 4 and Token-Bucket Rate Limiter (5 req/sec)...');

  const worker = queue.process(
    async (task) => {
      console.log(`[Worker] Starting task ${task.id.slice(0, 8)} (${task.name}) - Attempt ${task.attempts + 1}`);

      // Simulate sending email
      await new Promise((r) => setTimeout(r, 100));

      console.log(`[Worker] ✔ Finished task ${task.id.slice(0, 8)}`);
      return { sent: true, timestamp: Date.now() };
    },
    {
      id: 'worker-mailer-1',
      concurrency: 4,
      rateLimiter,
    }
  );

  // Graceful shutdown on Ctrl+C
  process.on('SIGINT', async () => {
    console.log('\n[Worker] Draining active tasks and shutting down cleanly...');
    await worker.stop();
    await queue.close();
    process.exit(0);
  });
}

main().catch(console.error);

