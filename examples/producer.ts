import { NexusQueue } from '../src/core/queue.js';
import { TaskPriority } from '../src/core/types.js';

async function main() {
  const queue = new NexusQueue('email-notifications');

  console.log('📬 Enqueuing sample notifications with various priorities & delays...');

  // 1. High priority welcome email
  const welcomeTask = await queue.add(
    'send-welcome-email',
    { email: 'user@example.com', templateId: 'welcome_2026' },
    { priority: TaskPriority.HIGH }
  );
  console.log(`[+] Enqueued High-Priority Task: ${welcomeTask.id} (${welcomeTask.name})`);

  // 2. Delayed password reset reminder (runs after 3 seconds)
  const delayedTask = await queue.add(
    'send-reminder-email',
    { email: 'user@example.com', daysInactive: 14 },
    { delayMs: 3000, priority: TaskPriority.NORMAL }
  );
  console.log(`[+] Enqueued Delayed Task: ${delayedTask.id} (Scheduled in 3s)`);

  // 3. Flaky task configured with full-jitter exponential backoff
  const flakyTask = await queue.add(
    'sync-third-party-crm',
    { contactId: 'crm_981' },
    {
      maxRetries: 3,
      backoff: { type: 'exponential', baseMs: 500, maxMs: 10000, jitter: true },
    }
  );
  console.log(`[+] Enqueued Flaky Task with Jitter Backoff: ${flakyTask.id}`);

  console.log('\n✔ All producer tasks enqueued successfully.');
  await queue.close();
}

main().catch(console.error);

