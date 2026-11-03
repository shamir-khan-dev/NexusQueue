import { BackoffConfig } from './types.js';

/**
 * Calculates retry delay based on attempt count and configured backoff strategy.
 * Implements AWS standard "Full Jitter" to eliminate thundering herd synchronization.
 */
export function calculateBackoff(config: BackoffConfig, attempt: number): number {
  const { type, baseMs, maxMs, jitter = true } = config;
  const safeAttempt = Math.max(0, attempt);

  let calculatedDelay: number;

  switch (type) {
    case 'exponential': {
      // Exponential growth: baseMs * 2^attempt
      // Cap at 30 shifts to avoid 32-bit integer overflow
      const multiplier = Math.pow(2, Math.min(safeAttempt, 30));
      calculatedDelay = Math.min(maxMs, baseMs * multiplier);
      break;
    }

    case 'linear': {
      // Linear growth: baseMs * (attempt + 1)
      calculatedDelay = Math.min(maxMs, baseMs * (safeAttempt + 1));
      break;
    }

    case 'fixed':
    default: {
      calculatedDelay = Math.min(maxMs, baseMs);
      break;
    }
  }

  // Full Jitter: random value between 0 and calculatedDelay
  // E[delay] = calculatedDelay / 2, completely desynchronizes concurrent retries
  if (jitter) {
    return Math.floor(Math.random() * (calculatedDelay + 1));
  }

  return Math.floor(calculatedDelay);
}

/**
 * Decorrelated Jitter backoff (AWS architecture pattern)
 * sleep = min(maxMs, random_between(baseMs, prevSleep * 3))
 */
export function calculateDecorrelatedJitter(
  baseMs: number,
  maxMs: number,
  prevSleep: number = baseMs
): number {
  const high = Math.min(maxMs, prevSleep * 3);
  const low = Math.min(baseMs, high);
  return Math.floor(low + Math.random() * (high - low + 1));
}

