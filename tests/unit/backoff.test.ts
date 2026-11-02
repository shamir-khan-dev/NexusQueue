import { describe, it } from 'node:test';
import assert from 'node:assert';
import { calculateBackoff, calculateDecorrelatedJitter } from '../../src/core/backoff.js';

describe('Exponential Backoff & Full Jitter Engine', () => {
  it('should calculate non-jittered exponential backoff correctly', () => {
    const config = {
      type: 'exponential' as const,
      baseMs: 1000,
      maxMs: 16000,
      jitter: false,
    };

    assert.strictEqual(calculateBackoff(config, 0), 1000);  // 1000 * 2^0
    assert.strictEqual(calculateBackoff(config, 1), 2000);  // 1000 * 2^1
    assert.strictEqual(calculateBackoff(config, 2), 4000);  // 1000 * 2^2
    assert.strictEqual(calculateBackoff(config, 3), 8000);  // 1000 * 2^3
    assert.strictEqual(calculateBackoff(config, 4), 16000); // 1000 * 2^4 = 16000 (cap)
    assert.strictEqual(calculateBackoff(config, 5), 16000); // capped at maxMs
  });

  it('should generate delays within full jitter boundary [0, cap]', () => {
    const config = {
      type: 'exponential' as const,
      baseMs: 500,
      maxMs: 5000,
      jitter: true,
    };

    for (let attempt = 0; attempt <= 4; attempt++) {
      const cap = Math.min(5000, 500 * Math.pow(2, attempt));
      for (let run = 0; run < 30; run++) {
        const delay = calculateBackoff(config, attempt);
        assert.ok(delay >= 0, `Delay ${delay} should be >= 0`);
        assert.ok(delay <= cap, `Delay ${delay} should be <= cap ${cap}`);
      }
    }
  });

  it('should calculate linear backoff correctly', () => {
    const config = {
      type: 'linear' as const,
      baseMs: 200,
      maxMs: 1000,
      jitter: false,
    };

    assert.strictEqual(calculateBackoff(config, 0), 200);  // 200 * 1
    assert.strictEqual(calculateBackoff(config, 1), 400);  // 200 * 2
    assert.strictEqual(calculateBackoff(config, 2), 600);  // 200 * 3
    assert.strictEqual(calculateBackoff(config, 5), 1000); // capped
  });

  it('should calculate fixed backoff correctly', () => {
    const config = {
      type: 'fixed' as const,
      baseMs: 350,
      maxMs: 1000,
      jitter: false,
    };

    assert.strictEqual(calculateBackoff(config, 0), 350);
    assert.strictEqual(calculateBackoff(config, 3), 350);
  });

  it('should keep decorrelated jitter within configured bounds', () => {
    const base = 100;
    const max = 2000;
    let sleep = base;

    for (let i = 0; i < 20; i++) {
      sleep = calculateDecorrelatedJitter(base, max, sleep);
      assert.ok(sleep >= base, `Sleep ${sleep} should be >= base ${base}`);
      assert.ok(sleep <= max, `Sleep ${sleep} should be <= max ${max}`);
    }
  });
});
