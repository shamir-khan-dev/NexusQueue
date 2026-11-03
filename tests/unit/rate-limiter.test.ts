import { describe, it } from 'node:test';
import assert from 'node:assert';
import {
  TokenBucketRateLimiter,
  SlidingWindowRateLimiter,
} from '../../src/core/rate-limiter.js';

describe('Rate Limiting Algorithms', () => {
  describe('TokenBucketRateLimiter', () => {
    it('should allow burst consumption up to capacity', () => {
      const bucket = new TokenBucketRateLimiter(5, 1);

      // Consume full capacity
      for (let i = 0; i < 5; i++) {
        const res = bucket.tryConsume(1);
        assert.strictEqual(res.allowed, true);
      }

      // Next request must be rejected
      const rejected = bucket.tryConsume(1);
      assert.strictEqual(rejected.allowed, false);
      assert.ok(rejected.retryAfterMs > 0);
    });

    it('should refill tokens over elapsed time', async () => {
      // 5 capacity, 10 tokens per second (1 token every 100ms)
      const bucket = new TokenBucketRateLimiter(5, 10);

      // Drain bucket
      bucket.tryConsume(5);
      assert.strictEqual(bucket.tryConsume(1).allowed, false);

      // Wait 150ms -> at least 1 token refilled
      await new Promise((r) => setTimeout(r, 150));

      const afterWait = bucket.tryConsume(1);
      assert.strictEqual(afterWait.allowed, true);
    });
  });

  describe('SlidingWindowRateLimiter', () => {
    it('should allow requests within limit and reject excess', () => {
      const limiter = new SlidingWindowRateLimiter(1000, 3); // 3 req per 1000ms

      assert.strictEqual(limiter.tryConsume().allowed, true);
      assert.strictEqual(limiter.tryConsume().allowed, true);
      assert.strictEqual(limiter.tryConsume().allowed, true);

      const rejected = limiter.tryConsume();
      assert.strictEqual(rejected.allowed, false);
      assert.ok(rejected.retryAfterMs > 0);
    });

    it('should allow new requests once the sliding window rolls forward', async () => {
      const limiter = new SlidingWindowRateLimiter(200, 2);

      assert.strictEqual(limiter.tryConsume().allowed, true);
      assert.strictEqual(limiter.tryConsume().allowed, true);
      assert.strictEqual(limiter.tryConsume().allowed, false);

      await new Promise((r) => setTimeout(r, 220));

      assert.strictEqual(limiter.tryConsume().allowed, true);
    });
  });
});

