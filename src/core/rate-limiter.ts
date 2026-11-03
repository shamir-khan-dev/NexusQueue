export interface RateLimiterResult {
  allowed: boolean;
  remaining: number;
  retryAfterMs: number;
}

/**
 * Token Bucket Rate Limiter
 * Handles bursty traffic while enforcing a smooth average rate.
 */
export class TokenBucketRateLimiter {
  private capacity: number;
  private refillRatePerSec: number;
  private tokens: number;
  private lastRefillTimestamp: number;

  constructor(capacity: number, refillRatePerSec: number) {
    if (capacity <= 0 || refillRatePerSec <= 0) {
      throw new Error('Capacity and refillRatePerSec must be positive numbers');
    }
    this.capacity = capacity;
    this.refillRatePerSec = refillRatePerSec;
    this.tokens = capacity;
    this.lastRefillTimestamp = Date.now();
  }

  private refill(): void {
    const now = Date.now();
    const elapsedSeconds = (now - this.lastRefillTimestamp) / 1000;
    if (elapsedSeconds > 0) {
      const addedTokens = elapsedSeconds * this.refillRatePerSec;
      this.tokens = Math.min(this.capacity, this.tokens + addedTokens);
      this.lastRefillTimestamp = now;
    }
  }

  tryConsume(tokensToConsume: number = 1): RateLimiterResult {
    this.refill();

    if (this.tokens >= tokensToConsume) {
      this.tokens -= tokensToConsume;
      return {
        allowed: true,
        remaining: Math.floor(this.tokens),
        retryAfterMs: 0,
      };
    }

    const deficit = tokensToConsume - this.tokens;
    const retryAfterMs = Math.ceil((deficit / this.refillRatePerSec) * 1000);

    return {
      allowed: false,
      remaining: Math.floor(this.tokens),
      retryAfterMs,
    };
  }

  getTokens(): number {
    this.refill();
    return this.tokens;
  }

  reset(): void {
    this.tokens = this.capacity;
    this.lastRefillTimestamp = Date.now();
  }
}

/**
 * Sliding Window Log Rate Limiter
 * Enforces strict request quotas across arbitrary time windows with millisecond accuracy.
 */
export class SlidingWindowRateLimiter {
  private windowMs: number;
  private maxRequests: number;
  private log: number[] = [];

  constructor(windowMs: number, maxRequests: number) {
    if (windowMs <= 0 || maxRequests <= 0) {
      throw new Error('windowMs and maxRequests must be positive numbers');
    }
    this.windowMs = windowMs;
    this.maxRequests = maxRequests;
  }

  tryConsume(): RateLimiterResult {
    const now = Date.now();
    const windowStart = now - this.windowMs;

    // Prune entries outside the sliding window
    while (this.log.length > 0 && this.log[0] <= windowStart) {
      this.log.shift();
    }

    if (this.log.length < this.maxRequests) {
      this.log.push(now);
      return {
        allowed: true,
        remaining: this.maxRequests - this.log.length,
        retryAfterMs: 0,
      };
    }

    // Earliest timestamp determines when next slot opens
    const oldest = this.log[0];
    const retryAfterMs = Math.max(0, oldest + this.windowMs - now);

    return {
      allowed: false,
      remaining: 0,
      retryAfterMs,
    };
  }

  reset(): void {
    this.log = [];
  }
}

/**
 * Atomic Redis Lua Script for Token Bucket
 * KEYS[1]: bucket key
 * ARGV[1]: capacity
 * ARGV[2]: refillRatePerSec
 * ARGV[3]: now (epoch ms)
 * ARGV[4]: requested tokens
 */
export const REDIS_TOKEN_BUCKET_LUA = `
local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local refillRate = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local requested = tonumber(ARGV[4])

local data = redis.call('HMGET', key, 'tokens', 'last_updated')
local tokens = tonumber(data[1])
local last_updated = tonumber(data[2])

if not tokens then
  tokens = capacity
  last_updated = now
else
  local elapsed = (now - last_updated) / 1000
  if elapsed > 0 then
    tokens = math.min(capacity, tokens + (elapsed * refillRate))
    last_updated = now
  end
end

if tokens >= requested then
  tokens = tokens - requested
  redis.call('HMSET', key, 'tokens', tokens, 'last_updated', last_updated)
  redis.call('PEXPIRE', key, math.ceil((capacity / refillRate) * 1000 * 2))
  return {1, math.floor(tokens), 0}
else
  local deficit = requested - tokens
  local retry_after = math.ceil((deficit / refillRate) * 1000)
  return {0, math.floor(tokens), retry_after}
end
`;

