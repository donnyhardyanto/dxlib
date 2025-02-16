package rate_limiter

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"time"
)

type RateLimiter struct {
	redisRing     *redis.Ring
	attempts      int           // Max attempts allowed
	window        time.Duration // Time window for attempts
	blockDuration time.Duration // How long to block after max attempts
}

func NewRateLimiter(redisRing *redis.Ring, attempts int, window time.Duration, blockDuration time.Duration) *RateLimiter {
	return &RateLimiter{
		redisRing:     redisRing,
		attempts:      attempts,
		window:        window,
		blockDuration: blockDuration,
	}
}

func (r *RateLimiter) getAttemptKey(identifier string) string {
	return fmt.Sprintf("login_attempts:%s", identifier)
}

func (r *RateLimiter) getBlockKey(identifier string) string {
	return fmt.Sprintf("login_blocked:%s", identifier)
}

func (r *RateLimiter) IsAllowed(ctx context.Context, identifier string) (bool, error) {
	// Check if the identifier is blocked
	blockedKey := r.getBlockKey(identifier)
	blocked, err := r.redisRing.Exists(ctx, blockedKey).Result()
	if err != nil {
		return false, err
	}
	if blocked == 1 {
		return false, nil
	}

	// Get current attempts
	attemptsKey := r.getAttemptKey(identifier)
	attempts, err := r.redisRing.Get(ctx, attemptsKey).Int()
	if err == redis.Nil {
		// Key doesn't exist, first attempt
		err = r.redisRing.Set(ctx, attemptsKey, 1, r.window).Err()
		return err == nil, err
	}
	if err != nil {
		return false, err
	}

	// Check if attempts exceeded
	if attempts >= r.attempts {
		// Block the identifier
		err = r.redisRing.Set(ctx, blockedKey, true, r.blockDuration).Err()
		if err != nil {
			return false, err
		}
		// Reset attempts counter
		err = r.redisRing.Del(ctx, attemptsKey).Err()
		return false, err
	}

	// Increment attempts
	err = r.redisRing.Incr(ctx, attemptsKey).Err()
	return err == nil, err
}

func (r *RateLimiter) Reset(ctx context.Context, identifier string) error {
	attemptsKey := r.getAttemptKey(identifier)
	blockedKey := r.getBlockKey(identifier)

	pipe := r.redisRing.Pipeline()
	pipe.Del(ctx, attemptsKey)
	pipe.Del(ctx, blockedKey)
	_, err := pipe.Exec(ctx)
	return err
}

func (r *RateLimiter) GetRemainingAttempts(ctx context.Context, identifier string) (int, error) {
	attemptsKey := r.getAttemptKey(identifier)
	attempts, err := r.redisRing.Get(ctx, attemptsKey).Int()
	if err == redis.Nil {
		return r.attempts, nil
	}
	if err != nil {
		return 0, err
	}
	return r.attempts - attempts, nil
}
