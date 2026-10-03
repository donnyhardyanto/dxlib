package endpoint_rate_limiter

import (
	"context"
	"fmt"
	"time"

	"github.com/donnyhardyanto/dxlib/errors"
	dxRedis "github.com/donnyhardyanto/dxlib/redis"
	"github.com/go-redis/redis/v8"
)

// RateLimitConfig defines the rate limit settings for an API
type RateLimitConfig struct {
	MaxAttempts   int           // Max attempts allowed
	TimeWindow    time.Duration // Time window for attempts
	BlockDuration time.Duration // How long to block after max attempts
}

// EndpointRateLimiter manages rate limiting for multiple endpoints and APIs
type EndpointRateLimiter struct {
	RedisInstance **dxRedis.DXRedis
	KeyPrefix     string                     // Prefix for Redis keys to separate endpoints
	Group         map[string]RateLimitConfig // Map of API paths to their configurations
	DefaultConfig RateLimitConfig            // Default configuration if specific API config not found
}

// NewEndpointRateLimiter creates a new instance of EndpointRateLimiter
func NewEndpointRateLimiter(
	redisInstance **dxRedis.DXRedis,
	keyPrefix string,
	defaultConfig RateLimitConfig,
) *EndpointRateLimiter {
	return &EndpointRateLimiter{
		RedisInstance: redisInstance,
		KeyPrefix:     keyPrefix,
		Group:         make(map[string]RateLimitConfig),
		DefaultConfig: defaultConfig,
	}
}

// RegisterGroup registers a specific API path with custom rate limit configuration
func (e *EndpointRateLimiter) RegisterGroup(groupNameId string, config RateLimitConfig) {
	e.Group[groupNameId] = config
}

// getConfig returns the rate limit configuration for a specific API
func (e *EndpointRateLimiter) getConfig(groupNameId string) RateLimitConfig {
	config, exists := e.Group[groupNameId]
	if !exists {
		return e.DefaultConfig
	}
	return config
}

// getAttemptKey generates a Redis key for tracking attempts
func (e *EndpointRateLimiter) getAttemptKey(groupNameId, identifier string) string {
	return fmt.Sprintf("%s:attempts:%s:%s", e.KeyPrefix, groupNameId, identifier)
}

// getBlockKey generates a Redis key for tracking blocked status
func (e *EndpointRateLimiter) getBlockKey(groupNameId, identifier string) string {
	return fmt.Sprintf("%s:blocked:%s:%s", e.KeyPrefix, groupNameId, identifier)
}

// IsAllowed checks if a request is allowed based on the rate limit configuration
func (e *EndpointRateLimiter) IsAllowed(ctx context.Context, groupNameId, identifier string) (bool, error) {
	config := e.getConfig(groupNameId)

	// bypass if 0
	if config.MaxAttempts == 0 {
		return true, nil
	}

	p := *(e.RedisInstance)
	allowed, err := isAllowedScript.Run(ctx, p.Connection,
		[]string{e.getAttemptKey(groupNameId, identifier), e.getBlockKey(groupNameId, identifier)},
		config.TimeWindow.Milliseconds(), config.MaxAttempts, config.BlockDuration.Milliseconds(),
	).Int()
	if err != nil {
		return false, err
	}
	return allowed == 1, nil
}

// isAllowedScript checks the block, counts the attempt and sets the block as
// one step. Done as separate commands, parallel requests all read the same
// count and all passed, requests already past the block check started a fresh
// window once the counter was reset, and an INCR on a counter that expired in
// between left one with no TTL. Attempts 1..MaxAttempts pass; the next one
// blocks the identifier for BlockDuration and resets the counter. A zero
// duration means no expiry, as it did with SET.
//
// KEYS[1] attempts key, KEYS[2] blocked key
// ARGV[1] window ms, ARGV[2] max attempts, ARGV[3] block ms
var isAllowedScript = redis.NewScript(`
if redis.call('EXISTS', KEYS[2]) == 1 then
	return 0
end
local attempts = redis.call('INCR', KEYS[1])
if attempts == 1 and tonumber(ARGV[1]) > 0 then
	redis.call('PEXPIRE', KEYS[1], ARGV[1])
end
if attempts > tonumber(ARGV[2]) then
	if tonumber(ARGV[3]) > 0 then
		redis.call('SET', KEYS[2], '1', 'PX', ARGV[3])
	else
		redis.call('SET', KEYS[2], '1')
	end
	redis.call('DEL', KEYS[1])
	return 0
end
return 1
`)

// Reset clears the rate limit counters and blocked status for a specific identifier and API
func (e *EndpointRateLimiter) Reset(ctx context.Context, groupNameId, identifier string) error {
	attemptsKey := e.getAttemptKey(groupNameId, identifier)
	blockedKey := e.getBlockKey(groupNameId, identifier)
	p := *(e.RedisInstance)

	pipe := p.Connection.Pipeline()
	pipe.Del(ctx, attemptsKey)
	pipe.Del(ctx, blockedKey)
	_, err := pipe.Exec(ctx)
	return errors.Wrap(err, "ERROR_IN_ENDPOINT_RATE_LIMITER_RESET")
}

// GetRemainingAttempts returns the number of remaining attempts for an identifier on a specific API
func (e *EndpointRateLimiter) GetRemainingAttempts(ctx context.Context, groupNameId, identifier string) (int, error) {
	config := e.getConfig(groupNameId)
	attemptsKey := e.getAttemptKey(groupNameId, identifier)
	p := *(e.RedisInstance)

	attempts, err := p.Connection.Get(ctx, attemptsKey).Int()
	if err == redis.Nil {
		return config.MaxAttempts, nil
	}
	if err != nil {
		return 0, err
	}
	return config.MaxAttempts - attempts, nil
}

// ResetAll clears all rate limit data for a specific API path
func (e *EndpointRateLimiter) ResetAll(ctx context.Context, groupNameId string) error {
	pattern := fmt.Sprintf("%s:*:%s:*", e.KeyPrefix, groupNameId)
	p := *(e.RedisInstance)

	var cursor uint64
	var keys []string
	var err error

	// Scan for all keys matching the pattern
	for {
		keys, cursor, err = p.Connection.Scan(ctx, cursor, pattern, 100).Result()
		if err != nil {
			return errors.Wrap(err, "ERROR_IN_ENDPOINT_RATE_LIMITER_RESET_ALL_SCAN_PATTERN")
		}

		if len(keys) > 0 {
			// Delete all found keys in a pipeline
			pipe := p.Connection.Pipeline()
			for _, key := range keys {
				pipe.Del(ctx, key)
			}
			_, err = pipe.Exec(ctx)
			if err != nil {
				return errors.Wrap(err, "ERROR_IN_ENDPOINT_RATE_LIMITER_RESET_ALL_DELETE_KEYS")
			}
		}

		if cursor == 0 {
			break
		}
	}

	return nil
}

// GetBlockedStatus checks if an identifier is currently blocked for a specific API
func (e *EndpointRateLimiter) GetBlockedStatus(ctx context.Context, groupNameId, identifier string) (bool, time.Duration, error) {
	blockedKey := e.getBlockKey(groupNameId, identifier)
	p := *(e.RedisInstance)

	exists, err := p.Connection.Exists(ctx, blockedKey).Result()
	if err != nil {
		return false, 0, err
	}

	if exists == 0 {
		return false, 0, nil
	}

	// Get remaining TTL
	ttl, err := p.Connection.TTL(ctx, blockedKey).Result()
	if err != nil {
		return true, 0, err
	}

	return true, ttl, nil
}
