package endpoint_rate_limiter

import (
	"context"
	"fmt"
	dxRedis "github.com/donnyhardyanto/dxlib/redis"
	"github.com/go-redis/redis/v8"
	"time"
)

// RateLimitConfig defines the rate limit settings for an API
type RateLimitConfig struct {
	Attempts      int           // Max attempts allowed
	Window        time.Duration // Time window for attempts
	BlockDuration time.Duration // How long to block after max attempts
}

// EndpointRateLimiter manages rate limiting for multiple endpoints and APIs
type EndpointRateLimiter struct {
	redisInstance **dxRedis.DXRedis
	keyPrefix     string                     // Prefix for Redis keys to separate endpoints
	apiConfigs    map[string]RateLimitConfig // Map of API paths to their configurations
	defaultConfig RateLimitConfig            // Default configuration if specific API config not found
}

// NewEndpointRateLimiter creates a new instance of EndpointRateLimiter
func NewEndpointRateLimiter(
	redisInstance **dxRedis.DXRedis,
	keyPrefix string,
	defaultConfig RateLimitConfig,
) *EndpointRateLimiter {
	return &EndpointRateLimiter{
		redisInstance: redisInstance,
		keyPrefix:     keyPrefix,
		apiConfigs:    make(map[string]RateLimitConfig),
		defaultConfig: defaultConfig,
	}
}

// RegisterAPI registers a specific API path with custom rate limit configuration
func (e *EndpointRateLimiter) RegisterAPI(apiPath string, config RateLimitConfig) {
	e.apiConfigs[apiPath] = config
}

// getConfig returns the rate limit configuration for a specific API
func (e *EndpointRateLimiter) getConfig(apiPath string) RateLimitConfig {
	config, exists := e.apiConfigs[apiPath]
	if !exists {
		return e.defaultConfig
	}
	return config
}

// getAttemptKey generates a Redis key for tracking attempts
func (e *EndpointRateLimiter) getAttemptKey(apiPath, identifier string) string {
	return fmt.Sprintf("%s:attempts:%s:%s", e.keyPrefix, apiPath, identifier)
}

// getBlockKey generates a Redis key for tracking blocked status
func (e *EndpointRateLimiter) getBlockKey(apiPath, identifier string) string {
	return fmt.Sprintf("%s:blocked:%s:%s", e.keyPrefix, apiPath, identifier)
}

// IsAllowed checks if a request is allowed based on the rate limit configuration
func (e *EndpointRateLimiter) IsAllowed(ctx context.Context, apiPath, identifier string) (bool, error) {
	config := e.getConfig(apiPath)

	// Check if the identifier is blocked for this API
	blockedKey := e.getBlockKey(apiPath, identifier)
	p := *(e.redisInstance)
	blocked, err := p.Connection.Exists(ctx, blockedKey).Result()
	if err != nil {
		return false, err
	}
	if blocked == 1 {
		return false, nil
	}

	// Get current attempts
	attemptsKey := e.getAttemptKey(apiPath, identifier)
	attempts, err := p.Connection.Get(ctx, attemptsKey).Int()
	if err == redis.Nil {
		// Key doesn't exist, first attempt
		err = p.Connection.Set(ctx, attemptsKey, 1, config.Window).Err()
		return err == nil, err
	}
	if err != nil {
		return false, err
	}

	// Check if attempts exceeded
	if attempts >= config.Attempts {
		// Block the identifier for this API
		err = p.Connection.Set(ctx, blockedKey, true, config.BlockDuration).Err()
		if err != nil {
			return false, err
		}
		// Reset attempts counter
		err = p.Connection.Del(ctx, attemptsKey).Err()
		return false, err
	}

	// Increment attempts
	err = p.Connection.Incr(ctx, attemptsKey).Err()
	return err == nil, err
}

// Reset clears the rate limit counters and blocked status for a specific identifier and API
func (e *EndpointRateLimiter) Reset(ctx context.Context, apiPath, identifier string) error {
	attemptsKey := e.getAttemptKey(apiPath, identifier)
	blockedKey := e.getBlockKey(apiPath, identifier)
	p := *(e.redisInstance)

	pipe := p.Connection.Pipeline()
	pipe.Del(ctx, attemptsKey)
	pipe.Del(ctx, blockedKey)
	_, err := pipe.Exec(ctx)
	return err
}

// GetRemainingAttempts returns the number of remaining attempts for an identifier on a specific API
func (e *EndpointRateLimiter) GetRemainingAttempts(ctx context.Context, apiPath, identifier string) (int, error) {
	config := e.getConfig(apiPath)
	attemptsKey := e.getAttemptKey(apiPath, identifier)
	p := *(e.redisInstance)

	attempts, err := p.Connection.Get(ctx, attemptsKey).Int()
	if err == redis.Nil {
		return config.Attempts, nil
	}
	if err != nil {
		return 0, err
	}
	return config.Attempts - attempts, nil
}

// ResetAll clears all rate limit data for a specific API path
func (e *EndpointRateLimiter) ResetAll(ctx context.Context, apiPath string) error {
	pattern := fmt.Sprintf("%s:*:%s:*", e.keyPrefix, apiPath)
	p := *(e.redisInstance)

	var cursor uint64
	var keys []string
	var err error

	// Scan for all keys matching the pattern
	for {
		keys, cursor, err = p.Connection.Scan(ctx, cursor, pattern, 100).Result()
		if err != nil {
			return err
		}

		if len(keys) > 0 {
			// Delete all found keys in a pipeline
			pipe := p.Connection.Pipeline()
			for _, key := range keys {
				pipe.Del(ctx, key)
			}
			_, err = pipe.Exec(ctx)
			if err != nil {
				return err
			}
		}

		if cursor == 0 {
			break
		}
	}

	return nil
}

// GetBlockedStatus checks if an identifier is currently blocked for a specific API
func (e *EndpointRateLimiter) GetBlockedStatus(ctx context.Context, apiPath, identifier string) (bool, time.Duration, error) {
	blockedKey := e.getBlockKey(apiPath, identifier)
	p := *(e.redisInstance)

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
