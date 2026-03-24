package login_system

import (
	"context"
	"errors"
	"time"
)

// RedisGetRaw performs a raw Redis GET on the given key.
// Returns the string value and error. Error is non-nil if key doesn't exist or Redis is unavailable.
func (l *LoginSystem) RedisGetRaw(ctx context.Context, key string) (string, error) {
	if l.RedisClient == nil {
		return "", errors.New("redis_client_nil")
	}
	return l.RedisClient.Get(ctx, key).Result()
}

// RedisSetRaw performs a raw Redis SET on the given key with an optional expiration.
// Pass 0 for expiration to keep the key indefinitely.
func (l *LoginSystem) RedisSetRaw(ctx context.Context, key string, value any, expiration time.Duration) error {
	if l.RedisClient == nil {
		return errors.New("redis_client_nil")
	}
	return l.RedisClient.Set(ctx, key, value, expiration).Err()
}

// RedisIncr performs an atomic Redis INCR on the given key. Returns the new value after increment.
func (l *LoginSystem) RedisIncr(ctx context.Context, key string) (int64, error) {
	if l.RedisClient == nil {
		return 0, errors.New("redis_client_nil")
	}
	return l.RedisClient.Incr(ctx, key).Result()
}
