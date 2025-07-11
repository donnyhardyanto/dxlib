package endpoint_rate_limiter

import (
	"github.com/donnyhardyanto/dxlib/redis"
)

type DXDEndpointRateLimiterManager struct {
	EndpointRateLimiter *EndpointRateLimiter
}

var Manager = DXDEndpointRateLimiterManager{}

func (d *DXDEndpointRateLimiterManager) Get(key string) *EndpointRateLimiter {
	return nil
}

func (d *DXDEndpointRateLimiterManager) Init(keyPrefix string, defaultConfig RateLimitConfig,
	redisInstance *redis.DXRedis) {
	d.EndpointRateLimiter = NewEndpointRateLimiter(
		&redisInstance,
		keyPrefix, // Prefix for this endpoint group
		defaultConfig,
	)
}

func (d *DXDEndpointRateLimiterManager) RegisterGroup(apiPath string, config RateLimitConfig) {
	d.EndpointRateLimiter.RegisterGroup(apiPath, config)
}
