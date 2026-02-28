package otel

import (
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

var (
	HTTPRequestDuration metric.Float64Histogram
	HTTPRequestCount    metric.Int64Counter
	DBQueryDuration     metric.Float64Histogram
	DBQueryCount        metric.Int64Counter
	RedisOpDuration     metric.Float64Histogram
	RedisOpCount        metric.Int64Counter
	HTTPClientDuration  metric.Float64Histogram
	HTTPClientCount     metric.Int64Counter
)

func InitMetrics() error {
	meter := otel.Meter("dxlib")

	var err error

	HTTPRequestDuration, err = meter.Float64Histogram("http.server.request.duration",
		metric.WithUnit("s"),
		metric.WithDescription("Duration of HTTP server requests"),
	)
	if err != nil {
		return err
	}

	HTTPRequestCount, err = meter.Int64Counter("http.server.request.count",
		metric.WithDescription("Total number of HTTP server requests"),
	)
	if err != nil {
		return err
	}

	DBQueryDuration, err = meter.Float64Histogram("db.client.operation.duration",
		metric.WithUnit("s"),
		metric.WithDescription("Duration of database operations"),
	)
	if err != nil {
		return err
	}

	DBQueryCount, err = meter.Int64Counter("db.client.operation.count",
		metric.WithDescription("Total number of database operations"),
	)
	if err != nil {
		return err
	}

	RedisOpDuration, err = meter.Float64Histogram("redis.client.operation.duration",
		metric.WithUnit("s"),
		metric.WithDescription("Duration of Redis operations"),
	)
	if err != nil {
		return err
	}

	RedisOpCount, err = meter.Int64Counter("redis.client.operation.count",
		metric.WithDescription("Total number of Redis operations"),
	)
	if err != nil {
		return err
	}

	HTTPClientDuration, err = meter.Float64Histogram("http.client.request.duration",
		metric.WithUnit("s"),
		metric.WithDescription("Duration of HTTP client requests"),
	)
	if err != nil {
		return err
	}

	HTTPClientCount, err = meter.Int64Counter("http.client.request.count",
		metric.WithDescription("Total number of HTTP client requests"),
	)
	if err != nil {
		return err
	}

	return nil
}
