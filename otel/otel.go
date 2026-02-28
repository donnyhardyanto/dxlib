package otel

import (
	"context"
	"time"

	"github.com/donnyhardyanto/dxlib/core"
	"github.com/donnyhardyanto/dxlib/log"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

var (
	tracerProvider *sdktrace.TracerProvider
	meterProvider  *sdkmetric.MeterProvider
)

func SetupOpenTelemetry(serviceName string) error {
	if !core.IsOtelEnabled {
		return nil
	}

	ctx := context.Background()

	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceNameKey.String(serviceName),
		),
	)
	if err != nil {
		log.Log.Warnf("OTel resource creation failed (continuing without OTel): %v", err)
		core.IsOtelEnabled = false
		return nil
	}

	// Trace exporter
	traceExporter, err := otlptracehttp.New(ctx)
	if err != nil {
		log.Log.Warnf("OTel trace exporter creation failed (continuing without OTel): %v", err)
		core.IsOtelEnabled = false
		return nil
	}

	tracerProvider = sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(traceExporter,
			sdktrace.WithBatchTimeout(1*time.Second),
			sdktrace.WithMaxExportBatchSize(128),
		),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tracerProvider)

	// Metric exporter
	metricExporter, err := otlpmetrichttp.New(ctx)
	if err != nil {
		log.Log.Warnf("OTel metric exporter creation failed (continuing without OTel): %v", err)
		core.IsOtelEnabled = false
		return nil
	}

	meterProvider = sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExporter)),
		sdkmetric.WithResource(res),
	)
	otel.SetMeterProvider(meterProvider)

	// Propagator
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	err = InitMetrics()
	if err != nil {
		log.Log.Warnf("OTel metrics initialization failed (continuing without OTel): %v", err)
		core.IsOtelEnabled = false
		return nil
	}

	log.Log.Infof("OpenTelemetry initialized for service %s", serviceName)
	return nil
}

func ShutdownOpenTelemetry() error {
	if !core.IsOtelEnabled {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if tracerProvider != nil {
		if err := tracerProvider.Shutdown(ctx); err != nil {
			log.Log.Warnf("OTel tracer provider shutdown error: %v", err)
		}
	}

	if meterProvider != nil {
		if err := meterProvider.Shutdown(ctx); err != nil {
			log.Log.Warnf("OTel meter provider shutdown error: %v", err)
		}
	}

	log.Log.Info("OpenTelemetry shut down")
	return nil
}
