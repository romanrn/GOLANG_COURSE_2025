package telemetry

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// MetricsProvider holds the OpenTelemetry metrics infrastructure
type MetricsProvider struct {
	meterProvider *sdkmetric.MeterProvider

	// Server metrics
	ShutdownDuration metric.Float64Histogram
	ShutdownStatus   metric.Int64Counter
}

// InitMetrics initializes the OpenTelemetry metrics with OTLP exporter
func InitMetrics(ctx context.Context, cfg Config) (*MetricsProvider, func(context.Context) error, error) {
	if !cfg.Enabled {
		// Return no-op provider if telemetry is disabled
		return &MetricsProvider{}, func(context.Context) error { return nil }, nil
	}

	// Create resource with service name
	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName(cfg.ServiceName),
		),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create resource: %w", err)
	}

	// Set up OTLP metrics exporter with gRPC
	conn, err := grpc.NewClient(
		cfg.Endpoint,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create gRPC connection: %w", err)
	}

	exporter, err := otlpmetricgrpc.New(ctx, otlpmetricgrpc.WithGRPCConn(conn))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create OTLP metric exporter: %w", err)
	}

	// Create meter provider with periodic reader
	meterProvider := sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(res),
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(exporter,
			sdkmetric.WithInterval(10*time.Second), // Export metrics every 10 seconds
		)),
	)

	// Set global meter provider
	otel.SetMeterProvider(meterProvider)

	// Create meter for this service
	meter := meterProvider.Meter(cfg.ServiceName)

	// Initialize server metrics
	shutdownDuration, err := meter.Float64Histogram(
		"http.server.shutdown.duration",
		metric.WithDescription("Time taken to gracefully shutdown HTTP server"),
		metric.WithUnit("s"),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create shutdown duration histogram: %w", err)
	}

	shutdownStatus, err := meter.Int64Counter(
		"http.server.shutdown.total",
		metric.WithDescription("Total number of server shutdowns by status"),
		metric.WithUnit("{shutdown}"),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create shutdown status counter: %w", err)
	}

	provider := &MetricsProvider{
		meterProvider:    meterProvider,
		ShutdownDuration: shutdownDuration,
		ShutdownStatus:   shutdownStatus,
	}

	// Return shutdown function
	shutdownFunc := func(ctx context.Context) error {
		shutdownCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()

		if err := meterProvider.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("failed to shutdown meter provider: %w", err)
		}
		return nil
	}

	return provider, shutdownFunc, nil
}

// RecordShutdown records metrics for server shutdown
func (m *MetricsProvider) RecordShutdown(ctx context.Context, duration time.Duration, success bool) {
	if m.meterProvider == nil {
		return // Metrics disabled
	}

	// Record duration
	m.ShutdownDuration.Record(ctx, duration.Seconds())

	// Record status with attribute
	status := "success"
	if !success {
		status = "forced"
	}
	m.ShutdownStatus.Add(ctx, 1, metric.WithAttributes(
		attribute.String("status", status),
	))
}
