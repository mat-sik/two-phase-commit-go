package setup

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/log/global"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/resource"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.41.0"
)

func InitOTelSDK(ctx context.Context, collectorHost string, serviceName string) (joinedShutdowns ShutdownFunc, err error) {
	defer func() {
		if err != nil {
			err = errors.Join(err, joinedShutdowns(ctx))
			joinedShutdowns = nil
		}
	}()

	var shutdowns []ShutdownFunc
	shutdowns, err = initOTelSDK(ctx, collectorHost, serviceName)
	if err != nil {
		return join(shutdowns), err
	}

	return join(shutdowns), nil
}

func initOTelSDK(ctx context.Context, collectorHost string, serviceName string) ([]ShutdownFunc, error) {
	var exporterConn *grpc.ClientConn
	var shutdownExporterConn ShutdownFunc
	var err error
	exporterConn, shutdownExporterConn, err = newCollectorConn(collectorHost)
	if err != nil {
		return nil, err
	}
	var shutdowns []ShutdownFunc
	shutdowns = append(shutdowns, shutdownExporterConn)

	var res *resource.Resource
	res, err = newResource(serviceName)
	if err != nil {
		return shutdowns, err
	}

	var shutdownTracerProvider ShutdownFunc
	shutdownTracerProvider, err = registerTracerProvider(ctx, res, exporterConn)
	if err != nil {
		return shutdowns, err
	}
	shutdowns = append(shutdowns, shutdownTracerProvider)

	var shutdownMeterProvider ShutdownFunc
	shutdownMeterProvider, err = registerMeterProvider(ctx, res, exporterConn)
	if err != nil {
		return shutdowns, err
	}
	shutdowns = append(shutdowns, shutdownMeterProvider)

	var shutdownLoggerProvider ShutdownFunc
	shutdownLoggerProvider, err = registerLoggerProvider(ctx, res, exporterConn)
	if err != nil {
		return shutdowns, err
	}
	shutdowns = append(shutdowns, shutdownLoggerProvider)

	registerPropagator()

	return shutdowns, nil
}

func newCollectorConn(target string) (*grpc.ClientConn, ShutdownFunc, error) {
	conn, err := grpc.NewClient(target,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create gRPC connection to collector: %w", err)
	}

	shutdownConn := func(_ context.Context) error {
		return conn.Close()
	}

	return conn, shutdownConn, err
}

func newResource(serviceName string) (*resource.Resource, error) {
	res, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName(serviceName),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("creating resource: %w", err)
	}
	return res, nil
}

func registerTracerProvider(ctx context.Context, res *resource.Resource, conn *grpc.ClientConn) (ShutdownFunc, error) {
	provider, err := newTracerProvider(ctx, res, conn)
	if err != nil {
		return nil, err
	}

	otel.SetTracerProvider(provider)

	return provider.Shutdown, nil
}

func newTracerProvider(ctx context.Context, res *resource.Resource, conn *grpc.ClientConn) (*trace.TracerProvider, error) {
	exporter, err := otlptracegrpc.New(ctx, otlptracegrpc.WithGRPCConn(conn))
	if err != nil {
		return nil, fmt.Errorf("creating trace exporter: %w", err)
	}

	return trace.NewTracerProvider(
		trace.WithResource(res),
		trace.WithSampler(trace.AlwaysSample()),
		trace.WithSpanProcessor(trace.NewBatchSpanProcessor(exporter)),
	), nil
}

func registerMeterProvider(
	ctx context.Context,
	res *resource.Resource,
	conn *grpc.ClientConn,
) (ShutdownFunc, error) {
	provider, err := newMeterProvider(ctx, res, conn)
	if err != nil {
		return nil, err
	}

	otel.SetMeterProvider(provider)

	return provider.Shutdown, nil
}

func newMeterProvider(ctx context.Context, res *resource.Resource, conn *grpc.ClientConn) (*metric.MeterProvider, error) {
	exporter, err := otlpmetricgrpc.New(ctx, otlpmetricgrpc.WithGRPCConn(conn))
	if err != nil {
		return nil, fmt.Errorf("creating metrics exporter: %w", err)
	}

	return metric.NewMeterProvider(
		metric.WithReader(metric.NewPeriodicReader(
			exporter,
			metric.WithInterval(100*time.Millisecond),
		)),
		metric.WithResource(res),
	), nil
}

func registerLoggerProvider(ctx context.Context, res *resource.Resource, conn *grpc.ClientConn) (ShutdownFunc, error) {
	provider, err := newLoggerProvider(ctx, res, conn)
	if err != nil {
		return nil, err
	}

	global.SetLoggerProvider(provider)

	return provider.Shutdown, nil
}

func newLoggerProvider(ctx context.Context, res *resource.Resource, conn *grpc.ClientConn) (*log.LoggerProvider, error) {
	exporter, err := otlploggrpc.New(ctx, otlploggrpc.WithGRPCConn(conn))
	if err != nil {
		return nil, fmt.Errorf("creating logs exporter: %w", err)
	}

	provider := log.NewLoggerProvider(
		log.WithProcessor(log.NewBatchProcessor(exporter)),
		log.WithResource(res),
	)

	return provider, nil
}

func registerPropagator() {
	prop := newPropagator()
	otel.SetTextMapPropagator(prop)
}

func newPropagator() propagation.TextMapPropagator {
	return propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	)
}

type ShutdownFunc func(ctx context.Context) error

func join(shutdowns []ShutdownFunc) ShutdownFunc {
	return func(ctx context.Context) error {
		var shutdownErr error
		for _, fn := range shutdowns {
			shutdownErr = errors.Join(shutdownErr, fn(ctx))
		}
		return shutdownErr
	}
}
