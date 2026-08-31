package telemetry

import (
	"context"
	"errors"
	"net/http"
	"os"
	"strings"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.37.0"
	"go.opentelemetry.io/otel/trace"
)

const instrumentationName = "github.com/thiagomontozo/agentmesh"

var (
	tracer       = otel.Tracer(instrumentationName)
	meter        = otel.Meter(instrumentationName)
	runAttempts  metric.Int64Counter
	runDurations metric.Float64Histogram
)

func init() { initializeInstruments() }

// Setup installs OTLP/HTTP providers only when a standard OTLP endpoint is
// configured. An empty environment keeps AgentMesh dependency-free at runtime
// and all instrumentation becomes the OpenTelemetry no-op implementation.
func Setup(ctx context.Context, serviceName, instanceID string) (func(context.Context) error, error) {
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	if strings.EqualFold(strings.TrimSpace(os.Getenv("OTEL_SDK_DISABLED")), "true") || !hasEndpoint() {
		initializeInstruments()
		return func(context.Context) error { return nil }, nil
	}
	if configured := strings.TrimSpace(os.Getenv("OTEL_SERVICE_NAME")); configured != "" {
		serviceName = configured
	}
	res, err := resource.New(ctx, resource.WithFromEnv(), resource.WithTelemetrySDK(), resource.WithAttributes(
		semconv.ServiceName(strings.TrimSpace(serviceName)),
		attribute.String("service.instance.id", strings.TrimSpace(instanceID)),
	))
	if err != nil {
		return nil, err
	}
	commonEndpoint := strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")) != ""
	shutdowns := make([]func(context.Context) error, 0, 2)
	if commonEndpoint || strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")) != "" {
		traceExporter, err := otlptracehttp.New(ctx)
		if err != nil {
			return nil, err
		}
		traceProvider := sdktrace.NewTracerProvider(sdktrace.WithBatcher(traceExporter), sdktrace.WithResource(res))
		otel.SetTracerProvider(traceProvider)
		shutdowns = append(shutdowns, traceProvider.Shutdown)
	}
	if commonEndpoint || strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT")) != "" {
		metricExporter, err := otlpmetrichttp.New(ctx)
		if err != nil {
			for _, shutdown := range shutdowns {
				_ = shutdown(ctx)
			}
			return nil, err
		}
		meterProvider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExporter)), sdkmetric.WithResource(res))
		otel.SetMeterProvider(meterProvider)
		shutdowns = append(shutdowns, meterProvider.Shutdown)
	}
	tracer = otel.Tracer(instrumentationName)
	meter = otel.Meter(instrumentationName)
	initializeInstruments()
	return func(shutdownCtx context.Context) error {
		joined := make([]error, 0, len(shutdowns))
		for _, shutdown := range shutdowns {
			joined = append(joined, shutdown(shutdownCtx))
		}
		return errors.Join(joined...)
	}, nil
}

func hasEndpoint() bool {
	for _, key := range []string{"OTEL_EXPORTER_OTLP_ENDPOINT", "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT"} {
		if strings.TrimSpace(os.Getenv(key)) != "" {
			return true
		}
	}
	return false
}

func initializeInstruments() {
	runAttempts, _ = meter.Int64Counter("agentmesh.run.attempts", metric.WithDescription("Run execution attempts"))
	runDurations, _ = meter.Float64Histogram("agentmesh.run.duration", metric.WithUnit("s"), metric.WithDescription("Terminal Run duration"))
}

func HTTPHandler(next http.Handler) http.Handler {
	return otelhttp.NewHandler(next, "agentmesh.http", otelhttp.WithSpanNameFormatter(func(_ string, request *http.Request) string {
		return "HTTP " + request.Method
	}))
}

func StartRun(ctx context.Context, runID, agentID, requestID, instanceID string) (context.Context, trace.Span) {
	return tracer.Start(ctx, "agentmesh.run.execute", trace.WithSpanKind(trace.SpanKindConsumer), trace.WithAttributes(
		attribute.String("run.id", runID), attribute.String("agent.id", agentID),
		attribute.String("request.id", requestID), attribute.String("service.instance.id", instanceID),
	))
}

func StartAttempt(ctx context.Context, runID, agentID string, attempt int) (context.Context, trace.Span, time.Time) {
	ctx, span := tracer.Start(ctx, "agentmesh.run.attempt", trace.WithAttributes(
		attribute.String("run.id", runID), attribute.String("agent.id", agentID), attribute.Int("run.attempt", attempt),
	))
	runAttempts.Add(ctx, 1)
	return ctx, span, time.Now()
}

func FinishAttempt(span trace.Span, err error, started time.Time) {
	span.SetAttributes(attribute.Float64("run.attempt.duration_seconds", time.Since(started).Seconds()))
	if err != nil {
		span.RecordError(err)
	}
	span.End()
}

func RecordRunDuration(ctx context.Context, status string, durationMS int64) {
	runDurations.Record(ctx, float64(durationMS)/1000, metric.WithAttributes(attribute.String("run.status", status)))
}
