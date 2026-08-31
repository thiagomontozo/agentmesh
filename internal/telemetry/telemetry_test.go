package telemetry

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestHTTPAndRunSpansShareRequestContext(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	previous := otel.GetTracerProvider()
	otel.SetTracerProvider(provider)
	tracer = otel.Tracer(instrumentationName)
	t.Cleanup(func() {
		_ = provider.Shutdown(context.Background())
		otel.SetTracerProvider(previous)
		tracer = otel.Tracer(instrumentationName)
	})

	handler := HTTPHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, span := StartRun(r.Context(), "run_1", "agt_1", "req_1", "instance_1")
		_, attempt, started := StartAttempt(ctx, "run_1", "agt_1", 1)
		FinishAttempt(attempt, nil, started)
		span.End()
		w.WriteHeader(http.StatusNoContent)
	}))
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/api/v1/runs", nil))

	spans := recorder.Ended()
	if len(spans) != 3 {
		t.Fatalf("expected HTTP, Run and attempt spans, got %d", len(spans))
	}
	traceID := spans[0].SpanContext().TraceID()
	for _, span := range spans[1:] {
		if span.SpanContext().TraceID() != traceID {
			t.Fatalf("spans were not correlated: %s != %s", span.SpanContext().TraceID(), traceID)
		}
	}
}

func TestSetupIsNoopWithoutEndpoint(t *testing.T) {
	t.Setenv("OTEL_SDK_DISABLED", "")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "")
	t.Setenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", "")
	shutdown, err := Setup(context.Background(), "agentmesh", "test")
	if err != nil {
		t.Fatal(err)
	}
	if err := shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}
