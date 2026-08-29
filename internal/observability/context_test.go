package observability

import (
	"context"
	"testing"
)

func TestExecutionIdentifiersRoundTripThroughContext(t *testing.T) {
	ctx := WithRequestID(context.Background(), "req_1")
	ctx = WithInstanceID(ctx, "instance_1")
	ctx = WithWorkerID(ctx, "worker_1")
	if RequestID(ctx) != "req_1" || InstanceID(ctx) != "instance_1" || WorkerID(ctx) != "worker_1" {
		t.Fatalf("identifiers were not preserved: %v", ContextAttrs(ctx))
	}
	attributes := ContextAttrs(ctx)
	if len(attributes) != 6 {
		t.Fatalf("expected three structured attributes, got %v", attributes)
	}
}
