package observability

import "context"

type contextKey string

const (
	requestIDKey  contextKey = "request_id"
	instanceIDKey contextKey = "instance_id"
	workerIDKey   contextKey = "worker_id"
)

func WithRequestID(ctx context.Context, id string) context.Context {
	if id == "" {
		return ctx
	}
	return context.WithValue(ctx, requestIDKey, id)
}

func RequestID(ctx context.Context) string {
	value, _ := ctx.Value(requestIDKey).(string)
	return value
}

func WithInstanceID(ctx context.Context, id string) context.Context {
	if id == "" {
		return ctx
	}
	return context.WithValue(ctx, instanceIDKey, id)
}

func InstanceID(ctx context.Context) string {
	value, _ := ctx.Value(instanceIDKey).(string)
	return value
}

func WithWorkerID(ctx context.Context, id string) context.Context {
	if id == "" {
		return ctx
	}
	return context.WithValue(ctx, workerIDKey, id)
}

func WorkerID(ctx context.Context) string {
	value, _ := ctx.Value(workerIDKey).(string)
	return value
}

func ContextAttrs(ctx context.Context) []any {
	attributes := make([]any, 0, 6)
	if value := RequestID(ctx); value != "" {
		attributes = append(attributes, "request_id", value)
	}
	if value := InstanceID(ctx); value != "" {
		attributes = append(attributes, "instance_id", value)
	}
	if value := WorkerID(ctx); value != "" {
		attributes = append(attributes, "worker_id", value)
	}
	return attributes
}
