package queue

import "context"

type Handler func(ctx context.Context, runID string) error

type Queue interface {
	Enqueue(ctx context.Context, runID string) error
	Consume(ctx context.Context, workers int, handler Handler) error
	DeadLetter(ctx context.Context, runID string, cause error) error
	Ping(ctx context.Context) error
	Close() error
}
