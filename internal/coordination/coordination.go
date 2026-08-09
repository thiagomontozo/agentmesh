package coordination

import (
	"context"
	"time"
)

type Lease interface {
	Release(ctx context.Context) error
}

type Coordinator interface {
	Acquire(ctx context.Context, key string, ttl time.Duration) (lease Lease, acquired bool, err error)
	Ping(ctx context.Context) error
	Close() error
}
