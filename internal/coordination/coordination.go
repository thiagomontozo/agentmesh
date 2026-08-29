package coordination

import (
	"context"
	"errors"
	"time"
)

var ErrLeaseLost = errors.New("lease ownership lost")
var ErrInvalidLeaseTTL = errors.New("lease TTL must be positive")

type Lease interface {
	FencingToken() int64
	Renew(ctx context.Context, ttl time.Duration) error
	Release(ctx context.Context) error
}

type Coordinator interface {
	Acquire(ctx context.Context, key string, ttl time.Duration) (lease Lease, acquired bool, err error)
	Ping(ctx context.Context) error
	Close() error
}
