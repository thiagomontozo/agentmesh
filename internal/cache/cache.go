package cache

import (
	"context"
	"time"
)

type Cache interface {
	Get(ctx context.Context, key string, destination any) (bool, error)
	Set(ctx context.Context, key string, value any, ttl time.Duration) error
	Delete(ctx context.Context, keys ...string) error
	Ping(ctx context.Context) error
	Close() error
}
