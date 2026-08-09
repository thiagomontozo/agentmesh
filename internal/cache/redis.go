package cache

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/thiagomontozo/agentmesh/internal/coordination"
)

type Redis struct {
	client *redis.Client
}

func NewRedis(redisURL string) (*Redis, error) {
	options, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, fmt.Errorf("parse Redis URL: %w", err)
	}
	return &Redis{client: redis.NewClient(options)}, nil
}

func (r *Redis) Get(ctx context.Context, key string, destination any) (bool, error) {
	value, err := r.client.Get(ctx, key).Bytes()
	if err == redis.Nil {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("get Redis key: %w", err)
	}
	if err := json.Unmarshal(value, destination); err != nil {
		return false, fmt.Errorf("decode Redis value: %w", err)
	}
	return true, nil
}

func (r *Redis) Set(ctx context.Context, key string, value any, ttl time.Duration) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("encode Redis value: %w", err)
	}
	if err := r.client.Set(ctx, key, payload, ttl).Err(); err != nil {
		return fmt.Errorf("set Redis key: %w", err)
	}
	return nil
}

func (r *Redis) Delete(ctx context.Context, keys ...string) error {
	if len(keys) == 0 {
		return nil
	}
	return r.client.Del(ctx, keys...).Err()
}

func (r *Redis) Ping(ctx context.Context) error {
	if err := r.client.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("ping Redis: %w", err)
	}
	return nil
}

func (r *Redis) Close() error { return r.client.Close() }

func (r *Redis) Acquire(ctx context.Context, key string, ttl time.Duration) (coordination.Lease, bool, error) {
	random := make([]byte, 16)
	if _, err := rand.Read(random); err != nil {
		return nil, false, fmt.Errorf("generate lease token: %w", err)
	}
	token := hex.EncodeToString(random)
	redisKey := "agentmesh:lease:" + key
	acquired, err := r.client.SetNX(ctx, redisKey, token, ttl).Result()
	if err != nil {
		return nil, false, fmt.Errorf("acquire Redis lease: %w", err)
	}
	if !acquired {
		return nil, false, nil
	}
	return &redisLease{client: r.client, key: redisKey, token: token}, true, nil
}

type redisLease struct {
	client *redis.Client
	key    string
	token  string
}

var releaseLease = redis.NewScript(`
	if redis.call("GET", KEYS[1]) == ARGV[1] then
		return redis.call("DEL", KEYS[1])
	end
	return 0
`)

func (l *redisLease) Release(ctx context.Context) error {
	if err := releaseLease.Run(ctx, l.client, []string{l.key}, l.token).Err(); err != nil {
		return fmt.Errorf("release Redis lease: %w", err)
	}
	return nil
}

var _ Cache = (*Redis)(nil)
var _ coordination.Coordinator = (*Redis)(nil)
