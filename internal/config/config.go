package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Addr                string
	Mode                string
	Workers             int
	QueueSize           int
	ExecutionDelay      time.Duration
	AttemptTimeout      time.Duration
	ShutdownTimeout     time.Duration
	DatabaseURL         string
	NATSURL             string
	RedisURL            string
	MaxAttempts         int
	RetryInitial        time.Duration
	RetryMax            time.Duration
	NATSAckWait         time.Duration
	CacheTTL            time.Duration
	LeaseTTL            time.Duration
	EventRetention      time.Duration
	EventHistoryLimit   int
	InstanceID          string
	AgentHealthPath     string
	AgentHealthInterval time.Duration
	AgentHealthTimeout  time.Duration
	AgentHealthWorkers  int
}

func Load() (Config, error) {
	mode := stringEnv("AGENTMESH_MODE", "memory")
	if mode != "memory" && mode != "distributed" {
		return Config{}, fmt.Errorf("AGENTMESH_MODE must be memory or distributed")
	}
	workers, err := intEnv("AGENTMESH_WORKERS", 4)
	if err != nil {
		return Config{}, err
	}
	if workers < 1 {
		return Config{}, fmt.Errorf("AGENTMESH_WORKERS must be >= 1")
	}

	queueSize, err := intEnv("AGENTMESH_QUEUE_SIZE", 128)
	if err != nil {
		return Config{}, err
	}
	if queueSize < 1 {
		return Config{}, fmt.Errorf("AGENTMESH_QUEUE_SIZE must be >= 1")
	}

	executionDelay, err := durationEnv("AGENTMESH_EXECUTION_DELAY", 750*time.Millisecond)
	if err != nil {
		return Config{}, err
	}
	attemptTimeout, err := durationEnv("AGENTMESH_ATTEMPT_TIMEOUT", 30*time.Second)
	if err != nil {
		return Config{}, err
	}
	if attemptTimeout <= 0 {
		return Config{}, fmt.Errorf("AGENTMESH_ATTEMPT_TIMEOUT must be > 0")
	}

	shutdownTimeout, err := durationEnv("AGENTMESH_SHUTDOWN_TIMEOUT", 10*time.Second)
	if err != nil {
		return Config{}, err
	}
	maxAttempts, err := intEnv("AGENTMESH_MAX_ATTEMPTS", 3)
	if err != nil {
		return Config{}, err
	}
	if maxAttempts < 1 {
		return Config{}, fmt.Errorf("AGENTMESH_MAX_ATTEMPTS must be >= 1")
	}
	retryInitial, err := durationEnv("AGENTMESH_RETRY_INITIAL_BACKOFF", 250*time.Millisecond)
	if err != nil {
		return Config{}, err
	}
	retryMax, err := durationEnv("AGENTMESH_RETRY_MAX_BACKOFF", 5*time.Second)
	if err != nil {
		return Config{}, err
	}
	if retryInitial <= 0 || retryMax < retryInitial {
		return Config{}, fmt.Errorf("retry backoff must be positive and max must be >= initial")
	}
	natsAckWait, err := durationEnv("AGENTMESH_NATS_ACK_WAIT", 2*time.Minute)
	if err != nil {
		return Config{}, err
	}
	if natsAckWait <= 0 {
		return Config{}, fmt.Errorf("AGENTMESH_NATS_ACK_WAIT must be > 0")
	}
	cacheTTL, err := durationEnv("AGENTMESH_CACHE_TTL", 30*time.Second)
	if err != nil {
		return Config{}, err
	}
	if cacheTTL <= 0 {
		return Config{}, fmt.Errorf("AGENTMESH_CACHE_TTL must be > 0")
	}
	leaseTTL, err := durationEnv("AGENTMESH_LEASE_TTL", 5*time.Minute)
	if err != nil {
		return Config{}, err
	}
	if leaseTTL <= 0 {
		return Config{}, fmt.Errorf("AGENTMESH_LEASE_TTL must be > 0")
	}
	eventRetention, err := durationEnv("AGENTMESH_EVENT_RETENTION", 7*24*time.Hour)
	if err != nil {
		return Config{}, err
	}
	if eventRetention <= 0 {
		return Config{}, fmt.Errorf("AGENTMESH_EVENT_RETENTION must be > 0")
	}
	eventHistoryLimit, err := intEnv("AGENTMESH_EVENT_HISTORY_LIMIT", 1000)
	if err != nil {
		return Config{}, err
	}
	if eventHistoryLimit < 1 {
		return Config{}, fmt.Errorf("AGENTMESH_EVENT_HISTORY_LIMIT must be >= 1")
	}
	instanceID := stringEnv("AGENTMESH_INSTANCE_ID", "")
	if len(instanceID) > 128 {
		return Config{}, fmt.Errorf("AGENTMESH_INSTANCE_ID must be at most 128 characters")
	}
	agentHealthPath := stringEnv("AGENTMESH_AGENT_HEALTH_PATH", "/healthz")
	if !strings.HasPrefix(agentHealthPath, "/") || strings.HasPrefix(agentHealthPath, "//") || strings.ContainsAny(agentHealthPath, "?#") {
		return Config{}, fmt.Errorf("AGENTMESH_AGENT_HEALTH_PATH must be a path starting with / without query or fragment")
	}
	agentHealthInterval, err := durationEnv("AGENTMESH_AGENT_HEALTH_INTERVAL", 30*time.Second)
	if err != nil {
		return Config{}, err
	}
	if agentHealthInterval <= 0 {
		return Config{}, fmt.Errorf("AGENTMESH_AGENT_HEALTH_INTERVAL must be > 0")
	}
	agentHealthTimeout, err := durationEnv("AGENTMESH_AGENT_HEALTH_TIMEOUT", 2*time.Second)
	if err != nil {
		return Config{}, err
	}
	if agentHealthTimeout <= 0 {
		return Config{}, fmt.Errorf("AGENTMESH_AGENT_HEALTH_TIMEOUT must be > 0")
	}
	agentHealthWorkers, err := intEnv("AGENTMESH_AGENT_HEALTH_WORKERS", 2)
	if err != nil {
		return Config{}, err
	}
	if agentHealthWorkers < 1 {
		return Config{}, fmt.Errorf("AGENTMESH_AGENT_HEALTH_WORKERS must be >= 1")
	}

	databaseURL := stringEnv("AGENTMESH_DATABASE_URL", "")
	natsURL := stringEnv("AGENTMESH_NATS_URL", "")
	redisURL := stringEnv("AGENTMESH_REDIS_URL", "")
	if mode == "distributed" && (databaseURL == "" || natsURL == "" || redisURL == "") {
		return Config{}, fmt.Errorf("distributed mode requires AGENTMESH_DATABASE_URL, AGENTMESH_NATS_URL and AGENTMESH_REDIS_URL")
	}

	return Config{
		Addr:                stringEnv("AGENTMESH_ADDR", ":8080"),
		Mode:                mode,
		Workers:             workers,
		QueueSize:           queueSize,
		ExecutionDelay:      executionDelay,
		AttemptTimeout:      attemptTimeout,
		ShutdownTimeout:     shutdownTimeout,
		DatabaseURL:         databaseURL,
		NATSURL:             natsURL,
		RedisURL:            redisURL,
		MaxAttempts:         maxAttempts,
		RetryInitial:        retryInitial,
		RetryMax:            retryMax,
		NATSAckWait:         natsAckWait,
		CacheTTL:            cacheTTL,
		LeaseTTL:            leaseTTL,
		EventRetention:      eventRetention,
		EventHistoryLimit:   eventHistoryLimit,
		InstanceID:          instanceID,
		AgentHealthPath:     agentHealthPath,
		AgentHealthInterval: agentHealthInterval,
		AgentHealthTimeout:  agentHealthTimeout,
		AgentHealthWorkers:  agentHealthWorkers,
	}, nil
}

func stringEnv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func intEnv(key string, fallback int) (int, error) {
	value := os.Getenv(key)
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer: %w", key, err)
	}
	return parsed, nil
}

func durationEnv(key string, fallback time.Duration) (time.Duration, error) {
	value := os.Getenv(key)
	if value == "" {
		return fallback, nil
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("%s must be a duration: %w", key, err)
	}
	return parsed, nil
}
