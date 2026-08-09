package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	Addr            string
	Workers         int
	QueueSize       int
	ExecutionDelay  time.Duration
	ShutdownTimeout time.Duration
}

func Load() (Config, error) {
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

	shutdownTimeout, err := durationEnv("AGENTMESH_SHUTDOWN_TIMEOUT", 10*time.Second)
	if err != nil {
		return Config{}, err
	}

	return Config{
		Addr:            stringEnv("AGENTMESH_ADDR", ":8080"),
		Workers:         workers,
		QueueSize:       queueSize,
		ExecutionDelay:  executionDelay,
		ShutdownTimeout: shutdownTimeout,
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
