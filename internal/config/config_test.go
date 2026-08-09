package config

import (
	"testing"
	"time"
)

func TestLoadDefaults(t *testing.T) {
	clearEnvironment(t)

	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Addr != ":8080" || cfg.Mode != "memory" || cfg.Workers != 4 || cfg.QueueSize != 128 {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}
	if cfg.ExecutionDelay != 750*time.Millisecond || cfg.ShutdownTimeout != 10*time.Second {
		t.Fatalf("unexpected duration defaults: %+v", cfg)
	}
	if cfg.MaxAttempts != 3 || cfg.RetryInitial != 250*time.Millisecond || cfg.RetryMax != 5*time.Second {
		t.Fatalf("unexpected retry defaults: %+v", cfg)
	}
}

func TestLoadRejectsInvalidValues(t *testing.T) {
	tests := []struct {
		name  string
		key   string
		value string
	}{
		{name: "mode is unknown", key: "AGENTMESH_MODE", value: "cloud"},
		{name: "workers is not an integer", key: "AGENTMESH_WORKERS", value: "many"},
		{name: "workers is zero", key: "AGENTMESH_WORKERS", value: "0"},
		{name: "queue is zero", key: "AGENTMESH_QUEUE_SIZE", value: "0"},
		{name: "delay is invalid", key: "AGENTMESH_EXECUTION_DELAY", value: "later"},
		{name: "shutdown is invalid", key: "AGENTMESH_SHUTDOWN_TIMEOUT", value: "soon"},
		{name: "attempts is zero", key: "AGENTMESH_MAX_ATTEMPTS", value: "0"},
		{name: "initial backoff is zero", key: "AGENTMESH_RETRY_INITIAL_BACKOFF", value: "0s"},
		{name: "max backoff is below initial", key: "AGENTMESH_RETRY_MAX_BACKOFF", value: "1ms"},
		{name: "cache ttl is zero", key: "AGENTMESH_CACHE_TTL", value: "0s"},
		{name: "ack wait is zero", key: "AGENTMESH_NATS_ACK_WAIT", value: "0s"},
		{name: "lease ttl is zero", key: "AGENTMESH_LEASE_TTL", value: "0s"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			clearEnvironment(t)
			t.Setenv(test.key, test.value)
			if _, err := Load(); err == nil {
				t.Fatal("expected configuration error")
			}
		})
	}
}

func TestDistributedModeRequiresURLs(t *testing.T) {
	clearEnvironment(t)
	t.Setenv("AGENTMESH_MODE", "distributed")
	if _, err := Load(); err == nil {
		t.Fatal("expected missing distributed URLs to fail")
	}

	t.Setenv("AGENTMESH_DATABASE_URL", "postgres://localhost/agentmesh")
	t.Setenv("AGENTMESH_NATS_URL", "nats://localhost:4222")
	t.Setenv("AGENTMESH_REDIS_URL", "redis://localhost:6379")
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Mode != "distributed" {
		t.Fatalf("expected distributed mode, got %q", cfg.Mode)
	}
}

func clearEnvironment(t *testing.T) {
	t.Helper()
	for _, key := range []string{
		"AGENTMESH_ADDR",
		"AGENTMESH_MODE",
		"AGENTMESH_WORKERS",
		"AGENTMESH_QUEUE_SIZE",
		"AGENTMESH_EXECUTION_DELAY",
		"AGENTMESH_SHUTDOWN_TIMEOUT",
		"AGENTMESH_DATABASE_URL",
		"AGENTMESH_NATS_URL",
		"AGENTMESH_REDIS_URL",
		"AGENTMESH_MAX_ATTEMPTS",
		"AGENTMESH_RETRY_INITIAL_BACKOFF",
		"AGENTMESH_RETRY_MAX_BACKOFF",
		"AGENTMESH_NATS_ACK_WAIT",
		"AGENTMESH_CACHE_TTL",
		"AGENTMESH_LEASE_TTL",
	} {
		t.Setenv(key, "")
	}
}
