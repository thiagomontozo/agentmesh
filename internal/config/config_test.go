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
	if cfg.ExecutionDelay != 750*time.Millisecond || cfg.AttemptTimeout != 30*time.Second || cfg.ShutdownTimeout != 10*time.Second {
		t.Fatalf("unexpected duration defaults: %+v", cfg)
	}
	if cfg.MaxAttempts != 3 || cfg.RetryInitial != 250*time.Millisecond || cfg.RetryMax != 5*time.Second {
		t.Fatalf("unexpected retry defaults: %+v", cfg)
	}
	if cfg.EventRetention != 7*24*time.Hour || cfg.EventHistoryLimit != 1000 {
		t.Fatalf("unexpected event history defaults: %+v", cfg)
	}
	if cfg.AgentHealthPath != "/healthz" || cfg.AgentHealthInterval != 30*time.Second ||
		cfg.AgentHealthTimeout != 2*time.Second || cfg.AgentHealthWorkers != 2 || cfg.WorkflowConcurrency != 4 ||
		cfg.AgentCallMaxDepth != 8 || cfg.AgentCallMaxChildren != 16 {
		t.Fatalf("unexpected agent health defaults: %+v", cfg)
	}
	if cfg.HTTPRequireHTTPS || !cfg.HTTPAllowPrivate || !cfg.HTTPAllowLoopback || cfg.HTTPAllowLinkLocal ||
		cfg.HTTPMaxRequestBytes != 1<<20 || cfg.HTTPMaxResponseBytes != 1<<20 {
		t.Fatalf("unexpected HTTP security defaults: %+v", cfg)
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
		{name: "attempt timeout is invalid", key: "AGENTMESH_ATTEMPT_TIMEOUT", value: "later"},
		{name: "attempt timeout is zero", key: "AGENTMESH_ATTEMPT_TIMEOUT", value: "0s"},
		{name: "shutdown is invalid", key: "AGENTMESH_SHUTDOWN_TIMEOUT", value: "soon"},
		{name: "attempts is zero", key: "AGENTMESH_MAX_ATTEMPTS", value: "0"},
		{name: "initial backoff is zero", key: "AGENTMESH_RETRY_INITIAL_BACKOFF", value: "0s"},
		{name: "max backoff is below initial", key: "AGENTMESH_RETRY_MAX_BACKOFF", value: "1ms"},
		{name: "cache ttl is zero", key: "AGENTMESH_CACHE_TTL", value: "0s"},
		{name: "ack wait is zero", key: "AGENTMESH_NATS_ACK_WAIT", value: "0s"},
		{name: "lease ttl is zero", key: "AGENTMESH_LEASE_TTL", value: "0s"},
		{name: "event retention is zero", key: "AGENTMESH_EVENT_RETENTION", value: "0s"},
		{name: "event history limit is zero", key: "AGENTMESH_EVENT_HISTORY_LIMIT", value: "0"},
		{name: "instance ID is too long", key: "AGENTMESH_INSTANCE_ID", value: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
		{name: "agent health path is relative", key: "AGENTMESH_AGENT_HEALTH_PATH", value: "healthz"},
		{name: "agent health interval is zero", key: "AGENTMESH_AGENT_HEALTH_INTERVAL", value: "0s"},
		{name: "agent health timeout is zero", key: "AGENTMESH_AGENT_HEALTH_TIMEOUT", value: "0s"},
		{name: "agent health workers is zero", key: "AGENTMESH_AGENT_HEALTH_WORKERS", value: "0"},
		{name: "workflow concurrency is zero", key: "AGENTMESH_WORKFLOW_CONCURRENCY", value: "0"},
		{name: "Agent call depth is zero", key: "AGENTMESH_AGENT_CALL_MAX_DEPTH", value: "0"},
		{name: "Agent call fan-out is zero", key: "AGENTMESH_AGENT_CALL_MAX_CHILDREN", value: "0"},
		{name: "HTTP require HTTPS is invalid", key: "AGENTMESH_HTTP_REQUIRE_HTTPS", value: "sometimes"},
		{name: "HTTP request limit is zero", key: "AGENTMESH_HTTP_MAX_REQUEST_BYTES", value: "0"},
		{name: "HTTP response limit is invalid", key: "AGENTMESH_HTTP_MAX_RESPONSE_BYTES", value: "large"},
		{name: "HTTP blocked CIDR is invalid", key: "AGENTMESH_HTTP_BLOCKED_CIDRS", value: "private"},
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

func TestLoadConfiguresAttemptTimeout(t *testing.T) {
	clearEnvironment(t)
	t.Setenv("AGENTMESH_ATTEMPT_TIMEOUT", "45s")

	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.AttemptTimeout != 45*time.Second {
		t.Fatalf("expected 45s attempt timeout, got %s", cfg.AttemptTimeout)
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
		"AGENTMESH_ATTEMPT_TIMEOUT",
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
		"AGENTMESH_EVENT_RETENTION",
		"AGENTMESH_EVENT_HISTORY_LIMIT",
		"AGENTMESH_INSTANCE_ID",
		"AGENTMESH_AGENT_HEALTH_PATH",
		"AGENTMESH_AGENT_HEALTH_INTERVAL",
		"AGENTMESH_AGENT_HEALTH_TIMEOUT",
		"AGENTMESH_AGENT_HEALTH_WORKERS",
		"AGENTMESH_WORKFLOW_CONCURRENCY",
		"AGENTMESH_AGENT_CALL_MAX_DEPTH",
		"AGENTMESH_AGENT_CALL_MAX_CHILDREN",
		"AGENTMESH_HTTP_REQUIRE_HTTPS",
		"AGENTMESH_HTTP_ALLOW_PRIVATE_NETWORKS",
		"AGENTMESH_HTTP_ALLOW_LOOPBACK",
		"AGENTMESH_HTTP_ALLOW_LINK_LOCAL",
		"AGENTMESH_HTTP_ALLOWED_HOSTS",
		"AGENTMESH_HTTP_BLOCKED_CIDRS",
		"AGENTMESH_HTTP_MAX_REQUEST_BYTES",
		"AGENTMESH_HTTP_MAX_RESPONSE_BYTES",
		"AGENTMESH_AGENT_AUTH_CONFIG",
	} {
		t.Setenv(key, "")
	}
}
