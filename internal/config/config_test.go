package config

import (
	"testing"
	"time"
)

func TestLoadDefaults(t *testing.T) {
	for _, key := range []string{
		"AGENTMESH_ADDR",
		"AGENTMESH_WORKERS",
		"AGENTMESH_QUEUE_SIZE",
		"AGENTMESH_EXECUTION_DELAY",
		"AGENTMESH_SHUTDOWN_TIMEOUT",
	} {
		t.Setenv(key, "")
	}

	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Addr != ":8080" || cfg.Workers != 4 || cfg.QueueSize != 128 {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}
	if cfg.ExecutionDelay != 750*time.Millisecond || cfg.ShutdownTimeout != 10*time.Second {
		t.Fatalf("unexpected duration defaults: %+v", cfg)
	}
}

func TestLoadRejectsInvalidValues(t *testing.T) {
	keys := []string{
		"AGENTMESH_ADDR",
		"AGENTMESH_WORKERS",
		"AGENTMESH_QUEUE_SIZE",
		"AGENTMESH_EXECUTION_DELAY",
		"AGENTMESH_SHUTDOWN_TIMEOUT",
	}
	tests := []struct {
		name  string
		key   string
		value string
	}{
		{name: "workers is not an integer", key: "AGENTMESH_WORKERS", value: "many"},
		{name: "workers is zero", key: "AGENTMESH_WORKERS", value: "0"},
		{name: "queue is zero", key: "AGENTMESH_QUEUE_SIZE", value: "0"},
		{name: "delay is invalid", key: "AGENTMESH_EXECUTION_DELAY", value: "later"},
		{name: "shutdown is invalid", key: "AGENTMESH_SHUTDOWN_TIMEOUT", value: "soon"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			for _, key := range keys {
				t.Setenv(key, "")
			}
			t.Setenv(test.key, test.value)
			if _, err := Load(); err == nil {
				t.Fatal("expected configuration error")
			}
		})
	}
}
