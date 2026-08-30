package mcp

import (
	"errors"
	"testing"
	"time"
)

func TestParseRegistryNormalizesPoliciesAndOrdersServers(t *testing.T) {
	registry, err := ParseRegistry(`[
		{"id":"search","endpoint":"https://search.example/mcp","allowed_tools":["query","query"],"denied_tools":["delete"],"timeout":"2s"},
		{"id":"code","endpoint":"http://code.internal/mcp"}
	]`, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	servers := registry.List()
	if len(servers) != 2 || servers[0].ID != "code" || servers[0].TimeoutMS != 5000 || servers[1].TimeoutMS != 2000 {
		t.Fatalf("unexpected servers: %+v", servers)
	}
	search, err := registry.Get("search")
	if err != nil {
		t.Fatal(err)
	}
	if !search.Allows("query") || search.Allows("other") || search.Allows("delete") || len(search.AllowedTools) != 1 {
		t.Fatalf("unexpected policy: %+v", search)
	}
}

func TestRegistryRejectsInvalidConfiguration(t *testing.T) {
	tests := []string{
		`[{"id":"bad id","endpoint":"https://example.com/mcp"}]`,
		`[{"id":"one","endpoint":"file:///tmp/mcp"}]`,
		`[{"id":"one","endpoint":"https://user:secret@example.com/mcp"}]`,
		`[{"id":"one","endpoint":"https://example.com/mcp","timeout":"0s"}]`,
		`[{"id":"one","endpoint":"https://example.com/mcp","allowed_tools":["bad name"]}]`,
		`[{"id":"one","endpoint":"https://example.com/mcp"},{"id":"one","endpoint":"https://other.example/mcp"}]`,
		`[{"id":"one","endpoint":"https://example.com/mcp"}] trailing`,
	}
	for _, raw := range tests {
		if _, err := ParseRegistry(raw, time.Second); err == nil {
			t.Fatalf("expected invalid configuration for %s", raw)
		}
	}
}

func TestRegistryReportsMissingServer(t *testing.T) {
	_, err := NewRegistry().Get("missing")
	if !errors.Is(err, ErrServerNotFound) {
		t.Fatalf("expected ErrServerNotFound, got %v", err)
	}
}
