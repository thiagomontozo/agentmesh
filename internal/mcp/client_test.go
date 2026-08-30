package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thiagomontozo/agentmesh/internal/domain"
)

type agentAuthFunc func(context.Context, domain.Agent, *http.Request) error

func (f agentAuthFunc) Authenticate(ctx context.Context, agent domain.Agent, request *http.Request) error {
	return f(ctx, agent, request)
}

func TestClientListsAndCallsAllowedTools(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if r.Header.Get("MCP-Protocol-Version") != ProtocolVersion || r.Header.Get("Authorization") != "Bearer mcp" {
			t.Fatalf("missing protocol/auth headers: %+v", r.Header)
		}
		var request rpcRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatal(err)
		}
		params, ok := request.Params.(map[string]any)
		if !ok || params["_meta"] == nil {
			t.Fatalf("missing request metadata: %+v", request.Params)
		}
		w.Header().Set("Content-Type", "application/json")
		switch request.Method {
		case "tools/list":
			if r.Header.Get("Mcp-Method") != "tools/list" || r.Header.Get("Mcp-Name") != "" {
				t.Fatalf("unexpected list routing headers: %+v", r.Header)
			}
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":` + jsonID(request.ID) + `,"result":{"resultType":"complete","tools":[{"name":"query","inputSchema":{"type":"object"}},{"name":"delete","inputSchema":{"type":"object"}}]}}`))
		case "tools/call":
			if r.Header.Get("Mcp-Method") != "tools/call" || r.Header.Get("Mcp-Name") != "query" {
				t.Fatalf("unexpected call routing headers: %+v", r.Header)
			}
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":` + jsonID(request.ID) + `,"result":{"resultType":"complete","content":[{"type":"text","text":"found"}]}}`))
		default:
			t.Fatalf("unexpected method %s", request.Method)
		}
	}))
	defer server.Close()
	auth := agentAuthFunc(func(_ context.Context, agent domain.Agent, request *http.Request) error {
		if agent.ID != "mcp:search" {
			t.Fatalf("unexpected credential identity %s", agent.ID)
		}
		request.Header.Set("Authorization", "Bearer mcp")
		return nil
	})
	client := NewClient(server.Client(), 4096, 4096, auth)
	definition := Server{ID: "search", Endpoint: server.URL, AllowedTools: []string{"query"}, Timeout: time.Second}
	listed, err := client.ListTools(context.Background(), definition, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(listed.Tools) != 1 || listed.Tools[0].Name != "query" {
		t.Fatalf("policy did not filter discovery: %+v", listed)
	}
	result, err := client.CallTool(context.Background(), definition, "query", map[string]any{"q": "agentmesh"})
	if err != nil || len(result.Content) != 1 || result.Content[0]["text"] != "found" {
		t.Fatalf("unexpected call result: %+v err=%v", result, err)
	}
	_, err = client.CallTool(context.Background(), definition, "delete", nil)
	if !errors.Is(err, ErrToolDenied) || requests.Load() != 2 {
		t.Fatalf("denied tool reached network: requests=%d err=%v", requests.Load(), err)
	}
}

func TestClientEnforcesTimeoutAndResponseLimit(t *testing.T) {
	t.Run("timeout", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			time.Sleep(100 * time.Millisecond)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{"tools":[]}}`))
		}))
		defer server.Close()
		_, err := NewClient(server.Client(), 1024, 1024, nil).ListTools(context.Background(), Server{ID: "slow", Endpoint: server.URL, Timeout: 20 * time.Millisecond}, "")
		if !errors.Is(err, ErrTimeout) {
			t.Fatalf("expected ErrTimeout, got %v", err)
		}
	})
	t.Run("response limit", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(make([]byte, 128))
		}))
		defer server.Close()
		_, err := NewClient(server.Client(), 1024, 32, nil).ListTools(context.Background(), Server{ID: "large", Endpoint: server.URL, Timeout: time.Second}, "")
		if !errors.Is(err, ErrProtocol) {
			t.Fatalf("expected protocol limit error, got %v", err)
		}
	})
}

func jsonID(id uint64) string {
	payload, _ := json.Marshal(id)
	return string(payload)
}
