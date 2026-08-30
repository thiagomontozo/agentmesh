package apiauth_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/thiagomontozo/agentmesh/internal/apiauth"
)

func TestBearerAuthenticationAndRBAC(t *testing.T) {
	secrets := map[string]string{"READER": "read-token", "OPERATOR": "operate-token", "ADMIN": "admin-token", "AGENT": "agent-token"}
	authenticator, err := apiauth.New(`{
		"reader":{"secret_env":"READER","roles":["reader"]},
		"operator":{"secret_env":"OPERATOR","roles":["operator"]},
		"admin":{"secret_env":"ADMIN","roles":["admin"]},
		"agent-a":{"secret_env":"AGENT","roles":["agent"],"agent_id":"agt_a"}
	}`, func(name string) (string, bool) { value, ok := secrets[name]; return value, ok })
	if err != nil {
		t.Fatal(err)
	}
	handler := authenticator.Middleware(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		identity, ok := apiauth.FromContext(request.Context())
		if !ok && request.URL.Path != "/healthz" {
			t.Fatal("authenticated identity missing from context")
		}
		if ok {
			response.Header().Set("X-Test-Subject", identity.Subject)
		}
		response.WriteHeader(http.StatusNoContent)
	}))

	assert := func(method, path, token string, want int) *httptest.ResponseRecorder {
		t.Helper()
		request := httptest.NewRequest(method, path, nil)
		if token != "" {
			request.Header.Set("Authorization", "Bearer "+token)
		}
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != want {
			t.Fatalf("%s %s returned %d, want %d: %s", method, path, response.Code, want, response.Body.String())
		}
		return response
	}

	assert(http.MethodGet, "/healthz", "", http.StatusNoContent)
	assert(http.MethodGet, "/api/v1/agents", "", http.StatusUnauthorized)
	assert(http.MethodGet, "/api/v1/agents", "read-token", http.StatusNoContent)
	assert(http.MethodPost, "/api/v1/agents", "read-token", http.StatusForbidden)
	assert(http.MethodPost, "/api/v1/agents", "operate-token", http.StatusNoContent)
	assert(http.MethodPost, "/api/v1/approvals/apr_1/approve", "operate-token", http.StatusForbidden)
	assert(http.MethodPost, "/api/v1/approvals/apr_1/approve", "admin-token", http.StatusNoContent)
	response := assert(http.MethodPost, "/api/v1/runs/run_1/children", "agent-token", http.StatusNoContent)
	if response.Header().Get("X-Test-Subject") != "agent-a" {
		t.Fatalf("unexpected authenticated subject: %q", response.Header().Get("X-Test-Subject"))
	}
}

func TestAuthenticationConfigRejectsUnsafeIdentities(t *testing.T) {
	lookup := func(string) (string, bool) { return "same-token", true }
	for _, raw := range []string{
		`{"reader":{"secret_env":"TOKEN","roles":[]}}`,
		`{"agent":{"secret_env":"TOKEN","roles":["agent"]}}`,
		`{"user":{"secret_env":"TOKEN","roles":["reader"],"agent_id":"agt_1"}}`,
		`{"user":{"secret_env":"TOKEN","roles":["root"]}}`,
		`{"a":{"secret_env":"A","roles":["reader"]},"b":{"secret_env":"B","roles":["reader"]}}`,
	} {
		if _, err := apiauth.New(raw, lookup); err == nil {
			t.Fatalf("expected invalid configuration: %s", raw)
		}
	}
}
