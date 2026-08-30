package runtime_test

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/thiagomontozo/agentmesh/internal/domain"
	agentruntime "github.com/thiagomontozo/agentmesh/internal/runtime"
)

func TestEnvironmentAuthenticatorAppliesBearerAndAPIKey(t *testing.T) {
	secrets := map[string]string{"LEGAL_TOKEN": "legal-secret", "CODE_KEY": "code-secret"}
	authenticator, err := agentruntime.NewEnvironmentAuthenticator(`{
		"agt_legal":{"type":"bearer","secret_env":"LEGAL_TOKEN"},
		"agt_code":{"type":"api_key","secret_env":"CODE_KEY","header":"X-Agent-Key"}
	}`, func(key string) (string, bool) { value, ok := secrets[key]; return value, ok })
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		agentID string
		header  string
		want    string
	}{
		{agentID: "agt_legal", header: "Authorization", want: "Bearer legal-secret"},
		{agentID: "agt_code", header: "X-Agent-Key", want: "code-secret"},
	}
	for _, test := range tests {
		request, _ := http.NewRequest(http.MethodPost, "http://agent/v1/runs", nil)
		if err := authenticator.Authenticate(context.Background(), domain.Agent{ID: test.agentID}, request); err != nil {
			t.Fatal(err)
		}
		if got := request.Header.Get(test.header); got != test.want {
			t.Fatalf("Agent %s header=%q, want %q", test.agentID, got, test.want)
		}
	}
}

func TestEnvironmentAuthenticatorLeavesUnconfiguredAgentAnonymous(t *testing.T) {
	authenticator, err := agentruntime.NewEnvironmentAuthenticator("", nil)
	if err != nil {
		t.Fatal(err)
	}
	request, _ := http.NewRequest(http.MethodPost, "http://agent/v1/runs", nil)
	if err := authenticator.Authenticate(context.Background(), domain.Agent{ID: "agt_public"}, request); err != nil {
		t.Fatal(err)
	}
	if len(request.Header) != 0 {
		t.Fatalf("unexpected authentication headers: %v", request.Header)
	}
}

func TestEnvironmentAuthenticatorRejectsInvalidConfigurationWithoutLeakingSecret(t *testing.T) {
	tests := []struct {
		name string
		raw  string
	}{
		{name: "unknown type", raw: `{"agt_1":{"type":"basic","secret_env":"TOKEN"}}`},
		{name: "missing reference", raw: `{"agt_1":{"type":"bearer"}}`},
		{name: "custom bearer header", raw: `{"agt_1":{"type":"bearer","secret_env":"TOKEN","header":"X-Key"}}`},
		{name: "reserved API key header", raw: `{"agt_1":{"type":"api_key","secret_env":"TOKEN","header":"Content-Type"}}`},
		{name: "header injection", raw: `{"agt_1":{"type":"api_key","secret_env":"TOKEN","header":"X-Key\\r\\nInjected"}}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := agentruntime.NewEnvironmentAuthenticator(test.raw, func(string) (string, bool) { return "do-not-leak", true })
			if !errors.Is(err, agentruntime.ErrAuthentication) {
				t.Fatalf("expected authentication error, got %v", err)
			}
			if strings.Contains(err.Error(), "do-not-leak") {
				t.Fatalf("error leaked secret: %v", err)
			}
		})
	}
}

func TestEnvironmentAuthenticatorRejectsMissingOrUnsafeSecret(t *testing.T) {
	raw := `{"agt_1":{"type":"bearer","secret_env":"TOKEN"}}`
	for _, secret := range []string{"", "secret\r\ninjected"} {
		_, err := agentruntime.NewEnvironmentAuthenticator(raw, func(string) (string, bool) { return secret, secret != "" })
		if !errors.Is(err, agentruntime.ErrAuthentication) || strings.Contains(err.Error(), secret) && secret != "" {
			t.Fatalf("unexpected safe error for secret %q: %v", secret, err)
		}
	}
}
