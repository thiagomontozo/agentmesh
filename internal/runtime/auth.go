package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"unicode"

	"github.com/thiagomontozo/agentmesh/internal/domain"
)

type AuthScheme string

const (
	AuthBearer AuthScheme = "bearer"
	AuthAPIKey AuthScheme = "api_key"
)

var ErrAuthentication = errors.New("agent request authentication failed")

// RequestAuthenticator decorates one outbound Agent request. Implementations
// must never return an error containing credential material.
type RequestAuthenticator interface {
	Authenticate(context.Context, domain.Agent, *http.Request) error
}

type NoAuthentication struct{}

func (NoAuthentication) Authenticate(context.Context, domain.Agent, *http.Request) error { return nil }

type authReference struct {
	Type      AuthScheme `json:"type"`
	SecretEnv string     `json:"secret_env"`
	Header    string     `json:"header,omitempty"`
}

type credential struct {
	scheme AuthScheme
	header string
	secret string
}

type StaticAuthenticator struct {
	credentials map[string]credential
}

// NewEnvironmentAuthenticator parses a JSON object keyed by Agent ID. The JSON
// contains environment-variable references, never credential values.
func NewEnvironmentAuthenticator(raw string, lookup func(string) (string, bool)) (*StaticAuthenticator, error) {
	authenticator := &StaticAuthenticator{credentials: make(map[string]credential)}
	if strings.TrimSpace(raw) == "" {
		return authenticator, nil
	}
	if lookup == nil {
		return nil, fmt.Errorf("%w: environment lookup is required", ErrAuthentication)
	}
	var references map[string]authReference
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&references); err != nil {
		return nil, fmt.Errorf("%w: invalid credential reference configuration", ErrAuthentication)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return nil, fmt.Errorf("%w: invalid credential reference configuration", ErrAuthentication)
	}
	for agentID, reference := range references {
		agentID = strings.TrimSpace(agentID)
		if agentID == "" {
			return nil, fmt.Errorf("%w: blank Agent ID", ErrAuthentication)
		}
		secretEnv := strings.TrimSpace(reference.SecretEnv)
		if secretEnv == "" {
			return nil, fmt.Errorf("%w: secret_env is required for Agent %s", ErrAuthentication, agentID)
		}
		secret, exists := lookup(secretEnv)
		if !exists || secret == "" {
			return nil, fmt.Errorf("%w: referenced secret is unavailable for Agent %s", ErrAuthentication, agentID)
		}
		if strings.ContainsAny(secret, "\r\n") {
			return nil, fmt.Errorf("%w: referenced secret is invalid for Agent %s", ErrAuthentication, agentID)
		}
		resolved := credential{scheme: reference.Type, secret: secret}
		switch reference.Type {
		case AuthBearer:
			if reference.Header != "" {
				return nil, fmt.Errorf("%w: bearer authentication does not accept a custom header", ErrAuthentication)
			}
			resolved.header = "Authorization"
		case AuthAPIKey:
			resolved.header = strings.TrimSpace(reference.Header)
			if resolved.header == "" {
				resolved.header = "X-API-Key"
			}
			if !safeCredentialHeader(resolved.header) {
				return nil, fmt.Errorf("%w: invalid API-key header for Agent %s", ErrAuthentication, agentID)
			}
		default:
			return nil, fmt.Errorf("%w: unsupported authentication type for Agent %s", ErrAuthentication, agentID)
		}
		authenticator.credentials[agentID] = resolved
	}
	return authenticator, nil
}

func (a *StaticAuthenticator) Authenticate(_ context.Context, agent domain.Agent, request *http.Request) error {
	if a == nil {
		return nil
	}
	credential, configured := a.credentials[agent.ID]
	if !configured {
		return nil
	}
	if request == nil {
		return fmt.Errorf("%w: outbound request is unavailable", ErrAuthentication)
	}
	value := credential.secret
	if credential.scheme == AuthBearer {
		value = "Bearer " + value
	}
	request.Header.Set(credential.header, value)
	return nil
}

func safeCredentialHeader(header string) bool {
	if header == "" {
		return false
	}
	for _, character := range header {
		if character > unicode.MaxASCII || !(character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' || character >= '0' && character <= '9' || strings.ContainsRune("!#$%&'*+-.^_`|~", character)) {
			return false
		}
	}
	switch strings.ToLower(header) {
	case "authorization", "host", "content-length", "content-type", "accept", "accept-encoding", "idempotency-key", "connection", "transfer-encoding":
		return false
	default:
		return true
	}
}

var _ RequestAuthenticator = NoAuthentication{}
var _ RequestAuthenticator = (*StaticAuthenticator)(nil)
