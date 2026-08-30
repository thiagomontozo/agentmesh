package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
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
	Type       AuthScheme `json:"type"`
	SecretEnv  string     `json:"secret_env"`
	SecretFile string     `json:"secret_file,omitempty"`
	Header     string     `json:"header,omitempty"`
}

type credential struct {
	scheme    AuthScheme
	header    string
	reference string
}

type StaticAuthenticator struct {
	credentials map[string]credential
	provider    SecretProvider
}

type SecretProvider interface {
	Resolve(context.Context, string) (string, error)
}

type SecretProviderFunc func(context.Context, string) (string, error)

func (f SecretProviderFunc) Resolve(ctx context.Context, reference string) (string, error) {
	return f(ctx, reference)
}

const maxSecretFileBytes = 64 << 10

// NewEnvironmentFileSecretProvider resolves env: and file: references on every
// request. Re-reading the source makes mounted-file and in-process environment
// rotation visible without rebuilding the HTTP Runtime.
func NewEnvironmentFileSecretProvider(lookup func(string) (string, bool)) SecretProvider {
	return SecretProviderFunc(func(ctx context.Context, reference string) (string, error) {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		switch {
		case strings.HasPrefix(reference, "env:"):
			if lookup == nil {
				return "", ErrAuthentication
			}
			secret, exists := lookup(strings.TrimPrefix(reference, "env:"))
			if !exists {
				return "", ErrAuthentication
			}
			return validSecret(secret)
		case strings.HasPrefix(reference, "file:"):
			path := strings.TrimPrefix(reference, "file:")
			if !filepath.IsAbs(path) {
				return "", ErrAuthentication
			}
			file, err := os.Open(path)
			if err != nil {
				return "", ErrAuthentication
			}
			defer file.Close()
			payload, err := io.ReadAll(io.LimitReader(file, maxSecretFileBytes+1))
			if err != nil || len(payload) > maxSecretFileBytes {
				return "", ErrAuthentication
			}
			return validSecret(strings.TrimRight(string(payload), "\r\n"))
		default:
			return "", ErrAuthentication
		}
	})
}

// NewEnvironmentAuthenticator parses a JSON object keyed by Agent ID. The JSON
// contains environment-variable references, never credential values.
func NewEnvironmentAuthenticator(raw string, lookup func(string) (string, bool)) (*StaticAuthenticator, error) {
	return NewReloadingAuthenticator(raw, NewEnvironmentFileSecretProvider(lookup))
}

// NewReloadingAuthenticator parses non-secret Agent credential references and
// asks the provider for the current value on every outbound request.
func NewReloadingAuthenticator(raw string, provider SecretProvider) (*StaticAuthenticator, error) {
	authenticator := &StaticAuthenticator{credentials: make(map[string]credential), provider: provider}
	if strings.TrimSpace(raw) == "" {
		return authenticator, nil
	}
	if provider == nil {
		return nil, fmt.Errorf("%w: secret provider is required", ErrAuthentication)
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
		secretFile := strings.TrimSpace(reference.SecretFile)
		if (secretEnv == "") == (secretFile == "") {
			return nil, fmt.Errorf("%w: exactly one secret_env or secret_file is required for Agent %s", ErrAuthentication, agentID)
		}
		referenceKey := "env:" + secretEnv
		if secretFile != "" {
			if !filepath.IsAbs(secretFile) {
				return nil, fmt.Errorf("%w: secret_file must be absolute for Agent %s", ErrAuthentication, agentID)
			}
			referenceKey = "file:" + filepath.Clean(secretFile)
		}
		initialSecret, err := provider.Resolve(context.Background(), referenceKey)
		if err != nil {
			return nil, fmt.Errorf("%w: referenced secret is unavailable for Agent %s", ErrAuthentication, agentID)
		}
		if _, err := validSecret(initialSecret); err != nil {
			return nil, fmt.Errorf("%w: referenced secret is invalid for Agent %s", ErrAuthentication, agentID)
		}
		resolved := credential{scheme: reference.Type, reference: referenceKey}
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

func (a *StaticAuthenticator) Authenticate(ctx context.Context, agent domain.Agent, request *http.Request) error {
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
	value, err := a.provider.Resolve(ctx, credential.reference)
	if err != nil {
		return fmt.Errorf("%w: current credential unavailable for Agent %s", ErrAuthentication, agent.ID)
	}
	value, err = validSecret(value)
	if err != nil {
		return fmt.Errorf("%w: current credential invalid for Agent %s", ErrAuthentication, agent.ID)
	}
	if credential.scheme == AuthBearer {
		value = "Bearer " + value
	}
	request.Header.Set(credential.header, value)
	return nil
}

func validSecret(secret string) (string, error) {
	if secret == "" || strings.ContainsAny(secret, "\r\n") {
		return "", ErrAuthentication
	}
	return secret, nil
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
