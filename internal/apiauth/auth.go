package apiauth

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
)

type Role string

const (
	RoleReader   Role = "reader"
	RoleOperator Role = "operator"
	RoleAdmin    Role = "admin"
	RoleAgent    Role = "agent"
)

type Identity struct {
	Subject string `json:"subject"`
	Roles   []Role `json:"roles"`
	AgentID string `json:"agent_id,omitempty"`
}

func (i Identity) HasAny(roles ...Role) bool {
	for _, actual := range i.Roles {
		for _, required := range roles {
			if actual == required {
				return true
			}
		}
	}
	return false
}

type credential struct {
	identity Identity
	hash     [sha256.Size]byte
}

type reference struct {
	SecretEnv string `json:"secret_env"`
	Roles     []Role `json:"roles"`
	AgentID   string `json:"agent_id,omitempty"`
}

type Authenticator struct {
	credentials []credential
}

var ErrInvalidConfig = errors.New("invalid API authentication configuration")

// New parses a JSON object keyed by subject. Configuration contains only
// environment-variable references; the resolved token is retained only as a
// SHA-256 comparison digest.
func New(raw string, lookup func(string) (string, bool)) (*Authenticator, error) {
	authenticator := &Authenticator{}
	if strings.TrimSpace(raw) == "" {
		return authenticator, nil
	}
	if lookup == nil {
		return nil, fmt.Errorf("%w: environment lookup is required", ErrInvalidConfig)
	}
	var references map[string]reference
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&references); err != nil {
		return nil, fmt.Errorf("%w: malformed JSON", ErrInvalidConfig)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return nil, fmt.Errorf("%w: trailing JSON data", ErrInvalidConfig)
	}
	seenTokens := make(map[[sha256.Size]byte]struct{}, len(references))
	for subject, ref := range references {
		subject = strings.TrimSpace(subject)
		secretEnv := strings.TrimSpace(ref.SecretEnv)
		if subject == "" || secretEnv == "" || len(ref.Roles) == 0 {
			return nil, fmt.Errorf("%w: subject, secret_env and roles are required", ErrInvalidConfig)
		}
		identity := Identity{Subject: subject, AgentID: strings.TrimSpace(ref.AgentID)}
		seenRoles := make(map[Role]struct{}, len(ref.Roles))
		for _, role := range ref.Roles {
			role = Role(strings.ToLower(strings.TrimSpace(string(role))))
			switch role {
			case RoleReader, RoleOperator, RoleAdmin, RoleAgent:
			default:
				return nil, fmt.Errorf("%w: unsupported role for subject %s", ErrInvalidConfig, subject)
			}
			if _, duplicate := seenRoles[role]; !duplicate {
				seenRoles[role] = struct{}{}
				identity.Roles = append(identity.Roles, role)
			}
		}
		if identity.HasAny(RoleAgent) != (identity.AgentID != "") {
			return nil, fmt.Errorf("%w: agent role and agent_id must be declared together for subject %s", ErrInvalidConfig, subject)
		}
		token, exists := lookup(secretEnv)
		if !exists || strings.TrimSpace(token) == "" || strings.ContainsAny(token, "\r\n") {
			return nil, fmt.Errorf("%w: referenced token unavailable for subject %s", ErrInvalidConfig, subject)
		}
		digest := sha256.Sum256([]byte(token))
		if _, duplicate := seenTokens[digest]; duplicate {
			return nil, fmt.Errorf("%w: duplicate token", ErrInvalidConfig)
		}
		seenTokens[digest] = struct{}{}
		authenticator.credentials = append(authenticator.credentials, credential{identity: identity, hash: digest})
	}
	return authenticator, nil
}

func (a *Authenticator) Enabled() bool { return a != nil && len(a.credentials) > 0 }

func (a *Authenticator) Middleware(next http.Handler) http.Handler {
	if !a.Enabled() {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/healthz" || r.URL.Path == "/readyz" {
			next.ServeHTTP(w, r)
			return
		}
		identity, ok := a.authenticate(r.Header.Get("Authorization"))
		if !ok {
			w.Header().Set("WWW-Authenticate", `Bearer realm="agentmesh"`)
			writeError(w, http.StatusUnauthorized, "authentication required")
			return
		}
		if !authorized(identity, r) {
			writeError(w, http.StatusForbidden, "insufficient permissions")
			return
		}
		next.ServeHTTP(w, r.WithContext(WithIdentity(r.Context(), identity)))
	})
}

func (a *Authenticator) authenticate(header string) (Identity, bool) {
	const prefix = "Bearer "
	if !strings.HasPrefix(header, prefix) || len(header) == len(prefix) {
		return Identity{}, false
	}
	digest := sha256.Sum256([]byte(header[len(prefix):]))
	for _, candidate := range a.credentials {
		if subtle.ConstantTimeCompare(digest[:], candidate.hash[:]) == 1 {
			return candidate.identity, true
		}
	}
	return Identity{}, false
}

func authorized(identity Identity, r *http.Request) bool {
	if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/children") {
		return identity.HasAny(RoleAgent)
	}
	if r.URL.Path == "/api/v1/audit-events" {
		return identity.HasAny(RoleAdmin)
	}
	if r.Method == http.MethodGet || r.Method == http.MethodHead {
		return identity.HasAny(RoleReader, RoleOperator, RoleAdmin)
	}
	return identity.HasAny(RoleOperator, RoleAdmin)
}

type identityKey struct{}

func WithIdentity(ctx context.Context, identity Identity) context.Context {
	return context.WithValue(ctx, identityKey{}, identity)
}

func FromContext(ctx context.Context) (Identity, bool) {
	identity, ok := ctx.Value(identityKey{}).(Identity)
	return identity, ok
}

func writeError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]string{"message": message}})
}
