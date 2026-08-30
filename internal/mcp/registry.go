package mcp

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"
)

const ProtocolVersion = "2026-07-28"

var (
	ErrServerNotFound = errors.New("MCP server not found")
	ErrToolDenied     = errors.New("MCP tool denied by policy")
)

type Server struct {
	ID                    string        `json:"id"`
	Endpoint              string        `json:"endpoint"`
	AllowedTools          []string      `json:"allowed_tools,omitempty"`
	DeniedTools           []string      `json:"denied_tools,omitempty"`
	ApprovalRequiredTools []string      `json:"approval_required_tools,omitempty"`
	Timeout               time.Duration `json:"-"`
}

type ServerView struct {
	ID                    string   `json:"id"`
	Endpoint              string   `json:"endpoint"`
	AllowedTools          []string `json:"allowed_tools,omitempty"`
	DeniedTools           []string `json:"denied_tools,omitempty"`
	ApprovalRequiredTools []string `json:"approval_required_tools,omitempty"`
	TimeoutMS             int64    `json:"timeout_ms"`
}

func (s Server) View() ServerView {
	return ServerView{ID: s.ID, Endpoint: s.Endpoint, AllowedTools: append([]string(nil), s.AllowedTools...), DeniedTools: append([]string(nil), s.DeniedTools...), ApprovalRequiredTools: append([]string(nil), s.ApprovalRequiredTools...), TimeoutMS: s.Timeout.Milliseconds()}
}

func (s Server) RequiresApproval(tool string) bool {
	for _, required := range s.ApprovalRequiredTools {
		if required == tool {
			return true
		}
	}
	return false
}

func (s Server) Allows(tool string) bool {
	for _, denied := range s.DeniedTools {
		if denied == tool {
			return false
		}
	}
	if len(s.AllowedTools) == 0 {
		return true
	}
	for _, allowed := range s.AllowedTools {
		if allowed == tool {
			return true
		}
	}
	return false
}

type Registry struct {
	mu      sync.RWMutex
	servers map[string]Server
}

func NewRegistry() *Registry { return &Registry{servers: make(map[string]Server)} }

func (r *Registry) Register(server Server) error {
	if err := validateServer(&server); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.servers[server.ID]; exists {
		return fmt.Errorf("duplicate MCP server %q", server.ID)
	}
	r.servers[server.ID] = server
	return nil
}

func (r *Registry) Get(id string) (Server, error) {
	r.mu.RLock()
	server, ok := r.servers[strings.TrimSpace(id)]
	r.mu.RUnlock()
	if !ok {
		return Server{}, fmt.Errorf("%w %q", ErrServerNotFound, id)
	}
	return server, nil
}

func (r *Registry) List() []ServerView {
	r.mu.RLock()
	servers := make([]Server, 0, len(r.servers))
	for _, server := range r.servers {
		servers = append(servers, server)
	}
	r.mu.RUnlock()
	sort.Slice(servers, func(i, j int) bool { return servers[i].ID < servers[j].ID })
	views := make([]ServerView, len(servers))
	for index, server := range servers {
		views[index] = server.View()
	}
	return views
}

type serverConfig struct {
	ID                    string   `json:"id"`
	Endpoint              string   `json:"endpoint"`
	AllowedTools          []string `json:"allowed_tools"`
	DeniedTools           []string `json:"denied_tools"`
	ApprovalRequiredTools []string `json:"approval_required_tools"`
	Timeout               string   `json:"timeout"`
}

func ParseRegistry(raw string, defaultTimeout time.Duration) (*Registry, error) {
	registry := NewRegistry()
	if strings.TrimSpace(raw) == "" {
		return registry, nil
	}
	var configs []serverConfig
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&configs); err != nil {
		return nil, fmt.Errorf("invalid MCP server configuration: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return nil, fmt.Errorf("invalid MCP server configuration: trailing JSON")
	}
	for _, config := range configs {
		timeout := defaultTimeout
		if strings.TrimSpace(config.Timeout) != "" {
			parsed, err := time.ParseDuration(config.Timeout)
			if err != nil || parsed <= 0 {
				return nil, fmt.Errorf("MCP server %q timeout must be a positive duration", config.ID)
			}
			timeout = parsed
		}
		if err := registry.Register(Server{ID: config.ID, Endpoint: config.Endpoint, AllowedTools: config.AllowedTools, DeniedTools: config.DeniedTools, ApprovalRequiredTools: config.ApprovalRequiredTools, Timeout: timeout}); err != nil {
			return nil, err
		}
	}
	return registry, nil
}

func validateServer(server *Server) error {
	server.ID = strings.TrimSpace(server.ID)
	server.Endpoint = strings.TrimSpace(server.Endpoint)
	if server.ID == "" || len(server.ID) > 128 || !validName(server.ID) {
		return fmt.Errorf("MCP server ID must use 1-128 letters, numbers, dots, hyphens or underscores")
	}
	parsed, err := url.Parse(server.Endpoint)
	if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return fmt.Errorf("MCP server %q endpoint must be an absolute HTTP URL without credentials, query or fragment", server.ID)
	}
	if server.Timeout <= 0 {
		return fmt.Errorf("MCP server %q timeout must be positive", server.ID)
	}
	var policyErr error
	server.AllowedTools, policyErr = normalizeToolNames(server.AllowedTools)
	if policyErr != nil {
		return policyErr
	}
	server.DeniedTools, policyErr = normalizeToolNames(server.DeniedTools)
	if policyErr != nil {
		return policyErr
	}
	server.ApprovalRequiredTools, policyErr = normalizeToolNames(server.ApprovalRequiredTools)
	return policyErr
}

func normalizeToolNames(values []string) ([]string, error) {
	result := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if !validName(value) {
			return nil, fmt.Errorf("MCP tool name %q must use 1-128 letters, numbers, dots, hyphens or underscores", value)
		}
		if _, duplicate := seen[value]; duplicate {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	return result, nil
}

func validName(value string) bool {
	if len(value) < 1 || len(value) > 128 {
		return false
	}
	for _, character := range value {
		if character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' || character >= '0' && character <= '9' || strings.ContainsRune("._-", character) {
			continue
		}
		return false
	}
	return true
}
