package domain

import (
	"fmt"
	"net/url"
	"strings"
	"time"
)

type Agent struct {
	ID                string    `json:"id"`
	Name              string    `json:"name"`
	SystemPrompt      string    `json:"system_prompt"`
	Runtime           string    `json:"runtime,omitempty"`
	Protocol          string    `json:"protocol,omitempty"`
	Endpoint          string    `json:"endpoint,omitempty"`
	Capabilities      []string  `json:"capabilities,omitempty"`
	EffectIdempotency string    `json:"effect_idempotency,omitempty"`
	MaxConcurrency    int       `json:"max_concurrency,omitempty"`
	Priority          int       `json:"priority,omitempty"`
	CreatedAt         time.Time `json:"created_at"`
	UpdatedAt         time.Time `json:"updated_at"`
	Version           int64     `json:"version"`
}

const EffectIdempotencyRequired = "required"

func (a *Agent) InitializeForCreate(now time.Time) error {
	if err := a.NormalizeAndValidate(); err != nil {
		return err
	}
	now = now.UTC()
	if a.CreatedAt.IsZero() {
		a.CreatedAt = now
	} else {
		a.CreatedAt = a.CreatedAt.UTC()
	}
	if a.UpdatedAt.IsZero() {
		a.UpdatedAt = a.CreatedAt
	} else {
		a.UpdatedAt = a.UpdatedAt.UTC()
	}
	if a.Version == 0 {
		a.Version = 1
	}
	if a.Version != 1 {
		return fmt.Errorf("new agent version must be 1")
	}
	return nil
}

func (a *Agent) NormalizeAndValidate() error {
	a.ID = strings.TrimSpace(a.ID)
	a.Name = strings.TrimSpace(a.Name)
	a.SystemPrompt = strings.TrimSpace(a.SystemPrompt)
	a.Runtime = strings.ToLower(strings.TrimSpace(a.Runtime))
	a.Protocol = strings.ToLower(strings.TrimSpace(a.Protocol))
	a.Endpoint = strings.TrimSpace(a.Endpoint)
	a.EffectIdempotency = strings.ToLower(strings.TrimSpace(a.EffectIdempotency))

	if a.ID == "" {
		return fmt.Errorf("agent id is required")
	}
	if a.Name == "" {
		return fmt.Errorf("agent name is required")
	}
	if _, err := NormalizeAgentIdentifier("runtime", a.Runtime); err != nil {
		return err
	}
	if _, err := NormalizeAgentIdentifier("protocol", a.Protocol); err != nil {
		return err
	}
	if a.Protocol != "" && a.Runtime == "" {
		return fmt.Errorf("protocol requires runtime")
	}
	if a.Endpoint != "" {
		if a.Runtime == "" || a.Protocol == "" {
			return fmt.Errorf("endpoint requires runtime and protocol")
		}
		parsed, err := url.Parse(a.Endpoint)
		if err != nil || parsed.Scheme == "" || (parsed.Host == "" && parsed.Opaque == "" && parsed.Path == "") {
			return fmt.Errorf("endpoint must be an absolute URI")
		}
	}
	if a.EffectIdempotency != "" && a.EffectIdempotency != EffectIdempotencyRequired {
		return fmt.Errorf("effect_idempotency must be empty or %q", EffectIdempotencyRequired)
	}
	if a.EffectIdempotency != "" && (a.Runtime == "" || a.Protocol == "") {
		return fmt.Errorf("effect_idempotency requires runtime and protocol")
	}
	if a.MaxConcurrency < 0 {
		return fmt.Errorf("max_concurrency cannot be negative")
	}
	if a.Priority < -1000 || a.Priority > 1000 {
		return fmt.Errorf("priority must be between -1000 and 1000")
	}

	normalizedCapabilities := make([]string, 0, len(a.Capabilities))
	seenCapabilities := make(map[string]struct{}, len(a.Capabilities))
	for _, capability := range a.Capabilities {
		normalized, err := NormalizeCapability(capability)
		if err != nil {
			return err
		}
		if _, duplicate := seenCapabilities[normalized]; duplicate {
			continue
		}
		seenCapabilities[normalized] = struct{}{}
		normalizedCapabilities = append(normalizedCapabilities, normalized)
	}
	a.Capabilities = normalizedCapabilities
	return nil
}

// NormalizeCapability returns the canonical key used for persistence and exact
// capability discovery. Capability keys intentionally remain small identifiers,
// not a centrally managed taxonomy.
func NormalizeCapability(value string) (string, error) {
	value = strings.ToLower(strings.TrimSpace(value))
	var normalized strings.Builder
	separator := false
	for _, character := range value {
		switch {
		case character >= 'a' && character <= 'z', character >= '0' && character <= '9', character == '.':
			if separator && normalized.Len() > 0 {
				normalized.WriteByte('-')
			}
			separator = false
			normalized.WriteRune(character)
		case character == '-' || character == '_' || character == ' ' || character == '\t' || character == '\n' || character == '\r':
			separator = normalized.Len() > 0
		default:
			return "", fmt.Errorf("capability %q must contain only letters, numbers, dots, hyphens, underscores or spaces", value)
		}
	}
	result := normalized.String()
	if result == "" {
		return "", fmt.Errorf("capabilities cannot contain blank values")
	}
	return result, nil
}

func NormalizeAgentIdentifier(field, value string) (string, error) {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "" {
		return "", nil
	}
	for _, character := range value {
		if character >= 'a' && character <= 'z' ||
			character >= '0' && character <= '9' ||
			character == '-' || character == '_' || character == '.' {
			continue
		}
		return "", fmt.Errorf("%s must contain only lowercase letters, numbers, hyphens, underscores or dots", field)
	}
	return value, nil
}
