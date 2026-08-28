package domain

import (
	"fmt"
	"net/url"
	"strings"
	"time"
)

type Agent struct {
	ID           string    `json:"id"`
	Name         string    `json:"name"`
	SystemPrompt string    `json:"system_prompt"`
	Runtime      string    `json:"runtime,omitempty"`
	Protocol     string    `json:"protocol,omitempty"`
	Endpoint     string    `json:"endpoint,omitempty"`
	Capabilities []string  `json:"capabilities,omitempty"`
	CreatedAt    time.Time `json:"created_at"`
}

func (a *Agent) NormalizeAndValidate() error {
	a.ID = strings.TrimSpace(a.ID)
	a.Name = strings.TrimSpace(a.Name)
	a.SystemPrompt = strings.TrimSpace(a.SystemPrompt)
	a.Runtime = strings.ToLower(strings.TrimSpace(a.Runtime))
	a.Protocol = strings.ToLower(strings.TrimSpace(a.Protocol))
	a.Endpoint = strings.TrimSpace(a.Endpoint)

	if a.ID == "" {
		return fmt.Errorf("agent id is required")
	}
	if a.Name == "" {
		return fmt.Errorf("agent name is required")
	}
	if err := validateAgentIdentifier("runtime", a.Runtime); err != nil {
		return err
	}
	if err := validateAgentIdentifier("protocol", a.Protocol); err != nil {
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

	normalizedCapabilities := make([]string, len(a.Capabilities))
	for index, capability := range a.Capabilities {
		normalizedCapabilities[index] = strings.TrimSpace(capability)
		if normalizedCapabilities[index] == "" {
			return fmt.Errorf("capabilities cannot contain blank values")
		}
	}
	a.Capabilities = normalizedCapabilities
	return nil
}

func validateAgentIdentifier(field, value string) error {
	if value == "" {
		return nil
	}
	for _, character := range value {
		if character >= 'a' && character <= 'z' ||
			character >= '0' && character <= '9' ||
			character == '-' || character == '_' || character == '.' {
			continue
		}
		return fmt.Errorf("%s must contain only lowercase letters, numbers, hyphens, underscores or dots", field)
	}
	return nil
}
