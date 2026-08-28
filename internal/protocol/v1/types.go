// Package v1 defines the language-neutral JSON contract exchanged with remote
// Agents. It contains wire types only; HTTP transport belongs to runtime code.
package v1

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

const Version = "1"

var ErrInvalidMessage = errors.New("invalid agent protocol v1 message")

type RunStatus string

const (
	StatusSucceeded RunStatus = "succeeded"
	StatusFailed    RunStatus = "failed"
)

type RunRequest struct {
	ProtocolVersion string `json:"protocol_version"`
	RunID           string `json:"run_id"`
	AgentID         string `json:"agent_id"`
	Attempt         int    `json:"attempt"`
	IdempotencyKey  string `json:"idempotency_key"`
	Input           string `json:"input"`
}

func (r RunRequest) Validate() error {
	if r.ProtocolVersion != Version {
		return invalidMessage("unsupported protocol_version %q", r.ProtocolVersion)
	}
	if strings.TrimSpace(r.RunID) == "" {
		return invalidMessage("run_id is required")
	}
	if strings.TrimSpace(r.AgentID) == "" {
		return invalidMessage("agent_id is required")
	}
	if r.Attempt < 1 {
		return invalidMessage("attempt must be at least 1")
	}
	if strings.TrimSpace(r.IdempotencyKey) == "" {
		return invalidMessage("idempotency_key is required")
	}
	return nil
}

// AttemptIdempotencyKey returns the stable identity for one execution attempt.
// Repeating the same attempt must reuse this value; a retry represented by a new
// attempt receives a different value.
func AttemptIdempotencyKey(runID string, attempt int) string {
	return runID + ":" + strconv.Itoa(attempt)
}

type RunResponse struct {
	ProtocolVersion string    `json:"protocol_version"`
	RunID           string    `json:"run_id"`
	Status          RunStatus `json:"status"`
	Output          string    `json:"output,omitempty"`
	Error           *RunError `json:"error,omitempty"`
}

func (r RunResponse) Validate() error {
	if r.ProtocolVersion != Version {
		return invalidMessage("unsupported protocol_version %q", r.ProtocolVersion)
	}
	if strings.TrimSpace(r.RunID) == "" {
		return invalidMessage("run_id is required")
	}
	switch r.Status {
	case StatusSucceeded:
		if r.Error != nil {
			return invalidMessage("succeeded response cannot contain error")
		}
	case StatusFailed:
		if r.Error == nil {
			return invalidMessage("failed response requires error")
		}
		if r.Output != "" {
			return invalidMessage("failed response cannot contain output")
		}
		if err := r.Error.Validate(); err != nil {
			return err
		}
	default:
		return invalidMessage("unsupported status %q", r.Status)
	}
	return nil
}

type RunError struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	Retryable bool   `json:"retryable"`
}

func (e RunError) Validate() error {
	if strings.TrimSpace(e.Code) == "" {
		return invalidMessage("error.code is required")
	}
	if strings.TrimSpace(e.Message) == "" {
		return invalidMessage("error.message is required")
	}
	return nil
}

func invalidMessage(format string, arguments ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidMessage, fmt.Sprintf(format, arguments...))
}
