package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

type ApprovalStatus string

const (
	ApprovalPending  ApprovalStatus = "pending"
	ApprovalApproved ApprovalStatus = "approved"
	ApprovalRejected ApprovalStatus = "rejected"
	ApprovalConsumed ApprovalStatus = "consumed"
)

type Approval struct {
	ID            string          `json:"id"`
	ServerID      string          `json:"server_id"`
	ToolName      string          `json:"tool_name"`
	Arguments     json.RawMessage `json:"arguments"`
	ArgumentsHash string          `json:"arguments_hash"`
	Reason        string          `json:"reason,omitempty"`
	Status        ApprovalStatus  `json:"status"`
	RequestedBy   string          `json:"requested_by"`
	DecidedBy     string          `json:"decided_by,omitempty"`
	CreatedAt     time.Time       `json:"created_at"`
	ExpiresAt     time.Time       `json:"expires_at"`
	DecidedAt     *time.Time      `json:"decided_at,omitempty"`
	ConsumedAt    *time.Time      `json:"consumed_at,omitempty"`
	Version       int64           `json:"version"`
}

func (a *Approval) Initialize(now time.Time, ttl time.Duration) error {
	a.ID = strings.TrimSpace(a.ID)
	a.ServerID = strings.TrimSpace(a.ServerID)
	a.ToolName = strings.TrimSpace(a.ToolName)
	a.Reason = strings.TrimSpace(a.Reason)
	a.RequestedBy = strings.TrimSpace(a.RequestedBy)
	if a.ID == "" || a.ServerID == "" || a.ToolName == "" || a.RequestedBy == "" {
		return fmt.Errorf("approval ID, server_id, tool_name and requested_by are required")
	}
	if ttl <= 0 {
		return fmt.Errorf("approval TTL must be positive")
	}
	var arguments map[string]any
	if len(a.Arguments) == 0 {
		a.Arguments = json.RawMessage(`{}`)
	}
	if err := json.Unmarshal(a.Arguments, &arguments); err != nil || arguments == nil {
		return fmt.Errorf("approval arguments must be a JSON object")
	}
	canonical, err := json.Marshal(arguments)
	if err != nil {
		return fmt.Errorf("canonicalize approval arguments: %w", err)
	}
	a.Arguments = canonical
	a.ArgumentsHash = ApprovalArgumentsHash(a.ServerID, a.ToolName, canonical)
	now = now.UTC()
	a.Status = ApprovalPending
	a.CreatedAt = now
	a.ExpiresAt = now.Add(ttl)
	a.Version = 1
	return nil
}

func ApprovalArgumentsHash(serverID, toolName string, arguments []byte) string {
	digest := sha256.Sum256([]byte(strings.TrimSpace(serverID) + "\x00" + strings.TrimSpace(toolName) + "\x00" + string(arguments)))
	return hex.EncodeToString(digest[:])
}

func (a Approval) Expired(now time.Time) bool {
	return !now.UTC().Before(a.ExpiresAt)
}
