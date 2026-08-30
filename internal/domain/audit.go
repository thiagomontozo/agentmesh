package domain

import "time"

type AuditEvent struct {
	ID         string    `json:"id"`
	Timestamp  time.Time `json:"timestamp"`
	RequestID  string    `json:"request_id,omitempty"`
	InstanceID string    `json:"instance_id,omitempty"`
	Subject    string    `json:"subject"`
	Roles      []string  `json:"roles,omitempty"`
	AgentID    string    `json:"agent_id,omitempty"`
	Method     string    `json:"method"`
	Path       string    `json:"path"`
	Status     int       `json:"status"`
}
