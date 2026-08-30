package domain

import (
	"testing"
	"time"
)

func TestApprovalInitializeCanonicalizesAndHashesArguments(t *testing.T) {
	now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.FixedZone("local", -3*60*60))
	first := Approval{ID: " apr_1 ", ServerID: " tools ", ToolName: " deploy ", RequestedBy: " operator ", Arguments: []byte(`{"b":2,"a":1}`)}
	second := Approval{ID: "apr_2", ServerID: "tools", ToolName: "deploy", RequestedBy: "operator", Arguments: []byte(`{"a":1,"b":2}`)}
	if err := first.Initialize(now, time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := second.Initialize(now, time.Minute); err != nil {
		t.Fatal(err)
	}
	if first.ArgumentsHash != second.ArgumentsHash || string(first.Arguments) != `{"a":1,"b":2}` {
		t.Fatalf("arguments were not canonicalized consistently: %s %s", first.Arguments, first.ArgumentsHash)
	}
	if first.Status != ApprovalPending || first.Version != 1 || first.CreatedAt.Location() != time.UTC || first.ExpiresAt.Sub(first.CreatedAt) != time.Minute {
		t.Fatalf("unexpected initialized approval: %+v", first)
	}
}

func TestApprovalInitializeRejectsInvalidInput(t *testing.T) {
	for _, approval := range []Approval{
		{ID: "", ServerID: "tools", ToolName: "deploy", RequestedBy: "operator"},
		{ID: "apr", ServerID: "tools", ToolName: "deploy", RequestedBy: "operator", Arguments: []byte(`[]`)},
	} {
		if err := approval.Initialize(time.Now(), time.Minute); err == nil {
			t.Fatalf("expected invalid approval to fail: %+v", approval)
		}
	}
}
