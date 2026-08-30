package domain

import (
	"strings"
	"testing"
	"time"
)

func TestWorkflowInitializesValidDAG(t *testing.T) {
	w := Workflow{ID: "wf_1", Input: "document", Steps: []WorkflowStep{
		{ID: "Extract", AgentID: "agt_extract", InputFrom: []string{"workflow"}},
		{ID: "search", AgentID: "agt_search", DependsOn: []string{"extract"}, InputFrom: []string{"extract"}},
		{ID: "review", AgentID: "agt_review", DependsOn: []string{"extract", "search"}, InputFrom: []string{"extract", "search"}, InputAggregation: "json-array"},
	}}
	if err := w.InitializeForCreate(time.Now()); err != nil {
		t.Fatal(err)
	}
	if w.Status != WorkflowPending || w.Version != 1 || w.Steps[0].ID != "extract" {
		t.Fatalf("workflow was not normalized: %+v", w)
	}
	if w.Steps[0].InputAggregation != WorkflowInputSingle {
		t.Fatalf("single input aggregation not defaulted: %+v", w.Steps[0])
	}
}

func TestWorkflowRejectsInvalidDefinitions(t *testing.T) {
	tests := []struct {
		name string
		w    Workflow
		want string
	}{
		{name: "empty", w: Workflow{ID: "wf"}, want: "at least one"},
		{name: "duplicate", w: Workflow{ID: "wf", Steps: []WorkflowStep{{ID: "a", AgentID: "x", Input: "x"}, {ID: "A", AgentID: "y", Input: "y"}}}, want: "duplicate"},
		{name: "unknown dependency", w: Workflow{ID: "wf", Steps: []WorkflowStep{{ID: "a", AgentID: "x", Input: "x", DependsOn: []string{"missing"}}}}, want: "unknown"},
		{name: "cycle", w: Workflow{ID: "wf", Steps: []WorkflowStep{{ID: "a", AgentID: "x", Input: "x", DependsOn: []string{"b"}}, {ID: "b", AgentID: "x", Input: "x", DependsOn: []string{"a"}}}}, want: "cycle"},
		{name: "source not dependency", w: Workflow{ID: "wf", Steps: []WorkflowStep{{ID: "a", AgentID: "x", Input: "x"}, {ID: "b", AgentID: "x", InputFrom: []string{"a"}}}}, want: "must be a dependency"},
		{name: "fan in without aggregation", w: Workflow{ID: "wf", Steps: []WorkflowStep{{ID: "a", AgentID: "x", Input: "x"}, {ID: "b", AgentID: "x", Input: "x"}, {ID: "c", AgentID: "x", DependsOn: []string{"a", "b"}, InputFrom: []string{"a", "b"}}}}, want: "json-array"},
		{name: "condition source not dependency", w: Workflow{ID: "wf", Steps: []WorkflowStep{{ID: "a", AgentID: "x", Input: "x"}, {ID: "b", AgentID: "x", Input: "x", Condition: &WorkflowCondition{Source: "a", Operator: "equals", Value: "yes"}}}}, want: "must be a dependency"},
		{name: "condition operator", w: Workflow{ID: "wf", Input: "x", Steps: []WorkflowStep{{ID: "a", AgentID: "x", Input: "x", Condition: &WorkflowCondition{Source: "workflow", Operator: "eval", Value: "yes"}}}}, want: "unsupported"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.w.InitializeForCreate(time.Now())
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("expected %q error, got %v", test.want, err)
			}
		})
	}
}

func TestWorkflowNormalizesCondition(t *testing.T) {
	w := Workflow{ID: "wf", Input: "YES", Steps: []WorkflowStep{{
		ID: "branch", AgentID: "agt", Input: "x",
		Condition: &WorkflowCondition{Source: " WORKFLOW ", Operator: " EQUALS ", Value: "YES"},
	}}}
	if err := w.InitializeForCreate(time.Now()); err != nil {
		t.Fatal(err)
	}
	if w.Steps[0].Condition.Source != "workflow" || w.Steps[0].Condition.Operator != "equals" || w.Steps[0].Condition.Value != "YES" {
		t.Fatalf("unexpected normalized condition: %+v", w.Steps[0].Condition)
	}
}
