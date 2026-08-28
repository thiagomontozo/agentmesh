package domain

import "testing"

func TestAgentLegacyDefinitionRemainsValid(t *testing.T) {
	agent := Agent{ID: "agt_legacy", Name: "Legacy"}
	if err := agent.NormalizeAndValidate(); err != nil {
		t.Fatal(err)
	}
	if agent.Runtime != "" || agent.Protocol != "" || agent.Endpoint != "" || len(agent.Capabilities) != 0 {
		t.Fatalf("unexpected legacy execution metadata: %+v", agent)
	}
}

func TestAgentNormalizesExecutionMetadata(t *testing.T) {
	agent := Agent{
		ID: " agt_remote ", Name: " Legal Agent ", SystemPrompt: " Be precise ",
		Runtime: " REMOTE ", Protocol: " HTTP ", Endpoint: " http://legal-agent:9000 ",
		Capabilities: []string{" legal-search ", "summarization"},
	}
	if err := agent.NormalizeAndValidate(); err != nil {
		t.Fatal(err)
	}
	if agent.ID != "agt_remote" || agent.Name != "Legal Agent" || agent.SystemPrompt != "Be precise" {
		t.Fatalf("unexpected normalized identity: %+v", agent)
	}
	if agent.Runtime != "remote" || agent.Protocol != "http" || agent.Endpoint != "http://legal-agent:9000" {
		t.Fatalf("unexpected execution metadata: %+v", agent)
	}
	if len(agent.Capabilities) != 2 || agent.Capabilities[0] != "legal-search" {
		t.Fatalf("unexpected capabilities: %#v", agent.Capabilities)
	}
}

func TestAgentRejectsInvalidExecutionMetadata(t *testing.T) {
	tests := []struct {
		name  string
		agent Agent
	}{
		{name: "invalid runtime identifier", agent: Agent{ID: "agt_1", Name: "test", Runtime: "remote http"}},
		{name: "invalid protocol identifier", agent: Agent{ID: "agt_1", Name: "test", Runtime: "remote", Protocol: "http/v1"}},
		{name: "protocol without runtime", agent: Agent{ID: "agt_1", Name: "test", Protocol: "http"}},
		{name: "endpoint without protocol", agent: Agent{ID: "agt_1", Name: "test", Runtime: "remote", Endpoint: "http://agent:9000"}},
		{name: "relative endpoint", agent: Agent{ID: "agt_1", Name: "test", Runtime: "remote", Protocol: "http", Endpoint: "/v1/runs"}},
		{name: "blank capability", agent: Agent{ID: "agt_1", Name: "test", Capabilities: []string{" "}}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := test.agent.NormalizeAndValidate(); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}
