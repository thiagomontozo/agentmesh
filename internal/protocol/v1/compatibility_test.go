package v1_test

import (
	"encoding/json"
	"testing"

	protocolv1 "github.com/thiagomontozo/agentmesh/internal/protocol/v1"
)

func TestLegacyV1PayloadsRemainValid(t *testing.T) {
	legacyRequest := []byte(`{"protocol_version":"1","run_id":"run_legacy","agent_id":"agt_legacy","attempt":1,"idempotency_key":"run_legacy:1","input":"hello"}`)
	var request protocolv1.RunRequest
	if err := json.Unmarshal(legacyRequest, &request); err != nil {
		t.Fatal(err)
	}
	if err := request.Validate(); err != nil {
		t.Fatalf("legacy V1 request became invalid: %v", err)
	}
	if request.EffectIdempotencyKey != "" {
		t.Fatalf("legacy request unexpectedly gained an effect key: %+v", request)
	}

	legacyResponse := []byte(`{"protocol_version":"1","run_id":"run_legacy","status":"succeeded","output":"done"}`)
	var response protocolv1.RunResponse
	if err := json.Unmarshal(legacyResponse, &response); err != nil {
		t.Fatal(err)
	}
	if err := response.Validate(); err != nil {
		t.Fatalf("legacy V1 response became invalid: %v", err)
	}
}

func TestLegacyV1ConsumerCanIgnoreCurrentAdditiveFields(t *testing.T) {
	current := protocolv1.RunRequest{
		ProtocolVersion: protocolv1.Version, RunID: "run_current", AgentID: "agt_1", Attempt: 2,
		IdempotencyKey:       protocolv1.AttemptIdempotencyKey("run_current", 2),
		EffectIdempotencyKey: protocolv1.EffectIdempotencyKey("run_current"), Input: "hello",
	}
	payload, err := json.Marshal(current)
	if err != nil {
		t.Fatal(err)
	}
	var legacy struct {
		ProtocolVersion string `json:"protocol_version"`
		RunID           string `json:"run_id"`
		AgentID         string `json:"agent_id"`
		Attempt         int    `json:"attempt"`
		IdempotencyKey  string `json:"idempotency_key"`
		Input           string `json:"input"`
	}
	if err := json.Unmarshal(payload, &legacy); err != nil {
		t.Fatal(err)
	}
	if legacy.RunID != current.RunID || legacy.IdempotencyKey != current.IdempotencyKey || legacy.Input != current.Input {
		t.Fatalf("legacy consumer lost core V1 fields: %+v", legacy)
	}
}
