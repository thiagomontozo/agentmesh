package v1_test

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"github.com/thiagomontozo/agentmesh/internal/protocol"
	protocolv1 "github.com/thiagomontozo/agentmesh/internal/protocol/v1"
)

func TestRunRequestJSONRoundTrip(t *testing.T) {
	request := protocolv1.RunRequest{
		ProtocolVersion: protocolv1.Version,
		RunID:           "run_123",
		AgentID:         "agent_456",
		Attempt:         1,
		IdempotencyKey:  protocolv1.AttemptIdempotencyKey("run_123", 1),
		Input:           "Analyze this document",
	}

	payload, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"protocol_version":"1","run_id":"run_123","agent_id":"agent_456","attempt":1,"idempotency_key":"run_123:1","input":"Analyze this document"}`
	if string(payload) != want {
		t.Fatalf("unexpected JSON:\nwant %s\n got %s", want, payload)
	}

	var decoded protocolv1.RunRequest
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(decoded, request) {
		t.Fatalf("round trip changed request: want=%+v got=%+v", request, decoded)
	}
	if err := decoded.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestUnsupportedVersionIsTypedAndHasControlledResponse(t *testing.T) {
	request := protocolv1.RunRequest{ProtocolVersion: "2", RunID: "run_1", AgentID: "agt_1", Attempt: 1, IdempotencyKey: "run_1:1"}
	if err := request.Validate(); !errors.Is(err, protocol.ErrUnsupportedVersion) || !errors.Is(err, protocolv1.ErrInvalidMessage) {
		t.Fatalf("expected wrapped version and message errors, got %v", err)
	}
	response := protocolv1.UnsupportedVersionResponse("run_1")
	if err := response.Validate(); err != nil {
		t.Fatal(err)
	}
	if response.Error == nil || response.Error.Code != protocol.CodeUnsupportedVersion || response.Error.Retryable {
		t.Fatalf("unexpected version response: %+v", response)
	}
}

func TestRunResponseJSONSuccess(t *testing.T) {
	payload := []byte(`{"protocol_version":"1","run_id":"run_123","status":"succeeded","output":"completed"}`)
	var response protocolv1.RunResponse
	if err := json.Unmarshal(payload, &response); err != nil {
		t.Fatal(err)
	}
	if err := response.Validate(); err != nil {
		t.Fatal(err)
	}
	if response.Output != "completed" || response.Error != nil {
		t.Fatalf("unexpected response: %+v", response)
	}
}

func TestRunResponseJSONStructuredError(t *testing.T) {
	response := protocolv1.RunResponse{
		ProtocolVersion: protocolv1.Version,
		RunID:           "run_123",
		Status:          protocolv1.StatusFailed,
		Error: &protocolv1.RunError{
			Code: "agent_overloaded", Message: "try again later", Retryable: true,
		},
	}
	payload, err := json.Marshal(response)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"protocol_version":"1","run_id":"run_123","status":"failed","error":{"code":"agent_overloaded","message":"try again later","retryable":true}}`
	if string(payload) != want {
		t.Fatalf("unexpected JSON:\nwant %s\n got %s", want, payload)
	}

	var decoded protocolv1.RunResponse
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatal(err)
	}
	if err := decoded.Validate(); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(decoded, response) {
		t.Fatalf("round trip changed response: want=%+v got=%+v", response, decoded)
	}
}

func TestProtocolValidationRejectsInvalidMessages(t *testing.T) {
	tests := []struct {
		name    string
		message interface{ Validate() error }
	}{
		{name: "request version", message: protocolv1.RunRequest{ProtocolVersion: "2", RunID: "run_1", AgentID: "agt_1", Attempt: 1, IdempotencyKey: "run_1:1"}},
		{name: "request run", message: protocolv1.RunRequest{ProtocolVersion: "1", AgentID: "agt_1", Attempt: 1, IdempotencyKey: "run_1:1"}},
		{name: "request agent", message: protocolv1.RunRequest{ProtocolVersion: "1", RunID: "run_1", Attempt: 1, IdempotencyKey: "run_1:1"}},
		{name: "request attempt", message: protocolv1.RunRequest{ProtocolVersion: "1", RunID: "run_1", AgentID: "agt_1", IdempotencyKey: "run_1:0"}},
		{name: "request idempotency", message: protocolv1.RunRequest{ProtocolVersion: "1", RunID: "run_1", AgentID: "agt_1", Attempt: 1}},
		{name: "response version", message: protocolv1.RunResponse{ProtocolVersion: "2", RunID: "run_1", Status: protocolv1.StatusSucceeded}},
		{name: "response run", message: protocolv1.RunResponse{ProtocolVersion: "1", Status: protocolv1.StatusSucceeded}},
		{name: "response status", message: protocolv1.RunResponse{ProtocolVersion: "1", RunID: "run_1", Status: "running"}},
		{name: "success with error", message: protocolv1.RunResponse{ProtocolVersion: "1", RunID: "run_1", Status: protocolv1.StatusSucceeded, Error: &protocolv1.RunError{Code: "error", Message: "error"}}},
		{name: "failure without error", message: protocolv1.RunResponse{ProtocolVersion: "1", RunID: "run_1", Status: protocolv1.StatusFailed}},
		{name: "failure with output", message: protocolv1.RunResponse{ProtocolVersion: "1", RunID: "run_1", Status: protocolv1.StatusFailed, Output: "partial", Error: &protocolv1.RunError{Code: "error", Message: "error"}}},
		{name: "error without code", message: protocolv1.RunResponse{ProtocolVersion: "1", RunID: "run_1", Status: protocolv1.StatusFailed, Error: &protocolv1.RunError{Message: "error"}}},
		{name: "error without message", message: protocolv1.RunResponse{ProtocolVersion: "1", RunID: "run_1", Status: protocolv1.StatusFailed, Error: &protocolv1.RunError{Code: "error"}}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := test.message.Validate(); !errors.Is(err, protocolv1.ErrInvalidMessage) {
				t.Fatalf("expected ErrInvalidMessage, got %v", err)
			}
		})
	}
}

func TestAttemptIdempotencyKeyIsStablePerAttempt(t *testing.T) {
	if first, second := protocolv1.AttemptIdempotencyKey("run_123", 2), protocolv1.AttemptIdempotencyKey("run_123", 2); first != second {
		t.Fatalf("expected stable key, got %q and %q", first, second)
	}
	if protocolv1.AttemptIdempotencyKey("run_123", 2) == protocolv1.AttemptIdempotencyKey("run_123", 3) {
		t.Fatal("different attempts must have different keys")
	}
}
