package runtime_test

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thiagomontozo/agentmesh/internal/domain"
	protocolv1 "github.com/thiagomontozo/agentmesh/internal/protocol/v1"
	agentruntime "github.com/thiagomontozo/agentmesh/internal/runtime"
)

func TestHTTPRuntimeSuccess(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost || request.URL.Path != "/v1/runs" {
			t.Errorf("unexpected request: %s %s", request.Method, request.URL.Path)
		}
		if request.Header.Get("Content-Type") != "application/json" || request.Header.Get("Accept") != "application/json" {
			t.Errorf("unexpected content negotiation headers: %v", request.Header)
		}
		var payload protocolv1.RunRequest
		if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
			t.Errorf("decode request: %v", err)
			return
		}
		if err := payload.Validate(); err != nil {
			t.Errorf("validate request: %v", err)
		}
		if payload.RunID != "run_1" || payload.AgentID != "agt_1" || payload.Attempt != 2 || payload.Input != "hello" {
			t.Errorf("unexpected protocol request: %+v", payload)
		}
		if payload.IdempotencyKey != "run_1:2" || request.Header.Get("Idempotency-Key") != payload.IdempotencyKey {
			t.Errorf("unexpected idempotency identity: body=%q header=%q", payload.IdempotencyKey, request.Header.Get("Idempotency-Key"))
		}
		writeRuntimeResponse(t, response, protocolv1.RunResponse{
			ProtocolVersion: protocolv1.Version, RunID: payload.RunID, Status: protocolv1.StatusSucceeded, Output: "done",
		})
	}))
	defer server.Close()

	result, err := executeRemote(context.Background(), agentruntime.NewHTTPRuntime(server.Client(), 0), server.URL)
	if err != nil {
		t.Fatal(err)
	}
	if result.Output != "done" {
		t.Fatalf("unexpected result: %+v", result)
	}
}

func TestHTTPRuntimeClassifiesHTTP500AsTemporary(t *testing.T) {
	server := statusServer(http.StatusInternalServerError)
	defer server.Close()

	_, err := executeRemote(context.Background(), agentruntime.NewHTTPRuntime(server.Client(), 0), server.URL)
	httpError := requireHTTPError(t, err, agentruntime.HTTPErrorTemporary, http.StatusInternalServerError)
	if !httpError.Retryable() {
		t.Fatal("expected temporary error to be retryable")
	}
}

func TestHTTPRuntimeClassifiesHTTP400AsPermanent(t *testing.T) {
	server := statusServer(http.StatusBadRequest)
	defer server.Close()

	_, err := executeRemote(context.Background(), agentruntime.NewHTTPRuntime(server.Client(), 0), server.URL)
	httpError := requireHTTPError(t, err, agentruntime.HTTPErrorPermanent, http.StatusBadRequest)
	if httpError.Retryable() {
		t.Fatal("expected permanent error not to be retryable")
	}
}

func TestHTTPRuntimeTimeout(t *testing.T) {
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		<-release
	}))
	defer func() {
		close(release)
		server.Close()
	}()
	client := server.Client()
	client.Timeout = 20 * time.Millisecond

	_, err := executeRemote(context.Background(), agentruntime.NewHTTPRuntime(client, 0), server.URL)
	httpError := requireHTTPError(t, err, agentruntime.HTTPErrorTimeout, 0)
	if !httpError.Retryable() {
		t.Fatal("expected timeout to be retryable")
	}
}

func TestHTTPRuntimeRejectsInvalidJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		response.Header().Set("Content-Type", "application/json")
		_, _ = response.Write([]byte(`{"status":`))
	}))
	defer server.Close()

	_, err := executeRemote(context.Background(), agentruntime.NewHTTPRuntime(server.Client(), 0), server.URL)
	requireHTTPError(t, err, agentruntime.HTTPErrorProtocol, 0)
}

func TestHTTPRuntimeRejectsEmptyResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		response.Header().Set("Content-Type", "application/json")
		response.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	_, err := executeRemote(context.Background(), agentruntime.NewHTTPRuntime(server.Client(), 0), server.URL)
	requireHTTPError(t, err, agentruntime.HTTPErrorProtocol, 0)
}

func TestHTTPRuntimeRejectsMismatchedRunID(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		writeRuntimeResponse(t, response, protocolv1.RunResponse{
			ProtocolVersion: protocolv1.Version, RunID: "run_other", Status: protocolv1.StatusSucceeded,
		})
	}))
	defer server.Close()

	_, err := executeRemote(context.Background(), agentruntime.NewHTTPRuntime(server.Client(), 0), server.URL)
	requireHTTPError(t, err, agentruntime.HTTPErrorProtocol, 0)
}

func TestHTTPRuntimeRejectsResponseAboveLimit(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		response.Header().Set("Content-Type", "application/json")
		_, _ = response.Write([]byte(strings.Repeat("x", 65)))
	}))
	defer server.Close()

	_, err := executeRemote(context.Background(), agentruntime.NewHTTPRuntime(server.Client(), 64), server.URL)
	requireHTTPError(t, err, agentruntime.HTTPErrorProtocol, 0)
}

func TestHTTPRuntimeClassifiesUnavailableEndpointAsTemporary(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	endpoint := "http://" + listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Timeout: time.Second}

	_, err = executeRemote(context.Background(), agentruntime.NewHTTPRuntime(client, 0), endpoint)
	requireHTTPError(t, err, agentruntime.HTTPErrorTemporary, 0)
}

func TestHTTPRuntimePropagatesContextCancellation(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		close(started)
		<-release
	}))
	defer func() {
		close(release)
		server.Close()
	}()
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := executeRemote(ctx, agentruntime.NewHTTPRuntime(server.Client(), 0), server.URL)
		result <- err
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for request")
	}
	cancel()

	select {
	case err := <-result:
		requireHTTPError(t, err, agentruntime.HTTPErrorCanceled, 0)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context cancellation, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("runtime did not return after cancellation")
	}
}

func TestHTTPRuntimeCancellationReachesHTTPRequest(t *testing.T) {
	requestStarted := make(chan struct{})
	requestCanceled := make(chan struct{})
	client := &http.Client{Transport: roundTripperFunc(func(request *http.Request) (*http.Response, error) {
		close(requestStarted)
		<-request.Context().Done()
		close(requestCanceled)
		return nil, request.Context().Err()
	})}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := executeRemote(ctx, agentruntime.NewHTTPRuntime(client, 0), "http://remote-agent")
		result <- err
	}()
	select {
	case <-requestStarted:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for HTTP request")
	}
	cancel()
	select {
	case <-requestCanceled:
	case <-time.After(time.Second):
		t.Fatal("HTTP request context was not canceled")
	}
	select {
	case err := <-result:
		requireHTTPError(t, err, agentruntime.HTTPErrorCanceled, 0)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context cancellation, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("HTTP runtime did not return after request cancellation")
	}
}

func TestHTTPRuntimeDoesNotFollowRedirects(t *testing.T) {
	var targetCalls atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		targetCalls.Add(1)
	}))
	defer target.Close()
	redirect := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		http.Redirect(response, request, target.URL, http.StatusTemporaryRedirect)
	}))
	defer redirect.Close()

	_, err := executeRemote(context.Background(), agentruntime.NewHTTPRuntime(redirect.Client(), 0), redirect.URL)
	requireHTTPError(t, err, agentruntime.HTTPErrorPermanent, http.StatusTemporaryRedirect)
	if targetCalls.Load() != 0 {
		t.Fatalf("redirect target received %d requests", targetCalls.Load())
	}
}

func TestHTTPRuntimeUsesStructuredAgentFailure(t *testing.T) {
	tests := []struct {
		name      string
		retryable bool
		wantKind  agentruntime.HTTPErrorKind
	}{
		{name: "retryable", retryable: true, wantKind: agentruntime.HTTPErrorTemporary},
		{name: "permanent", retryable: false, wantKind: agentruntime.HTTPErrorPermanent},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
				writeRuntimeResponse(t, response, protocolv1.RunResponse{
					ProtocolVersion: protocolv1.Version,
					RunID:           "run_1",
					Status:          protocolv1.StatusFailed,
					Error: &protocolv1.RunError{
						Code: "agent_busy", Message: "agent is busy", Retryable: test.retryable,
					},
				})
			}))
			defer server.Close()

			_, err := executeRemote(context.Background(), agentruntime.NewHTTPRuntime(server.Client(), 0), server.URL)
			httpError := requireHTTPError(t, err, test.wantKind, 0)
			if httpError.Code != "agent_busy" {
				t.Fatalf("unexpected agent error code: %+v", httpError)
			}
		})
	}
}

func TestHTTPRuntimeRejectsInvalidAgentConfiguration(t *testing.T) {
	runtime := agentruntime.NewHTTPRuntime(nil, 0)
	tests := []struct {
		name     string
		protocol string
		endpoint string
		wantKind agentruntime.HTTPErrorKind
	}{
		{name: "protocol", protocol: "grpc", endpoint: "http://agent", wantKind: agentruntime.HTTPErrorProtocol},
		{name: "scheme", protocol: "http", endpoint: "file:///tmp/agent", wantKind: agentruntime.HTTPErrorPermanent},
		{name: "credentials", protocol: "http", endpoint: "http://user:secret@agent", wantKind: agentruntime.HTTPErrorPermanent},
		{name: "query", protocol: "http", endpoint: "http://agent?token=secret", wantKind: agentruntime.HTTPErrorPermanent},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := runtime.Execute(context.Background(), agentruntime.ExecutionRequest{
				RunID: "run_1", Attempt: 1, Agent: domain.Agent{ID: "agt_1", Runtime: "remote", Protocol: test.protocol, Endpoint: test.endpoint},
			})
			requireHTTPError(t, err, test.wantKind, 0)
		})
	}
}

func TestSecureHTTPRuntimeNetworkPolicy(t *testing.T) {
	tests := []struct {
		name     string
		endpoint string
		policy   agentruntime.HTTPPolicy
	}{
		{name: "requires HTTPS", endpoint: "http://agent.example", policy: agentruntime.HTTPPolicy{RequireHTTPS: true}},
		{name: "blocks loopback", endpoint: "http://127.0.0.1:9000", policy: agentruntime.HTTPPolicy{AllowPrivate: true}},
		{name: "blocks private", endpoint: "http://10.0.0.10:9000", policy: agentruntime.HTTPPolicy{AllowLoopback: true}},
		{name: "blocks metadata link-local", endpoint: "http://169.254.169.254", policy: agentruntime.DefaultHTTPPolicy()},
		{name: "enforces host allowlist", endpoint: "https://other.example", policy: agentruntime.HTTPPolicy{AllowedHosts: []string{"agent.example"}}},
		{name: "enforces blocked CIDR", endpoint: "http://192.0.2.10", policy: agentruntime.HTTPPolicy{BlockedCIDRs: []netip.Prefix{netip.MustParsePrefix("192.0.2.0/24")}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runtime, err := agentruntime.NewSecureHTTPRuntime(nil, agentruntime.HTTPOptions{Policy: test.policy})
			if err != nil {
				t.Fatal(err)
			}
			_, err = executeRemote(context.Background(), runtime, test.endpoint)
			requireHTTPError(t, err, agentruntime.HTTPErrorPermanent, 0)
			if !errors.Is(err, agentruntime.ErrHTTPNetworkPolicy) {
				t.Fatalf("expected policy error, got %v", err)
			}
		})
	}
}

func TestSecureHTTPRuntimeAllowsConfiguredPrivateEndpoint(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		writeRuntimeResponse(t, response, protocolv1.RunResponse{ProtocolVersion: protocolv1.Version, RunID: "run_1", Status: protocolv1.StatusSucceeded})
	}))
	defer server.Close()
	runtime, err := agentruntime.NewSecureHTTPRuntime(server.Client(), agentruntime.HTTPOptions{Policy: agentruntime.DefaultHTTPPolicy()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := executeRemote(context.Background(), runtime, server.URL); err != nil {
		t.Fatal(err)
	}
}

func TestHTTPRuntimeLimitsRequestAndRejectsCompressedResponse(t *testing.T) {
	runtime, err := agentruntime.NewSecureHTTPRuntime(nil, agentruntime.HTTPOptions{MaxRequestBytes: 32, Policy: agentruntime.DefaultHTTPPolicy()})
	if err != nil {
		t.Fatal(err)
	}
	_, err = executeRemote(context.Background(), runtime, "http://127.0.0.1")
	requireHTTPError(t, err, agentruntime.HTTPErrorProtocol, 0)

	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Accept-Encoding") != "identity" {
			t.Errorf("unexpected Accept-Encoding: %q", request.Header.Get("Accept-Encoding"))
		}
		response.Header().Set("Content-Encoding", "gzip")
		response.Header().Set("Content-Type", "application/json")
		_, _ = response.Write([]byte("compressed"))
	}))
	defer server.Close()
	runtime, err = agentruntime.NewSecureHTTPRuntime(server.Client(), agentruntime.HTTPOptions{Policy: agentruntime.DefaultHTTPPolicy()})
	if err != nil {
		t.Fatal(err)
	}
	_, err = executeRemote(context.Background(), runtime, server.URL)
	requireHTTPError(t, err, agentruntime.HTTPErrorProtocol, 0)
}

func executeRemote(ctx context.Context, runtime agentruntime.Runtime, endpoint string) (agentruntime.ExecutionResult, error) {
	return runtime.Execute(ctx, agentruntime.ExecutionRequest{
		RunID: "run_1",
		Agent: domain.Agent{
			ID: "agt_1", Name: "remote", Runtime: agentruntime.RemoteRuntime, Protocol: agentruntime.HTTPProtocol, Endpoint: endpoint,
		},
		Attempt: 2,
		Input:   "hello",
	})
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func statusServer(status int) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		response.WriteHeader(status)
	}))
}

func writeRuntimeResponse(t *testing.T, response http.ResponseWriter, payload protocolv1.RunResponse) {
	t.Helper()
	response.Header().Set("Content-Type", "application/json; charset=utf-8")
	if err := json.NewEncoder(response).Encode(payload); err != nil {
		t.Errorf("encode response: %v", err)
	}
}

func requireHTTPError(t *testing.T, err error, kind agentruntime.HTTPErrorKind, status int) *agentruntime.HTTPError {
	t.Helper()
	var httpError *agentruntime.HTTPError
	if !errors.As(err, &httpError) {
		t.Fatalf("expected HTTPError, got %T %v", err, err)
	}
	if httpError.Kind != kind || httpError.StatusCode != status {
		t.Fatalf("unexpected HTTP error: want kind=%s status=%d, got %+v", kind, status, httpError)
	}
	return httpError
}
