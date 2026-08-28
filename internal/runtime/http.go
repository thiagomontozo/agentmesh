package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	protocolv1 "github.com/thiagomontozo/agentmesh/internal/protocol/v1"
)

const (
	RemoteRuntime           = "remote"
	HTTPProtocol            = "http"
	DefaultHTTPTimeout      = 30 * time.Second
	DefaultMaxResponseBytes = int64(1 << 20)
)

type HTTPErrorKind string

const (
	HTTPErrorTemporary HTTPErrorKind = "temporary"
	HTTPErrorPermanent HTTPErrorKind = "permanent"
	HTTPErrorTimeout   HTTPErrorKind = "timeout"
	HTTPErrorProtocol  HTTPErrorKind = "protocol"
	HTTPErrorCanceled  HTTPErrorKind = "canceled"
)

// HTTPError classifies remote execution failures without making the Engine
// depend on HTTP details. StatusCode and Code are populated when available.
type HTTPError struct {
	Kind       HTTPErrorKind
	StatusCode int
	Code       string
	Err        error
}

func (e *HTTPError) Error() string {
	detail := string(e.Kind) + " HTTP runtime error"
	if e.StatusCode != 0 {
		detail += fmt.Sprintf(" (status %d)", e.StatusCode)
	}
	if e.Code != "" {
		detail += fmt.Sprintf(" [%s]", e.Code)
	}
	if e.Err != nil {
		detail += ": " + e.Err.Error()
	}
	return detail
}

// Unwrap preserves explicit cancellation for graceful shutdown. Timeout errors
// remain classified by Kind so the current Engine can apply its normal retry
// policy; per-attempt timeout lifecycle is introduced separately.
func (e *HTTPError) Unwrap() error {
	if e.Kind == HTTPErrorTimeout {
		return nil
	}
	return e.Err
}

func (e *HTTPError) Retryable() bool {
	return e.Kind == HTTPErrorTemporary || e.Kind == HTTPErrorTimeout
}

type HTTPRuntime struct {
	client           *http.Client
	maxResponseBytes int64
}

// NewHTTPRuntime clones the supplied client and always disables redirects. A
// nil client and non-positive response limit select conservative defaults.
func NewHTTPRuntime(client *http.Client, maxResponseBytes int64) *HTTPRuntime {
	if client == nil {
		client = &http.Client{Timeout: DefaultHTTPTimeout}
	}
	clientCopy := *client
	clientCopy.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}
	if maxResponseBytes <= 0 {
		maxResponseBytes = DefaultMaxResponseBytes
	}
	return &HTTPRuntime{client: &clientCopy, maxResponseBytes: maxResponseBytes}
}

func (h *HTTPRuntime) Execute(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
	if strings.ToLower(strings.TrimSpace(request.Agent.Protocol)) != HTTPProtocol {
		return ExecutionResult{}, protocolError("agent protocol must be %q", HTTPProtocol)
	}
	executionURL, err := remoteExecutionURL(request.Agent.Endpoint)
	if err != nil {
		return ExecutionResult{}, &HTTPError{Kind: HTTPErrorPermanent, Err: err}
	}

	wireRequest := protocolv1.RunRequest{
		ProtocolVersion: protocolv1.Version,
		RunID:           request.RunID,
		AgentID:         request.AgentID(),
		Attempt:         request.Attempt,
		IdempotencyKey:  protocolv1.AttemptIdempotencyKey(request.RunID, request.Attempt),
		Input:           request.Input,
	}
	if err := wireRequest.Validate(); err != nil {
		return ExecutionResult{}, &HTTPError{Kind: HTTPErrorProtocol, Err: err}
	}
	payload, err := json.Marshal(wireRequest)
	if err != nil {
		return ExecutionResult{}, &HTTPError{Kind: HTTPErrorProtocol, Err: fmt.Errorf("encode request: %w", err)}
	}

	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, executionURL, bytes.NewReader(payload))
	if err != nil {
		return ExecutionResult{}, &HTTPError{Kind: HTTPErrorPermanent, Err: fmt.Errorf("create request: %w", err)}
	}
	httpRequest.Header.Set("Content-Type", "application/json")
	httpRequest.Header.Set("Accept", "application/json")
	httpRequest.Header.Set("Idempotency-Key", wireRequest.IdempotencyKey)

	response, err := h.client.Do(httpRequest)
	if err != nil {
		return ExecutionResult{}, classifyTransportError(ctx, err)
	}
	defer response.Body.Close()

	responsePayload, err := io.ReadAll(io.LimitReader(response.Body, h.maxResponseBytes+1))
	if err != nil {
		return ExecutionResult{}, classifyTransportError(ctx, fmt.Errorf("read response: %w", err))
	}
	if int64(len(responsePayload)) > h.maxResponseBytes {
		return ExecutionResult{}, protocolError("response exceeds %d bytes", h.maxResponseBytes)
	}
	if response.StatusCode != http.StatusOK {
		return ExecutionResult{}, statusError(response.StatusCode)
	}
	if len(bytes.TrimSpace(responsePayload)) == 0 {
		return ExecutionResult{}, protocolError("empty response")
	}
	mediaType, _, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		return ExecutionResult{}, protocolError("response Content-Type must be application/json")
	}

	var wireResponse protocolv1.RunResponse
	if err := json.Unmarshal(responsePayload, &wireResponse); err != nil {
		return ExecutionResult{}, protocolError("decode response: %v", err)
	}
	if err := wireResponse.Validate(); err != nil {
		return ExecutionResult{}, &HTTPError{Kind: HTTPErrorProtocol, Err: err}
	}
	if wireResponse.RunID != request.RunID {
		return ExecutionResult{}, protocolError("response run_id %q does not match request", wireResponse.RunID)
	}
	if wireResponse.Status == protocolv1.StatusFailed {
		kind := HTTPErrorPermanent
		if wireResponse.Error.Retryable {
			kind = HTTPErrorTemporary
		}
		return ExecutionResult{}, &HTTPError{
			Kind: kind, Code: wireResponse.Error.Code, Err: errors.New(wireResponse.Error.Message),
		}
	}
	return ExecutionResult{Output: wireResponse.Output}, nil
}

func remoteExecutionURL(endpoint string) (string, error) {
	parsed, err := url.Parse(strings.TrimSpace(endpoint))
	if err != nil {
		return "", fmt.Errorf("invalid agent endpoint: %w", err)
	}
	parsed.Scheme = strings.ToLower(parsed.Scheme)
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return "", fmt.Errorf("agent endpoint must use http or https")
	}
	if parsed.Host == "" {
		return "", fmt.Errorf("agent endpoint must be absolute")
	}
	if parsed.User != nil {
		return "", fmt.Errorf("agent endpoint cannot contain credentials")
	}
	if parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", fmt.Errorf("agent endpoint cannot contain query or fragment")
	}
	joined, err := url.JoinPath(parsed.String(), "v1/runs")
	if err != nil {
		return "", fmt.Errorf("build agent execution URL: %w", err)
	}
	return joined, nil
}

func statusError(statusCode int) error {
	kind := HTTPErrorPermanent
	if statusCode == http.StatusRequestTimeout || statusCode == http.StatusTooManyRequests || statusCode >= 500 {
		kind = HTTPErrorTemporary
	}
	return &HTTPError{Kind: kind, StatusCode: statusCode, Err: fmt.Errorf("unexpected HTTP status")}
}

func classifyTransportError(ctx context.Context, err error) error {
	if errors.Is(ctx.Err(), context.Canceled) || errors.Is(err, context.Canceled) {
		return &HTTPError{Kind: HTTPErrorCanceled, Err: context.Canceled}
	}
	if errors.Is(ctx.Err(), context.DeadlineExceeded) || errors.Is(err, context.DeadlineExceeded) {
		return &HTTPError{Kind: HTTPErrorTimeout, Err: err}
	}
	var networkError net.Error
	if errors.As(err, &networkError) && networkError.Timeout() {
		return &HTTPError{Kind: HTTPErrorTimeout, Err: err}
	}
	return &HTTPError{Kind: HTTPErrorTemporary, Err: err}
}

func protocolError(format string, arguments ...any) error {
	return &HTTPError{Kind: HTTPErrorProtocol, Err: fmt.Errorf(format, arguments...)}
}

var _ Runtime = (*HTTPRuntime)(nil)
