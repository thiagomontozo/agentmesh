package runtime

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strings"
	"time"

	protocolv1 "github.com/thiagomontozo/agentmesh/internal/protocol/v1"
)

const (
	RemoteRuntime           = "remote"
	HTTPProtocol            = "http"
	DefaultHTTPTimeout      = 30 * time.Second
	DefaultMaxRequestBytes  = int64(1 << 20)
	DefaultMaxResponseBytes = int64(1 << 20)
)

var ErrHTTPNetworkPolicy = errors.New("HTTP runtime network policy denied endpoint")

// HTTPPolicy controls the network destinations available to remote Agents.
// Private and loopback destinations remain allowed by default because the
// control plane is designed to call internal services. Link-local destinations,
// including common cloud metadata endpoints, are denied by default.
type HTTPPolicy struct {
	RequireHTTPS   bool
	AllowPrivate   bool
	AllowLoopback  bool
	AllowLinkLocal bool
	AllowedHosts   []string
	BlockedCIDRs   []netip.Prefix
}

func DefaultHTTPPolicy() HTTPPolicy {
	return HTTPPolicy{AllowPrivate: true, AllowLoopback: true}
}

type HTTPOptions struct {
	MaxRequestBytes  int64
	MaxResponseBytes int64
	Policy           HTTPPolicy
}

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
	maxRequestBytes  int64
	maxResponseBytes int64
	policy           HTTPPolicy
}

// NewHTTPRuntime clones the supplied client and always disables redirects. A
// nil client and non-positive response limit select conservative defaults.
func NewHTTPRuntime(client *http.Client, maxResponseBytes int64) *HTTPRuntime {
	runtime, _ := newHTTPRuntime(client, HTTPOptions{MaxResponseBytes: maxResponseBytes, Policy: DefaultHTTPPolicy()}, false)
	return runtime
}

// NewSecureHTTPRuntime creates the production HTTP runtime. It rejects custom
// RoundTrippers that cannot be wrapped with destination checks.
func NewSecureHTTPRuntime(client *http.Client, options HTTPOptions) (*HTTPRuntime, error) {
	return newHTTPRuntime(client, options, true)
}

func newHTTPRuntime(client *http.Client, options HTTPOptions, requirePolicyTransport bool) (*HTTPRuntime, error) {
	if client == nil {
		client = &http.Client{Timeout: DefaultHTTPTimeout}
	}
	clientCopy := *client
	clientCopy.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}
	if options.MaxRequestBytes <= 0 {
		options.MaxRequestBytes = DefaultMaxRequestBytes
	}
	if options.MaxResponseBytes <= 0 {
		options.MaxResponseBytes = DefaultMaxResponseBytes
	}
	if err := normalizeHTTPPolicy(&options.Policy); err != nil {
		return nil, err
	}
	transport, ok := clientCopy.Transport.(*http.Transport)
	if clientCopy.Transport == nil {
		transport = http.DefaultTransport.(*http.Transport)
		ok = true
	}
	if ok {
		transport = secureTransport(transport, options.Policy)
		clientCopy.Transport = transport
	} else if requirePolicyTransport {
		return nil, fmt.Errorf("secure HTTP runtime requires *http.Transport")
	}
	return &HTTPRuntime{
		client: &clientCopy, maxRequestBytes: options.MaxRequestBytes,
		maxResponseBytes: options.MaxResponseBytes, policy: options.Policy,
	}, nil
}

func (h *HTTPRuntime) Execute(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
	if strings.ToLower(strings.TrimSpace(request.Agent.Protocol)) != HTTPProtocol {
		return ExecutionResult{}, protocolError("agent protocol must be %q", HTTPProtocol)
	}
	executionURL, err := remoteExecutionURL(request.Agent.Endpoint, h.policy)
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
	if int64(len(payload)) > h.maxRequestBytes {
		return ExecutionResult{}, protocolError("request exceeds %d bytes", h.maxRequestBytes)
	}

	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, executionURL, bytes.NewReader(payload))
	if err != nil {
		return ExecutionResult{}, &HTTPError{Kind: HTTPErrorPermanent, Err: fmt.Errorf("create request: %w", err)}
	}
	httpRequest.Header.Set("Content-Type", "application/json")
	httpRequest.Header.Set("Accept", "application/json")
	httpRequest.Header.Set("Accept-Encoding", "identity")
	httpRequest.Header.Set("Idempotency-Key", wireRequest.IdempotencyKey)

	response, err := h.client.Do(httpRequest)
	if err != nil {
		return ExecutionResult{}, classifyTransportError(ctx, err)
	}
	defer response.Body.Close()
	if encoding := strings.ToLower(strings.TrimSpace(response.Header.Get("Content-Encoding"))); encoding != "" && encoding != "identity" {
		return ExecutionResult{}, protocolError("response Content-Encoding %q is not supported", encoding)
	}

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

func remoteExecutionURL(endpoint string, policy HTTPPolicy) (string, error) {
	parsed, err := url.Parse(strings.TrimSpace(endpoint))
	if err != nil {
		return "", fmt.Errorf("invalid agent endpoint: %w", err)
	}
	parsed.Scheme = strings.ToLower(parsed.Scheme)
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return "", fmt.Errorf("agent endpoint must use http or https")
	}
	if policy.RequireHTTPS && parsed.Scheme != "https" {
		return "", fmt.Errorf("%w: endpoint must use https", ErrHTTPNetworkPolicy)
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
	if err := policy.validateHost(parsed.Hostname()); err != nil {
		return "", err
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
	if errors.Is(err, ErrHTTPNetworkPolicy) {
		return &HTTPError{Kind: HTTPErrorPermanent, Err: ErrHTTPNetworkPolicy}
	}
	var networkError net.Error
	if errors.As(err, &networkError) && networkError.Timeout() {
		return &HTTPError{Kind: HTTPErrorTimeout, Err: err}
	}
	return &HTTPError{Kind: HTTPErrorTemporary, Err: err}
}

func normalizeHTTPPolicy(policy *HTTPPolicy) error {
	for index, host := range policy.AllowedHosts {
		host = strings.ToLower(strings.TrimSpace(host))
		if strings.HasPrefix(host, "*.") {
			host = strings.TrimPrefix(host, "*.")
			if host == "" {
				return fmt.Errorf("allowed host wildcard requires a suffix")
			}
			policy.AllowedHosts[index] = "*." + host
		} else {
			policy.AllowedHosts[index] = host
		}
		if host == "" || strings.ContainsAny(host, "/:@?# \\") {
			return fmt.Errorf("invalid allowed host %q", host)
		}
	}
	for index, prefix := range policy.BlockedCIDRs {
		if !prefix.IsValid() {
			return fmt.Errorf("invalid blocked CIDR at index %d", index)
		}
		policy.BlockedCIDRs[index] = prefix.Masked()
	}
	return nil
}

func (policy HTTPPolicy) validateHost(host string) error {
	host = strings.ToLower(strings.TrimSuffix(strings.TrimSpace(host), "."))
	if host == "" {
		return fmt.Errorf("%w: endpoint hostname is empty", ErrHTTPNetworkPolicy)
	}
	if len(policy.AllowedHosts) > 0 && !hostAllowed(host, policy.AllowedHosts) {
		return fmt.Errorf("%w: hostname is not allowed", ErrHTTPNetworkPolicy)
	}
	if address, err := netip.ParseAddr(host); err == nil {
		return policy.validateAddress(address)
	}
	return nil
}

func hostAllowed(host string, allowed []string) bool {
	for _, candidate := range allowed {
		if strings.HasPrefix(candidate, "*.") {
			suffix := strings.TrimPrefix(candidate, "*")
			if strings.HasSuffix(host, suffix) && host != strings.TrimPrefix(suffix, ".") {
				return true
			}
		} else if host == candidate {
			return true
		}
	}
	return false
}

func (policy HTTPPolicy) validateAddress(address netip.Addr) error {
	address = address.Unmap()
	for _, prefix := range policy.BlockedCIDRs {
		if prefix.Contains(address) {
			return fmt.Errorf("%w: address is in a blocked CIDR", ErrHTTPNetworkPolicy)
		}
	}
	if address.IsLinkLocalUnicast() || address.IsLinkLocalMulticast() {
		if !policy.AllowLinkLocal {
			return fmt.Errorf("%w: link-local address is disabled", ErrHTTPNetworkPolicy)
		}
	}
	if address.IsLoopback() && !policy.AllowLoopback {
		return fmt.Errorf("%w: loopback address is disabled", ErrHTTPNetworkPolicy)
	}
	if address.IsPrivate() && !policy.AllowPrivate {
		return fmt.Errorf("%w: private address is disabled", ErrHTTPNetworkPolicy)
	}
	return nil
}

func secureTransport(base *http.Transport, policy HTTPPolicy) *http.Transport {
	transport := base.Clone()
	transport.Proxy = nil
	transport.DisableCompression = true
	if transport.TLSClientConfig == nil {
		transport.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	} else {
		transport.TLSClientConfig = transport.TLSClientConfig.Clone()
		if transport.TLSClientConfig.MinVersion < tls.VersionTLS12 {
			transport.TLSClientConfig.MinVersion = tls.VersionTLS12
		}
	}
	baseDial := transport.DialContext
	if baseDial == nil {
		baseDial = (&net.Dialer{}).DialContext
	}
	transport.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(address)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid dial address", ErrHTTPNetworkPolicy)
		}
		addresses, err := net.DefaultResolver.LookupNetIP(ctx, "ip", host)
		if err != nil {
			return nil, err
		}
		var dialErrors []error
		for _, candidate := range addresses {
			if err := policy.validateAddress(candidate); err != nil {
				dialErrors = append(dialErrors, err)
				continue
			}
			connection, err := baseDial(ctx, network, net.JoinHostPort(candidate.String(), port))
			if err == nil {
				return connection, nil
			}
			dialErrors = append(dialErrors, err)
		}
		if len(dialErrors) == 0 {
			return nil, fmt.Errorf("resolve %s: no addresses", host)
		}
		return nil, errors.Join(dialErrors...)
	}
	return transport
}

func protocolError(format string, arguments ...any) error {
	return &HTTPError{Kind: HTTPErrorProtocol, Err: fmt.Errorf(format, arguments...)}
}

var _ Runtime = (*HTTPRuntime)(nil)
