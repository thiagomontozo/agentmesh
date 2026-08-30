package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/thiagomontozo/agentmesh/internal/domain"
)

var (
	ErrProtocol = errors.New("invalid MCP response")
	ErrTimeout  = errors.New("MCP tool call timed out")
)

type Tool struct {
	Name         string          `json:"name"`
	Title        string          `json:"title,omitempty"`
	Description  string          `json:"description,omitempty"`
	InputSchema  json.RawMessage `json:"inputSchema"`
	OutputSchema json.RawMessage `json:"outputSchema,omitempty"`
	Annotations  json.RawMessage `json:"annotations,omitempty"`
}

type ListToolsResult struct {
	ResultType string `json:"resultType,omitempty"`
	Tools      []Tool `json:"tools"`
	NextCursor string `json:"nextCursor,omitempty"`
	TTLMS      int64  `json:"ttlMs,omitempty"`
	CacheScope string `json:"cacheScope,omitempty"`
}

type CallToolResult struct {
	ResultType        string                     `json:"resultType,omitempty"`
	Content           []map[string]any           `json:"content,omitempty"`
	StructuredContent map[string]any             `json:"structuredContent,omitempty"`
	IsError           bool                       `json:"isError,omitempty"`
	InputRequests     map[string]json.RawMessage `json:"inputRequests,omitempty"`
	RequestState      string                     `json:"requestState,omitempty"`
}

type AgentAuthenticator interface {
	Authenticate(context.Context, domain.Agent, *http.Request) error
}

type Client struct {
	httpClient       *http.Client
	maxRequestBytes  int64
	maxResponseBytes int64
	authenticator    AgentAuthenticator
	sequence         atomic.Uint64
}

func NewClient(httpClient *http.Client, maxRequestBytes, maxResponseBytes int64, authenticator AgentAuthenticator) *Client {
	if httpClient == nil {
		httpClient = &http.Client{}
	}
	clientCopy := *httpClient
	clientCopy.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	if maxRequestBytes <= 0 {
		maxRequestBytes = 1 << 20
	}
	if maxResponseBytes <= 0 {
		maxResponseBytes = 1 << 20
	}
	return &Client{httpClient: &clientCopy, maxRequestBytes: maxRequestBytes, maxResponseBytes: maxResponseBytes, authenticator: authenticator}
}

type rpcRequest struct {
	JSONRPC string `json:"jsonrpc"`
	ID      uint64 `json:"id"`
	Method  string `json:"method"`
	Params  any    `json:"params"`
}

type rpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  json.RawMessage `json:"result"`
	Error   *struct {
		Code    int             `json:"code"`
		Message string          `json:"message"`
		Data    json.RawMessage `json:"data,omitempty"`
	} `json:"error,omitempty"`
}

func (c *Client) ListTools(ctx context.Context, server Server, cursor string) (ListToolsResult, error) {
	params := map[string]any{"_meta": requestMeta()}
	if cursor = strings.TrimSpace(cursor); cursor != "" {
		params["cursor"] = cursor
	}
	var result ListToolsResult
	if err := c.call(ctx, server, "tools/list", "", params, &result); err != nil {
		return ListToolsResult{}, err
	}
	filtered := result.Tools[:0]
	for _, tool := range result.Tools {
		if !validName(tool.Name) {
			return ListToolsResult{}, fmt.Errorf("%w: invalid tool name %q", ErrProtocol, tool.Name)
		}
		if server.Allows(tool.Name) {
			filtered = append(filtered, tool)
		}
	}
	result.Tools = filtered
	return result, nil
}

func (c *Client) CallTool(ctx context.Context, server Server, name string, arguments map[string]any) (CallToolResult, error) {
	name = strings.TrimSpace(name)
	if !validName(name) {
		return CallToolResult{}, fmt.Errorf("invalid MCP tool name %q", name)
	}
	if !server.Allows(name) {
		return CallToolResult{}, fmt.Errorf("%w: %s/%s", ErrToolDenied, server.ID, name)
	}
	if arguments == nil {
		arguments = map[string]any{}
	}
	params := map[string]any{"name": name, "arguments": arguments, "_meta": requestMeta()}
	var result CallToolResult
	if err := c.call(ctx, server, "tools/call", name, params, &result); err != nil {
		return CallToolResult{}, err
	}
	return result, nil
}

func (c *Client) call(parent context.Context, server Server, method, name string, params any, target any) error {
	ctx, cancel := context.WithTimeout(parent, server.Timeout)
	defer cancel()
	id := c.sequence.Add(1)
	payload, err := json.Marshal(rpcRequest{JSONRPC: "2.0", ID: id, Method: method, Params: params})
	if err != nil {
		return err
	}
	if int64(len(payload)) > c.maxRequestBytes {
		return fmt.Errorf("MCP request exceeds %d bytes", c.maxRequestBytes)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, server.Endpoint, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json, text/event-stream")
	request.Header.Set("Accept-Encoding", "identity")
	request.Header.Set("MCP-Protocol-Version", ProtocolVersion)
	request.Header.Set("Mcp-Method", method)
	if name != "" {
		request.Header.Set("Mcp-Name", name)
	}
	if c.authenticator != nil {
		if err := c.authenticator.Authenticate(ctx, domain.Agent{ID: "mcp:" + server.ID}, request); err != nil {
			return errors.New("MCP authentication failed")
		}
	}
	response, err := c.httpClient.Do(request)
	if err != nil {
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return fmt.Errorf("%w after %s", ErrTimeout, server.Timeout)
		}
		return err
	}
	defer response.Body.Close()
	if encoding := strings.ToLower(strings.TrimSpace(response.Header.Get("Content-Encoding"))); encoding != "" && encoding != "identity" {
		return fmt.Errorf("%w: compressed responses are not supported", ErrProtocol)
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, c.maxResponseBytes+1))
	if err != nil {
		return err
	}
	if int64(len(body)) > c.maxResponseBytes {
		return fmt.Errorf("%w: response exceeds %d bytes", ErrProtocol, c.maxResponseBytes)
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("MCP server returned HTTP %d", response.StatusCode)
	}
	mediaType, _, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		return fmt.Errorf("%w: only application/json responses are supported", ErrProtocol)
	}
	var envelope rpcResponse
	if err := json.Unmarshal(body, &envelope); err != nil {
		return fmt.Errorf("%w: %v", ErrProtocol, err)
	}
	if envelope.JSONRPC != "2.0" || strings.TrimSpace(string(envelope.ID)) != strconv.FormatUint(id, 10) {
		return fmt.Errorf("%w: JSON-RPC version or response ID mismatch", ErrProtocol)
	}
	if envelope.Error != nil {
		return fmt.Errorf("MCP JSON-RPC error %d: %s", envelope.Error.Code, envelope.Error.Message)
	}
	if len(envelope.Result) == 0 || string(envelope.Result) == "null" {
		return fmt.Errorf("%w: missing result", ErrProtocol)
	}
	if err := json.Unmarshal(envelope.Result, target); err != nil {
		return fmt.Errorf("%w: decode result: %v", ErrProtocol, err)
	}
	return nil
}

func requestMeta() map[string]any {
	return map[string]any{
		"io.modelcontextprotocol/protocolVersion":    ProtocolVersion,
		"io.modelcontextprotocol/clientInfo":         map[string]string{"name": "agentmesh", "version": "1"},
		"io.modelcontextprotocol/clientCapabilities": map[string]any{},
	}
}
