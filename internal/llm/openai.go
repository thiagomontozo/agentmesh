package llm

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"strings"

	"github.com/thiagomontozo/agentmesh/internal/domain"
)

const DefaultMaxResponseBytes = int64(1 << 20)

type RequestAuthenticator interface {
	Authenticate(context.Context, domain.Agent, *http.Request) error
}

type NoAuthentication struct{}

func (NoAuthentication) Authenticate(context.Context, domain.Agent, *http.Request) error { return nil }

type ProviderError struct {
	StatusCode int
	Code       string
	Retry      bool
	Err        error
}

func (e *ProviderError) Error() string {
	message := "LLM provider error"
	if e.StatusCode != 0 {
		message += fmt.Sprintf(" (status %d)", e.StatusCode)
	}
	if e.Code != "" {
		message += " [" + e.Code + "]"
	}
	if e.Err != nil {
		message += ": " + e.Err.Error()
	}
	return message
}

func (e *ProviderError) Unwrap() error   { return e.Err }
func (e *ProviderError) Retryable() bool { return e.Retry }

type OpenAICompatible struct {
	client           *http.Client
	maxRequestBytes  int64
	maxResponseBytes int64
	authenticator    RequestAuthenticator
}

func NewOpenAICompatible(client *http.Client, maxRequestBytes, maxResponseBytes int64, authenticator RequestAuthenticator) *OpenAICompatible {
	if client == nil {
		client = &http.Client{}
	}
	clientCopy := *client
	clientCopy.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	if maxResponseBytes <= 0 {
		maxResponseBytes = DefaultMaxResponseBytes
	}
	if maxRequestBytes <= 0 {
		maxRequestBytes = DefaultMaxResponseBytes
	}
	if authenticator == nil {
		authenticator = NoAuthentication{}
	}
	return &OpenAICompatible{client: &clientCopy, maxRequestBytes: maxRequestBytes, maxResponseBytes: maxResponseBytes, authenticator: authenticator}
}

type chatRequest struct {
	Model    string        `json:"model"`
	Messages []chatMessage `json:"messages"`
}

type chatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type chatResponse struct {
	Choices []struct {
		Message chatMessage `json:"message"`
	} `json:"choices"`
	Usage struct {
		PromptTokens     int `json:"prompt_tokens"`
		CompletionTokens int `json:"completion_tokens"`
		TotalTokens      int `json:"total_tokens"`
	} `json:"usage"`
	Error *struct {
		Message string `json:"message"`
		Code    string `json:"code"`
	} `json:"error,omitempty"`
}

func (p *OpenAICompatible) Complete(ctx context.Context, request CompletionRequest) (CompletionResult, error) {
	if strings.TrimSpace(request.RunID) == "" || strings.TrimSpace(request.Agent.ID) == "" {
		return CompletionResult{}, permanentError("invalid_request", "Run ID and Agent ID are required")
	}
	if strings.TrimSpace(request.Agent.Model) == "" {
		return CompletionResult{}, permanentError("model_required", "Agent model is required")
	}
	endpoint, err := chatCompletionsURL(request.Agent.Endpoint)
	if err != nil {
		return CompletionResult{}, &ProviderError{Code: "invalid_endpoint", Err: err}
	}
	messages := make([]chatMessage, 0, 2)
	if systemPrompt := strings.TrimSpace(request.Agent.SystemPrompt); systemPrompt != "" {
		messages = append(messages, chatMessage{Role: "system", Content: systemPrompt})
	}
	messages = append(messages, chatMessage{Role: "user", Content: request.Input})
	payload, err := json.Marshal(chatRequest{Model: request.Agent.Model, Messages: messages})
	if err != nil {
		return CompletionResult{}, permanentError("encode_request", err.Error())
	}
	if int64(len(payload)) > p.maxRequestBytes {
		return CompletionResult{}, permanentError("request_too_large", fmt.Sprintf("request exceeds %d bytes", p.maxRequestBytes))
	}
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(payload))
	if err != nil {
		return CompletionResult{}, permanentError("invalid_request", err.Error())
	}
	httpRequest.Header.Set("Content-Type", "application/json")
	httpRequest.Header.Set("Accept", "application/json")
	httpRequest.Header.Set("Accept-Encoding", "identity")
	httpRequest.Header.Set("Idempotency-Key", fmt.Sprintf("%s:%d", request.RunID, request.Attempt))
	if err := p.authenticator.Authenticate(ctx, request.Agent, httpRequest); err != nil {
		return CompletionResult{}, &ProviderError{Code: "authentication_failed", Err: errors.New("outbound credential unavailable")}
	}
	response, err := p.client.Do(httpRequest)
	if err != nil {
		if ctx.Err() != nil {
			return CompletionResult{}, &ProviderError{Code: "canceled", Retry: errors.Is(ctx.Err(), context.DeadlineExceeded), Err: ctx.Err()}
		}
		return CompletionResult{}, &ProviderError{Code: "transport_error", Retry: true, Err: err}
	}
	defer response.Body.Close()
	if encoding := strings.ToLower(strings.TrimSpace(response.Header.Get("Content-Encoding"))); encoding != "" && encoding != "identity" {
		return CompletionResult{}, permanentError("invalid_response", "compressed responses are not supported")
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, p.maxResponseBytes+1))
	if err != nil {
		return CompletionResult{}, &ProviderError{Code: "read_error", Retry: true, Err: err}
	}
	if int64(len(body)) > p.maxResponseBytes {
		return CompletionResult{}, permanentError("response_too_large", fmt.Sprintf("response exceeds %d bytes", p.maxResponseBytes))
	}
	var decoded chatResponse
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		_ = json.Unmarshal(body, &decoded)
		code := "http_error"
		message := http.StatusText(response.StatusCode)
		if decoded.Error != nil {
			if strings.TrimSpace(decoded.Error.Code) != "" {
				code = decoded.Error.Code
			}
			if strings.TrimSpace(decoded.Error.Message) != "" {
				message = decoded.Error.Message
			}
		}
		return CompletionResult{}, &ProviderError{
			StatusCode: response.StatusCode, Code: code,
			Retry: response.StatusCode == http.StatusTooManyRequests || response.StatusCode >= 500,
			Err:   errors.New(message),
		}
	}
	mediaType, _, mediaErr := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if mediaErr != nil || mediaType != "application/json" {
		return CompletionResult{}, permanentError("invalid_content_type", "response Content-Type must be application/json")
	}
	if len(bytes.TrimSpace(body)) == 0 || json.Unmarshal(body, &decoded) != nil {
		return CompletionResult{}, permanentError("invalid_json", "response must contain valid JSON")
	}
	if len(decoded.Choices) == 0 || strings.TrimSpace(decoded.Choices[0].Message.Content) == "" {
		return CompletionResult{}, permanentError("empty_completion", "response has no assistant content")
	}
	return CompletionResult{
		Output: decoded.Choices[0].Message.Content,
		Usage:  Usage{InputTokens: decoded.Usage.PromptTokens, OutputTokens: decoded.Usage.CompletionTokens, TotalTokens: decoded.Usage.TotalTokens},
	}, nil
}

func chatCompletionsURL(endpoint string) (string, error) {
	parsed, err := url.Parse(strings.TrimSpace(endpoint))
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return "", fmt.Errorf("LLM endpoint must be an absolute HTTP URL")
	}
	parsed.Scheme = strings.ToLower(parsed.Scheme)
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return "", fmt.Errorf("LLM endpoint must use HTTP or HTTPS")
	}
	if parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", fmt.Errorf("LLM endpoint cannot contain query or fragment")
	}
	if parsed.User != nil {
		return "", fmt.Errorf("LLM endpoint cannot contain credentials")
	}
	path := strings.TrimRight(parsed.Path, "/")
	if strings.HasSuffix(path, "/v1") {
		path += "/chat/completions"
	} else {
		path += "/v1/chat/completions"
	}
	parsed.Path = path
	return parsed.String(), nil
}

func permanentError(code, message string) error {
	return &ProviderError{Code: code, Err: errors.New(message)}
}

var _ Provider = (*OpenAICompatible)(nil)
