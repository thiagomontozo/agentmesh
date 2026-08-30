package mcp

import "context"

type Gateway struct {
	registry *Registry
	client   *Client
}

func NewGateway(registry *Registry, client *Client) *Gateway {
	if registry == nil {
		registry = NewRegistry()
	}
	return &Gateway{registry: registry, client: client}
}

func (g *Gateway) Servers() []ServerView { return g.registry.List() }

func (g *Gateway) ListTools(ctx context.Context, serverID, cursor string) (ListToolsResult, error) {
	server, err := g.registry.Get(serverID)
	if err != nil {
		return ListToolsResult{}, err
	}
	return g.client.ListTools(ctx, server, cursor)
}

func (g *Gateway) CallTool(ctx context.Context, serverID, name string, arguments map[string]any) (CallToolResult, error) {
	server, err := g.registry.Get(serverID)
	if err != nil {
		return CallToolResult{}, err
	}
	return g.client.CallTool(ctx, server, name, arguments)
}
