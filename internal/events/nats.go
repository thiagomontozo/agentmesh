package events

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/thiagomontozo/agentmesh/internal/domain"
)

const runEventsSubject = "agentmesh.events.runs"

type NATS struct {
	conn      *nats.Conn
	local     *Bus
	closeOnce sync.Once
	closeErr  error
}

func NewNATS(url string) (*NATS, error) {
	conn, err := nats.Connect(
		url,
		nats.Name("agentmesh-events"),
		nats.Timeout(5*time.Second),
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(-1),
		nats.NoEcho(),
	)
	if err != nil {
		return nil, fmt.Errorf("connect NATS event bus: %w", err)
	}
	bus := &NATS{conn: conn, local: NewBus()}
	if _, err := conn.Subscribe(runEventsSubject, bus.receive); err != nil {
		conn.Close()
		return nil, fmt.Errorf("subscribe NATS event bus: %w", err)
	}
	if err := conn.FlushTimeout(5 * time.Second); err != nil {
		conn.Close()
		return nil, fmt.Errorf("initialize NATS event bus: %w", err)
	}
	return bus, nil
}

func (b *NATS) Publish(event domain.RunEvent) {
	b.local.Publish(event)
	payload, err := json.Marshal(event)
	if err != nil {
		slog.Error("run event encoding failed", "run_id", event.RunID, "type", event.Type, "error", err)
		return
	}
	if err := b.conn.Publish(runEventsSubject, payload); err != nil {
		slog.Error("run event publish failed", "run_id", event.RunID, "type", event.Type, "error", err)
	}
}

func (b *NATS) Subscribe(runID string) (<-chan domain.RunEvent, func()) {
	return b.local.Subscribe(runID)
}

func (b *NATS) receive(message *nats.Msg) {
	var event domain.RunEvent
	if err := json.Unmarshal(message.Data, &event); err != nil {
		slog.Warn("invalid distributed run event", "error", err)
		return
	}
	if event.RunID == "" || event.Type == "" {
		slog.Warn("incomplete distributed run event", "run_id", event.RunID, "type", event.Type)
		return
	}
	b.local.Publish(event)
}

func (b *NATS) Close() error {
	b.closeOnce.Do(func() {
		if b.conn == nil || b.conn.IsClosed() {
			return
		}
		if err := b.conn.Drain(); err != nil {
			b.conn.Close()
			b.closeErr = fmt.Errorf("drain NATS event bus: %w", err)
		}
	})
	return b.closeErr
}

var _ Broker = (*NATS)(nil)
