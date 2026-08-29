package events

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/thiagomontozo/agentmesh/internal/domain"
	"github.com/thiagomontozo/agentmesh/internal/store"
)

const runEventsSubject = "agentmesh.events.runs"

type NATS struct {
	conn      *nats.Conn
	local     *Bus
	history   store.EventRepository
	retention time.Duration
	limit     int
	closeOnce sync.Once
	closeErr  error
}

func NewNATS(url string) (*NATS, error) {
	return newNATS(url, nil, 0, 128)
}

func NewPersistentNATS(url string, history store.EventRepository, retention time.Duration, limit int) (*NATS, error) {
	if history == nil {
		return nil, fmt.Errorf("persistent event history repository is required")
	}
	if retention <= 0 {
		return nil, fmt.Errorf("event history retention must be positive")
	}
	if limit < 1 {
		return nil, fmt.Errorf("event history limit must be positive")
	}
	return newNATS(url, history, retention, limit)
}

func newNATS(url string, history store.EventRepository, retention time.Duration, limit int) (*NATS, error) {
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
	bus := &NATS{conn: conn, local: NewBusWithHistoryLimit(limit), history: history, retention: retention, limit: limit}
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
	event = prepare(event)
	if b.history != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		err := b.history.AppendRunEvent(ctx, event, b.retention, b.limit)
		cancel()
		if err != nil {
			slog.Error("run event persistence failed", "event_id", event.ID, "run_id", event.RunID, "type", event.Type, "error", err)
		}
	}
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
	if b.history != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		persisted, err := b.history.ListRunEvents(ctx, runID, b.limit)
		cancel()
		if err == nil {
			return b.local.SubscribeWithHistory(runID, persisted)
		}
		slog.Error("run event replay failed", "run_id", runID, "error", err)
	}
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
