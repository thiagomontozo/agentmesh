package events

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	"github.com/thiagomontozo/agentmesh/internal/domain"
)

type Broker interface {
	Publish(event domain.RunEvent)
	Subscribe(runID string) (<-chan domain.RunEvent, func())
}

type Bus struct {
	mu          sync.RWMutex
	subscribers map[string]map[chan domain.RunEvent]struct{}
	history     map[string][]domain.RunEvent
	seen        map[string]map[string]struct{}
	maxHistory  int
}

var _ Broker = (*Bus)(nil)

func NewBus() *Bus {
	return NewBusWithHistoryLimit(128)
}

func NewBusWithHistoryLimit(maxHistory int) *Bus {
	return &Bus{
		subscribers: make(map[string]map[chan domain.RunEvent]struct{}),
		history:     make(map[string][]domain.RunEvent),
		seen:        make(map[string]map[string]struct{}),
		maxHistory:  maxHistory,
	}
}

func (b *Bus) Publish(event domain.RunEvent) {
	event = prepare(event)
	b.mu.Lock()
	if b.hasSeen(event) {
		b.mu.Unlock()
		return
	}
	b.remember(event)
	b.history[event.RunID] = append(b.history[event.RunID], event)
	if b.maxHistory >= 0 && len(b.history[event.RunID]) > b.maxHistory {
		start := len(b.history[event.RunID]) - b.maxHistory
		b.replaceHistory(event.RunID, b.history[event.RunID][start:])
	}
	for ch := range b.subscribers[event.RunID] {
		select {
		case ch <- event:
		default:
		}
	}
	b.mu.Unlock()
}

func (b *Bus) Subscribe(runID string) (<-chan domain.RunEvent, func()) {
	return b.subscribe(runID, nil)
}

func (b *Bus) SubscribeWithHistory(runID string, persisted []domain.RunEvent) (<-chan domain.RunEvent, func()) {
	return b.subscribe(runID, persisted)
}

func (b *Bus) subscribe(runID string, persisted []domain.RunEvent) (<-chan domain.RunEvent, func()) {
	b.mu.Lock()
	merged := make([]domain.RunEvent, 0, len(persisted)+len(b.history[runID]))
	mergedIDs := make(map[string]struct{}, cap(merged))
	for _, event := range persisted {
		event = prepare(event)
		if _, exists := mergedIDs[event.ID]; exists {
			continue
		}
		merged = append(merged, event)
		mergedIDs[event.ID] = struct{}{}
	}
	for _, event := range b.history[runID] {
		if _, exists := mergedIDs[event.ID]; exists {
			continue
		}
		merged = append(merged, event)
		mergedIDs[event.ID] = struct{}{}
	}
	history := merged
	if b.maxHistory >= 0 && len(history) > b.maxHistory {
		history = history[len(history)-b.maxHistory:]
	}
	b.replaceHistory(runID, history)
	ch := make(chan domain.RunEvent, len(history)+16)
	for _, event := range history {
		ch <- event
	}
	if b.subscribers[runID] == nil {
		b.subscribers[runID] = make(map[chan domain.RunEvent]struct{})
	}
	b.subscribers[runID][ch] = struct{}{}
	b.mu.Unlock()

	unsubscribe := func() {
		b.mu.Lock()
		if subscribers, ok := b.subscribers[runID]; ok {
			delete(subscribers, ch)
			if len(subscribers) == 0 {
				delete(b.subscribers, runID)
			}
		}
		b.mu.Unlock()
	}

	return ch, unsubscribe
}

func (b *Bus) hasSeen(event domain.RunEvent) bool {
	_, ok := b.seen[event.RunID][event.ID]
	return ok
}

func (b *Bus) remember(event domain.RunEvent) {
	if b.seen[event.RunID] == nil {
		b.seen[event.RunID] = make(map[string]struct{})
	}
	b.seen[event.RunID][event.ID] = struct{}{}
}

func (b *Bus) replaceHistory(runID string, history []domain.RunEvent) {
	b.history[runID] = append([]domain.RunEvent(nil), history...)
	seen := make(map[string]struct{}, len(history))
	for _, event := range history {
		seen[event.ID] = struct{}{}
	}
	b.seen[runID] = seen
}

func prepare(event domain.RunEvent) domain.RunEvent {
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now().UTC()
	} else {
		event.Timestamp = event.Timestamp.UTC()
	}
	if event.ID == "" {
		random := make([]byte, 8)
		if _, err := rand.Read(random); err == nil {
			event.ID = fmt.Sprintf("evt_%020d_%s", event.Timestamp.UnixNano(), hex.EncodeToString(random))
		} else {
			event.ID = fmt.Sprintf("evt_%020d", event.Timestamp.UnixNano())
		}
	}
	return event
}
