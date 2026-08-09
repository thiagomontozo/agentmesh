package events

import (
	"sync"

	"github.com/thiagomontozo/agentmesh/internal/domain"
)

type Bus struct {
	mu          sync.RWMutex
	subscribers map[string]map[chan domain.RunEvent]struct{}
	history     map[string][]domain.RunEvent
}

func NewBus() *Bus {
	return &Bus{
		subscribers: make(map[string]map[chan domain.RunEvent]struct{}),
		history:     make(map[string][]domain.RunEvent),
	}
}

func (b *Bus) Publish(event domain.RunEvent) {
	b.mu.Lock()
	b.history[event.RunID] = append(b.history[event.RunID], event)
	for ch := range b.subscribers[event.RunID] {
		select {
		case ch <- event:
		default:
		}
	}
	b.mu.Unlock()
}

func (b *Bus) Subscribe(runID string) (<-chan domain.RunEvent, func()) {
	b.mu.Lock()
	history := b.history[runID]
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
