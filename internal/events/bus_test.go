package events

import (
	"testing"
	"time"

	"github.com/thiagomontozo/agentmesh/internal/domain"
)

func TestSubscribeReplaysHistoryAndReceivesLiveEvents(t *testing.T) {
	bus := NewBus()
	historical := domain.RunEvent{RunID: "run_1", Type: "run.queued"}
	live := domain.RunEvent{RunID: "run_1", Type: "run.started"}
	bus.Publish(historical)

	events, unsubscribe := bus.Subscribe("run_1")
	defer unsubscribe()
	bus.Publish(live)

	for _, want := range []domain.RunEvent{historical, live} {
		select {
		case got := <-events:
			if got.RunID != want.RunID || got.Type != want.Type || got.ID == "" || got.Timestamp.IsZero() {
				t.Fatalf("expected %+v, got %+v", want, got)
			}
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for %s", want.Type)
		}
	}
}

func TestSubscribeMergesPersistentAndLocalHistoryWithoutDuplicates(t *testing.T) {
	bus := NewBusWithHistoryLimit(4)
	first := domain.RunEvent{ID: "evt_1", RunID: "run_1", Type: "run.queued", Timestamp: time.Unix(1, 0)}
	second := domain.RunEvent{ID: "evt_2", RunID: "run_1", Type: "run.started", Timestamp: time.Unix(2, 0)}
	bus.Publish(second)
	events, unsubscribe := bus.SubscribeWithHistory("run_1", []domain.RunEvent{first, second})
	defer unsubscribe()
	for _, want := range []string{"evt_1", "evt_2"} {
		if got := <-events; got.ID != want {
			t.Fatalf("expected %s, got %+v", want, got)
		}
	}
	bus.Publish(second)
	select {
	case duplicate := <-events:
		t.Fatalf("received duplicate event: %+v", duplicate)
	case <-time.After(20 * time.Millisecond):
	}
}

func TestEventsAreIsolatedByRun(t *testing.T) {
	bus := NewBus()
	events, unsubscribe := bus.Subscribe("run_1")
	defer unsubscribe()

	bus.Publish(domain.RunEvent{RunID: "run_2", Type: "run.started"})
	select {
	case event := <-events:
		t.Fatalf("received event for another run: %+v", event)
	case <-time.After(20 * time.Millisecond):
	}
}

func TestHistoryIsBounded(t *testing.T) {
	bus := NewBusWithHistoryLimit(2)
	for _, eventType := range []string{"one", "two", "three"} {
		bus.Publish(domain.RunEvent{RunID: "run_1", Type: eventType})
	}
	events, unsubscribe := bus.Subscribe("run_1")
	defer unsubscribe()
	for _, want := range []string{"two", "three"} {
		if got := <-events; got.Type != want {
			t.Fatalf("expected %s, got %s", want, got.Type)
		}
	}
}
