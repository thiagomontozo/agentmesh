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
			if got != want {
				t.Fatalf("expected %+v, got %+v", want, got)
			}
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for %s", want.Type)
		}
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
