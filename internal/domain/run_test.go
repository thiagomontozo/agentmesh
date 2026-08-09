package domain

import (
	"errors"
	"testing"
	"time"
)

func TestRunSuccessfulLifecycle(t *testing.T) {
	run := Run{Status: RunQueued}
	started := time.Date(2026, time.August, 8, 12, 0, 0, 0, time.FixedZone("test", -3*60*60))
	completed := started.Add(time.Second)

	if err := run.Start(started); err != nil {
		t.Fatal(err)
	}
	if run.Status != RunRunning || run.StartedAt == nil || run.StartedAt.Location() != time.UTC {
		t.Fatalf("unexpected running state: %+v", run)
	}
	if err := run.Succeed("done", completed); err != nil {
		t.Fatal(err)
	}
	if run.Status != RunSucceeded || run.Output != "done" || run.CompletedAt == nil {
		t.Fatalf("unexpected succeeded state: %+v", run)
	}
}

func TestRunRejectsInvalidTransition(t *testing.T) {
	run := Run{Status: RunQueued}
	if err := run.Succeed("done", time.Now()); err == nil {
		t.Fatal("expected queued to succeeded transition to fail")
	}
}

func TestRunCanFailWhileQueued(t *testing.T) {
	run := Run{Status: RunQueued, Output: "stale"}
	wantErr := errors.New("queue is full")
	if err := run.Fail(wantErr, time.Now()); err != nil {
		t.Fatal(err)
	}
	if run.Status != RunFailed || run.Error != wantErr.Error() || run.Output != "" {
		t.Fatalf("unexpected failed state: %+v", run)
	}
}
