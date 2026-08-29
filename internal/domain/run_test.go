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
	if run.Status != RunSucceeded || run.Output != "done" || run.CompletedAt == nil || run.DurationMS != 1000 {
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

func TestRunCanBeCanceledWhileQueuedOrRunning(t *testing.T) {
	for _, status := range []RunStatus{RunQueued, RunRunning} {
		t.Run(string(status), func(t *testing.T) {
			run := Run{Status: status, Output: "stale", Error: "stale"}
			at := time.Date(2026, time.August, 28, 10, 0, 0, 0, time.FixedZone("test", -3*60*60))
			if err := run.Cancel(at); err != nil {
				t.Fatal(err)
			}
			if run.Status != RunCanceled || run.Output != "" || run.Error != "" || run.CompletedAt == nil || run.CompletedAt.Location() != time.UTC {
				t.Fatalf("unexpected canceled Run: %+v", run)
			}
		})
	}
}

func TestRunRejectsCancelFromTerminalState(t *testing.T) {
	for _, status := range []RunStatus{RunSucceeded, RunFailed, RunCanceled} {
		run := Run{Status: status}
		if err := run.Cancel(time.Now()); !errors.Is(err, ErrRunNotCancelable) {
			t.Fatalf("status %s: expected ErrRunNotCancelable, got %v", status, err)
		}
	}
}
