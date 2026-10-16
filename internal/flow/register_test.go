package flow

import (
	"testing"
	"time"
)

func TestWorkerOptionsShortSticky(t *testing.T) {
	if got := WorkerOptions().StickyScheduleToStartTimeout; got != time.Second {
		t.Fatalf("StickyScheduleToStartTimeout %s, want 1s", got)
	}
}
