package agent

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"in.dmart.pulse.conflux/internal/observability"
)

type slowTask struct {
	started atomic.Int32
	block   chan struct{}
}

func (s *slowTask) Name() string { return "slow" }
func (s *slowTask) Run(ctx context.Context) error {
	s.started.Add(1)
	select {
	case <-s.block:
	case <-ctx.Done():
	}
	return nil
}

// A run that overruns its interval must not start a second copy of itself:
// queueing would let a slow task build an unbounded backlog of its own runs.
func TestOverlappingRunIsSkippedNotQueued(t *testing.T) {
	task := &slowTask{block: make(chan struct{})}
	m := observability.NewMetrics()
	log := observability.NewLogger("error")

	a := NewPeriodicAgent("housekeeping", 10*time.Millisecond, task, m, log)
	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = a.Run(ctx) }()

	// Let several ticks fire while the first run is still blocked.
	time.Sleep(120 * time.Millisecond)

	if got := task.started.Load(); got != 1 {
		t.Fatalf("task started %d times while one run was in flight, want exactly 1", got)
	}
	skipped := testutil.ToFloat64(m.AgentRuns.WithLabelValues("housekeeping", "skipped_overlap"))
	if skipped < 1 {
		t.Fatalf("skipped_overlap counter = %v, want at least 1 — a skip must be visible, not silent", skipped)
	}

	close(task.block)
	cancel()
}

type countingTask struct{ runs atomic.Int32 }

func (c *countingTask) Name() string              { return "counting" }
func (c *countingTask) Run(context.Context) error { c.runs.Add(1); return nil }

func TestPeriodicAgentRunsOnInterval(t *testing.T) {
	task := &countingTask{}
	m := observability.NewMetrics()
	a := NewPeriodicAgent("tick", 10*time.Millisecond, task, m, observability.NewLogger("error"))

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Millisecond)
	defer cancel()
	_ = a.Run(ctx)

	if got := task.runs.Load(); got < 2 {
		t.Fatalf("task ran %d times in ~90ms at a 10ms interval, want at least 2", got)
	}
}
