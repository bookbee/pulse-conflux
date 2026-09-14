package agent

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"in.dmart.pulse.conflux/internal/observability"
)

type fnAgent struct {
	id   string
	src  string
	runs atomic.Int32
	fn   func(ctx context.Context) error
}

func (a *fnAgent) ID() string         { return a.id }
func (a *fnAgent) SourceName() string { return a.src }
func (a *fnAgent) Run(ctx context.Context) error {
	a.runs.Add(1)
	return a.fn(ctx)
}

// FR-004: a panicking agent must not take the process down and must not disturb
// any other agent. An unrecovered panic in any goroutine kills the process, so
// this is the most direct way the requirement can be violated.
func TestPanicIsContainedAndAttributed(t *testing.T) {
	panicky := &fnAgent{id: "panicky", src: "s1", fn: func(context.Context) error {
		panic("boom")
	}}
	healthy := &fnAgent{id: "healthy", src: "s2", fn: func(ctx context.Context) error {
		<-ctx.Done()
		return nil
	}}

	m := observability.NewMetrics()
	r := NewRunner(observability.NewLogger("error"), m, time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	r.Start(ctx, []Agent{panicky, healthy})
	time.Sleep(200 * time.Millisecond)

	if got := testutil.ToFloat64(m.AgentPanics.WithLabelValues("panicky")); got < 1 {
		t.Fatalf("panic counter for panicky = %v, want at least 1 (attributed by name)", got)
	}
	if got := testutil.ToFloat64(m.AgentPanics.WithLabelValues("healthy")); got != 0 {
		t.Fatalf("healthy agent has %v panics; failure leaked across agents", got)
	}
	if healthy.runs.Load() != 1 {
		t.Fatalf("healthy agent ran %d times, want 1 — it should never have been restarted", healthy.runs.Load())
	}

	var panickyHealth, healthyHealth Health
	for _, h := range r.Health() {
		switch h.ID {
		case "panicky":
			panickyHealth = h
		case "healthy":
			healthyHealth = h
		}
	}
	if panickyHealth.Panics < 1 {
		t.Errorf("panicky health = %+v, want panics recorded", panickyHealth)
	}
	if healthyHealth.State != string(StateRunning) {
		t.Errorf("healthy state = %q, want running", healthyHealth.State)
	}

	cancel()
	r.Wait()
}

// Repeated failure marks an agent degraded — a health signal, not a stop. It
// keeps being restarted so a transient cause can still clear.
func TestRepeatedFailureMarksDegradedButKeepsRestarting(t *testing.T) {
	failing := &fnAgent{id: "failing", src: "s", fn: func(context.Context) error {
		return errors.New("always fails")
	}}

	m := observability.NewMetrics()
	r := NewRunner(observability.NewLogger("error"), m, time.Second)
	r.SetRestartBackoff(5 * time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	r.Start(ctx, []Agent{failing})
	time.Sleep(300 * time.Millisecond)

	var h Health
	for _, x := range r.Health() {
		if x.ID == "failing" {
			h = x
		}
	}
	if h.State != string(StateDegraded) && h.State != string(StateRecovering) {
		t.Fatalf("state = %q, want degraded or recovering", h.State)
	}
	if h.Restarts < 2 {
		t.Fatalf("restarts = %d, want at least 2 — a degraded agent must keep being retried", h.Restarts)
	}

	cancel()
	r.Wait()
}

func TestDisabledAgentsAreNeverStarted(t *testing.T) {
	// Build() filters on Enabled, so the runner only ever sees what should run.
	// This asserts the runner adds nothing of its own.
	m := observability.NewMetrics()
	r := NewRunner(observability.NewLogger("error"), m, time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r.Start(ctx, nil)

	if got := len(r.Health()); got != 0 {
		t.Fatalf("runner reports %d agents for an empty set", got)
	}
}
