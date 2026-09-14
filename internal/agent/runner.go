package agent

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"in.dmart.pulse.conflux/internal/observability"
)

// Runner supervises every agent in the process.
//
// One goroutine per agent, each with panic recovery at this boundary — the
// place where the agent's identity is still known, so a failure can be
// attributed by name rather than taking the process down with it.
type Runner struct {
	log     *slog.Logger
	metrics *observability.Metrics
	grace   time.Duration

	// backoffInitial is a field rather than the constant directly so tests can
	// exercise the restart path without waiting real seconds for it.
	backoffInitial time.Duration

	mu       sync.RWMutex
	statuses map[string]*status

	wg sync.WaitGroup
}

// restart backoff bounds. A crash-looping agent must not become a hot loop, but
// it must also recover quickly from a transient fault.
const (
	restartBackoffInitial = 500 * time.Millisecond
	restartBackoffMax     = 30 * time.Second
	// Consecutive failures after which an agent is reported degraded. It keeps
	// being restarted — degraded is a health signal, not a stop.
	degradedAfter = 3
)

// NewRunner builds a runner.
func NewRunner(log *slog.Logger, m *observability.Metrics, grace time.Duration) *Runner {
	return &Runner{
		log: log, metrics: m, grace: grace,
		backoffInitial: restartBackoffInitial,
		statuses:       make(map[string]*status),
	}
}

// Start launches every agent. It returns immediately; Wait blocks for shutdown.
func (r *Runner) Start(ctx context.Context, agents []Agent) {
	for _, a := range agents {
		st := &status{state: StateConfigured}
		r.mu.Lock()
		r.statuses[a.ID()] = st
		r.mu.Unlock()

		r.wg.Add(1)
		go r.supervise(ctx, a, st)
	}
}

// supervise runs one agent, restarting it on failure with backoff, forever,
// until the context is cancelled. Nothing here can affect another agent.
func (r *Runner) supervise(ctx context.Context, a Agent, st *status) {
	defer r.wg.Done()

	log := observability.ForAgent(r.log, a.ID(), a.SourceName())
	backoff := r.backoffInitial
	consecutive := 0

	for {
		if ctx.Err() != nil {
			st.set(StateStopped)
			return
		}

		st.set(StateRunning)
		err := r.runOnce(ctx, a, st, log)

		switch {
		case ctx.Err() != nil:
			// Shutdown, not failure: drain finished and we are done.
			st.set(StateStopped)
			log.Info("agent stopped")
			return
		case err == nil:
			// A clean return without cancellation means the agent finished its
			// work; restart it so the trigger keeps firing.
			consecutive = 0
			backoff = r.backoffInitial
		default:
			consecutive++
			st.mu.Lock()
			st.restarts++
			st.mu.Unlock()

			state := StateRecovering
			if consecutive >= degradedAfter {
				state = StateDegraded
			}
			st.fail(state, err.Error())
			log.Error("agent failed; restarting",
				slog.String("error", err.Error()),
				slog.Int("consecutive_failures", consecutive),
				slog.String("state", string(state)),
				slog.Duration("backoff", backoff))
		}

		select {
		case <-ctx.Done():
			st.set(StateStopped)
			return
		case <-time.After(backoff):
		}
		if backoff *= 2; backoff > restartBackoffMax {
			backoff = restartBackoffMax
		}
	}
}

// runOnce calls the agent and converts a panic into an ordinary error.
//
// An unrecovered panic in any goroutine takes the whole process down, which
// would violate FR-004 in the most direct way available. Recovery here keeps it
// to the one agent and names it.
func (r *Runner) runOnce(ctx context.Context, a Agent, st *status, log *slog.Logger) (err error) {
	defer func() {
		if p := recover(); p != nil {
			st.mu.Lock()
			st.panics++
			st.mu.Unlock()
			r.metrics.AgentPanics.WithLabelValues(a.ID()).Inc()
			log.Error("agent panicked; contained", slog.Any("panic", p))
			err = fmt.Errorf("panic: %v", p)
		}
	}()
	return a.Run(ctx)
}

// Wait blocks until every agent has stopped, or the grace period expires.
//
// Draining must not acknowledge work that did not complete (FR-006): agents
// stop reading on cancellation and only ack what they already resolved.
func (r *Runner) Wait() {
	done := make(chan struct{})
	go func() { r.wg.Wait(); close(done) }()

	select {
	case <-done:
		r.log.Info("all agents drained")
	case <-time.After(r.grace):
		r.log.Warn("shutdown grace expired; exiting with agents still draining",
			slog.Duration("grace", r.grace))
	}
}

// Health is one agent's observable condition, for readiness reporting.
type Health struct {
	ID       string `json:"id"`
	State    string `json:"state"`
	LastErr  string `json:"last_error,omitempty"`
	Panics   int    `json:"panics,omitempty"`
	Restarts int    `json:"restarts,omitempty"`
}

// Health snapshots every agent's condition.
func (r *Runner) Health() []Health {
	r.mu.RLock()
	defer r.mu.RUnlock()

	out := make([]Health, 0, len(r.statuses))
	for id, st := range r.statuses {
		state, lastErr, panics, restarts := st.snapshot()
		out = append(out, Health{
			ID: id, State: string(state), LastErr: lastErr,
			Panics: panics, Restarts: restarts,
		})
	}
	return out
}

// SetRestartBackoff overrides the initial restart backoff. Tests use it to
// exercise the restart path quickly; production keeps the default.
func (r *Runner) SetRestartBackoff(d time.Duration) { r.backoffInitial = d }
