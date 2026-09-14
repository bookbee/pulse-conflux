package agent

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"in.dmart.pulse.conflux/internal/observability"
)

// Task is the work a periodic agent performs on each tick.
type Task interface {
	Name() string
	Run(ctx context.Context) error
}

// PeriodicAgent runs a Task on a fixed interval.
type PeriodicAgent struct {
	id       string
	interval time.Duration
	task     Task
	metrics  *observability.Metrics
	log      *slog.Logger

	// inFlight guards against overlap. A run that overruns its next due time
	// must not start a second copy of itself (FR-005) — two reclaim passes
	// racing each other is exactly the corruption this prevents. Skipped, not
	// queued: queueing would let a slow task build a backlog of its own runs.
	inFlight atomic.Bool
}

// NewPeriodicAgent builds an interval-triggered agent.
func NewPeriodicAgent(id string, interval time.Duration, task Task, m *observability.Metrics, log *slog.Logger) *PeriodicAgent {
	return &PeriodicAgent{
		id: id, interval: interval, task: task, metrics: m,
		log: observability.ForAgent(log, id, task.Name()),
	}
}

func (a *PeriodicAgent) ID() string         { return a.id }
func (a *PeriodicAgent) SourceName() string { return a.task.Name() }

// Run ticks until the context is cancelled.
func (a *PeriodicAgent) Run(ctx context.Context) error {
	a.log.Info("periodic agent started", slog.Duration("interval", a.interval))

	ticker := time.NewTicker(a.interval)
	defer ticker.Stop()

	// The task runs in its own goroutine so the loop keeps receiving ticks
	// while a run is in flight. Running it inline would make overlap impossible
	// by construction — and therefore invisible, which defeats the point of
	// counting skips: an agent whose work outruns its interval is exactly what
	// an operator needs told about.
	var inFlight sync.WaitGroup

	for {
		select {
		case <-ctx.Done():
			inFlight.Wait() // never acknowledge work that did not finish (FR-006)
			return nil
		case <-ticker.C:
			if !a.inFlight.CompareAndSwap(false, true) {
				a.metrics.AgentRuns.WithLabelValues(a.id, "skipped_overlap").Inc()
				a.log.Warn("run skipped; previous run still in flight",
					slog.Duration("interval", a.interval))
				continue
			}
			inFlight.Add(1)
			go func() {
				defer inFlight.Done()
				defer a.inFlight.Store(false)
				a.run(ctx)
			}()
		}
	}
}

// run executes the task once and records how it went. The overlap guard is
// held by the caller.
func (a *PeriodicAgent) run(ctx context.Context) {
	start := time.Now()
	err := a.task.Run(ctx)
	elapsed := time.Since(start)
	a.metrics.AgentRunDuration.WithLabelValues(a.id).Observe(elapsed.Seconds())

	switch {
	case err != nil && ctx.Err() == nil:
		a.metrics.AgentRuns.WithLabelValues(a.id, "failed").Inc()
		a.log.Error("run failed",
			slog.String("error", err.Error()),
			slog.Duration("elapsed", elapsed))
	case err == nil:
		a.metrics.AgentRuns.WithLabelValues(a.id, "ok").Inc()
		a.log.Debug("run complete", slog.Duration("elapsed", elapsed))
	}
}

var _ Agent = (*PeriodicAgent)(nil)
