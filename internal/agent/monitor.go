package agent

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"in.dmart.pulse.conflux/internal/config"
	"in.dmart.pulse.conflux/internal/ingest"
	"in.dmart.pulse.conflux/internal/observability"
)

// LagSource is an agent that can report how far behind it is.
type LagSource interface {
	Agent
	Lag(ctx context.Context) (ingest.LagReading, error)
	Kind() ingest.Kind
}

// Lag exposes the stream agent's position for monitoring.
func (a *StreamAgent) Lag(ctx context.Context) (ingest.LagReading, error) {
	return a.source.Lag(ctx)
}

// Kind reports which primitive this agent consumes.
func (a *StreamAgent) Kind() ingest.Kind { return a.source.Ref().Kind }

// Lag exposes the log drain's backlog: for a list, depth IS lag.
func (a *LogDrainAgent) Lag(ctx context.Context) (ingest.LagReading, error) {
	return a.source.Lag(ctx)
}

// Kind reports which primitive this agent consumes.
func (a *LogDrainAgent) Kind() ingest.Kind { return a.source.Ref().Kind }

// Monitor samples lag, feeds metrics and readiness, and decides what support
// hears about.
//
// It runs on its own interval rather than inside an agent, so an agent that is
// wedged still gets measured — a lag signal that stops when the thing it
// measures stops is worse than no signal.
type Monitor struct {
	agents   []Agent
	cfg      *config.Config
	metrics  *observability.Metrics
	detector *observability.Detector
	runner   *Runner
	log      *slog.Logger

	latest map[string]observability.AgentHealth
	mu     chan struct{} // 1-buffered, used as a mutex
}

// NewMonitor builds the monitor.
func NewMonitor(agents []Agent, cfg *config.Config, m *observability.Metrics, d *observability.Detector, r *Runner, log *slog.Logger) *Monitor {
	mon := &Monitor{
		agents: agents, cfg: cfg, metrics: m, detector: d, runner: r, log: log,
		latest: map[string]observability.AgentHealth{},
		mu:     make(chan struct{}, 1),
	}
	mon.mu <- struct{}{}
	return mon
}

// Run samples until cancelled.
func (mon *Monitor) Run(ctx context.Context) {
	// Sample once immediately so readiness is meaningful before the first tick.
	mon.sample(ctx)

	t := time.NewTicker(mon.cfg.Anomaly.CheckInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			mon.sample(ctx)
		}
	}
}

func (mon *Monitor) sample(ctx context.Context) {
	states := map[string]string{}
	errs := map[string]string{}
	for _, h := range mon.runner.Health() {
		states[h.ID] = h.State
		errs[h.ID] = h.LastErr
	}

	now := time.Now()
	snapshot := map[string]observability.AgentHealth{}

	for _, a := range mon.agents {
		health := observability.AgentHealth{
			ID: a.ID(), Source: a.SourceName(),
			State: states[a.ID()], LastError: errs[a.ID()],
		}

		ls, ok := a.(LagSource)
		if !ok {
			snapshot[a.ID()] = health
			continue
		}

		readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		reading, err := ls.Lag(readCtx)
		cancel()

		if err != nil {
			mon.detector.Observe(observability.SourceUnavailable, a.SourceName(),
				true, false, err.Error(), now)
			health.State = states[a.ID()]
			snapshot[a.ID()] = health
			continue
		}
		mon.detector.Observe(observability.SourceUnavailable, a.SourceName(), false, false, "", now)

		health.LagEntries = reading.Entries
		health.LagApproximate = reading.Approximate

		mon.metrics.LagEntries.WithLabelValues(a.ID(), a.SourceName()).Set(float64(reading.Entries))
		mon.metrics.LagApproximate.WithLabelValues(a.ID(), a.SourceName()).Set(boolGauge(reading.Approximate))

		// Lag over threshold must SUSTAIN before support hears; readiness
		// reacts immediately and is handled in the readyz handler.
		mon.detector.Observe(observability.LagThreshold, a.ID(),
			reading.Entries > mon.cfg.Anomaly.LagThresholdEntries, true,
			fmt.Sprintf("lag %d entries exceeds threshold %d on %s",
				reading.Entries, mon.cfg.Anomaly.LagThresholdEntries, a.SourceName()), now)

		// An approximate reading is not a missing number, it is a lost one:
		// entries were trimmed before this agent read them.
		mon.detector.Observe(observability.EntriesTrimmed, a.ID(), reading.Approximate, false,
			"entries were trimmed from under the consumer group on "+a.SourceName(), now)

		// The log list refuses the gateway's writes at its cap, so depth over
		// the drain target is upstream loss in the making, not a local slowdown.
		if ls.Kind() == ingest.KindList {
			mon.metrics.ListDepth.WithLabelValues(a.SourceName()).Set(float64(reading.Entries))
			mon.detector.Observe(observability.LogListDepth, a.SourceName(),
				reading.Entries > mon.cfg.House.SummaryMaxDepth, false,
				fmt.Sprintf("log list depth %d exceeds drain target %d; the gateway REFUSES log writes at the list cap, so this becomes upstream loss",
					reading.Entries, mon.cfg.House.SummaryMaxDepth), now)
		}

		snapshot[a.ID()] = health
	}

	<-mon.mu
	mon.latest = snapshot
	mon.mu <- struct{}{}
}

// AgentHealth implements observability.HealthReporter.
//
// Lag comes from the last sample, because reading it costs a round trip per
// agent. Agent STATE is read live: it is free, and a readiness response that
// reports a stale "configured" for a whole check interval after start is
// misleading exactly when someone is most likely to be looking.
func (mon *Monitor) AgentHealth() []observability.AgentHealth {
	live := map[string]Health{}
	for _, h := range mon.runner.Health() {
		live[h.ID] = h
	}

	<-mon.mu
	defer func() { mon.mu <- struct{}{} }()

	out := make([]observability.AgentHealth, 0, len(mon.latest))
	for id, h := range mon.latest {
		if cur, ok := live[id]; ok {
			h.State = cur.State
			h.LastError = cur.LastErr
		}
		out = append(out, h)
	}
	return out
}

func boolGauge(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

var _ observability.HealthReporter = (*Monitor)(nil)
