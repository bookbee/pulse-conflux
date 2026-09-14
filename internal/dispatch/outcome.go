package dispatch

import (
	"context"
	"log/slog"
	"time"

	"in.dmart.pulse.conflux/internal/envelope"
	"in.dmart.pulse.conflux/internal/observability"
)

// Dispatcher sends one envelope to every destination an agent is configured
// for, applying the retry policy to each and recording how it resolved.
type Dispatcher struct {
	agentID string
	dests   []Destination
	policy  Policy
	metrics *observability.Metrics
	log     *slog.Logger
}

// NewDispatcher builds a dispatcher for one agent.
func NewDispatcher(agentID string, dests []Destination, policy Policy, m *observability.Metrics, log *slog.Logger) *Dispatcher {
	return &Dispatcher{agentID: agentID, dests: dests, policy: policy, metrics: m, log: log}
}

// Send dispatches to every destination and returns the outcomes.
//
// A failing destination never stops the others, and never blocks: the budget is
// bounded and the event is dropped when it runs out.
func (d *Dispatcher) Send(ctx context.Context, env envelope.Envelope) []Outcome {
	log := observability.ForEnvelope(d.log, env)
	outcomes := make([]Outcome, 0, len(d.dests))

	for _, dest := range d.dests {
		start := time.Now()
		attempts, err := d.policy.Do(ctx, dest, env, func(_ int, outcome string) {
			d.metrics.DispatchAttempts.WithLabelValues(d.agentID, dest.Name(), outcome).Inc()
		})
		elapsed := time.Since(start)
		d.metrics.DispatchDuration.WithLabelValues(d.agentID, dest.Name()).Observe(elapsed.Seconds())

		out := Outcome{
			EventID: env.EventID, GatewayID: env.GatewayID,
			AgentID: d.agentID, Destination: dest.Name(),
			Attempts: attempts, DurationMS: elapsed.Milliseconds(),
		}

		if err == nil {
			out.Result = Delivered
			log.Debug("dispatched",
				slog.String("destination", dest.Name()),
				slog.Int("attempts", attempts))
		} else {
			out.Result = Dropped
			out.Reason = err.Error()
			d.metrics.DispatchDropped.WithLabelValues(d.agentID, dest.Name(), reasonLabel(err)).Inc()

			// This line is the ONLY record that the event existed: feature 001
			// introduces no datastore, and the source is lossy. It carries the
			// full identity deliberately and must never be reduced to a counter.
			log.Error("event dropped",
				slog.String("destination", dest.Name()),
				slog.Int("attempts", attempts),
				slog.String("reason", err.Error()),
				slog.Int64("duration_ms", elapsed.Milliseconds()),
				slog.String("stream_name", env.StreamName),
				slog.Time("received_at", env.ReceivedAt))
		}
		outcomes = append(outcomes, out)
	}
	return outcomes
}

// reasonLabel keeps metric cardinality bounded: the free-text reason goes to the
// log line, the label stays one of two values.
func reasonLabel(err error) string {
	var de *Error
	if ok := asDispatchError(err, &de); ok && de.Kind == Permanent {
		return "permanent"
	}
	return "budget_exhausted"
}
