package agent

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"in.dmart.pulse.conflux/internal/dispatch"
	"in.dmart.pulse.conflux/internal/envelope"
	"in.dmart.pulse.conflux/internal/housekeeping"
	"in.dmart.pulse.conflux/internal/ingest"
	"in.dmart.pulse.conflux/internal/observability"
)

// LogDrainAgent drains the log list continuously and emits one summary per
// closed period.
//
// Draining is continuous rather than on the summary interval for a reason that
// has nothing to do with summarising: the gateway's Lua script REFUSES log
// writes once the list reaches its cap instead of trimming to make room, so an
// agent that woke hourly would let the list fill and make this service the
// cause of upstream log loss (FR-018c).
type LogDrainAgent struct {
	id         string
	source     ingest.Source
	agg        *housekeeping.Aggregator
	dispatcher *dispatch.Dispatcher
	metrics    *observability.Metrics
	log        *slog.Logger
}

// NewLogDrainAgent builds the continuous log-drain agent.
func NewLogDrainAgent(id string, src ingest.Source, period time.Duration, d *dispatch.Dispatcher, m *observability.Metrics, log *slog.Logger) *LogDrainAgent {
	return &LogDrainAgent{
		id: id, source: src,
		agg:        housekeeping.NewAggregator(period),
		dispatcher: d, metrics: m,
		log: observability.ForAgent(log, id, src.Ref().Name),
	}
}

func (a *LogDrainAgent) ID() string         { return a.id }
func (a *LogDrainAgent) SourceName() string { return a.source.Ref().Name }

// Run drains until cancelled, emitting summaries as periods close.
func (a *LogDrainAgent) Run(ctx context.Context) error {
	a.log.Info("log drain agent started")

	// A ticker closes a quiet period even when no entry arrives to trigger it.
	tick := time.NewTicker(time.Second)
	defer tick.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-tick.C:
			if s := a.agg.Tick(time.Now()); s != nil {
				a.emit(ctx, s)
			}
		default:
		}

		deliveries, err := a.source.Read(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}

		for _, d := range deliveries {
			src := d.Source.Name
			if d.Unprocessable != nil {
				// The pop already consumed it and nothing can restore it, so
				// this line is the only trace the entry existed.
				a.log.Error("unprocessable log entry (consumed, unrecoverable)",
					slog.String("source", src),
					slog.String("reason", d.Unprocessable.Error()),
					slog.String("raw", string(d.Raw)))
				a.metrics.EventsUnprocessable.WithLabelValues(a.id, src, "parse").Inc()
				continue
			}
			if s := a.agg.Observe(d.Envelope, time.Now()); s != nil {
				a.emit(ctx, s)
			}
			a.metrics.EventsProcessed.WithLabelValues(a.id, src).Inc()
			_ = d.Ack(ctx) // no-op for a list; kept so the shape stays uniform
		}
	}
}

// emit publishes a closed period's summary to structured output and to the
// configured destination.
func (a *LogDrainAgent) emit(ctx context.Context, s *housekeeping.LogSummary) {
	housekeeping.Emit(a.log, s)
	if a.dispatcher == nil {
		return
	}
	body, err := json.Marshal(s)
	if err != nil {
		return
	}
	// A summary travels as an envelope so it reuses the dispatch path, retry
	// budget and drop accounting. Its id is the period, which is what makes a
	// re-send absorbable rather than double-counted (FR-018a).
	a.dispatcher.Send(ctx, envelope.Envelope{
		EventID:    "summary:" + s.PeriodStart.Format(time.RFC3339),
		GatewayID:  "pulse-conflux",
		ReceivedAt: s.GeneratedAt,
		StreamName: a.SourceName(),
		Payload:    body,
	})
}

var _ Agent = (*LogDrainAgent)(nil)
