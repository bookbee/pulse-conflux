package agent

import (
	"context"
	"errors"
	"log/slog"

	"in.dmart.pulse.conflux/internal/dispatch"
	"in.dmart.pulse.conflux/internal/ingest"
	"in.dmart.pulse.conflux/internal/observability"
)

// StreamAgent is the continuous trigger: read, dispatch, acknowledge, repeat.
type StreamAgent struct {
	id         string
	source     ingest.Source
	dispatcher *dispatch.Dispatcher
	metrics    *observability.Metrics
	log        *slog.Logger
}

// NewStreamAgent builds a continuously running agent.
func NewStreamAgent(id string, src ingest.Source, d *dispatch.Dispatcher, m *observability.Metrics, log *slog.Logger) *StreamAgent {
	return &StreamAgent{
		id: id, source: src, dispatcher: d, metrics: m,
		log: observability.ForAgent(log, id, src.Ref().Name),
	}
}

func (a *StreamAgent) ID() string         { return a.id }
func (a *StreamAgent) SourceName() string { return a.source.Ref().Name }

// Run consumes until the context is cancelled.
func (a *StreamAgent) Run(ctx context.Context) error {
	a.log.Info("stream agent started")
	for {
		if ctx.Err() != nil {
			return nil
		}

		deliveries, err := a.source.Read(ctx)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		}

		for _, d := range deliveries {
			// Cancellation mid-batch stops reading, and anything unresolved is
			// left unacknowledged so it is redelivered rather than lost (FR-006).
			if ctx.Err() != nil {
				return nil
			}
			a.handle(ctx, d)
		}
	}
}

// handle resolves one delivery. It never returns an error: one bad entry must
// not stop progress on the rest (FR-012).
func (a *StreamAgent) handle(ctx context.Context, d ingest.Delivery) {
	src := d.Source.Name

	if d.Unprocessable != nil {
		// With no datastore, this line is the only trace the entry existed, so
		// it carries the raw bytes and position in full rather than a counter.
		a.log.Error("unprocessable entry",
			slog.String("source", src),
			slog.String("position", d.Position),
			slog.String("reason", d.Unprocessable.Error()),
			slog.String("raw", string(d.Raw)))
		a.metrics.EventsUnprocessable.WithLabelValues(a.id, src, "parse").Inc()

		// Acknowledge it: leaving it pending would make every reclaim cycle
		// retry an entry that can never succeed.
		if err := d.Ack(ctx); err != nil {
			a.log.Error("ack failed for unprocessable entry", slog.String("error", err.Error()))
		}
		return
	}

	outcomes := a.dispatcher.Send(ctx, d.Envelope)

	// Every dispatch is resolved — delivered or dropped — before the delivery is
	// acknowledged. A drop is a resolution: the bounded budget is what keeps an
	// unavailable destination from turning into lag, and lag into upstream loss.
	if err := d.Ack(ctx); err != nil {
		observability.ForEnvelope(a.log, d.Envelope).Error("ack failed",
			slog.String("position", d.Position),
			slog.String("error", err.Error()))
		return
	}

	delivered := false
	for _, o := range outcomes {
		if o.Result == dispatch.Delivered {
			delivered = true
		}
	}
	if delivered {
		a.metrics.EventsProcessed.WithLabelValues(a.id, src).Inc()
	}
}

var _ Agent = (*StreamAgent)(nil)
