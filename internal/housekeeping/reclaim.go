// Package housekeeping holds the periodic maintenance agents.
//
// Scope is deliberately narrow (FR-020/FR-020a): read-only reporting on the
// queue, plus upkeep of the consumer groups THIS service owns. Nothing here
// deletes, trims, expires or renames queue data outside its own groups —
// stream trimming and the log list's expiry belong to the gateway, and the
// eviction policy is set so writes fail loudly rather than keys vanishing
// under a consumer.
package housekeeping

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"in.dmart.pulse.conflux/internal/ingest"
	ingestredis "in.dmart.pulse.conflux/internal/ingest/redis"
	"in.dmart.pulse.conflux/internal/observability"
)

// ReclaimTask recovers entries abandoned by consumers that are no longer
// active, and retires consumer names that hold nothing.
type ReclaimTask struct {
	client     *ingestredis.Client
	sources    []ingest.SourceRef
	group      string
	consumer   string
	minIdle    time.Duration
	batch      int
	retireIdle time.Duration
	agentID    string
	metrics    *observability.Metrics
	log        *slog.Logger
}

// NewReclaimTask builds the reclaim pass.
func NewReclaimTask(c *ingestredis.Client, sources []ingest.SourceRef, group, consumer string, minIdle time.Duration, batch int, retireIdle time.Duration, agentID string, m *observability.Metrics, log *slog.Logger) *ReclaimTask {
	return &ReclaimTask{
		client: c, sources: sources, group: group, consumer: consumer,
		minIdle: minIdle, batch: batch, retireIdle: retireIdle,
		agentID: agentID, metrics: m, log: log,
	}
}

func (t *ReclaimTask) Name() string { return "pending-reclaim" }

// Run reclaims and acknowledges abandoned entries across every configured source.
func (t *ReclaimTask) Run(ctx context.Context) error {
	for _, ref := range t.sources {
		if ref.Kind != ingest.KindStream {
			continue // lists have no pending set
		}
		if err := t.reclaimOne(ctx, ref); err != nil {
			return err
		}
	}
	return nil
}

func (t *ReclaimTask) reclaimOne(ctx context.Context, ref ingest.SourceRef) error {
	stats, err := t.client.Pending(ctx, ref.Name, t.group)
	if err != nil {
		return err
	}
	t.metrics.PendingEntries.WithLabelValues(t.agentID, ref.Name).Set(float64(stats.Count))
	if stats.Count == 0 {
		return nil
	}

	deliveries, err := t.client.Reclaim(ctx, ref, t.group, t.consumer, t.minIdle, t.batch)
	if err != nil {
		return err
	}

	for _, d := range deliveries {
		log := observability.ForEnvelope(t.log, d.Envelope)
		if d.Unprocessable != nil {
			log.Error("reclaimed entry is unprocessable",
				slog.String("source", ref.Name),
				slog.String("position", d.Position),
				slog.String("raw", string(d.Raw)))
			t.metrics.EventsUnprocessable.WithLabelValues(t.agentID, ref.Name, "parse").Inc()
		} else {
			log.Warn("reclaimed abandoned entry",
				slog.String("source", ref.Name),
				slog.String("position", d.Position))
		}
		if err := d.Ack(ctx); err != nil {
			return fmt.Errorf("ack reclaimed entry %s: %w", d.Position, err)
		}
		t.metrics.EntriesReclaimed.WithLabelValues(t.agentID, ref.Name).Inc()
	}

	retired, err := t.client.RetireConsumers(ctx, ref.Name, t.group, t.retireIdle, t.log)
	if err != nil {
		return err
	}
	if retired > 0 {
		t.log.Info("retired idle consumers",
			slog.String("source", ref.Name), slog.Int("count", retired))
	}
	return nil
}
