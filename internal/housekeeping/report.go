package housekeeping

import (
	"context"
	"log/slog"

	"in.dmart.pulse.conflux/internal/ingest"
	ingestredis "in.dmart.pulse.conflux/internal/ingest/redis"
	"in.dmart.pulse.conflux/internal/observability"
)

// ReportTask publishes read-only facts about the queue: lengths, group counts,
// pending counts, list depth.
//
// Read-only is the entire contract. It issues no write of any kind, which is
// what FR-020 authorises and FR-020a bounds.
type ReportTask struct {
	client  *ingestredis.Client
	sources []ingest.SourceRef
	group   string
	agentID string
	metrics *observability.Metrics
	log     *slog.Logger
}

// NewReportTask builds the reporting pass.
func NewReportTask(c *ingestredis.Client, sources []ingest.SourceRef, group, agentID string, m *observability.Metrics, log *slog.Logger) *ReportTask {
	return &ReportTask{client: c, sources: sources, group: group, agentID: agentID, metrics: m, log: log}
}

func (t *ReportTask) Name() string { return "queue-report" }

// Run gathers and logs the current state of every configured source.
func (t *ReportTask) Run(ctx context.Context) error {
	for _, ref := range t.sources {
		switch ref.Kind {
		case ingest.KindStream:
			rep, err := t.client.ReportStream(ctx, ref.Name)
			if err != nil {
				return err
			}
			pending, perr := t.client.Pending(ctx, ref.Name, t.group)
			if perr == nil {
				t.metrics.PendingEntries.WithLabelValues(t.agentID, ref.Name).Set(float64(pending.Count))
			}
			t.log.Info("queue report",
				slog.String("component", "housekeeping"),
				slog.String("source", ref.Name),
				slog.Int64("length", rep.Length),
				slog.Int64("groups", rep.Groups),
				slog.Int64("pending", pending.Count))

		case ingest.KindList:
			depth, err := t.client.ListDepth(ctx, ref.Name)
			if err != nil {
				return err
			}
			t.metrics.ListDepth.WithLabelValues(ref.Name).Set(float64(depth))
			t.log.Info("queue report",
				slog.String("component", "housekeeping"),
				slog.String("source", ref.Name),
				slog.Int64("depth", depth))
		}
	}
	return nil
}
