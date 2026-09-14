package housekeeping

import (
	"context"
	"fmt"
	"log/slog"

	"in.dmart.pulse.conflux/internal/config"
	"in.dmart.pulse.conflux/internal/ingest"
	ingestredis "in.dmart.pulse.conflux/internal/ingest/redis"
	"in.dmart.pulse.conflux/internal/observability"
)

// Task is what a periodic agent needs. Declared here rather than imported
// from internal/agent, because agent imports this package and the reverse would
// be a cycle.
type Task interface {
	Name() string
	Run(ctx context.Context) error
}

// TaskFor builds the periodic task an agent's id selects.
//
// Periodic agents are named by what they do, so the mapping is explicit rather
// than inferred: an unrecognised id is a configuration error, not a silent no-op.
func TaskFor(ac config.Agent, cfg *config.Config, client *ingestredis.Client, m *observability.Metrics, log *slog.Logger) (Task, error) {
	sources := sourceRefs(cfg)
	group := ingestredis.GroupName(cfg.Redis.ConsumerGrp, ac.ID)
	consumer := ingestredis.ConsumerName(cfg.Redis.ConsumerName, ac.ID, cfg.InstanceID)

	switch {
	case ac.Source == "reclaim" || ac.ID == "pending_reclaim" || ac.ID == "pending-reclaim":
		return NewReclaimTask(client, sources, group, consumer,
			cfg.House.PendingMinIdle, cfg.House.PendingBatch,
			cfg.House.ConsumerRetireIdle, ac.ID, m, log), nil
	case ac.Source == "report" || ac.ID == "queue_report" || ac.ID == "queue-report":
		return NewReportTask(client, sources, group, ac.ID, m, log), nil
	default:
		return nil, fmt.Errorf("no periodic task known for agent %q (known: pending-reclaim, queue-report; set AGENT_%s_SOURCE to reclaim or report)", ac.ID, ac.Key)
	}
}

// sourceRefs derives the set of sources housekeeping touches from the agents
// that are configured — never from a literal, since these names are contracts.
func sourceRefs(cfg *config.Config) []ingest.SourceRef {
	seen := map[string]bool{}
	var out []ingest.SourceRef
	for _, a := range cfg.Agents {
		if a.Source == "" || seen[a.Source] {
			continue
		}
		kind := ingest.Kind(a.SourceKind)
		if kind != ingest.KindStream && kind != ingest.KindList {
			continue
		}
		seen[a.Source] = true
		out = append(out, ingest.SourceRef{Name: a.Source, Kind: kind})
	}
	return out
}
