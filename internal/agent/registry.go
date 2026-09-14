package agent

import (
	"context"
	"fmt"
	"log/slog"

	"in.dmart.pulse.conflux/internal/config"
	"in.dmart.pulse.conflux/internal/dispatch"
	"in.dmart.pulse.conflux/internal/dispatch/stub"
	"in.dmart.pulse.conflux/internal/housekeeping"
	"in.dmart.pulse.conflux/internal/ingest"
	ingestredis "in.dmart.pulse.conflux/internal/ingest/redis"
	"in.dmart.pulse.conflux/internal/observability"
)

// Deps is everything an agent might need, supplied once by main.
type Deps struct {
	Log     *slog.Logger
	Metrics *observability.Metrics
	Redis   *ingestredis.Client
}

// Build constructs every enabled agent from configuration.
//
// Disabled agents are not constructed at all, which is what makes disabling one
// free of side effects on the others (FR-003).
func Build(ctx context.Context, cfg *config.Config, deps Deps) ([]Agent, func(), error) {
	var (
		agents  []Agent
		closers []func()
	)
	cleanup := func() {
		for _, c := range closers {
			c()
		}
	}

	for _, ac := range cfg.Enabled() {
		a, closer, err := buildOne(ctx, cfg, ac, deps)
		if err != nil {
			cleanup()
			return nil, func() {}, fmt.Errorf("agent %q: %w", ac.ID, err)
		}
		if closer != nil {
			closers = append(closers, closer)
		}
		agents = append(agents, a)
	}
	return agents, cleanup, nil
}

func buildOne(ctx context.Context, cfg *config.Config, ac config.Agent, deps Deps) (Agent, func(), error) {
	dests, err := destinationsFor(cfg, ac)
	if err != nil {
		return nil, nil, err
	}
	dispatcher := dispatch.NewDispatcher(ac.ID, dests, dispatch.NewPolicy(cfg.Dispatch), deps.Metrics, deps.Log)

	switch ac.Mode {
	case config.ModeStream:
		src, err := sourceFor(ctx, cfg, ac, deps)
		if err != nil {
			return nil, nil, err
		}
		// A continuous agent over the LIST is the log drain: it folds entries
		// into period summaries rather than forwarding each one (FR-018c).
		if ingest.Kind(ac.SourceKind) == ingest.KindList {
			return NewLogDrainAgent(ac.ID, src, cfg.House.SummaryPeriod, dispatcher, deps.Metrics, deps.Log),
				func() { _ = src.Close() }, nil
		}
		return NewStreamAgent(ac.ID, src, dispatcher, deps.Metrics, deps.Log),
			func() { _ = src.Close() }, nil

	case config.ModePeriodic:
		task, err := housekeeping.TaskFor(ac, cfg, deps.Redis, deps.Metrics, deps.Log)
		if err != nil {
			return nil, nil, err
		}
		return NewPeriodicAgent(ac.ID, ac.Interval, task, deps.Metrics, deps.Log), nil, nil

	default:
		// config validation rejects this first; this is the belt to that braces.
		return nil, nil, fmt.Errorf("unsupported mode %q", ac.Mode)
	}
}

func sourceFor(ctx context.Context, cfg *config.Config, ac config.Agent, deps Deps) (ingest.Source, error) {
	ref := ingest.SourceRef{Name: ac.Source, Kind: ingest.Kind(ac.SourceKind)}
	switch ref.Kind {
	case ingest.KindStream:
		return ingestredis.NewStreamSource(ctx, deps.Redis, ref,
			ingestredis.GroupName(cfg.Redis.ConsumerGrp, ac.ID),
			ingestredis.ConsumerName(cfg.Redis.ConsumerName, ac.ID, cfg.InstanceID),
			ac.BatchSize, ac.BlockTimeout, deps.Log)
	case ingest.KindList:
		return ingestredis.NewListSource(deps.Redis, ref, ac.BlockTimeout, deps.Log), nil
	default:
		return nil, fmt.Errorf("unsupported source kind %q", ref.Kind)
	}
}

func destinationsFor(cfg *config.Config, ac config.Agent) ([]dispatch.Destination, error) {
	var out []dispatch.Destination
	for _, name := range ac.Destinations {
		switch name {
		case "ltr":
			out = append(out, stub.NewLTR(cfg.Dispatch.LTRURL, ac.ID))
		case "cep":
			out = append(out, stub.NewCEP(cfg.Dispatch.CEPURL, ac.ID))
		case "summary":
			out = append(out, stub.NewSummary(cfg.House.SummaryDestination, ac.ID))
		case "none":
			// A periodic agent whose work has no outbound call (reclaim).
		default:
			return nil, fmt.Errorf("unknown destination %q (known: ltr, cep, summary, none)", name)
		}
	}
	return out, nil
}
