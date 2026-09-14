// Command conflux runs the pulse-conflux event agent runtime.
//
// This file wires; it does not decide. Configuration comes from the
// environment, agents are built from it, and the runner supervises them until a
// signal arrives.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"in.dmart.pulse.conflux/internal/agent"
	"in.dmart.pulse.conflux/internal/config"
	"in.dmart.pulse.conflux/internal/dispatch/stub"
	ingestredis "in.dmart.pulse.conflux/internal/ingest/redis"
	"in.dmart.pulse.conflux/internal/observability"
)

func main() {
	if err := run(); err != nil {
		// The logger may not exist yet if config failed, so use the default.
		slog.Error("fatal", slog.String("error", err.Error()))
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("configuration: %w", err)
	}

	log := observability.NewLogger(cfg.LogLevel)
	metrics := observability.NewMetrics()

	// Signals cancel the root context; everything downstream drains from there.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	log.Info("starting",
		slog.String("component", "main"),
		slog.String("instance_id", cfg.InstanceID),
		slog.Int("agents_configured", len(cfg.Agents)),
		slog.Int("agents_enabled", len(cfg.Enabled())))

	redisClient, err := ingestredis.NewClient(ctx, cfg.Redis, log)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := redisClient.Close(); cerr != nil {
			log.Warn("redis close", slog.String("error", cerr.Error()))
		}
	}()

	agents, cleanup, err := agent.Build(ctx, cfg, agent.Deps{
		Log:     log,
		Metrics: metrics,
		Redis:   redisClient,
	})
	if err != nil {
		return fmt.Errorf("building agents: %w", err)
	}
	defer cleanup()

	if len(agents) == 0 {
		// Legitimate: the service serves its probes and processes nothing.
		log.Warn("no agents enabled; serving probes only",
			slog.String("component", "main"))
	}

	runner := agent.NewRunner(log, metrics, cfg.ShutdownGrace)
	runner.Start(ctx, agents)

	// Support notification is stubbed while EMAIL_ENABLED=false.
	notifier := stub.NewEmailNotifier(cfg.Anomaly, log)
	detector := observability.NewDetector(
		cfg.Anomaly.NotifyCooldown, cfg.Anomaly.MaxPerHour,
		cfg.Anomaly.LagSustainedFor, notifier, metrics, log)

	// The monitor samples lag on its own interval rather than from inside an
	// agent, so a wedged agent still gets measured.
	monitor := agent.NewMonitor(agents, cfg, metrics, detector, runner, log)
	go monitor.Run(ctx)

	srv := observability.NewServer(cfg, metrics, monitor, redisClient, log)
	srvErr := srv.Start()

	select {
	case <-ctx.Done():
		log.Info("signal received; draining",
			slog.String("component", "main"),
			slog.Duration("grace", cfg.ShutdownGrace))
	case err := <-srvErr:
		if err != nil && !errors.Is(err, context.Canceled) {
			log.Error("operational server failed", slog.String("error", err.Error()))
		}
		stop()
	}

	runner.Wait()
	srv.Shutdown()
	log.Info("stopped", slog.String("component", "main"))
	return nil
}
