// Package observability owns structured logging, metrics, and the operational
// HTTP endpoints. Lag is the health signal here, not process liveness
// (Constitution Principle IV).
package observability

import (
	"log/slog"
	"os"
	"strings"

	"in.dmart.pulse.conflux/internal/envelope"
)

// NewLogger builds the base JSON logger. slog matches pulse-gateway's choice,
// so log shapes line up across the platform.
func NewLogger(level string) *slog.Logger {
	var l slog.Level
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "debug":
		l = slog.LevelDebug
	case "warn", "warning":
		l = slog.LevelWarn
	case "error":
		l = slog.LevelError
	default:
		l = slog.LevelInfo
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: l}))
}

// ForEnvelope returns a logger carrying the envelope's identity.
//
// FR-024 requires event_id and gateway_id on EVERY line about an envelope. The
// ingestion boundary attaches them once, here, because "every line" is
// otherwise unenforceable — a rule each call site has to remember is a rule
// that decays.
func ForEnvelope(base *slog.Logger, env envelope.Envelope) *slog.Logger {
	return base.With(
		slog.String("event_id", env.EventID),
		slog.String("gateway_id", env.GatewayID),
	)
}

// ForAgent returns a logger scoped to one agent and source.
func ForAgent(base *slog.Logger, agentID, source string) *slog.Logger {
	return base.With(slog.String("agent", agentID), slog.String("source", source))
}
