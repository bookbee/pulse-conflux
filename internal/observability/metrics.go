package observability

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics is the full instrument set from
// specs/001-event-agent-runtime/contracts/operational-endpoints.md.
type Metrics struct {
	Registry *prometheus.Registry

	LagEntries     *prometheus.GaugeVec
	LagApproximate *prometheus.GaugeVec

	EventsProcessed     *prometheus.CounterVec
	EventsUnprocessable *prometheus.CounterVec

	DispatchAttempts *prometheus.CounterVec
	DispatchDropped  *prometheus.CounterVec
	DispatchDuration *prometheus.HistogramVec

	AgentRuns        *prometheus.CounterVec
	AgentRunDuration *prometheus.HistogramVec
	AgentPanics      *prometheus.CounterVec

	PendingEntries   *prometheus.GaugeVec
	EntriesReclaimed *prometheus.CounterVec

	AnomaliesActive   *prometheus.GaugeVec
	NotificationsSent *prometheus.CounterVec

	ListDepth *prometheus.GaugeVec
}

// NewMetrics registers every instrument on its own registry, so tests can build
// an isolated set without colliding on the default one.
func NewMetrics() *Metrics {
	r := prometheus.NewRegistry()
	f := promauto.With(r)
	return &Metrics{
		Registry: r,
		LagEntries: f.NewGaugeVec(prometheus.GaugeOpts{
			Name: "conflux_agent_lag_entries",
			Help: "Entries this agent is behind on its source.",
		}, []string{"agent", "source"}),
		LagApproximate: f.NewGaugeVec(prometheus.GaugeOpts{
			Name: "conflux_agent_lag_approximate",
			Help: "1 when lag was derived because the server reported NULL, meaning entries were trimmed from under the group.",
		}, []string{"agent", "source"}),
		EventsProcessed: f.NewCounterVec(prometheus.CounterOpts{
			Name: "conflux_events_processed_total",
			Help: "Envelopes successfully processed.",
		}, []string{"agent", "source"}),
		EventsUnprocessable: f.NewCounterVec(prometheus.CounterOpts{
			Name: "conflux_events_unprocessable_total",
			Help: "Entries that could not be parsed or validated.",
		}, []string{"agent", "source", "reason"}),
		DispatchAttempts: f.NewCounterVec(prometheus.CounterOpts{
			Name: "conflux_dispatch_attempts_total",
			Help: "Dispatch attempts by outcome (delivered, transient, permanent).",
		}, []string{"agent", "destination", "outcome"}),
		DispatchDropped: f.NewCounterVec(prometheus.CounterOpts{
			Name: "conflux_dispatch_dropped_total",
			Help: "Events dropped after exhausting the attempt budget or on a permanent failure.",
		}, []string{"agent", "destination", "reason"}),
		DispatchDuration: f.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "conflux_dispatch_duration_seconds",
			Help:    "Time to resolve a dispatch across all attempts.",
			Buckets: prometheus.DefBuckets,
		}, []string{"agent", "destination"}),
		AgentRuns: f.NewCounterVec(prometheus.CounterOpts{
			Name: "conflux_agent_runs_total",
			Help: "Periodic agent runs by outcome, including skipped_overlap.",
		}, []string{"agent", "outcome"}),
		AgentRunDuration: f.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "conflux_agent_run_duration_seconds",
			Help:    "Periodic agent run duration.",
			Buckets: prometheus.DefBuckets,
		}, []string{"agent"}),
		AgentPanics: f.NewCounterVec(prometheus.CounterOpts{
			Name: "conflux_agent_panics_total",
			Help: "Recovered panics, attributed to the agent that raised them.",
		}, []string{"agent"}),
		PendingEntries: f.NewGaugeVec(prometheus.GaugeOpts{
			Name: "conflux_pending_entries",
			Help: "Entries pending for this agent's consumer group.",
		}, []string{"agent", "source"}),
		EntriesReclaimed: f.NewCounterVec(prometheus.CounterOpts{
			Name: "conflux_entries_reclaimed_total",
			Help: "Entries reclaimed from consumers that are no longer active.",
		}, []string{"agent", "source"}),
		AnomaliesActive: f.NewGaugeVec(prometheus.GaugeOpts{
			Name: "conflux_anomalies_active",
			Help: "Currently active anomalies by type.",
		}, []string{"type"}),
		NotificationsSent: f.NewCounterVec(prometheus.CounterOpts{
			Name: "conflux_notifications_sent_total",
			Help: "Support notifications by type and result, including suppressed.",
		}, []string{"type", "result"}),
		ListDepth: f.NewGaugeVec(prometheus.GaugeOpts{
			Name: "conflux_log_list_depth",
			Help: "Depth of the log list; the gateway refuses writes at its cap.",
		}, []string{"source"}),
	}
}
