package observability

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"in.dmart.pulse.conflux/internal/config"
)

// AgentHealth is one agent's condition as readiness reports it.
type AgentHealth struct {
	ID             string `json:"id"`
	Mode           string `json:"mode,omitempty"`
	Source         string `json:"source,omitempty"`
	State          string `json:"state"`
	LagEntries     int64  `json:"lag_entries"`
	LagApproximate bool   `json:"lag_approximate"`
	LastError      string `json:"last_error,omitempty"`
}

// HealthReporter supplies the live view readiness renders.
type HealthReporter interface {
	AgentHealth() []AgentHealth
}

// Pinger reports whether the queue is reachable.
type Pinger interface {
	Ping(ctx context.Context) error
}

// Server serves /livez, /readyz and /metrics.
type Server struct {
	cfg      *config.Config
	metrics  *Metrics
	reporter HealthReporter
	pinger   Pinger
	log      *slog.Logger
	srv      *http.Server
	handler  http.Handler
}

// Handler exposes the mux so the endpoints can be exercised without binding a
// port.
func (s *Server) Handler() http.Handler { return s.handler }

// NewServer builds the operational server.
func NewServer(cfg *config.Config, m *Metrics, reporter HealthReporter, pinger Pinger, log *slog.Logger) *Server {
	s := &Server{cfg: cfg, metrics: m, reporter: reporter, pinger: pinger, log: log}

	mux := http.NewServeMux()
	mux.HandleFunc("/livez", s.livez)
	mux.HandleFunc("/readyz", s.readyz)
	mux.Handle("/metrics", promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{}))

	s.handler = mux
	s.srv = &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	return s
}

// Start serves in the background and reports a listen failure on the channel.
func (s *Server) Start() <-chan error {
	errCh := make(chan error, 1)
	go func() {
		s.log.Info("operational endpoints listening",
			slog.String("component", "http"),
			slog.String("addr", s.cfg.HTTPAddr))
		if err := s.srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
			return
		}
		errCh <- nil
	}()
	return errCh
}

// Shutdown stops the server.
func (s *Server) Shutdown() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = s.srv.Shutdown(ctx)
}

// livez answers whether the process is alive — nothing more.
//
// It deliberately does NOT consult Redis or lag: a liveness probe that fails on
// a dependency outage causes a restart loop that fixes nothing.
func (s *Server) livez(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

// readyz answers whether the service is KEEPING UP.
//
// This is where lag becomes the health signal (FR-023): readiness degrades when
// the queue is unreachable, an agent is behind its threshold, an agent is
// degraded, or a lag reading is approximate — which means entries were trimmed
// from under the group and data was lost upstream.
func (s *Server) readyz(w http.ResponseWriter, r *http.Request) {
	type response struct {
		Status string        `json:"status"`
		Redis  string        `json:"redis"`
		Agents []AgentHealth `json:"agents"`
		Reason string        `json:"reason,omitempty"`
	}

	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()

	resp := response{Status: "ready", Redis: "ok"}
	degraded := ""

	if err := s.pinger.Ping(ctx); err != nil {
		resp.Redis = "unreachable"
		degraded = "redis unreachable: " + err.Error()
	}

	resp.Agents = s.reporter.AgentHealth()
	for _, a := range resp.Agents {
		switch {
		case a.State == "degraded":
			degraded = "agent " + a.ID + " is degraded: " + a.LastError
		case a.LagApproximate:
			// Not a missing number — a lost one. Entries were trimmed before
			// this agent read them.
			degraded = "agent " + a.ID + " lag is approximate on " + a.Source +
				": entries were trimmed from under the consumer group"
		case a.LagEntries > s.cfg.Anomaly.LagThresholdEntries:
			degraded = "agent " + a.ID + " is behind on " + a.Source
		}
		if degraded != "" {
			break
		}
	}

	code := http.StatusOK
	if degraded != "" {
		resp.Status = "degraded"
		resp.Reason = degraded
		code = http.StatusServiceUnavailable
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(resp)
}
