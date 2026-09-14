//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"in.dmart.pulse.conflux/internal/agent"
	"in.dmart.pulse.conflux/internal/observability"
)

// Quickstart Scenario 4: lag over the threshold degrades /readyz and names the
// agent, while /livez stays 200 — the process is alive, it is just behind
// (SC-004).
func TestLagDegradesReadinessButNotLiveness(t *testing.T) {
	rdb := dialRedis(t)
	stream := uniqueKey("lag")
	t.Cleanup(func() { rdb.Del(context.Background(), stream) })

	_, srv := newRecorder()
	defer srv.Close()

	m := observability.NewMetrics()
	// Built but never Run: the agent exists and holds a position, so lag grows.
	a, _ := buildStreamAgent(t, stream, srv.URL, "lagging_agent", m)

	cfg := testConfig()
	cfg.Anomaly.LagThresholdEntries = 3

	detector := observability.NewDetector(time.Hour, 4, 0, nil, m, testLogger())
	runner := agent.NewRunner(testLogger(), m, time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runner.Start(ctx, nil) // the agent is deliberately not started

	monitor := agent.NewMonitor([]agent.Agent{a}, cfg, m, detector, runner, testLogger())

	for i := range 10 {
		seed(t, rdb, stream, string(rune('a'+i)))
	}
	go monitor.Run(ctx)

	server := observability.NewServer(cfg, m, monitor,
		pingFunc(func(ctx context.Context) error { return rdb.Ping(ctx).Err() }), testLogger())

	// /livez never consults Redis or lag.
	live := httptest.NewRecorder()
	httpGet(t, server, "/livez", live)
	if live.Code != http.StatusOK {
		t.Fatalf("/livez = %d while merely behind; a liveness probe that fails on lag causes a restart loop that fixes nothing", live.Code)
	}

	eventually(t, 10*time.Second, "readiness to reflect lag", func() bool {
		rec := httptest.NewRecorder()
		httpGet(t, server, "/readyz", rec)
		return rec.Code == http.StatusServiceUnavailable
	})

	rec := httptest.NewRecorder()
	httpGet(t, server, "/readyz", rec)

	var body struct {
		Status string `json:"status"`
		Reason string `json:"reason"`
		Agents []struct {
			ID             string `json:"id"`
			LagEntries     int64  `json:"lag_entries"`
			LagApproximate bool   `json:"lag_approximate"`
		} `json:"agents"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("decode /readyz: %v", err)
	}
	if body.Status != "degraded" {
		t.Errorf("status = %q, want degraded", body.Status)
	}
	if body.Reason == "" {
		t.Error("a degraded readiness must say WHY and name the agent")
	}
	if len(body.Agents) == 0 || body.Agents[0].LagEntries < 3 {
		t.Errorf("readiness reports lag %+v, want at least 3 entries behind", body.Agents)
	}
}

// The case that matters more than the threshold: entries trimmed away before
// the group read them.
//
// Verified behaviour, not assumption: Redis keeps reporting a confident lag
// after MAXLEN trimming, so lag ALONE cannot see this — it understates reality
// and the stream looks healthy. Detection compares the group's position against
// the stream's first surviving entry; the reading is then marked approximate so
// readiness and the anomaly path treat it as the upstream loss it is.
func TestTrimmedEntriesProduceApproximateLagNotZero(t *testing.T) {
	rdb := dialRedis(t)
	stream := uniqueKey("trimmed")
	ctx := context.Background()
	t.Cleanup(func() { rdb.Del(ctx, stream) })

	_, srv := newRecorder()
	defer srv.Close()

	m := observability.NewMetrics()
	a, src := buildStreamAgent(t, stream, srv.URL, "trimmed_agent", m)
	_ = a

	for i := range 20 {
		seed(t, rdb, stream, string(rune('a'+i)))
	}
	// Trim hard, from under the group that has read nothing. This is the local
	// stack's 10k cap in miniature.
	if err := rdb.XTrimMaxLen(ctx, stream, 2).Err(); err != nil {
		t.Fatalf("XTRIM: %v", err)
	}

	reading, err := src.Lag(ctx)
	if err != nil {
		t.Fatalf("Lag: %v", err)
	}
	if !reading.Approximate {
		t.Fatalf("lag reading after trimming = %+v, want Approximate=true.\n"+
			"Redis still reports a confident lag after MAXLEN trimming, so a reading that "+
			"trusts it hides entries destroyed before this group ever read them.", reading)
	}

	// And the confident-looking number is indeed an undercount: 20 entries were
	// written, 18 trimmed away unread.
	t.Logf("lag reported %d entries with Approximate=%v after 18 of 20 were trimmed unread",
		reading.Entries, reading.Approximate)
}

// pingFunc adapts a plain function to observability.Pinger.
type pingFunc func(context.Context) error

func (f pingFunc) Ping(ctx context.Context) error { return f(ctx) }

func httpGet(t *testing.T, s *observability.Server, path string, rec *httptest.ResponseRecorder) {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	s.Handler().ServeHTTP(rec, req)
}
