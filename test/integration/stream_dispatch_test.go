//go:build integration

package integration

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"in.dmart.pulse.conflux/internal/agent"
	"in.dmart.pulse.conflux/internal/dispatch"
	"in.dmart.pulse.conflux/internal/dispatch/stub"
	"in.dmart.pulse.conflux/internal/envelope"
	"in.dmart.pulse.conflux/internal/ingest"
	ingestredis "in.dmart.pulse.conflux/internal/ingest/redis"
	"in.dmart.pulse.conflux/internal/observability"
)

// recorder is the asserted stub every dispatch lands on. No test reaches a real
// system (SC-010).
type recorder struct {
	mu      sync.Mutex
	keys    []string
	bodies  []string
	status  int
	failing bool
}

func newRecorder() (*recorder, *httptest.Server) {
	r := &recorder{status: http.StatusAccepted}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		r.mu.Lock()
		defer r.mu.Unlock()
		if r.failing {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		r.keys = append(r.keys, req.Header.Get("Idempotency-Key"))
		w.WriteHeader(r.status)
	}))
	return r, srv
}

func (r *recorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.keys)
}

func (r *recorder) seen() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.keys...)
}

func (r *recorder) setFailing(v bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.failing = v
}

// buildStreamAgent wires a real StreamSource over a real Redis stream.
func buildStreamAgent(t *testing.T, streamKey, destURL, agentID string, m *observability.Metrics) (*agent.StreamAgent, ingest.Source) {
	t.Helper()
	log := testLogger()
	cfg := testConfig()

	client, err := ingestredis.NewClient(context.Background(), cfg.Redis, log)
	if err != nil {
		t.Skipf("pulse-infra not reachable: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	ref := ingest.SourceRef{Name: streamKey, Kind: ingest.KindStream}
	src, err := ingestredis.NewStreamSource(context.Background(), client, ref,
		ingestredis.GroupName(cfg.Redis.ConsumerGrp, agentID),
		ingestredis.ConsumerName(cfg.Redis.ConsumerName, agentID, cfg.InstanceID),
		10, 200*time.Millisecond, log)
	if err != nil {
		t.Fatalf("NewStreamSource: %v", err)
	}

	dest := stub.NewLTR(destURL, agentID)
	d := dispatch.NewDispatcher(agentID, []dispatch.Destination{dest}, dispatch.NewPolicy(cfg.Dispatch), m, log)
	return agent.NewStreamAgent(agentID, src, d, m, log), src
}

// Quickstart Scenario 1: seed three envelopes, expect three dispatches, the
// group auto-created, and the position advanced past all three.
func TestStreamAgentDrainsAndAcknowledges(t *testing.T) {
	rdb := dialRedis(t)
	stream := uniqueKey("events")
	t.Cleanup(func() { rdb.Del(context.Background(), stream) })

	rec, srv := newRecorder()
	defer srv.Close()

	m := observability.NewMetrics()
	a, _ := buildStreamAgent(t, stream, srv.URL, "events_ltr", m)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = a.Run(ctx) }()

	for _, id := range []string{"evt-1", "evt-2", "evt-3"} {
		seed(t, rdb, stream, id)
	}

	eventually(t, 10*time.Second, "three dispatches", func() bool { return rec.count() == 3 })

	seen := rec.seen()
	want := map[string]bool{"evt-1": true, "evt-2": true, "evt-3": true}
	for _, k := range seen {
		if !want[k] {
			t.Errorf("unexpected Idempotency-Key %q", k)
		}
		delete(want, k)
	}
	if len(want) != 0 {
		t.Errorf("never dispatched: %v", want)
	}

	if got := testutil.ToFloat64(m.EventsProcessed.WithLabelValues("events_ltr", stream)); got != 3 {
		t.Errorf("events_processed = %v, want 3", got)
	}

	// Nothing pending once acknowledged.
	eventually(t, 5*time.Second, "empty pending set", func() bool {
		p, err := rdb.XPending(context.Background(), stream,
			ingestredis.GroupName("test-group", "events_ltr")).Result()
		return err == nil && p.Count == 0
	})
}

// FR-010: an envelope from an API-key gateway request carries NO event_header.
// That is ordinary traffic and must dispatch normally.
func TestEnvelopeWithoutEventHeaderDispatches(t *testing.T) {
	rdb := dialRedis(t)
	stream := uniqueKey("noheader")
	t.Cleanup(func() { rdb.Del(context.Background(), stream) })

	rec, srv := newRecorder()
	defer srv.Close()

	m := observability.NewMetrics()
	a, _ := buildStreamAgent(t, stream, srv.URL, "noheader_agent", m)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = a.Run(ctx) }()

	seed(t, rdb, stream, "evt-no-header") // seed() never sets event_header

	eventually(t, 10*time.Second, "the header-less envelope to dispatch", func() bool {
		return rec.count() == 1
	})
	if got := testutil.ToFloat64(m.EventsUnprocessable.WithLabelValues("noheader_agent", stream, "parse")); got != 0 {
		t.Fatalf("a missing event_header was treated as an error: unprocessable = %v", got)
	}
}

// FR-012: one bad entry must not stop the rest, and must be recorded rather
// than silently dropped.
func TestUnprocessableEntryDoesNotBlockTheStream(t *testing.T) {
	rdb := dialRedis(t)
	stream := uniqueKey("malformed")
	t.Cleanup(func() { rdb.Del(context.Background(), stream) })

	rec, srv := newRecorder()
	defer srv.Close()

	m := observability.NewMetrics()
	a, _ := buildStreamAgent(t, stream, srv.URL, "malformed_agent", m)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = a.Run(ctx) }()

	// A malformed entry between two good ones.
	seed(t, rdb, stream, "good-1")
	seedRaw(t, rdb, stream, "{not json")
	seed(t, rdb, stream, "good-2")

	eventually(t, 10*time.Second, "both good entries despite the bad one", func() bool {
		return rec.count() == 2
	})
	if got := testutil.ToFloat64(m.EventsUnprocessable.WithLabelValues("malformed_agent", stream, "parse")); got != 1 {
		t.Errorf("unprocessable counter = %v, want 1", got)
	}
}

// Guard against the envelope drifting from the gateway's shape.
func TestStreamEntryCarriesExactlyOneFieldNamedData(t *testing.T) {
	rdb := dialRedis(t)
	stream := uniqueKey("shape")
	t.Cleanup(func() { rdb.Del(context.Background(), stream) })

	seed(t, rdb, stream, "evt-shape")

	msgs, err := rdb.XRange(context.Background(), stream, "-", "+").Result()
	if err != nil || len(msgs) != 1 {
		t.Fatalf("XRANGE: %v (%d messages)", err, len(msgs))
	}
	if len(msgs[0].Values) != 1 {
		t.Fatalf("entry has %d fields, want exactly 1 — the contract is one field named `data`", len(msgs[0].Values))
	}
	raw, ok := msgs[0].Values["data"].(string)
	if !ok {
		t.Fatal("entry has no `data` field")
	}
	if _, err := envelope.Parse([]byte(raw)); err != nil {
		t.Fatalf("the `data` field does not hold a valid envelope: %v", err)
	}
}
