//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"in.dmart.pulse.conflux/internal/observability"
)

// Quickstart Scenario 2: stop mid-stream, restart, resume from the last
// acknowledged position with nothing skipped (SC-006). Then replay the same
// batch and confirm the destination's end state is unchanged (SC-003).
func TestRestartResumesWithoutGapAndReplayIsIdempotent(t *testing.T) {
	rdb := dialRedis(t)
	stream := uniqueKey("resume")
	t.Cleanup(func() { rdb.Del(context.Background(), stream) })

	rec, srv := newRecorder()
	defer srv.Close()

	// First run: consume two entries, then stop.
	m1 := observability.NewMetrics()
	a1, _ := buildStreamAgent(t, stream, srv.URL, "resume_agent", m1)

	ctx1, cancel1 := context.WithCancel(context.Background())
	go func() { _ = a1.Run(ctx1) }()

	seed(t, rdb, stream, "evt-1")
	seed(t, rdb, stream, "evt-2")
	eventually(t, 10*time.Second, "the first two entries", func() bool { return rec.count() == 2 })
	cancel1()
	time.Sleep(300 * time.Millisecond) // let the agent finish draining

	// More entries arrive while nothing is consuming.
	seed(t, rdb, stream, "evt-3")
	seed(t, rdb, stream, "evt-4")

	// Second run: same agent id, so the same consumer group and position.
	m2 := observability.NewMetrics()
	a2, _ := buildStreamAgent(t, stream, srv.URL, "resume_agent", m2)

	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	go func() { _ = a2.Run(ctx2) }()

	eventually(t, 10*time.Second, "the entries written while stopped", func() bool {
		return rec.count() == 4
	})

	seen := map[string]int{}
	for _, k := range rec.seen() {
		seen[k]++
	}
	for _, id := range []string{"evt-1", "evt-2", "evt-3", "evt-4"} {
		if seen[id] == 0 {
			t.Errorf("%s was never dispatched — the restart left a gap", id)
		}
	}

	// SC-003: replay the same batch. Redelivery is normal operation, so the
	// destination sees the same Idempotency-Key values it already absorbed and
	// its end state — the SET of keys — is unchanged.
	before := distinctKeys(rec.seen())

	seed(t, rdb, stream, "evt-1")
	seed(t, rdb, stream, "evt-2")
	eventually(t, 10*time.Second, "the replayed batch", func() bool { return rec.count() >= 6 })

	after := distinctKeys(rec.seen())
	if len(after) != len(before) {
		t.Fatalf("replay produced new distinct effects: %d keys before, %d after.\n"+
			"Redelivery must be absorbable — that is what Idempotency-Key is for.", len(before), len(after))
	}
}

func distinctKeys(keys []string) map[string]bool {
	out := map[string]bool{}
	for _, k := range keys {
		out[k] = true
	}
	return out
}
