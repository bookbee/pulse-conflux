//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"in.dmart.pulse.conflux/internal/observability"
)

// SC-011 — the criterion that justifies the whole bounded-retry decision.
//
// With a destination failing continuously, drops must climb while LAG STAYS
// FLAT. If lag grew instead, an unavailable destination would become the reason
// this service falls behind, and falling behind destroys data upstream where
// nothing can recover it. Losing the event at a dead destination is the cheaper
// of the two losses, and that trade is the entire point.
func TestFailingDestinationDropsWithoutGrowingLag(t *testing.T) {
	rdb := dialRedis(t)
	stream := uniqueKey("droplag")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	t.Cleanup(func() { rdb.Del(context.Background(), stream) })

	rec, srv := newRecorder()
	defer srv.Close()
	rec.setFailing(true) // every dispatch 503s, forever

	m := observability.NewMetrics()
	a, src := buildStreamAgent(t, stream, srv.URL, "drop_agent", m)
	go func() { _ = a.Run(ctx) }()

	const total = 30
	for i := range total {
		seed(t, rdb, stream, string(rune('a'+i%26))+string(rune('0'+i/26)))
	}

	// Every event resolves as a drop rather than piling up.
	eventually(t, 60*time.Second, "all events to resolve as drops", func() bool {
		return testutil.ToFloat64(m.DispatchDropped.WithLabelValues("drop_agent", "ltr", "budget_exhausted")) >= total
	})

	dropped := testutil.ToFloat64(m.DispatchDropped.WithLabelValues("drop_agent", "ltr", "budget_exhausted"))
	if dropped < total {
		t.Fatalf("dropped = %v of %d", dropped, total)
	}

	reading, err := src.Lag(context.Background())
	if err != nil {
		t.Fatalf("Lag: %v", err)
	}
	if reading.Entries > 5 {
		t.Fatalf("lag = %d entries while the destination was failing.\n"+
			"A dead destination must never become a backlog: the bounded budget exists so "+
			"loss happens at the destination, not upstream where the stream trims.", reading.Entries)
	}

	// Nothing pending: each event was resolved before its delivery was acked.
	processed := testutil.ToFloat64(m.EventsProcessed.WithLabelValues("drop_agent", stream))
	if processed != 0 {
		t.Errorf("events_processed = %v, want 0 — a dropped event was never delivered", processed)
	}
	t.Logf("%d events dropped, lag held at %d entries", int(dropped), reading.Entries)

	// Recovery: with the destination healthy again, dispatching resumes.
	rec.setFailing(false)
	seed(t, rdb, stream, "recovered")
	eventually(t, 30*time.Second, "dispatching to resume", func() bool { return rec.count() >= 1 })
}

// SC-002 at scale: every event is accounted for as delivered or recorded-dropped
// against an intermittently failing destination. Nothing disappears silently.
func TestEveryEventAccountedForAtScale(t *testing.T) {
	if testing.Short() {
		t.Skip("scale test skipped in -short mode")
	}

	rdb := dialRedis(t)
	stream := uniqueKey("scale")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	t.Cleanup(func() { rdb.Del(context.Background(), stream) })

	rec, srv := newRecorder()
	defer srv.Close()

	m := observability.NewMetrics()
	a, _ := buildStreamAgent(t, stream, srv.URL, "scale_agent", m)
	go func() { _ = a.Run(ctx) }()

	// Flip the destination in and out of failure while events flow.
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		failing := false
		for {
			select {
			case <-stop:
				return
			case <-time.After(300 * time.Millisecond):
				failing = !failing
				rec.setFailing(failing)
			}
		}
	}()

	const total = 2000 // a laptop-sized stand-in for SC-002's 10,000
	for i := range total {
		seed(t, rdb, stream, "evt-"+itoa(i))
	}

	// Every event ends as exactly one of: delivered, or dropped with a record.
	accounted := func() float64 {
		delivered := testutil.ToFloat64(m.EventsProcessed.WithLabelValues("scale_agent", stream))
		budget := testutil.ToFloat64(m.DispatchDropped.WithLabelValues("scale_agent", "ltr", "budget_exhausted"))
		permanent := testutil.ToFloat64(m.DispatchDropped.WithLabelValues("scale_agent", "ltr", "permanent"))
		return delivered + budget + permanent
	}

	eventually(t, 120*time.Second, "every event to be accounted for", func() bool {
		return accounted() >= total
	})

	delivered := testutil.ToFloat64(m.EventsProcessed.WithLabelValues("scale_agent", stream))
	t.Logf("%d events: %v delivered, %v dropped and recorded", total, delivered, accounted()-delivered)

	if got := accounted(); got < total {
		t.Fatalf("only %v of %d events accounted for; %v vanished without a record", got, total, float64(total)-got)
	}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var buf [12]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	return string(buf[pos:])
}
