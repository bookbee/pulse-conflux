package housekeeping

import (
	"testing"
	"time"

	"in.dmart.pulse.conflux/internal/envelope"
)

func at(s string) time.Time {
	t, _ := time.Parse(time.RFC3339, s)
	return t
}

func env(gateway, stamp string) envelope.Envelope {
	return envelope.Envelope{
		EventID: "e", GatewayID: gateway, ReceivedAt: at(stamp),
		Payload: []byte(`{}`),
	}
}

// Identity is the period boundary pair, which is what lets a re-sent summary be
// absorbed rather than double-counted.
//
// NOTE: this does NOT test recomputation. The log list is read destructively —
// entries are gone once consumed — so a period can never be read a second time.
// Any test asserting "recompute yields the same summary" would be asserting
// something the source makes impossible.
func TestOneSummaryPerClosedPeriodWithPeriodIdentity(t *testing.T) {
	agg := NewAggregator(time.Hour)

	for range 3 {
		if s := agg.Observe(env("gw-1", "2026-09-14T09:15:00Z"), at("2026-09-14T09:15:00Z")); s != nil {
			t.Fatal("period closed early")
		}
	}

	// An entry in the next hour closes the 09:00 bucket.
	s := agg.Observe(env("gw-1", "2026-09-14T10:01:00Z"), at("2026-09-14T10:01:00Z"))
	if s == nil {
		t.Fatal("crossing into the next period must close the previous one")
	}
	if !s.PeriodStart.Equal(at("2026-09-14T09:00:00Z")) {
		t.Errorf("PeriodStart = %v, want 09:00 (clock-aligned)", s.PeriodStart)
	}
	if !s.PeriodEnd.Equal(at("2026-09-14T10:00:00Z")) {
		t.Errorf("PeriodEnd = %v, want 10:00", s.PeriodEnd)
	}
	if s.EntryCount != 3 {
		t.Errorf("EntryCount = %d, want 3", s.EntryCount)
	}
	if s.Breakdown["gateway:gw-1"] != 3 {
		t.Errorf("Breakdown = %v, want 3 for gw-1", s.Breakdown)
	}
}

// The first bucket after a start is partial: entries consumed before the
// restart are gone, and with no datastore nothing holds them. Declaring the
// undercount beats presenting it as complete (FR-018b).
func TestFirstPeriodAfterStartIsMarkedPartial(t *testing.T) {
	agg := NewAggregator(time.Hour)

	agg.Observe(env("gw-1", "2026-09-14T09:30:00Z"), at("2026-09-14T09:30:00Z"))
	first := agg.Observe(env("gw-1", "2026-09-14T10:05:00Z"), at("2026-09-14T10:05:00Z"))
	if first == nil {
		t.Fatal("want the first period to close")
	}
	if !first.Partial {
		t.Error("the first period after a start must be marked partial — the process may have missed its beginning")
	}

	// The next period was observed from its start, so it is complete.
	second := agg.Observe(env("gw-1", "2026-09-14T11:05:00Z"), at("2026-09-14T11:05:00Z"))
	if second == nil {
		t.Fatal("want the second period to close")
	}
	if second.Partial {
		t.Error("a period observed from its beginning must not be marked partial")
	}
}

// A quiet period must still produce its summary rather than waiting for the
// next entry, which might be hours away.
func TestTickClosesAQuietPeriod(t *testing.T) {
	agg := NewAggregator(time.Hour)
	agg.Observe(env("gw-1", "2026-09-14T09:10:00Z"), at("2026-09-14T09:10:00Z"))

	if s := agg.Tick(at("2026-09-14T09:59:00Z")); s != nil {
		t.Fatal("ticked inside the open period")
	}
	s := agg.Tick(at("2026-09-14T10:00:01Z"))
	if s == nil {
		t.Fatal("a tick past the period boundary must close it even with no new entries")
	}
	if s.EntryCount != 1 {
		t.Errorf("EntryCount = %d, want 1", s.EntryCount)
	}
}

func TestBucketsAreClockAligned(t *testing.T) {
	agg := NewAggregator(15 * time.Minute)
	for _, stamp := range []string{
		"2026-09-14T09:00:00Z", "2026-09-14T09:07:30Z", "2026-09-14T09:14:59Z",
	} {
		if s := agg.Observe(env("g", stamp), at(stamp)); s != nil {
			t.Fatalf("%s closed a period; all three are in the same 15m bucket", stamp)
		}
	}
	s := agg.Observe(env("g", "2026-09-14T09:15:00Z"), at("2026-09-14T09:15:00Z"))
	if s == nil || s.EntryCount != 3 {
		t.Fatalf("want a 3-entry summary at the boundary, got %+v", s)
	}
}
