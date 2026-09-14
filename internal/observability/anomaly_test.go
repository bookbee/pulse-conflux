package observability

import (
	"sync"
	"testing"
	"time"
)

type recordingNotifier struct {
	mu   sync.Mutex
	sent []string
}

func (n *recordingNotifier) Notify(subject, _ string) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.sent = append(n.sent, subject)
	return nil
}

func (n *recordingNotifier) count() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return len(n.sent)
}

func newTestDetector(cooldown time.Duration, maxPerHour int, sustain time.Duration) (*Detector, *recordingNotifier) {
	n := &recordingNotifier{}
	d := NewDetector(cooldown, maxPerHour, sustain, n, NewMetrics(), NewLogger("error"))
	return d, n
}

// The sustain window is what stops a single burst paging someone.
func TestSustainWindowGatesNotification(t *testing.T) {
	d, n := newTestDetector(time.Minute, 10, 3*time.Minute)
	t0 := time.Now()

	d.Observe(LagThreshold, "events_ltr", true, true, "lag 5000", t0)
	if n.count() != 0 {
		t.Fatal("notified before the sustain window elapsed")
	}
	d.Observe(LagThreshold, "events_ltr", true, true, "lag 5000", t0.Add(2*time.Minute))
	if n.count() != 0 {
		t.Fatal("notified at 2m with a 3m sustain window")
	}
	d.Observe(LagThreshold, "events_ltr", true, true, "lag 5000", t0.Add(3*time.Minute))
	if n.count() != 1 {
		t.Fatalf("notifications = %d at the sustain boundary, want 1", n.count())
	}
}

// A brief breach that clears before the window must never notify at all.
func TestBurstBelowSustainWindowNeverNotifies(t *testing.T) {
	d, n := newTestDetector(time.Minute, 10, 3*time.Minute)
	t0 := time.Now()

	d.Observe(LagThreshold, "a", true, true, "spike", t0)
	d.Observe(LagThreshold, "a", false, true, "", t0.Add(30*time.Second))

	if n.count() != 0 {
		t.Fatalf("a burst that cleared produced %d notifications, want 0", n.count())
	}
}

// A persistent condition must produce a bounded number of messages, not one per
// check (SC-007).
func TestCooldownSuppressesRepeatNotifications(t *testing.T) {
	d, n := newTestDetector(30*time.Minute, 10, 0)
	t0 := time.Now()

	for i := range 20 {
		d.Observe(DispatchDropRate, "ltr", true, false, "dropping", t0.Add(time.Duration(i)*time.Minute))
	}
	if got := n.count(); got != 1 {
		t.Fatalf("20 checks over 20 minutes with a 30m cooldown produced %d notifications, want 1", got)
	}

	d.Observe(DispatchDropRate, "ltr", true, false, "dropping", t0.Add(31*time.Minute))
	if got := n.count(); got != 2 {
		t.Fatalf("after the cooldown elapsed, notifications = %d, want 2", got)
	}
}

func TestHourlyCeilingBoundsNotifications(t *testing.T) {
	d, n := newTestDetector(time.Millisecond, 2, 0)
	t0 := time.Now()

	for i := range 10 {
		d.Observe(SourceUnavailable, "redis", true, false, "down", t0.Add(time.Duration(i)*time.Minute))
	}
	if got := n.count(); got > 2 {
		t.Fatalf("notifications = %d, want at most the ceiling of 2", got)
	}
}

// Exactly one "resolved" message when the condition clears, however many were
// suppressed while it was active.
func TestClearSendsExactlyOneResolvedNotification(t *testing.T) {
	d, n := newTestDetector(30*time.Minute, 10, 0)
	t0 := time.Now()

	d.Observe(LagThreshold, "a", true, false, "behind", t0)
	for i := 1; i < 5; i++ {
		d.Observe(LagThreshold, "a", true, false, "behind", t0.Add(time.Duration(i)*time.Minute))
	}
	before := n.count()

	d.Observe(LagThreshold, "a", false, false, "", t0.Add(10*time.Minute))
	if got := n.count() - before; got != 1 {
		t.Fatalf("clearing produced %d notifications, want exactly 1", got)
	}
	// Staying clear must stay quiet.
	d.Observe(LagThreshold, "a", false, false, "", t0.Add(11*time.Minute))
	if got := n.count() - before; got != 1 {
		t.Fatalf("a second clear check notified again: %d", got)
	}
	if d.Active() != 0 {
		t.Fatalf("Active() = %d after clearing, want 0", d.Active())
	}
}

func TestScopesAreIndependent(t *testing.T) {
	d, n := newTestDetector(time.Hour, 10, 0)
	t0 := time.Now()

	d.Observe(LagThreshold, "agent_a", true, false, "behind", t0)
	d.Observe(LagThreshold, "agent_b", true, false, "behind", t0)

	if got := n.count(); got != 2 {
		t.Fatalf("two agents breaching produced %d notifications, want 2 — scopes must not share a cooldown", got)
	}
	if d.Active() != 2 {
		t.Fatalf("Active() = %d, want 2", d.Active())
	}
}
