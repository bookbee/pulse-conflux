package dispatch

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"in.dmart.pulse.conflux/internal/envelope"
)

type scriptedDest struct {
	name     string
	errs     []error
	calls    int
	blockFor time.Duration
}

func (d *scriptedDest) Name() string { return d.name }

func (d *scriptedDest) Dispatch(ctx context.Context, _ envelope.Envelope) error {
	d.calls++
	if d.blockFor > 0 {
		select {
		case <-time.After(d.blockFor):
		case <-ctx.Done():
			return &Error{Kind: Transient, Err: ctx.Err()}
		}
	}
	if d.calls <= len(d.errs) {
		return d.errs[d.calls-1]
	}
	return nil
}

func fastPolicy(maxAttempts int) Policy {
	return Policy{
		MaxAttempts: maxAttempts, Timeout: 100 * time.Millisecond,
		BackoffInitial: time.Millisecond, BackoffMax: 2 * time.Millisecond,
	}
}

func TestClassifyStatus(t *testing.T) {
	cases := []struct {
		status int
		kind   FailureKind
		ok     bool
	}{
		{200, Transient, true}, {202, Transient, true},
		{408, Transient, false}, {429, Transient, false},
		{500, Transient, false}, {503, Transient, false},
		{400, Permanent, false}, {401, Permanent, false},
		{404, Permanent, false}, {422, Permanent, false},
	}
	for _, c := range cases {
		kind, ok := ClassifyStatus(c.status)
		if ok != c.ok {
			t.Errorf("status %d: ok = %v, want %v", c.status, ok, c.ok)
			continue
		}
		if !ok && kind != c.kind {
			t.Errorf("status %d: kind = %v, want %v", c.status, kind, c.kind)
		}
	}
}

func TestTransientFailureRetriesWithinBudget(t *testing.T) {
	dest := &scriptedDest{name: "d", errs: []error{
		&Error{Kind: Transient, Status: http.StatusServiceUnavailable, Err: errors.New("503")},
		&Error{Kind: Transient, Status: http.StatusServiceUnavailable, Err: errors.New("503")},
	}}
	attempts, err := fastPolicy(3).Do(context.Background(), dest, envelope.Envelope{}, nil)
	if err != nil {
		t.Fatalf("third attempt should succeed: %v", err)
	}
	if attempts != 3 || dest.calls != 3 {
		t.Fatalf("attempts = %d, calls = %d, want 3 and 3", attempts, dest.calls)
	}
}

// A permanent failure must NOT consume the budget: retrying a payload the
// destination has rejected only burns attempts that a transient failure could
// have used.
func TestPermanentFailureDropsImmediately(t *testing.T) {
	dest := &scriptedDest{name: "d", errs: []error{
		&Error{Kind: Permanent, Status: http.StatusBadRequest, Err: errors.New("400")},
	}}
	attempts, err := fastPolicy(5).Do(context.Background(), dest, envelope.Envelope{}, nil)
	if err == nil {
		t.Fatal("want an error for a permanent failure")
	}
	if dest.calls != 1 {
		t.Fatalf("permanent failure retried: calls = %d, want 1", dest.calls)
	}
	if attempts != 1 {
		t.Fatalf("attempts = %d, want 1", attempts)
	}
}

func TestBudgetExhaustionDrops(t *testing.T) {
	transient := &Error{Kind: Transient, Status: 500, Err: errors.New("500")}
	dest := &scriptedDest{name: "d", errs: []error{transient, transient, transient, transient}}
	attempts, err := fastPolicy(3).Do(context.Background(), dest, envelope.Envelope{}, nil)
	if err == nil {
		t.Fatal("want an error when the budget is exhausted")
	}
	if dest.calls != 3 || attempts != 3 {
		t.Fatalf("calls = %d, attempts = %d, want 3 and 3", dest.calls, attempts)
	}
}

// A destination that HANGS rather than failing is the case the per-attempt
// timeout exists for: without it, one slow destination pins the agent forever
// and lag — not the destination — becomes the outage.
func TestHangingDestinationConsumesBudgetRatherThanBlocking(t *testing.T) {
	dest := &scriptedDest{name: "slow", blockFor: time.Hour}
	start := time.Now()
	_, err := fastPolicy(2).Do(context.Background(), dest, envelope.Envelope{}, nil)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("want an error from a hanging destination")
	}
	if elapsed > 2*time.Second {
		t.Fatalf("policy blocked for %v; the per-attempt timeout is not bounding it", elapsed)
	}
	if dest.calls != 2 {
		t.Fatalf("calls = %d, want 2 — each attempt should time out and count", dest.calls)
	}
}

func TestObserverSeesEveryAttempt(t *testing.T) {
	dest := &scriptedDest{name: "d", errs: []error{
		&Error{Kind: Transient, Status: 500, Err: errors.New("boom")},
	}}
	var outcomes []string
	_, _ = fastPolicy(3).Do(context.Background(), dest, envelope.Envelope{}, func(_ int, o string) {
		outcomes = append(outcomes, o)
	})
	if len(outcomes) != 2 || outcomes[0] != "transient" || outcomes[1] != "delivered" {
		t.Fatalf("outcomes = %v, want [transient delivered]", outcomes)
	}
}
