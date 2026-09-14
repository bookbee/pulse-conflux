package dispatch

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"net/http"
	"time"

	"in.dmart.pulse.conflux/internal/config"
	"in.dmart.pulse.conflux/internal/envelope"
)

// FailureKind separates a failure worth retrying from one that never will be.
type FailureKind int

const (
	Transient FailureKind = iota // timeout, connection error, 408, 429, 5xx
	Permanent                    // 4xx other than 408/429
)

func (k FailureKind) String() string {
	if k == Permanent {
		return "permanent"
	}
	return "transient"
}

// Error carries a failure and whether retrying it could ever help.
type Error struct {
	Kind   FailureKind
	Status int
	Err    error
}

func (e *Error) Error() string {
	if e.Status != 0 {
		return fmt.Sprintf("%s failure: status %d: %v", e.Kind, e.Status, e.Err)
	}
	return fmt.Sprintf("%s failure: %v", e.Kind, e.Err)
}

func (e *Error) Unwrap() error { return e.Err }

// ClassifyStatus maps an HTTP status onto the retry policy from
// contracts/destination-dispatch.md.
//
// Retrying a payload the destination has rejected only burns the budget, so a
// permanent failure short-circuits to a drop.
func ClassifyStatus(status int) (FailureKind, bool) {
	switch {
	case status >= 200 && status < 300:
		return Transient, true // ok; kind is unused
	case status == http.StatusRequestTimeout, status == http.StatusTooManyRequests:
		return Transient, false
	case status >= 500:
		return Transient, false
	case status >= 400:
		return Permanent, false
	default:
		return Transient, false
	}
}

// Policy is the bounded-loss dispatch policy.
//
// Liveness is preferred over completeness at the destination: an unavailable
// destination must never become the reason this service falls behind, because
// falling behind loses data upstream where nothing can recover it.
type Policy struct {
	MaxAttempts    int
	Timeout        time.Duration // PER ATTEMPT, not total
	BackoffInitial time.Duration
	BackoffMax     time.Duration
}

// NewPolicy builds a policy from configuration.
func NewPolicy(c config.Dispatch) Policy {
	return Policy{
		MaxAttempts:    c.MaxAttempts,
		Timeout:        c.Timeout,
		BackoffInitial: c.BackoffInitial,
		BackoffMax:     c.BackoffMax,
	}
}

type ctxKey string

// attemptKey carries the 1-based attempt number so a destination can report it
// without the policy having to reach into the destination's request building.
const attemptKey ctxKey = "conflux-attempt"

// AttemptFrom reads the current attempt number from a dispatch context.
func AttemptFrom(ctx context.Context) (int, bool) {
	n, ok := ctx.Value(attemptKey).(int)
	return n, ok
}

// AttemptObserver is notified after every attempt, so metrics stay out of the
// policy itself.
type AttemptObserver func(attempt int, outcome string)

// Do dispatches one envelope, retrying within the budget.
//
// It returns nil when delivered, or an error describing why the event was
// dropped. It never blocks longer than MaxAttempts × (Timeout + backoff).
func (p Policy) Do(ctx context.Context, dest Destination, env envelope.Envelope, obs AttemptObserver) (int, error) {
	var last error
	backoff := p.BackoffInitial

	for attempt := 1; attempt <= p.MaxAttempts; attempt++ {
		// A per-attempt timeout is what stops a destination that hangs — rather
		// than fails — from pinning the agent indefinitely.
		attemptCtx, cancel := context.WithTimeout(context.WithValue(ctx, attemptKey, attempt), p.Timeout)
		err := dest.Dispatch(attemptCtx, env)
		cancel()

		if err == nil {
			if obs != nil {
				obs(attempt, "delivered")
			}
			return attempt, nil
		}
		last = err

		var de *Error
		kind := Transient
		if errors.As(err, &de) {
			kind = de.Kind
		}
		if obs != nil {
			obs(attempt, kind.String())
		}

		if kind == Permanent {
			return attempt, fmt.Errorf("permanent failure on attempt %d: %w", attempt, err)
		}
		if attempt == p.MaxAttempts {
			break
		}

		// Full jitter: two agents failing together must not retry in lockstep.
		wait := time.Duration(rand.Int64N(int64(backoff) + 1))
		select {
		case <-ctx.Done():
			return attempt, fmt.Errorf("cancelled after attempt %d: %w", attempt, ctx.Err())
		case <-time.After(wait):
		}
		if backoff *= 2; backoff > p.BackoffMax {
			backoff = p.BackoffMax
		}
	}

	return p.MaxAttempts, fmt.Errorf("attempt budget of %d exhausted: %w", p.MaxAttempts, last)
}

// asDispatchError unwraps to a *Error if one is in the chain.
func asDispatchError(err error, target **Error) bool {
	return errors.As(err, target)
}
