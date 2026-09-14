// Package dispatch delivers envelopes to destinations and owns the retry policy.
//
// Every destination in this feature is a stub (Constitution Principle V): the
// search-ranking service (LTR), the internal CEP engine, and outbound support
// email have no live implementation here. What is fixed is the shape of the
// call, so a real implementation slots in without reshaping any agent.
package dispatch

import (
	"context"

	"in.dmart.pulse.conflux/internal/envelope"
)

// Destination delivers one envelope somewhere.
//
// Implementations MUST be safe for concurrent use and MUST NOT retry
// internally: the attempt budget, the backoff, and the per-attempt timeout
// belong to the caller. An implementation that retries on its own breaks the
// bounded-loss guarantee the drop metrics measure.
type Destination interface {
	Name() string
	Dispatch(ctx context.Context, env envelope.Envelope) error
}

// Result is how a dispatch resolved. There is no third state: a dispatch is
// resolved before its delivery is acknowledged.
type Result string

const (
	Delivered Result = "delivered"
	Dropped   Result = "dropped"
)

// Outcome records one agent handing one envelope to one destination.
//
// A Dropped outcome is the only record that the event existed — this feature
// introduces no datastore — so it is logged in full at error level and never
// reduced to a counter alone.
type Outcome struct {
	EventID     string
	GatewayID   string
	AgentID     string
	Destination string
	Attempts    int
	Result      Result
	Reason      string
	DurationMS  int64
}
