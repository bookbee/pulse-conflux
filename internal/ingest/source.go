// Package ingest is the ingestion boundary.
//
// Constitution Principle II: consumption, acknowledgement, consumer-group
// lifecycle, pending-entry recovery and claim logic live below this line and
// MUST NOT be visible above it. Agents receive a Delivery — a parsed envelope
// plus an opaque acknowledgement — and never learn which source produced it or
// what primitive carried it.
//
// Only this package tree may import a Redis client. test/integration/boundary_test.go
// fails the build if that is violated.
package ingest

import (
	"context"

	"in.dmart.pulse.conflux/internal/envelope"
)

// Kind distinguishes the two Redis primitives the gateway writes. It selects a
// consumption strategy below the boundary and means nothing above it.
type Kind string

const (
	KindStream Kind = "stream" // XADD; non-destructive; consumer groups
	KindList   Kind = "list"   // Lua RPUSH + EXPIRE; destructive read
)

// SourceRef names a source. Name always comes from configuration — never a
// literal in source, because these names are cross-repo contracts.
type SourceRef struct {
	Name string
	Kind Kind
}

func (s SourceRef) String() string { return s.Name }

// Delivery is one envelope with its acknowledgement still attached.
//
// The Ack/Nack asymmetry between the two kinds is the reason this type exists:
//
//	stream — Ack is XACK; Nack leaves the entry pending for later reclaim.
//	list   — Ack is a no-op, because the pop already consumed the entry;
//	         Nack cannot restore it and only records the loss.
//
// A list delivery cannot be un-consumed, and that fact must not escape upward
// into agent code.
type Delivery struct {
	Envelope envelope.Envelope
	Source   SourceRef

	// Position is opaque above the boundary — a stream entry ID or a list
	// marker. Agents must not parse it.
	Position string

	// Raw is the original bytes, kept so an unprocessable entry can be
	// recorded in full (FR-012). With no datastore, that record is the only
	// trace the entry existed.
	Raw []byte

	// Unprocessable is set when Raw could not be parsed into an Envelope. The
	// Envelope field is then zero and only Raw is meaningful.
	Unprocessable error

	ack  func(context.Context) error
	nack func(context.Context) error
}

// NewDelivery builds a Delivery. Only implementations inside this package tree
// call it; the unexported ack/nack funcs are what keep the acknowledgement
// mechanism from leaking upward.
func NewDelivery(env envelope.Envelope, src SourceRef, pos string, raw []byte, unprocessable error, ack, nack func(context.Context) error) Delivery {
	return Delivery{
		Envelope: env, Source: src, Position: pos, Raw: raw,
		Unprocessable: unprocessable, ack: ack, nack: nack,
	}
}

// Ack marks the delivery resolved.
func (d Delivery) Ack(ctx context.Context) error {
	if d.ack == nil {
		return nil
	}
	return d.ack(ctx)
}

// Nack abandons the delivery. For a stream it stays pending; for a list it is
// already gone and this only records the loss.
func (d Delivery) Nack(ctx context.Context) error {
	if d.nack == nil {
		return nil
	}
	return d.nack(ctx)
}

// Source is one place deliveries come from. A Kafka implementation would be a
// second Source and nothing above this line would change — which is why no
// Kafka package exists yet (Principle V forbids scaffolding what has no
// upstream producer).
type Source interface {
	// Ref identifies the source.
	Ref() SourceRef

	// Read blocks for up to the source's configured timeout and returns any
	// deliveries available. An empty slice with a nil error means the timeout
	// elapsed with nothing to read, which is normal.
	Read(ctx context.Context) ([]Delivery, error)

	// Lag reports how far behind this consumer is.
	Lag(ctx context.Context) (LagReading, error)

	// Close releases the source's resources.
	Close() error
}

// LagReading is how far behind a consumer is at a point in time.
type LagReading struct {
	Entries int64

	// Approximate is true when the server could not report lag directly —
	// which happens once entries have been trimmed out from under the group.
	// That means data was lost upstream, so the flag propagates into readiness
	// and raises an anomaly rather than being smoothed away. A reading of zero
	// there would invert the signal.
	Approximate bool
}
