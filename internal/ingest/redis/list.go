package redis

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"in.dmart.pulse.conflux/internal/envelope"
	"in.dmart.pulse.conflux/internal/ingest"
)

// ListSource consumes the log list.
//
// Three things about this list differ from the streams, and all three are the
// gateway's Lua script, not a choice available here:
//
//   - Reading is DESTRUCTIVE. A pop removes the entry; nothing can put it back,
//     and no period can ever be read twice.
//   - The cap REFUSES writes rather than trimming. At 10k entries the script
//     returns LOGS_LIST_FULL and the gateway's write is lost — so a slow drain
//     here causes upstream loss directly.
//   - The 120s TTL is SLIDING, re-issued on every push, so the key survives
//     under traffic and dies after two minutes of silence, unread entries
//     included.
type ListSource struct {
	client *Client
	ref    ingest.SourceRef
	block  time.Duration
	log    *slog.Logger
}

// NewListSource builds a destructive-read source over a Redis list.
func NewListSource(c *Client, ref ingest.SourceRef, block time.Duration, log *slog.Logger) *ListSource {
	return &ListSource{client: c, ref: ref, block: block, log: log}
}

func (s *ListSource) Ref() ingest.SourceRef { return s.ref }

// Read pops one entry, blocking for up to the configured timeout.
//
// The gateway RPUSHes, so LPOP is FIFO.
func (s *ListSource) Read(ctx context.Context) ([]ingest.Delivery, error) {
	res, err := s.client.rdb.BLPop(ctx, s.block, s.ref.Name).Result()
	switch {
	case errors.Is(err, goredis.Nil):
		return nil, nil // timed out with an empty list: normal
	case err != nil:
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, fmt.Errorf("redis: BLPOP %q: %w", s.ref.Name, err)
	case len(res) < 2:
		return nil, nil
	}

	raw := []byte(res[1])
	// Ack is a no-op: the pop already consumed the entry. Nack cannot restore
	// it either — it only records that the entry is gone.
	ack := func(context.Context) error { return nil }
	nack := func(context.Context) error {
		s.log.Error("list entry lost",
			slog.String("component", "redis"),
			slog.String("source", s.ref.Name),
			slog.String("raw", string(raw)))
		return nil
	}

	env, perr := envelope.Parse(raw)
	if perr != nil {
		return []ingest.Delivery{ingest.NewDelivery(envelope.Envelope{}, s.ref, "", raw, perr, ack, nack)}, nil
	}
	return []ingest.Delivery{ingest.NewDelivery(env, s.ref, "", raw, nil, ack, nack)}, nil
}

// Lag for a list is simply its depth: everything still in it is unread.
func (s *ListSource) Lag(ctx context.Context) (ingest.LagReading, error) {
	n, err := s.client.rdb.LLen(ctx, s.ref.Name).Result()
	if err != nil {
		return ingest.LagReading{}, fmt.Errorf("redis: LLEN %q: %w", s.ref.Name, err)
	}
	return ingest.LagReading{Entries: n}, nil
}

func (s *ListSource) Close() error { return nil }

// Depth reports the list's current length, for the drain-depth anomaly.
func (s *ListSource) Depth(ctx context.Context) (int64, error) {
	return s.client.rdb.LLen(ctx, s.ref.Name).Result()
}

var _ ingest.Source = (*ListSource)(nil)
