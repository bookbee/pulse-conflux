package redis

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"in.dmart.pulse.conflux/internal/envelope"
	"in.dmart.pulse.conflux/internal/ingest"
)

// dataField is the single field a stream entry carries.
//
// The gateway writes `Values: []string{"data", payloadJSON}` — exactly one
// field, named `data`. Reading it BY NAME is deliberate: a consumer that
// iterates fields generically works by accident today and breaks on the first
// field anyone adds.
const dataField = "data"

// StreamSource consumes a Redis stream through a consumer group this service
// creates and owns.
type StreamSource struct {
	client   *Client
	ref      ingest.SourceRef
	group    string
	consumer string
	count    int64
	block    time.Duration
	log      *slog.Logger
}

// NewStreamSource creates the consumer group if absent and returns the source.
//
// The gateway only XADDs and never creates a group, so group creation, pending
// entries and claims are all this service's responsibility (FR-009).
func NewStreamSource(ctx context.Context, c *Client, ref ingest.SourceRef, group, consumer string, count int, block time.Duration, log *slog.Logger) (*StreamSource, error) {
	// MKSTREAM so an agent can start against a stream the gateway has not
	// written to yet. BUSYGROUP means someone got there first, which is benign.
	err := c.rdb.XGroupCreateMkStream(ctx, ref.Name, group, "$").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return nil, fmt.Errorf("redis: create group %q on %q: %w", group, ref.Name, err)
	}
	log.Info("consumer group ready",
		slog.String("component", "redis"),
		slog.String("source", ref.Name),
		slog.String("group", group),
		slog.String("consumer", consumer))

	return &StreamSource{
		client: c, ref: ref, group: group, consumer: consumer,
		count: int64(count), block: block, log: log,
	}, nil
}

func (s *StreamSource) Ref() ingest.SourceRef { return s.ref }

// Read blocks for up to the configured timeout and returns what arrived.
func (s *StreamSource) Read(ctx context.Context) ([]ingest.Delivery, error) {
	res, err := s.client.rdb.XReadGroup(ctx, &goredis.XReadGroupArgs{
		Group:    s.group,
		Consumer: s.consumer,
		Streams:  []string{s.ref.Name, ">"},
		Count:    s.count,
		Block:    s.block,
	}).Result()

	switch {
	case errors.Is(err, goredis.Nil):
		return nil, nil // block elapsed with nothing to read: normal
	case err != nil:
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, fmt.Errorf("redis: XREADGROUP %q: %w", s.ref.Name, err)
	}

	var out []ingest.Delivery
	for _, stream := range res {
		for _, msg := range stream.Messages {
			out = append(out, s.toDelivery(msg))
		}
	}
	return out, nil
}

func (s *StreamSource) toDelivery(msg goredis.XMessage) ingest.Delivery {
	id := msg.ID
	ack := func(ctx context.Context) error {
		return s.client.rdb.XAck(ctx, s.ref.Name, s.group, id).Err()
	}
	// Nack leaves the entry pending, where XAUTOCLAIM can recover it once it has
	// been idle long enough. Nothing is deleted.
	nack := func(context.Context) error { return nil }

	raw := rawData(msg)
	if raw == nil {
		return ingest.NewDelivery(envelope.Envelope{}, s.ref, id, nil,
			fmt.Errorf("%w: entry has no %q field", envelope.ErrUnprocessable, dataField),
			ack, nack)
	}

	env, err := envelope.Parse(raw)
	if err != nil {
		return ingest.NewDelivery(envelope.Envelope{}, s.ref, id, raw, err, ack, nack)
	}
	return ingest.NewDelivery(env, s.ref, id, raw, nil, ack, nack)
}

func rawData(msg goredis.XMessage) []byte {
	v, ok := msg.Values[dataField]
	if !ok {
		return nil
	}
	switch t := v.(type) {
	case string:
		return []byte(t)
	case []byte:
		return t
	default:
		return []byte(fmt.Sprint(t))
	}
}

// Lag reports how far behind this group is.
//
// Verified against Redis 7.4 on the local stack rather than assumed, because
// the obvious assumption is wrong in both directions:
//
//   - MAXLEN trimming does NOT make XINFO GROUPS report NULL lag. Redis keeps
//     computing it, so a trimmed stream looks healthy on that field alone.
//   - What DOES make lag NULL is XDEL tombstones, which the gateway never
//     creates but an operator might.
//
// The condition that actually matters — entries trimmed away before this group
// read them — is invisible in lag and has to be detected separately: if the
// group's last-delivered-id is older than the stream's first surviving entry,
// entries were destroyed unread. Lag then understates reality, so the reading
// is marked approximate and the anomaly path treats it as upstream loss.
func (s *StreamSource) Lag(ctx context.Context) (ingest.LagReading, error) {
	groups, err := s.client.rdb.XInfoGroups(ctx, s.ref.Name).Result()
	if err != nil {
		return ingest.LagReading{}, fmt.Errorf("redis: XINFO GROUPS %q: %w", s.ref.Name, err)
	}

	for _, g := range groups {
		if g.Name != s.group {
			continue
		}

		trimmedPast, terr := s.trimmedPastGroup(ctx, g.LastDeliveredID)
		if terr != nil {
			return ingest.LagReading{}, terr
		}

		// NULL lag (XDEL tombstones): derive from length and say so.
		if g.Lag == 0 && g.EntriesRead == 0 && g.LastDeliveredID != "0-0" {
			length, lerr := s.client.rdb.XLen(ctx, s.ref.Name).Result()
			if lerr != nil {
				return ingest.LagReading{}, fmt.Errorf("redis: XLEN %q: %w", s.ref.Name, lerr)
			}
			return ingest.LagReading{Entries: length, Approximate: true}, nil
		}

		return ingest.LagReading{Entries: g.Lag, Approximate: trimmedPast}, nil
	}
	return ingest.LagReading{}, fmt.Errorf("redis: group %q not found on %q", s.group, s.ref.Name)
}

// trimmedPastGroup reports whether entries were trimmed away before this group
// read them.
func (s *StreamSource) trimmedPastGroup(ctx context.Context, lastDelivered string) (bool, error) {
	info, err := s.client.rdb.XInfoStream(ctx, s.ref.Name).Result()
	if err != nil {
		return false, fmt.Errorf("redis: XINFO STREAM %q: %w", s.ref.Name, err)
	}
	if info.Length == 0 || info.FirstEntry.ID == "" {
		return false, nil
	}
	return compareIDs(lastDelivered, info.FirstEntry.ID) < 0, nil
}

// compareIDs orders two stream ids of the form "<ms>-<seq>".
func compareIDs(a, b string) int {
	ams, aseq := splitID(a)
	bms, bseq := splitID(b)
	switch {
	case ams != bms:
		if ams < bms {
			return -1
		}
		return 1
	case aseq != bseq:
		if aseq < bseq {
			return -1
		}
		return 1
	default:
		return 0
	}
}

func splitID(id string) (ms, seq int64) {
	msPart, seqPart, found := strings.Cut(id, "-")
	ms, _ = strconv.ParseInt(msPart, 10, 64)
	if found {
		seq, _ = strconv.ParseInt(seqPart, 10, 64)
	}
	return ms, seq
}

func (s *StreamSource) Close() error { return nil }

// GroupName composes a consumer group name.
//
// Per-agent so that two agents on one stream each see EVERY event rather than
// splitting it between them (US2 scenario 1).
func GroupName(base, agentID string) string {
	return base + ":" + agentID
}

// ConsumerName composes a consumer name.
//
// Per-agent AND per-instance: without the instance segment, two processes
// running the same agent share one consumer name, split the stream, and
// XAUTOCLAIM can steal entries that are in flight at the other live process
// (FR-009a).
func ConsumerName(base, agentID, instanceID string) string {
	return base + ":" + agentID + ":" + instanceID
}

var _ ingest.Source = (*StreamSource)(nil)
