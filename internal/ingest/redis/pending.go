package redis

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"in.dmart.pulse.conflux/internal/envelope"
	"in.dmart.pulse.conflux/internal/ingest"
)

// PendingStats is a read-only view of a group's pending entries.
type PendingStats struct {
	Count     int64
	Consumers map[string]int64
}

// Pending reports how many entries are pending for a group, and for whom.
func (c *Client) Pending(ctx context.Context, stream, group string) (PendingStats, error) {
	res, err := c.rdb.XPending(ctx, stream, group).Result()
	if err != nil {
		return PendingStats{}, fmt.Errorf("redis: XPENDING %q %q: %w", stream, group, err)
	}
	return PendingStats{Count: res.Count, Consumers: res.Consumers}, nil
}

// Reclaim claims entries idle longer than minIdle and returns them as deliveries.
//
// XAUTOCLAIM does the scan-and-claim atomically server-side and returns a
// cursor, so the pass is paginated and bounded. minIdle is what protects a LIVE
// consumer's in-flight work: an entry being actively processed has not been
// idle, so it is not eligible.
func (c *Client) Reclaim(ctx context.Context, ref ingest.SourceRef, group, consumer string, minIdle time.Duration, batch int) ([]ingest.Delivery, error) {
	var (
		out    []ingest.Delivery
		cursor = "0-0"
	)
	for {
		msgs, next, err := c.rdb.XAutoClaim(ctx, &goredis.XAutoClaimArgs{
			Stream:   ref.Name,
			Group:    group,
			Consumer: consumer,
			MinIdle:  minIdle,
			Start:    cursor,
			Count:    int64(batch),
		}).Result()
		if err != nil {
			return out, fmt.Errorf("redis: XAUTOCLAIM %q %q: %w", ref.Name, group, err)
		}

		for _, msg := range msgs {
			id := msg.ID
			ack := func(ctx context.Context) error {
				return c.rdb.XAck(ctx, ref.Name, group, id).Err()
			}
			nack := func(context.Context) error { return nil }

			raw := rawData(msg)
			if raw == nil {
				out = append(out, ingest.NewDelivery(envelope.Envelope{}, ref, id, nil,
					fmt.Errorf("%w: entry has no %q field", envelope.ErrUnprocessable, dataField), ack, nack))
				continue
			}
			env, perr := envelope.Parse(raw)
			out = append(out, ingest.NewDelivery(env, ref, id, raw, perr, ack, nack))
		}

		// "0-0" means the scan wrapped: the pass is complete.
		if next == "0-0" || next == "" || len(msgs) == 0 {
			return out, nil
		}
		cursor = next
	}
}

// RetireConsumers removes consumers that hold no pending entries and have been
// idle beyond idleFor.
//
// This is what keeps per-instance consumer names from accumulating in the group
// as instances churn: hostname-derived names would otherwise grow without bound
// across redeploys. A consumer holding pending entries is never retired —
// deleting it would orphan that work.
func (c *Client) RetireConsumers(ctx context.Context, stream, group string, idleFor time.Duration, log *slog.Logger) (int, error) {
	consumers, err := c.rdb.XInfoConsumers(ctx, stream, group).Result()
	if err != nil {
		return 0, fmt.Errorf("redis: XINFO CONSUMERS %q %q: %w", stream, group, err)
	}

	retired := 0
	for _, con := range consumers {
		if con.Pending > 0 || con.Idle < idleFor {
			continue
		}
		if err := c.rdb.XGroupDelConsumer(ctx, stream, group, con.Name).Err(); err != nil {
			return retired, fmt.Errorf("redis: XGROUP DELCONSUMER %q: %w", con.Name, err)
		}
		retired++
		log.Info("retired idle consumer",
			slog.String("component", "redis"),
			slog.String("stream", stream),
			slog.String("group", group),
			slog.String("consumer", con.Name),
			slog.Duration("idle", con.Idle))
	}
	return retired, nil
}

// StreamReport is read-only information about a stream, for queue reporting.
type StreamReport struct {
	Name    string `json:"name"`
	Length  int64  `json:"length"`
	Groups  int64  `json:"groups"`
	Pending int64  `json:"pending,omitempty"`
}

// ReportStream gathers read-only facts about a stream. It mutates nothing.
func (c *Client) ReportStream(ctx context.Context, stream string) (StreamReport, error) {
	info, err := c.rdb.XInfoStream(ctx, stream).Result()
	if err != nil {
		return StreamReport{}, fmt.Errorf("redis: XINFO STREAM %q: %w", stream, err)
	}
	return StreamReport{Name: stream, Length: info.Length, Groups: info.Groups}, nil
}

// ListDepth reports a list's length. Read-only.
func (c *Client) ListDepth(ctx context.Context, key string) (int64, error) {
	return c.rdb.LLen(ctx, key).Result()
}
