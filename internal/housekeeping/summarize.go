package housekeeping

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"in.dmart.pulse.conflux/internal/envelope"
)

// LogSummary is the compact form of one closed period of log entries.
//
// Identity is the period boundary pair, not the generation time: that is what
// lets a re-sent summary be absorbed rather than double-counted.
type LogSummary struct {
	PeriodStart time.Time        `json:"period_start"`
	PeriodEnd   time.Time        `json:"period_end"`
	EntryCount  int64            `json:"entry_count"`
	Breakdown   map[string]int64 `json:"breakdown"`
	GeneratedAt time.Time        `json:"generated_at"`

	// Partial is set when the process started mid-period, so the counts
	// undercount the real volume. Declaring that beats presenting an
	// undercount as complete — there is no datastore to recover the rest from,
	// and the list was read destructively so it cannot be re-read.
	Partial bool `json:"partial"`
}

// Aggregator folds drained log entries into per-period counters in memory.
//
// In memory is a consequence of FR-013, not a preference: with no datastore
// there is nowhere else to accumulate, which is exactly why Partial exists.
type Aggregator struct {
	period time.Duration

	mu      sync.Mutex
	current time.Time // start of the open bucket
	count   int64
	byKey   map[string]int64
	partial bool
}

// NewAggregator builds an aggregator over clock-aligned buckets.
func NewAggregator(period time.Duration) *Aggregator {
	return &Aggregator{period: period, byKey: map[string]int64{}, partial: true}
}

// bucketStart truncates a time to its clock-aligned period start, so two
// processes agree on where a period begins without coordinating.
func (a *Aggregator) bucketStart(t time.Time) time.Time {
	return t.UTC().Truncate(a.period)
}

// Observe folds one envelope into the open bucket, returning a summary when the
// envelope belongs to a later period than the one currently open.
func (a *Aggregator) Observe(env envelope.Envelope, now time.Time) *LogSummary {
	a.mu.Lock()
	defer a.mu.Unlock()

	stamp := env.ReceivedAt
	if stamp.IsZero() {
		stamp = now
	}
	bucket := a.bucketStart(stamp)

	if a.current.IsZero() {
		a.current = bucket
	}

	var closed *LogSummary
	if bucket.After(a.current) {
		closed = a.closeLocked(now)
		a.current = bucket
	}

	a.count++
	if env.GatewayID != "" {
		a.byKey["gateway:"+env.GatewayID]++
	}
	return closed
}

// Tick closes the open bucket if the clock has moved past it, so a quiet period
// still produces its summary rather than waiting for the next entry.
func (a *Aggregator) Tick(now time.Time) *LogSummary {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.current.IsZero() || !a.bucketStart(now).After(a.current) {
		return nil
	}
	closed := a.closeLocked(now)
	a.current = a.bucketStart(now)
	return closed
}

func (a *Aggregator) closeLocked(now time.Time) *LogSummary {
	s := &LogSummary{
		PeriodStart: a.current,
		PeriodEnd:   a.current.Add(a.period),
		EntryCount:  a.count,
		Breakdown:   a.byKey,
		GeneratedAt: now.UTC(),
		Partial:     a.partial,
	}
	a.count = 0
	a.byKey = map[string]int64{}
	// Only the first bucket after a start can be partial: every later one was
	// observed from its beginning.
	a.partial = false
	return s
}

// Emit writes a summary to structured output. The configured destination
// receives it as well, through the agent's dispatcher.
func Emit(log *slog.Logger, s *LogSummary) {
	body, _ := json.Marshal(s)
	level := slog.LevelInfo
	if s.Partial {
		level = slog.LevelWarn
	}
	log.Log(context.Background(), level, "log summary",
		slog.String("component", "summary"),
		slog.Time("period_start", s.PeriodStart),
		slog.Time("period_end", s.PeriodEnd),
		slog.Int64("entry_count", s.EntryCount),
		slog.Bool("partial", s.Partial),
		slog.String("summary", string(body)))
}
