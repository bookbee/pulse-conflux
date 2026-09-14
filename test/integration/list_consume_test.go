//go:build integration

package integration

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"in.dmart.pulse.conflux/internal/agent"
	"in.dmart.pulse.conflux/internal/dispatch"
	"in.dmart.pulse.conflux/internal/dispatch/stub"
	"in.dmart.pulse.conflux/internal/ingest"
	ingestredis "in.dmart.pulse.conflux/internal/ingest/redis"
	"in.dmart.pulse.conflux/internal/observability"
)

// The log-list ingestion path, end to end. The constitution requires every new
// ingestion path to arrive with an integration test against the local stack,
// and this path behaves unlike the streams in three ways that a mock would get
// wrong.
func TestLogListDrainKeepsDepthDownAndEmitsSummaries(t *testing.T) {
	rdb := dialRedis(t)
	list := uniqueKey("logs")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	t.Cleanup(func() { rdb.Del(context.Background(), list) })

	cfg := testConfig()
	log := testLogger()
	client, err := ingestredis.NewClient(ctx, cfg.Redis, log)
	if err != nil {
		t.Skipf("pulse-infra not reachable: %v", err)
	}
	defer func() { _ = client.Close() }()

	rec, srv := newRecorder()
	defer srv.Close()

	ref := ingest.SourceRef{Name: list, Kind: ingest.KindList}
	src := ingestredis.NewListSource(client, ref, 200*time.Millisecond, log)

	m := observability.NewMetrics()
	d := dispatch.NewDispatcher("log_drain",
		[]dispatch.Destination{stub.NewHTTP("summary", srv.URL, "log_drain")},
		dispatch.NewPolicy(cfg.Dispatch), m, log)

	// A one-second period so a summary actually closes during the test.
	a := agent.NewLogDrainAgent("log_drain", src, time.Second, d, m, log)
	go func() { _ = a.Run(ctx) }()

	for i := range 20 {
		seedList(t, rdb, list, string(rune('a'+i)))
	}

	eventually(t, 15*time.Second, "the list to be drained", func() bool {
		n, err := rdb.LLen(context.Background(), list).Result()
		return err == nil && n == 0
	})

	processed := testutil.ToFloat64(m.EventsProcessed.WithLabelValues("log_drain", list))
	if processed != 20 {
		t.Errorf("processed %v entries, want 20", processed)
	}

	// Draining is continuous, so depth stays near zero rather than climbing
	// toward the cap at which the GATEWAY's writes are refused.
	depth, _ := rdb.LLen(context.Background(), list).Result()
	if depth > cfg.House.SummaryMaxDepth {
		t.Errorf("list depth %d exceeds the drain target %d", depth, cfg.House.SummaryMaxDepth)
	}

	// Push once more after a period boundary to close the first bucket.
	time.Sleep(1200 * time.Millisecond)
	seedList(t, rdb, list, "trigger")

	eventually(t, 10*time.Second, "a summary to be emitted", func() bool { return rec.count() >= 1 })
	keys := rec.seen()
	if len(keys) == 0 || len(keys[0]) < len("summary:") || keys[0][:8] != "summary:" {
		t.Fatalf("summary idempotency key = %v, want one identified by its period", keys)
	}
	t.Logf("summary emitted with period identity %q", keys[0])
}

// The list's cap REFUSES the write rather than trimming, so a stalled drain
// becomes upstream loss. This reproduces the gateway's Lua semantics exactly.
func TestListCapRefusesWritesRatherThanTrimming(t *testing.T) {
	rdb := dialRedis(t)
	list := uniqueKey("capped")
	ctx := context.Background()
	t.Cleanup(func() { rdb.Del(ctx, list) })

	const cap = 5
	// The gateway's script, verbatim in behaviour: check length, refuse if full,
	// otherwise push and refresh the sliding TTL.
	push := func(payload string) error {
		return rdb.Eval(ctx, `
local len = redis.call('LLEN', KEYS[1])
if len >= tonumber(ARGV[2]) then
  return redis.error_reply('LOGS_LIST_FULL')
end
redis.call('RPUSH', KEYS[1], ARGV[1])
redis.call('EXPIRE', KEYS[1], ARGV[3])
return 1`, []string{list}, payload, cap, 120).Err()
	}

	for i := range cap {
		if err := push(string(rune('a' + i))); err != nil {
			t.Fatalf("push %d: %v", i, err)
		}
	}

	err := push("one-too-many")
	if err == nil {
		t.Fatal("the cap must REFUSE the write; a silent success would mean it trimmed instead")
	}
	// Redis prefixes a Lua error_reply with "ERR " when it carries no error code.
	if got := err.Error(); !strings.Contains(got, "LOGS_LIST_FULL") {
		t.Errorf("error = %q, want it to carry LOGS_LIST_FULL", got)
	}

	// Crucially: the existing entries are all still there. Nothing was evicted
	// to make room — the NEW write is what was lost, upstream, at the gateway.
	n, _ := rdb.LLen(ctx, list).Result()
	if n != cap {
		t.Fatalf("list length = %d, want %d — entries were evicted, which this list never does", n, cap)
	}
}

// The TTL is re-issued on every push, so the key lives under traffic and dies
// only after silence. A per-entry expiry reading would be wrong.
func TestListTTLIsSlidingNotPerEntry(t *testing.T) {
	rdb := dialRedis(t)
	list := uniqueKey("ttl")
	ctx := context.Background()
	t.Cleanup(func() { rdb.Del(ctx, list) })

	seedList(t, rdb, list, "first")
	firstTTL, err := rdb.TTL(ctx, list).Result()
	if err != nil {
		t.Fatalf("TTL: %v", err)
	}

	time.Sleep(1100 * time.Millisecond)
	beforeSecond, _ := rdb.TTL(ctx, list).Result()
	seedList(t, rdb, list, "second")
	afterSecond, _ := rdb.TTL(ctx, list).Result()

	if beforeSecond >= firstTTL {
		t.Fatalf("TTL did not decay: %v then %v", firstTTL, beforeSecond)
	}
	if afterSecond <= beforeSecond {
		t.Fatalf("TTL was not refreshed by the second push: %v then %v.\n"+
			"The expiry is sliding — the key survives under traffic and dies after silence.", beforeSecond, afterSecond)
	}
}
