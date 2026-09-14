//go:build integration

package integration

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	goredis "github.com/redis/go-redis/v9"

	"in.dmart.pulse.conflux/internal/housekeeping"
	"in.dmart.pulse.conflux/internal/ingest"
	ingestredis "in.dmart.pulse.conflux/internal/ingest/redis"
	"in.dmart.pulse.conflux/internal/observability"
)

// Quickstart Scenario 5: entries left pending by a consumer that is gone are
// reclaimed and processed, while a LIVE consumer's in-flight work is untouched.
// PENDING_MIN_IDLE is what draws that line.
func TestReclaimRecoversAbandonedButNotLiveWork(t *testing.T) {
	rdb := dialRedis(t)
	stream := uniqueKey("reclaim")
	ctx := context.Background()
	t.Cleanup(func() { rdb.Del(ctx, stream) })

	cfg := testConfig()
	log := testLogger()
	client, err := ingestredis.NewClient(ctx, cfg.Redis, log)
	if err != nil {
		t.Skipf("pulse-infra not reachable: %v", err)
	}
	defer func() { _ = client.Close() }()

	group := ingestredis.GroupName(cfg.Redis.ConsumerGrp, "reclaim_agent")
	ref := ingest.SourceRef{Name: stream, Kind: ingest.KindStream}

	// Create the group and seed four entries.
	if _, err := ingestredis.NewStreamSource(ctx, client, ref, group, "setup", 10, time.Second, log); err != nil {
		t.Fatalf("create group: %v", err)
	}
	for _, id := range []string{"e1", "e2", "e3", "e4"} {
		seed(t, rdb, stream, id)
	}

	// "dead-consumer" reads two and never acks, then goes away.
	readAs(t, rdb, stream, group, "dead-consumer", 2)

	// Let ONLY those entries age past the idle threshold.
	time.Sleep(cfg.House.PendingMinIdle + 50*time.Millisecond)

	// "live-consumer" claims the other two only now, so its entries have
	// effectively zero idle time and must be out of reclaim's reach.
	readAs(t, rdb, stream, group, "live-consumer", 2)

	pending, err := client.Pending(ctx, stream, group)
	if err != nil {
		t.Fatalf("XPENDING: %v", err)
	}
	if pending.Count != 4 {
		t.Fatalf("pending = %d, want 4", pending.Count)
	}

	m := observability.NewMetrics()
	task := housekeeping.NewReclaimTask(client, []ingest.SourceRef{ref}, group, "reclaimer",
		cfg.House.PendingMinIdle, cfg.House.PendingBatch, time.Hour, "reclaim_agent", m, log)

	if err := task.Run(ctx); err != nil {
		t.Fatalf("reclaim: %v", err)
	}

	reclaimed := testutil.ToFloat64(m.EntriesReclaimed.WithLabelValues("reclaim_agent", stream))
	if reclaimed != 2 {
		t.Fatalf("reclaimed %v entries, want exactly 2.\n"+
			"Fewer means abandoned work sits pending forever; more means reclaim stole "+
			"entries a live consumer was still working on — which PENDING_MIN_IDLE exists to prevent.", reclaimed)
	}

	after, err := client.Pending(ctx, stream, group)
	if err != nil {
		t.Fatalf("XPENDING after: %v", err)
	}
	if after.Count != 2 {
		t.Errorf("pending = %d after reclaim, want 2 — the live consumer's two entries must remain its own", after.Count)
	}
	if n := after.Consumers["live-consumer"]; n != 2 {
		t.Errorf("live-consumer holds %d entries after reclaim, want 2", n)
	}
}

// SC-012 and FR-020a: a full housekeeping cycle must leave every key outside
// this service's own consumer groups byte-for-byte unchanged.
func TestHousekeepingMutatesNothingOutsideItsOwnGroups(t *testing.T) {
	rdb := dialRedis(t)
	stream := uniqueKey("untouched")
	list := uniqueKey("untouchedlist")
	ctx := context.Background()
	t.Cleanup(func() { rdb.Del(ctx, stream, list) })

	cfg := testConfig()
	log := testLogger()
	client, err := ingestredis.NewClient(ctx, cfg.Redis, log)
	if err != nil {
		t.Skipf("pulse-infra not reachable: %v", err)
	}
	defer func() { _ = client.Close() }()

	group := ingestredis.GroupName(cfg.Redis.ConsumerGrp, "report_agent")
	refs := []ingest.SourceRef{
		{Name: stream, Kind: ingest.KindStream},
		{Name: list, Kind: ingest.KindList},
	}
	if _, err := ingestredis.NewStreamSource(ctx, client, refs[0], group, "setup", 10, time.Second, log); err != nil {
		t.Fatalf("create group: %v", err)
	}
	for _, id := range []string{"a", "b", "c"} {
		seed(t, rdb, stream, id)
		seedList(t, rdb, list, id)
	}

	before := snapshot(t, rdb, stream, list)

	m := observability.NewMetrics()
	report := housekeeping.NewReportTask(client, refs, group, "report_agent", m, log)
	reclaim := housekeeping.NewReclaimTask(client, refs, group, "reclaimer",
		time.Hour, 100, time.Hour, "report_agent", m, log)

	if err := report.Run(ctx); err != nil {
		t.Fatalf("report: %v", err)
	}
	if err := reclaim.Run(ctx); err != nil {
		t.Fatalf("reclaim: %v", err)
	}

	after := snapshot(t, rdb, stream, list)
	if before.streamLen != after.streamLen {
		t.Errorf("stream length changed: %d -> %d", before.streamLen, after.streamLen)
	}
	if before.listLen != after.listLen {
		t.Errorf("list length changed: %d -> %d — housekeeping must never drain the gateway's data", before.listLen, after.listLen)
	}
	if len(before.entryIDs) != len(after.entryIDs) {
		t.Fatalf("stream entries changed: %d -> %d", len(before.entryIDs), len(after.entryIDs))
	}
	for i := range before.entryIDs {
		if before.entryIDs[i] != after.entryIDs[i] {
			t.Errorf("entry %d changed: %q -> %q", i, before.entryIDs[i], after.entryIDs[i])
		}
	}
}

type keyspaceSnapshot struct {
	streamLen int64
	listLen   int64
	entryIDs  []string
}

func snapshot(t *testing.T, rdb *goredis.Client, stream, list string) keyspaceSnapshot {
	t.Helper()
	ctx := context.Background()

	sl, _ := rdb.XLen(ctx, stream).Result()
	ll, _ := rdb.LLen(ctx, list).Result()
	msgs, err := rdb.XRange(ctx, stream, "-", "+").Result()
	if err != nil {
		t.Fatalf("XRANGE: %v", err)
	}
	ids := make([]string, 0, len(msgs))
	for _, msg := range msgs {
		ids = append(ids, msg.ID)
	}
	sort.Strings(ids)
	return keyspaceSnapshot{streamLen: sl, listLen: ll, entryIDs: ids}
}

// readAs reads (and does not ack) as a named consumer, leaving entries pending.
func readAs(t *testing.T, rdb *goredis.Client, stream, group, consumer string, count int64) {
	t.Helper()
	if count == 0 {
		// Touch the consumer so its idle timer resets without claiming more.
		_, _ = rdb.XReadGroup(context.Background(), &goredis.XReadGroupArgs{
			Group: group, Consumer: consumer,
			Streams: []string{stream, "0"}, Count: 1,
		}).Result()
		return
	}
	if _, err := rdb.XReadGroup(context.Background(), &goredis.XReadGroupArgs{
		Group: group, Consumer: consumer,
		Streams: []string{stream, ">"}, Count: count,
	}).Result(); err != nil {
		t.Fatalf("XREADGROUP as %s: %v", consumer, err)
	}
}
