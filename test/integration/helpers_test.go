//go:build integration

// Integration tests run against the REAL pulse-infra stack, never a fake Redis.
// Constitution Principle V: the source's quirks — single-field stream entries,
// NULL group lag, the Lua-dropped log write — are exactly what a mock
// reproduces incorrectly.
//
//	cd ../pulse-infra && make up      # `full` profile; Redis exists only there
//	go test -tags=integration ./test/integration/...
package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"in.dmart.pulse.conflux/internal/config"
	"in.dmart.pulse.conflux/internal/observability"
)

const (
	redisAddr     = "localhost:6379"
	redisPassword = "pulse-local-not-a-secret"
)

// dialRedis connects directly, as a test fixture would — not through the
// ingestion boundary, which is the thing under test.
func dialRedis(t *testing.T) *goredis.Client {
	t.Helper()
	rdb := goredis.NewClient(&goredis.Options{Addr: redisAddr, Password: redisPassword, DB: 0})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skipf("pulse-infra not reachable at %s (%v).\nBring it up: cd ../pulse-infra && make up", redisAddr, err)
	}
	t.Cleanup(func() { _ = rdb.Close() })
	return rdb
}

// uniqueKey namespaces a test's stream or list so parallel runs and reruns do
// not collide, and so a test never touches the real contract keys.
func uniqueKey(prefix string) string {
	return fmt.Sprintf("test:%s:%d", prefix, time.Now().UnixNano())
}

// seed writes one envelope in the gateway's exact shape: ONE field named `data`
// holding the JSON envelope.
func seed(t *testing.T, rdb *goredis.Client, stream, eventID string) {
	t.Helper()
	body, err := json.Marshal(map[string]any{
		"event_id":    eventID,
		"gateway_id":  "gw-test",
		"received_at": time.Now().UTC().Format(time.RFC3339),
		"retry_count": 0,
		"stream_name": stream,
		"payload":     map[string]any{"n": eventID},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := rdb.XAdd(context.Background(), &goredis.XAddArgs{
		Stream: stream,
		Values: []string{"data", string(body)},
	}).Err(); err != nil {
		t.Fatalf("XADD %s: %v", stream, err)
	}
}

// seedList pushes an envelope onto a list the way the gateway's Lua script does
// (RPUSH + EXPIRE), including the sliding TTL.
func seedList(t *testing.T, rdb *goredis.Client, key, eventID string) {
	t.Helper()
	body, _ := json.Marshal(map[string]any{
		"event_id": eventID, "gateway_id": "gw-test",
		"received_at": time.Now().UTC().Format(time.RFC3339),
		"payload":     map[string]any{"n": eventID},
	})
	ctx := context.Background()
	if err := rdb.RPush(ctx, key, string(body)).Err(); err != nil {
		t.Fatalf("RPUSH %s: %v", key, err)
	}
	// The TTL is re-issued on every push, so the key survives under traffic and
	// dies only after silence.
	rdb.Expire(ctx, key, 120*time.Second)
}

func testConfig(agents ...config.Agent) *config.Config {
	return &config.Config{
		HTTPAddr: ":0", ShutdownGrace: 2 * time.Second,
		InstanceID: "test-instance", LogLevel: "error",
		Redis: config.Redis{
			Addr: redisAddr, Password: redisPassword,
			ConsumerGrp: "test-group", ConsumerName: "test-consumer",
		},
		Dispatch: config.Dispatch{
			MaxAttempts: 3, Timeout: 2 * time.Second,
			BackoffInitial: 5 * time.Millisecond, BackoffMax: 20 * time.Millisecond,
		},
		House: config.Housekeeping{
			PendingMinIdle: 50 * time.Millisecond, PendingBatch: 100,
			ConsumerRetireIdle: time.Hour, SummaryPeriod: time.Hour,
			SummaryMaxDepth: 2000,
		},
		Anomaly: config.Anomaly{
			LagThresholdEntries: 10, LagSustainedFor: 0,
			DropRateThreshold: 0.01, CheckInterval: 50 * time.Millisecond,
			NotifyCooldown: time.Hour, MaxPerHour: 4,
		},
		Agents: agents,
	}
}

func testLogger() *slog.Logger { return observability.NewLogger("error") }

func TestMain(m *testing.M) { os.Exit(m.Run()) }

// eventually polls until cond holds or the deadline passes.
func eventually(t *testing.T, within time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for: %s", within, what)
}

// seedRaw writes an arbitrary string as a stream entry's `data` field, for
// exercising entries the gateway would never produce.
func seedRaw(t *testing.T, rdb *goredis.Client, stream, raw string) {
	t.Helper()
	if err := rdb.XAdd(context.Background(), &goredis.XAddArgs{
		Stream: stream, Values: []string{"data", raw},
	}).Err(); err != nil {
		t.Fatalf("XADD raw %s: %v", stream, err)
	}
}
