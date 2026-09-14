//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"in.dmart.pulse.conflux/internal/agent"
	"in.dmart.pulse.conflux/internal/observability"
)

// US2 scenario 1: two agents on ONE stream must each see EVERY event, not split
// them. That is what per-agent consumer groups buy.
//
// SC-001 rides along: a third agent is added while the first two are mid-stream
// and neither may pause, lose position, or drop throughput.
func TestTwoAgentsEachSeeEveryEventAndAThirdJoinsWithoutDisruption(t *testing.T) {
	rdb := dialRedis(t)
	stream := uniqueKey("fanout")
	t.Cleanup(func() { rdb.Del(context.Background(), stream) })

	recA, srvA := newRecorder()
	defer srvA.Close()
	recB, srvB := newRecorder()
	defer srvB.Close()

	mA := observability.NewMetrics()
	mB := observability.NewMetrics()
	agentA, _ := buildStreamAgent(t, stream, srvA.URL, "agent_a", mA)
	agentB, _ := buildStreamAgent(t, stream, srvB.URL, "agent_b", mB)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = agentA.Run(ctx) }()
	go func() { _ = agentB.Run(ctx) }()

	for _, id := range []string{"evt-1", "evt-2", "evt-3"} {
		seed(t, rdb, stream, id)
	}

	eventually(t, 10*time.Second, "agent_a to see all three", func() bool { return recA.count() == 3 })
	eventually(t, 10*time.Second, "agent_b to see all three", func() bool { return recB.count() == 3 })

	if recA.count() != 3 || recB.count() != 3 {
		t.Fatalf("agents split the stream instead of each seeing it: a=%d b=%d", recA.count(), recB.count())
	}

	// SC-001: a third agent joins from configuration while the others run.
	recC, srvC := newRecorder()
	defer srvC.Close()
	mC := observability.NewMetrics()
	agentC, _ := buildStreamAgent(t, stream, srvC.URL, "agent_c", mC)
	go func() { _ = agentC.Run(ctx) }()

	aBefore, bBefore := recA.count(), recB.count()
	for _, id := range []string{"evt-4", "evt-5"} {
		seed(t, rdb, stream, id)
	}

	eventually(t, 10*time.Second, "all three agents to catch up", func() bool {
		return recA.count() == aBefore+2 && recB.count() == bBefore+2 && recC.count() == 2
	})

	// The new agent starts at $, so it sees only what arrived after it joined —
	// and crucially the running agents were not interrupted.
	if recA.count() != aBefore+2 {
		t.Errorf("agent_a processed %d after the join, want %d — adding an agent disturbed it", recA.count(), aBefore+2)
	}
	if recB.count() != bBefore+2 {
		t.Errorf("agent_b processed %d after the join, want %d", recB.count(), bBefore+2)
	}
}

// FR-004 at the runner level, against real sources: one agent failing
// continuously must not affect another's rate or position.
func TestFailingAgentDoesNotAffectHealthyAgent(t *testing.T) {
	rdb := dialRedis(t)
	streamOK := uniqueKey("healthy")
	streamBad := uniqueKey("failing")
	t.Cleanup(func() { rdb.Del(context.Background(), streamOK, streamBad) })

	recOK, srvOK := newRecorder()
	defer srvOK.Close()
	recBad, srvBad := newRecorder()
	defer srvBad.Close()
	recBad.setFailing(true) // every dispatch 503s

	m := observability.NewMetrics()
	healthy, _ := buildStreamAgent(t, streamOK, srvOK.URL, "healthy_agent", m)
	failing, _ := buildStreamAgent(t, streamBad, srvBad.URL, "failing_agent", m)

	runner := agent.NewRunner(testLogger(), m, 2*time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runner.Start(ctx, []agent.Agent{healthy, failing})

	for i := range 5 {
		seed(t, rdb, streamBad, string(rune('a'+i)))
		seed(t, rdb, streamOK, string(rune('a'+i)))
	}

	eventually(t, 20*time.Second, "the healthy agent to process everything", func() bool {
		return recOK.count() == 5
	})
	if recOK.count() != 5 {
		t.Fatalf("healthy agent processed %d of 5 while another agent failed", recOK.count())
	}
}
