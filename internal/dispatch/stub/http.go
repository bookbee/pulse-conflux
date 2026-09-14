// Package stub holds the stand-in destinations for feature 001.
//
// Constitution Principle V: external integrations are out of local scope. The
// search-ranking service (LTR) and the internal CEP engine have no specified
// contract yet, so what ships here is the SHAPE of the call — headers, body,
// status handling — and tests assert on the calls rather than reaching a real
// system.
package stub

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"

	"in.dmart.pulse.conflux/internal/dispatch"
	"in.dmart.pulse.conflux/internal/envelope"
)

// HTTPDestination posts envelopes to a URL.
//
// It does not retry: the attempt budget, backoff and per-attempt timeout belong
// to dispatch.Policy. An implementation that retried internally would break the
// bounded-loss guarantee the drop metrics measure.
type HTTPDestination struct {
	name   string
	url    string
	agent  string
	client *http.Client
}

// NewHTTP builds a destination. The client has no timeout of its own — the
// per-attempt deadline arrives on the context from the policy.
func NewHTTP(name, url, agentID string) *HTTPDestination {
	return &HTTPDestination{
		name: name, url: url, agent: agentID,
		client: &http.Client{Transport: http.DefaultTransport},
	}
}

// NewLTR is the search-ranking stub.
func NewLTR(url, agentID string) *HTTPDestination { return NewHTTP("ltr", url, agentID) }

// NewCEP is the internal CEP engine stub.
func NewCEP(url, agentID string) *HTTPDestination { return NewHTTP("cep", url, agentID) }

func (d *HTTPDestination) Name() string { return d.name }

// Dispatch posts one envelope.
//
// The body is re-serialised from the envelope so event_header is OMITTED when
// absent rather than sent as null, while payload and event_header themselves
// pass through byte-for-byte as raw JSON.
func (d *HTTPDestination) Dispatch(ctx context.Context, env envelope.Envelope) error {
	body, err := json.Marshal(env)
	if err != nil {
		// A payload that will not marshal will not marshal on a retry either.
		return &dispatch.Error{Kind: dispatch.Permanent, Err: fmt.Errorf("marshal envelope: %w", err)}
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, d.url, bytes.NewReader(body))
	if err != nil {
		return &dispatch.Error{Kind: dispatch.Permanent, Err: err}
	}
	req.Header.Set("Content-Type", "application/json")
	// Redelivery is normal operation, so the destination is given the key it
	// needs to absorb a duplicate (FR-011).
	req.Header.Set("Idempotency-Key", env.EventID)
	req.Header.Set("X-Conflux-Agent", d.agent)
	if n, ok := dispatch.AttemptFrom(ctx); ok {
		req.Header.Set("X-Conflux-Attempt", strconv.Itoa(n))
	}

	resp, err := d.client.Do(req)
	if err != nil {
		// Timeouts and connection errors are worth retrying.
		return &dispatch.Error{Kind: dispatch.Transient, Err: err}
	}
	defer func() {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4<<10))
		_ = resp.Body.Close()
	}()

	kind, ok := dispatch.ClassifyStatus(resp.StatusCode)
	if ok {
		return nil
	}
	return &dispatch.Error{
		Kind:   kind,
		Status: resp.StatusCode,
		Err:    fmt.Errorf("destination %s returned %s", d.name, resp.Status),
	}
}

var _ dispatch.Destination = (*HTTPDestination)(nil)

// Summary is a destination for log summaries. It reuses the HTTP shape when a
// URL is configured and otherwise writes to structured output only.
func NewSummary(url, agentID string) dispatch.Destination {
	if url == "" || url == "summary" {
		return &logOnly{name: "summary"}
	}
	return NewHTTP("summary", url, agentID)
}

type logOnly struct{ name string }

func (l *logOnly) Name() string                                      { return l.name }
func (l *logOnly) Dispatch(context.Context, envelope.Envelope) error { return nil }
