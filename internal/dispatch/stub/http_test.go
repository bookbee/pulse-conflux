package stub

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"in.dmart.pulse.conflux/internal/envelope"
)

func testEnvelope(t *testing.T, raw string) envelope.Envelope {
	t.Helper()
	env, err := envelope.Parse([]byte(raw))
	if err != nil {
		t.Fatalf("fixture does not parse: %v", err)
	}
	return env
}

// The wire contract in contracts/destination-dispatch.md, asserted against a
// stub. No test reaches a real system (SC-010).
func TestDispatchWireContract(t *testing.T) {
	var (
		gotHeaders http.Header
		gotBody    []byte
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHeaders = r.Header.Clone()
		gotBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()

	env := testEnvelope(t, `{"event_id":"evt-7","gateway_id":"gw-1",`+
		`"received_at":"2026-09-14T09:00:00Z","payload":{"z":1,"a":2.50}}`)

	if err := NewLTR(srv.URL, "events_ltr").Dispatch(context.Background(), env); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}

	if got := gotHeaders.Get("Idempotency-Key"); got != "evt-7" {
		t.Errorf("Idempotency-Key = %q, want the event_id — it is how a destination absorbs a duplicate", got)
	}
	if got := gotHeaders.Get("X-Conflux-Agent"); got != "events_ltr" {
		t.Errorf("X-Conflux-Agent = %q", got)
	}
	if got := gotHeaders.Get("Content-Type"); got != "application/json" {
		t.Errorf("Content-Type = %q", got)
	}

	var sent map[string]json.RawMessage
	if err := json.Unmarshal(gotBody, &sent); err != nil {
		t.Fatalf("body is not JSON: %v", err)
	}
	if got, want := string(sent["payload"]), `{"z":1,"a":2.50}`; got != want {
		t.Errorf("payload must travel byte-for-byte:\n got %s\nwant %s", got, want)
	}
}

// event_header is omitted entirely when absent — never sent as null. A
// destination distinguishing "no header" from "null header" would otherwise see
// the wrong thing for every API-key request.
func TestAbsentEventHeaderIsOmittedNotNull(t *testing.T) {
	var body []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	env := testEnvelope(t, `{"event_id":"e","gateway_id":"g","payload":{}}`)
	if err := NewLTR(srv.URL, "a").Dispatch(context.Background(), env); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}

	var sent map[string]json.RawMessage
	if err := json.Unmarshal(body, &sent); err != nil {
		t.Fatalf("body is not JSON: %v", err)
	}
	if _, present := sent["event_header"]; present {
		t.Fatalf("event_header must be omitted when absent, got %s", body)
	}
}

func TestPresentEventHeaderIsForwardedVerbatim(t *testing.T) {
	var body []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	env := testEnvelope(t, `{"event_id":"e","gateway_id":"g","payload":{},"event_header":{"sub":"u1","iat":1}}`)
	if err := NewLTR(srv.URL, "a").Dispatch(context.Background(), env); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	var sent map[string]json.RawMessage
	_ = json.Unmarshal(body, &sent)
	if got, want := string(sent["event_header"]), `{"sub":"u1","iat":1}`; got != want {
		t.Errorf("event_header mutated:\n got %s\nwant %s", got, want)
	}
}
