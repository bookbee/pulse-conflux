package envelope

import (
	"encoding/json"
	"errors"
	"testing"
)

const valid = `{"event_id":"evt-1","gateway_id":"gw-local",` +
	`"received_at":"2026-09-14T09:00:00Z","retry_count":0,` +
	`"stream_name":"ingestion-events","payload":{"b":2,"a":1.10}}`

func TestParseValid(t *testing.T) {
	e, err := Parse([]byte(valid))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if e.EventID != "evt-1" || e.GatewayID != "gw-local" {
		t.Fatalf("identity not parsed: %+v", e)
	}
	if e.ReceivedAt.IsZero() {
		t.Fatal("received_at not parsed")
	}
}

// The gateway omits event_header for API-key requests. That is normal traffic,
// not an error, and the most likely regression in this package (FR-010).
func TestParseWithoutEventHeaderSucceeds(t *testing.T) {
	e, err := Parse([]byte(valid))
	if err != nil {
		t.Fatalf("envelope without event_header must parse: %v", err)
	}
	if e.HasEventHeader() {
		t.Fatal("HasEventHeader true for an absent header")
	}
}

func TestParseWithEventHeader(t *testing.T) {
	raw := `{"event_id":"e","gateway_id":"g","payload":{},"event_header":{"sub":"u1"}}`
	e, err := Parse([]byte(raw))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if !e.HasEventHeader() {
		t.Fatal("HasEventHeader false for a present header")
	}
}

func TestParseRejectsMissingRequiredFields(t *testing.T) {
	for name, raw := range map[string]string{
		"no event_id":   `{"gateway_id":"g","payload":{}}`,
		"no gateway_id": `{"event_id":"e","payload":{}}`,
		"no payload":    `{"event_id":"e","gateway_id":"g"}`,
		"null payload":  `{"event_id":"e","gateway_id":"g","payload":null}`,
		"malformed":     `{"event_id":`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := Parse([]byte(raw)); !errors.Is(err, ErrUnprocessable) {
				t.Fatalf("want ErrUnprocessable, got %v", err)
			}
		})
	}
}

// Payload must survive byte-for-byte: key order and number formatting included.
// Re-marshalling would rewrite `1.10` as `1.1` and reorder b before a, which
// breaks any destination-side idempotency check over the bytes.
func TestPayloadPreservedByteForByte(t *testing.T) {
	e, err := Parse([]byte(valid))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if got, want := string(e.Payload), `{"b":2,"a":1.10}`; got != want {
		t.Fatalf("payload mutated:\n got %s\nwant %s", got, want)
	}
	var reencoded map[string]any
	if err := json.Unmarshal(e.Payload, &reencoded); err != nil {
		t.Fatalf("payload is not valid JSON after round-trip: %v", err)
	}
}
