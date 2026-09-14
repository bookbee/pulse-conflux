// Package envelope holds the JSON envelope the gateway writes to every source.
//
// The shape is a cross-repo contract, not this service's to choose: it mirrors
// pulse-gateway's internal/model/enriched_payload.go field-for-field. See
// ../pulse-infra/docs/stack-contract.md, which is the authority (Constitution
// Principle I). Parsing lives here and is shared across every source, because
// the envelope is identical across all of them; consumption and acknowledgement
// are not shared and live behind the ingestion boundary.
package envelope

import (
	"encoding/json"
	"time"
)

// Envelope is one unit of data as the gateway enriched it.
//
// Payload and EventHeader are held as raw JSON deliberately. Re-marshalling
// them would reorder keys and reformat numbers, which breaks a destination-side
// idempotency check keyed on the bytes.
type Envelope struct {
	EventID     string          `json:"event_id"`
	GatewayID   string          `json:"gateway_id"`
	ReceivedAt  time.Time       `json:"received_at"`
	RetryCount  int             `json:"retry_count"`
	StreamName  string          `json:"stream_name,omitempty"`
	EventHeader json.RawMessage `json:"event_header,omitempty"`
	Payload     json.RawMessage `json:"payload"`
}

// HasEventHeader reports whether the gateway attached a header.
//
// It is present ONLY for JWT-authenticated gateway requests and absent for
// API-key ones. Absence is normal and must never be treated as an error; code
// that requires it is a contract change under Principle I.
func (e Envelope) HasEventHeader() bool {
	return len(e.EventHeader) > 0 && string(e.EventHeader) != "null"
}
