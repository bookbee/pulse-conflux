package envelope

import (
	"encoding/json"
	"errors"
	"fmt"
)

// ErrUnprocessable marks an entry that cannot become an Envelope. Callers must
// not let one of these stop progress on the remaining entries (FR-012).
var ErrUnprocessable = errors.New("unprocessable entry")

// Parse turns one raw source entry into an Envelope.
//
// Required fields are event_id, gateway_id and payload. Everything else is
// optional, including event_header.
func Parse(raw []byte) (Envelope, error) {
	var e Envelope
	if err := json.Unmarshal(raw, &e); err != nil {
		return Envelope{}, fmt.Errorf("%w: malformed JSON: %v", ErrUnprocessable, err)
	}
	if err := e.Validate(); err != nil {
		return Envelope{}, err
	}
	return e, nil
}

// Validate enforces the required-field rules from the data model.
func (e Envelope) Validate() error {
	switch {
	case e.EventID == "":
		return fmt.Errorf("%w: event_id is empty", ErrUnprocessable)
	case e.GatewayID == "":
		return fmt.Errorf("%w: gateway_id is empty", ErrUnprocessable)
	case len(e.Payload) == 0 || string(e.Payload) == "null":
		return fmt.Errorf("%w: payload is empty", ErrUnprocessable)
	}
	return nil
}
