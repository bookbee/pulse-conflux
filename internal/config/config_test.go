package config

import (
	"strings"
	"testing"
)

// envFunc builds a lookup over a literal map so tests never touch the real
// environment.
func envFunc(kv map[string]string) func(string) string {
	return func(k string) string { return kv[k] }
}

func TestNormaliseID(t *testing.T) {
	for in, want := range map[string]string{
		"events-ltr":  "EVENTS_LTR",
		"events_ltr":  "EVENTS_LTR",
		"Events.LTR":  "EVENTS_LTR",
		"log summary": "LOG_SUMMARY",
		"--a--b--":    "A_B",
	} {
		if got := NormaliseID(in); got != want {
			t.Errorf("NormaliseID(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestDuplicateNormalisedIDsRejected(t *testing.T) {
	_, err := loadFrom(envFunc(map[string]string{
		"CONFLUX_AGENTS": "events-ltr,events_ltr",
	}))
	if err == nil || !strings.Contains(err.Error(), "unique after normalisation") {
		t.Fatalf("want duplicate-id error, got %v", err)
	}
}

// A missing required key must fail startup AND name the key, so an operator can
// act on the message without reading the source.
func TestMissingRequiredKeyNamesTheKey(t *testing.T) {
	_, err := loadFrom(envFunc(map[string]string{
		"CONFLUX_AGENTS":                "events-ltr",
		"AGENT_EVENTS_LTR_MODE":         "stream",
		"AGENT_EVENTS_LTR_DESTINATIONS": "ltr",
		// SOURCE deliberately absent
	}))
	if err == nil {
		t.Fatal("want an error for a stream agent with no source")
	}
	if !strings.Contains(err.Error(), "AGENT_EVENTS_LTR_SOURCE") {
		t.Fatalf("error must name the missing key, got: %v", err)
	}
}

// The same agent, disabled, must load cleanly: disabling must not require
// filling in configuration you do not use.
func TestDisabledAgentExemptFromValidation(t *testing.T) {
	cfg, err := loadFrom(envFunc(map[string]string{
		"CONFLUX_AGENTS":           "events-ltr",
		"AGENT_EVENTS_LTR_ENABLED": "false",
		// MODE, SOURCE and DESTINATIONS all absent
	}))
	if err != nil {
		t.Fatalf("disabled agent must not fail validation: %v", err)
	}
	if len(cfg.Enabled()) != 0 {
		t.Fatalf("want no enabled agents, got %d", len(cfg.Enabled()))
	}
	if len(cfg.Agents) != 1 {
		t.Fatalf("disabled agent should still be known, got %d", len(cfg.Agents))
	}
}

func TestPeriodicAgentRequiresInterval(t *testing.T) {
	_, err := loadFrom(envFunc(map[string]string{
		"CONFLUX_AGENTS":                 "log-summary",
		"AGENT_LOG_SUMMARY_MODE":         "periodic",
		"AGENT_LOG_SUMMARY_DESTINATIONS": "ltr",
	}))
	if err == nil || !strings.Contains(err.Error(), "AGENT_LOG_SUMMARY_INTERVAL") {
		t.Fatalf("want interval error naming the key, got %v", err)
	}
}

func TestNoAgentsIsValid(t *testing.T) {
	cfg, err := loadFrom(envFunc(map[string]string{}))
	if err != nil {
		t.Fatalf("empty CONFLUX_AGENTS must be valid: %v", err)
	}
	if len(cfg.Agents) != 0 {
		t.Fatalf("want 0 agents, got %d", len(cfg.Agents))
	}
	if cfg.InstanceID == "" {
		t.Fatal("InstanceID must default to something non-empty (FR-009a)")
	}
}

func TestDefaultsMatchContract(t *testing.T) {
	cfg, _ := loadFrom(envFunc(map[string]string{}))
	if cfg.HTTPAddr != ":8090" {
		t.Errorf("HTTP_ADDR default = %q", cfg.HTTPAddr)
	}
	if cfg.Dispatch.MaxAttempts != 3 {
		t.Errorf("DISPATCH_MAX_ATTEMPTS default = %d", cfg.Dispatch.MaxAttempts)
	}
	// 3m sustained + up to 1m check latency stays inside SC-007's 5m bound.
	if cfg.Anomaly.LagSustainedFor.Minutes() != 3 {
		t.Errorf("LAG_SUSTAINED_FOR default = %v", cfg.Anomaly.LagSustainedFor)
	}
	if cfg.House.SummaryMaxDepth != 2000 {
		t.Errorf("SUMMARY_MAX_LIST_DEPTH default = %d", cfg.House.SummaryMaxDepth)
	}
}
