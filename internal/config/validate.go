package config

import (
	"errors"
	"fmt"
	"strings"
)

// Validate checks the configuration the service will actually use.
//
// A missing required key for an ENABLED agent is a startup failure that names
// the key. A missing key for a disabled agent is ignored — disabling an agent
// must not require filling in configuration you do not use.
func (c *Config) Validate() error {
	var errs []error

	if c.Anomaly.EmailEnabled && strings.TrimSpace(c.Anomaly.SupportTo) == "" {
		errs = append(errs, errors.New("SUPPORT_EMAIL_TO is required when EMAIL_ENABLED=true"))
	}
	if c.Dispatch.MaxAttempts < 1 {
		errs = append(errs, errors.New("DISPATCH_MAX_ATTEMPTS must be at least 1"))
	}

	for _, a := range c.Agents {
		if !a.Enabled {
			continue
		}
		errs = append(errs, a.validate(c)...)
	}
	return errors.Join(errs...)
}

func (a Agent) validate(c *Config) []error {
	var errs []error
	k := func(suffix string) string { return "AGENT_" + a.Key + "_" + suffix }

	switch a.Mode {
	case ModeStream:
		if a.Source == "" {
			errs = append(errs, fmt.Errorf("%s is required for a stream agent", k("SOURCE")))
		}
		if a.SourceKind != string(kindStream) && a.SourceKind != string(kindList) {
			errs = append(errs, fmt.Errorf("%s must be %q or %q, got %q", k("SOURCE_KIND"), kindStream, kindList, a.SourceKind))
		}
	case ModePeriodic:
		if a.Interval <= 0 {
			errs = append(errs, fmt.Errorf("%s is required for a periodic agent and must be positive", k("INTERVAL")))
		}
	case "":
		errs = append(errs, fmt.Errorf("%s is required (%q or %q)", k("MODE"), ModeStream, ModePeriodic))
	default:
		errs = append(errs, fmt.Errorf("%s must be %q or %q, got %q", k("MODE"), ModeStream, ModePeriodic, a.Mode))
	}

	if len(a.Destinations) == 0 {
		errs = append(errs, fmt.Errorf("%s is required (comma-separated destination names)", k("DESTINATIONS")))
	}
	for _, d := range a.Destinations {
		if d == destSummary && c.House.SummaryDestination == "" {
			// Only surfaces when a summary agent is actually enabled.
			errs = append(errs, errors.New("SUMMARY_DESTINATION is required when the summary agent is enabled"))
		}
	}
	return errs
}

// Kept as local constants rather than importing internal/ingest: config must
// not depend on the ingestion boundary, only describe it.
const (
	kindStream = "stream"
	kindList   = "list"

	destSummary = "summary"
)
