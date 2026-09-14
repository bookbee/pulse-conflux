// Package config loads every tunable from the environment.
//
// Constitution Principle I: endpoints, credentials, source names, intervals and
// limits are read from configuration and never hardcoded, and every key here
// MUST also appear in .env.example with a comment naming what it contracts with.
// See specs/001-event-agent-runtime/contracts/configuration.md.
package config

import (
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"
)

// Mode is an agent's trigger. Exactly one per agent.
type Mode string

const (
	ModeStream   Mode = "stream"   // continuous
	ModePeriodic Mode = "periodic" // fixed interval
)

// Config is the whole runtime configuration.
type Config struct {
	HTTPAddr      string
	ShutdownGrace time.Duration
	InstanceID    string
	LogLevel      string

	Redis    Redis
	Dispatch Dispatch
	House    Housekeeping
	Anomaly  Anomaly

	Agents []Agent
}

// Redis is the queue this service consumes. Names are contracts.
type Redis struct {
	Addr         string
	Password     string
	DB           int
	ConsumerGrp  string
	ConsumerName string
}

// Dispatch is the bounded-loss policy: attempts, backoff, per-attempt timeout.
type Dispatch struct {
	MaxAttempts    int
	Timeout        time.Duration
	BackoffInitial time.Duration
	BackoffMax     time.Duration
	LTRURL         string
	CEPURL         string
}

// Housekeeping tunes reclaim, consumer retirement and summarisation.
type Housekeeping struct {
	PendingMinIdle     time.Duration
	PendingBatch       int
	ConsumerRetireIdle time.Duration
	SummaryPeriod      time.Duration
	SummaryDestination string
	SummaryMaxDepth    int64
}

// Anomaly tunes detection and how loudly support hears about it.
type Anomaly struct {
	LagThresholdEntries int64
	LagSustainedFor     time.Duration
	DropRateThreshold   float64
	CheckInterval       time.Duration
	NotifyCooldown      time.Duration
	MaxPerHour          int
	EmailEnabled        bool
	SMTPAddr            string
	SupportTo           string
	SupportFrom         string
}

// Agent is one configured unit of work.
type Agent struct {
	ID           string
	Key          string // normalised id used to build AGENT_<KEY>_* lookups
	Enabled      bool
	Mode         Mode
	Source       string
	SourceKind   string
	Destinations []string
	Interval     time.Duration
	BatchSize    int
	BlockTimeout time.Duration
}

var nonAlnum = regexp.MustCompile(`[^A-Z0-9]+`)

// NormaliseID uppercases an agent id and folds everything else to underscores,
// which is how AGENT_<ID>_* keys are spelled. Ids must be unique after this.
func NormaliseID(id string) string {
	return strings.Trim(nonAlnum.ReplaceAllString(strings.ToUpper(id), "_"), "_")
}

// Load reads .env if present, then the environment. A missing .env is not an
// error: in a container the values come from the environment directly.
func Load() (*Config, error) {
	_ = godotenv.Load()
	return loadFrom(os.Getenv)
}

func loadFrom(get func(string) string) (*Config, error) {
	host, _ := os.Hostname()
	c := &Config{
		HTTPAddr:      str(get, "HTTP_ADDR", ":8090"),
		ShutdownGrace: dur(get, "SHUTDOWN_GRACE", 15*time.Second),
		// Instance identity defaults to the hostname so containers and pods get
		// a unique consumer name for free. Without it two processes running the
		// same agent share one consumer name, split the stream, and reclaim can
		// steal work that is in flight at the other one (FR-009a).
		InstanceID: str(get, "CONFLUX_INSTANCE_ID", fallback(host, "conflux")),
		LogLevel:   str(get, "LOG_LEVEL", "info"),
		Redis: Redis{
			Addr:         str(get, "REDIS_ADDR", "localhost:6379"),
			Password:     get("REDIS_PASSWORD"),
			DB:           num(get, "REDIS_DB", 0),
			ConsumerGrp:  str(get, "REDIS_CONSUMER_GROUP", "pulse-conflux-local"),
			ConsumerName: str(get, "REDIS_CONSUMER_NAME", "conflux"),
		},
		Dispatch: Dispatch{
			MaxAttempts:    num(get, "DISPATCH_MAX_ATTEMPTS", 3),
			Timeout:        dur(get, "DISPATCH_TIMEOUT", 10*time.Second),
			BackoffInitial: dur(get, "DISPATCH_BACKOFF_INITIAL", 200*time.Millisecond),
			BackoffMax:     dur(get, "DISPATCH_BACKOFF_MAX", 5*time.Second),
			LTRURL:         str(get, "DEST_LTR_URL", "http://localhost:9999/stub/ltr"),
			CEPURL:         str(get, "DEST_CEP_URL", "http://localhost:9999/stub/cep"),
		},
		House: Housekeeping{
			PendingMinIdle:     dur(get, "PENDING_MIN_IDLE", 5*time.Minute),
			PendingBatch:       num(get, "PENDING_RECLAIM_BATCH", 100),
			ConsumerRetireIdle: dur(get, "CONSUMER_RETIRE_IDLE", 24*time.Hour),
			SummaryPeriod:      dur(get, "SUMMARY_PERIOD", time.Hour),
			SummaryDestination: get("SUMMARY_DESTINATION"),
			SummaryMaxDepth:    int64(num(get, "SUMMARY_MAX_LIST_DEPTH", 2000)),
		},
		Anomaly: Anomaly{
			LagThresholdEntries: int64(num(get, "LAG_THRESHOLD_ENTRIES", 1000)),
			LagSustainedFor:     dur(get, "LAG_SUSTAINED_FOR", 3*time.Minute),
			DropRateThreshold:   flt(get, "DROP_RATE_THRESHOLD", 0.01),
			CheckInterval:       dur(get, "ANOMALY_CHECK_INTERVAL", time.Minute),
			NotifyCooldown:      dur(get, "ANOMALY_NOTIFY_COOLDOWN", 30*time.Minute),
			MaxPerHour:          num(get, "ANOMALY_MAX_PER_HOUR", 4),
			EmailEnabled:        boolean(get, "EMAIL_ENABLED", false),
			SMTPAddr:            str(get, "SMTP_ADDR", "localhost:1025"),
			SupportTo:           get("SUPPORT_EMAIL_TO"),
			SupportFrom:         str(get, "SUPPORT_EMAIL_FROM", "pulse-conflux@localhost"),
		},
	}

	agents, err := loadAgents(get)
	if err != nil {
		return nil, err
	}
	c.Agents = agents

	if err := c.Validate(); err != nil {
		return nil, err
	}
	return c, nil
}

func loadAgents(get func(string) string) ([]Agent, error) {
	raw := strings.TrimSpace(get("CONFLUX_AGENTS"))
	if raw == "" {
		// Not an error: the service starts, serves probes, and processes
		// nothing. That is a legitimate way to run it.
		return nil, nil
	}
	seen := map[string]string{}
	var agents []Agent
	for _, id := range strings.Split(raw, ",") {
		id = strings.TrimSpace(id)
		if id == "" {
			continue
		}
		key := NormaliseID(id)
		if key == "" {
			return nil, fmt.Errorf("agent id %q normalises to an empty key", id)
		}
		if prev, dup := seen[key]; dup {
			return nil, fmt.Errorf("agent ids %q and %q both normalise to %q; ids must be unique after normalisation", prev, id, key)
		}
		seen[key] = id

		p := func(suffix string) string { return get("AGENT_" + key + "_" + suffix) }
		agents = append(agents, Agent{
			ID:           id,
			Key:          key,
			Enabled:      boolean(p, "ENABLED", true),
			Mode:         Mode(strings.TrimSpace(p("MODE"))),
			Source:       strings.TrimSpace(p("SOURCE")),
			SourceKind:   str(p, "SOURCE_KIND", "stream"),
			Destinations: csv(p("DESTINATIONS")),
			Interval:     dur(p, "INTERVAL", 0),
			BatchSize:    num(p, "BATCH_SIZE", 100),
			BlockTimeout: dur(p, "BLOCK_TIMEOUT", 5*time.Second),
		})
	}
	return agents, nil
}

// prefixed lookups reuse the same helpers by passing a closure, so AGENT_<K>_X
// and a bare X read identically.
func str(get func(string) string, k, def string) string {
	if v := strings.TrimSpace(get(k)); v != "" {
		return v
	}
	return def
}

func num(get func(string) string, k string, def int) int {
	if v := strings.TrimSpace(get(k)); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func flt(get func(string) string, k string, def float64) float64 {
	if v := strings.TrimSpace(get(k)); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return def
}

func dur(get func(string) string, k string, def time.Duration) time.Duration {
	if v := strings.TrimSpace(get(k)); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

func boolean(get func(string) string, k string, def bool) bool {
	if v := strings.TrimSpace(get(k)); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
	}
	return def
}

func csv(v string) []string {
	var out []string
	for _, p := range strings.Split(v, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func fallback(v, def string) string {
	if strings.TrimSpace(v) == "" {
		return def
	}
	return v
}

// Enabled returns only the agents that should be constructed.
func (c *Config) Enabled() []Agent {
	var out []Agent
	for _, a := range c.Agents {
		if a.Enabled {
			out = append(out, a)
		}
	}
	return out
}
