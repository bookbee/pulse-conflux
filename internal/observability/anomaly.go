package observability

import (
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// AnomalyType is a condition worth a human's attention.
type AnomalyType string

const (
	LagThreshold      AnomalyType = "lag_threshold"
	DispatchDropRate  AnomalyType = "dispatch_drop_rate"
	SourceUnavailable AnomalyType = "source_unavailable"
	EntriesTrimmed    AnomalyType = "entries_trimmed"
	LogListDepth      AnomalyType = "log_list_depth"
)

// Notifier delivers a support notification. Stubbed in this feature.
type Notifier interface {
	Notify(subject, body string) error
}

// anomalyState tracks one (type, scope) pair across checks.
type anomalyState struct {
	active       bool
	startedAt    time.Time
	breachedAt   time.Time // when the underlying condition first exceeded its threshold
	lastNotified time.Time
	sentThisHour int
	hourStart    time.Time
	detail       string
}

// Detector decides when a condition becomes an anomaly and how often support
// hears about it.
//
// Two separate gates, deliberately:
//   - a SUSTAIN window, so a single burst does not page anyone;
//   - a COOLDOWN and hourly ceiling, so a persistent condition produces a
//     bounded number of messages rather than one per check.
type Detector struct {
	cooldown   time.Duration
	maxPerHour int
	sustainFor time.Duration
	notifier   Notifier
	metrics    *Metrics
	log        *slog.Logger

	mu     sync.Mutex
	states map[string]*anomalyState
}

// NewDetector builds the anomaly detector.
func NewDetector(cooldown time.Duration, maxPerHour int, sustainFor time.Duration, n Notifier, m *Metrics, log *slog.Logger) *Detector {
	return &Detector{
		cooldown: cooldown, maxPerHour: maxPerHour, sustainFor: sustainFor,
		notifier: n, metrics: m, log: log,
		states: map[string]*anomalyState{},
	}
}

// Observe records the current truth of one condition.
//
// breached says whether the condition is over its threshold right now. sustain
// says whether this condition must hold for the sustain window before it counts
// — true for lag, false for conditions that are already meaningful instantly.
func (d *Detector) Observe(t AnomalyType, scope string, breached, sustain bool, detail string, now time.Time) {
	key := string(t) + "|" + scope

	d.mu.Lock()
	defer d.mu.Unlock()

	st, ok := d.states[key]
	if !ok {
		st = &anomalyState{hourStart: now}
		d.states[key] = st
	}

	if !breached {
		st.breachedAt = time.Time{}
		if st.active {
			st.active = false
			d.metrics.AnomaliesActive.WithLabelValues(string(t)).Set(0)
			d.log.Info("anomaly cleared",
				slog.String("component", "anomaly"),
				slog.String("type", string(t)),
				slog.String("scope", scope),
				slog.Duration("duration", now.Sub(st.startedAt)))
			// Exactly one notification when a condition clears, regardless of
			// how many were suppressed while it was active.
			d.send(t, scope, fmt.Sprintf("RESOLVED: %s on %s", t, scope),
				fmt.Sprintf("Cleared after %s. Last detail: %s", now.Sub(st.startedAt).Round(time.Second), st.detail), st, now, true)
		}
		return
	}

	st.detail = detail
	if st.breachedAt.IsZero() {
		st.breachedAt = now
	}
	// The sustain window gates the NOTIFICATION only. Readiness reflects a
	// breach immediately: readiness answers "are we behind now", a notification
	// answers "is this worth waking someone for".
	if sustain && now.Sub(st.breachedAt) < d.sustainFor {
		return
	}

	if !st.active {
		st.active = true
		st.startedAt = st.breachedAt
		d.metrics.AnomaliesActive.WithLabelValues(string(t)).Set(1)
		d.log.Warn("anomaly detected",
			slog.String("component", "anomaly"),
			slog.String("type", string(t)),
			slog.String("scope", scope),
			slog.String("detail", detail))
	}

	d.send(t, scope, fmt.Sprintf("%s on %s", t, scope), detail, st, now, false)
}

// send applies the cooldown and the hourly ceiling before notifying.
func (d *Detector) send(t AnomalyType, scope, subject, body string, st *anomalyState, now time.Time, force bool) {
	if now.Sub(st.hourStart) >= time.Hour {
		st.hourStart = now
		st.sentThisHour = 0
	}

	if !force {
		if !st.lastNotified.IsZero() && now.Sub(st.lastNotified) < d.cooldown {
			d.metrics.NotificationsSent.WithLabelValues(string(t), "suppressed_cooldown").Inc()
			return
		}
		if st.sentThisHour >= d.maxPerHour {
			d.metrics.NotificationsSent.WithLabelValues(string(t), "suppressed_ceiling").Inc()
			return
		}
	}

	if d.notifier == nil {
		d.metrics.NotificationsSent.WithLabelValues(string(t), "no_notifier").Inc()
		return
	}
	if err := d.notifier.Notify(subject, body); err != nil {
		d.metrics.NotificationsSent.WithLabelValues(string(t), "failed").Inc()
		d.log.Error("notification failed",
			slog.String("component", "anomaly"),
			slog.String("type", string(t)),
			slog.String("error", err.Error()))
		return
	}
	st.lastNotified = now
	st.sentThisHour++
	d.metrics.NotificationsSent.WithLabelValues(string(t), "sent").Inc()
}

// Active reports how many anomalies are currently active.
func (d *Detector) Active() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	n := 0
	for _, st := range d.states {
		if st.active {
			n++
		}
	}
	return n
}
