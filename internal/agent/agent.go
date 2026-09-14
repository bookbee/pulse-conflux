// Package agent hosts the runtime: several independently configured units of
// work in one process, each with its own trigger, position and destinations.
//
// The isolation guarantee is the point (FR-004): a failure in one agent must
// not stop, stall or corrupt any other, and the runtime must be able to say
// which agent failed.
package agent

import (
	"context"
	"sync"
)

// State is where an agent is in its lifecycle.
type State string

const (
	StateConfigured State = "configured"
	StateRunning    State = "running"
	StateRecovering State = "recovering"
	StateDegraded   State = "degraded"
	StateDraining   State = "draining"
	StateStopped    State = "stopped"
)

// Agent is one unit of work the runner supervises.
type Agent interface {
	ID() string
	SourceName() string
	// Run blocks until ctx is cancelled or an unrecoverable error occurs.
	Run(ctx context.Context) error
}

// status is an agent's observable condition, shared with readiness reporting.
type status struct {
	mu       sync.RWMutex
	state    State
	lastErr  string
	panics   int
	restarts int
}

func (s *status) set(st State) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state = st
}

func (s *status) fail(st State, err string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state = st
	s.lastErr = err
}

func (s *status) snapshot() (State, string, int, int) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.state, s.lastErr, s.panics, s.restarts
}
