package hlc

import (
	"fmt"
	"sync"
	"time"
)

// PhysicalClock provides physical time. Injecting this lets tests
// control time instead of depending on the real wall clock.
type PhysicalClock interface {
	Now() time.Time
}

// WallClock is the default PhysicalClock backed by time.Now.
type WallClock struct{}

// Now returns the current wall-clock time.
func (WallClock) Now() time.Time { return time.Now() }

// Timestamp is an HLC timestamp: a physical wall time paired with a
// logical counter for ordering events that share the same wall time.
//
// Compared lexicographically: WallTime first, then Counter.
type Timestamp struct {
	// WallTime is physical time in milliseconds since Unix epoch.
	WallTime uint64 `json:"wall_time"`

	// Counter breaks ties when multiple events share the same WallTime.
	// Resets to 0 whenever WallTime advances.
	Counter uint32 `json:"counter"`
}

// Before reports whether ts is strictly before other.
func (ts Timestamp) Before(other Timestamp) bool {
	if ts.WallTime != other.WallTime {
		return ts.WallTime < other.WallTime
	}
	return ts.Counter < other.Counter
}

// After reports whether ts is strictly after other.
func (ts Timestamp) After(other Timestamp) bool {
	return other.Before(ts)
}

// Equal reports whether ts and other represent the same timestamp.
func (ts Timestamp) Equal(other Timestamp) bool {
	return ts.WallTime == other.WallTime && ts.Counter == other.Counter
}

// IsZero reports whether the timestamp is the zero value.
func (ts Timestamp) IsZero() bool {
	return ts.WallTime == 0 && ts.Counter == 0
}

// Compare returns -1 if ts < other, 0 if ts == other, +1 if ts > other.
func (ts Timestamp) Compare(other Timestamp) int {
	switch {
	case ts.Before(other):
		return -1
	case ts.After(other):
		return 1
	default:
		return 0
	}
}

// String returns a human-readable representation for debugging.
func (ts Timestamp) String() string {
	return fmt.Sprintf("{wall:%d counter:%d}", ts.WallTime, ts.Counter)
}

// Clock is a Hybrid Logical Clock safe for concurrent use.
//
// Invariants:
//   - Successive timestamps from the same Clock are strictly increasing.
//   - Receive(remote) returns a timestamp strictly greater than remote.
//   - WallTime stays within max_clock_skew of the physical clock,
//     assuming all nodes are within max_clock_skew of each other.
type Clock struct {
	mu   sync.Mutex
	phys PhysicalClock
	last Timestamp
}

// NewClock creates a Clock driven by the given physical clock source.
func NewClock(phys PhysicalClock) *Clock {
	return &Clock{phys: phys}
}

func (c *Clock) wallTimeMs() uint64 {
	return uint64(c.phys.Now().UnixMilli())
}

// Now generates a timestamp for a local event.
//
// If the physical clock has advanced past the last wall time, adopt it
// and reset the counter. Otherwise keep the old wall time and bump the
// counter. This guarantees monotonicity even if the clock jumps backward.
func (c *Clock) Now() Timestamp {
	c.mu.Lock()
	defer c.mu.Unlock()

	pt := c.wallTimeMs()

	if pt > c.last.WallTime {
		c.last = Timestamp{WallTime: pt, Counter: 0}
	} else {
		c.last.Counter++
	}

	return c.last
}

// Send generates a timestamp to attach to an outgoing message.
// Semantically identical to Now; separated for clarity at call sites.
func (c *Clock) Send() Timestamp {
	return c.Now()
}

// Receive processes an incoming remote timestamp and returns a new
// timestamp strictly greater than both the local and remote timestamps.
//
// Takes max(physical_time, local_wall, remote_wall) as the new wall time.
// The counter is set based on which wall time won:
//   - Physical ahead of both: counter = 0.
//   - Local and remote tied: max(local_counter, remote_counter) + 1.
//   - Local ahead: local_counter + 1.
//   - Remote ahead: remote_counter + 1.
func (c *Clock) Receive(remote Timestamp) Timestamp {
	c.mu.Lock()
	defer c.mu.Unlock()

	pt := c.wallTimeMs()

	switch {
	case pt > c.last.WallTime && pt > remote.WallTime:
		c.last = Timestamp{WallTime: pt, Counter: 0}

	case c.last.WallTime == remote.WallTime && c.last.WallTime >= pt:
		// Tied wall times: merge counters.
		counter := c.last.Counter
		if remote.Counter > counter {
			counter = remote.Counter
		}
		c.last = Timestamp{WallTime: c.last.WallTime, Counter: counter + 1}

	case c.last.WallTime > remote.WallTime && c.last.WallTime >= pt:
		c.last.Counter++

	default:
		// Remote is ahead of both local and physical.
		c.last = Timestamp{WallTime: remote.WallTime, Counter: remote.Counter + 1}
	}

	return c.last
}

// Last returns the most recent timestamp held by the clock.
func (c *Clock) Last() Timestamp {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.last
}

// Seed advances the clock's state to at least ts.
// If ts is before or equal to the clock's current state, Seed is a no-op.
// This is typically called during crash recovery to restore persisted clock state.
func (c *Clock) Seed(ts Timestamp) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if ts.After(c.last) {
		c.last = ts
	}
}
