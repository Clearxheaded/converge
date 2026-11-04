package hlc

import (
	"sort"
	"sync"
	"testing"
	"time"
)

// ManualClock is a test double that returns a controllable time.
type ManualClock struct {
	mu  sync.Mutex
	now time.Time
}

func NewManualClock(t time.Time) *ManualClock {
	return &ManualClock{now: t}
}

func (c *ManualClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *ManualClock) Set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = t
}

func (c *ManualClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// Rewind moves the clock backward (simulates NTP backward correction).
func (c *ManualClock) Rewind(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(-d)
}

// --- Baseline ---

func TestNow_ReturnsNonZeroTimestamp(t *testing.T) {
	phys := NewManualClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	c := NewClock(phys)

	ts := c.Now()
	if ts.WallTime == 0 {
		t.Fatal("expected non-zero wall time")
	}
	if ts.Counter != 0 {
		t.Fatalf("expected counter=0 on first call, got %d", ts.Counter)
	}
}

func TestNow_UsesMillisecondPrecision(t *testing.T) {
	phys := NewManualClock(time.Date(2026, 1, 1, 0, 0, 0, 500_000, time.UTC))
	c := NewClock(phys)

	ts := c.Now()
	expected := uint64(time.Date(2026, 1, 1, 0, 0, 0, 500_000, time.UTC).UnixMilli())
	if ts.WallTime != expected {
		t.Fatalf("expected wall time %d, got %d", expected, ts.WallTime)
	}
}

// --- Monotonicity ---

func TestMonotonicity_SamePhysicalTime(t *testing.T) {
	phys := NewManualClock(time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC))
	c := NewClock(phys)

	prev := c.Now()
	for i := 0; i < 100; i++ {
		next := c.Now()
		if !next.After(prev) {
			t.Fatalf("iteration %d: did not advance: prev=%v next=%v", i, prev, next)
		}
		prev = next
	}
}

func TestMonotonicity_AdvancingPhysicalTime(t *testing.T) {
	phys := NewManualClock(time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC))
	c := NewClock(phys)

	prev := c.Now()
	for i := 0; i < 50; i++ {
		phys.Advance(time.Millisecond)
		next := c.Now()
		if !next.After(prev) {
			t.Fatalf("iteration %d: did not advance: prev=%v next=%v", i, prev, next)
		}
		if next.Counter != 0 {
			t.Fatalf("iteration %d: counter should reset when wall time advances, got %d", i, next.Counter)
		}
		prev = next
	}
}

func TestMonotonicity_ClockGoesBackward(t *testing.T) {
	phys := NewManualClock(time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC))
	c := NewClock(phys)

	ts1 := c.Now()
	phys.Rewind(5 * time.Second)

	ts2 := c.Now()
	if !ts2.After(ts1) {
		t.Fatalf("went backward after rewind: ts1=%v ts2=%v", ts1, ts2)
	}
	if ts2.WallTime != ts1.WallTime {
		t.Fatalf("wall time should be preserved: got %d, want %d", ts1.WallTime, ts2.WallTime)
	}
}

// --- Counter Behavior ---

func TestCounter_IncrementsWhenClockStalled(t *testing.T) {
	phys := NewManualClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	c := NewClock(phys)

	ts1 := c.Now()
	ts2 := c.Now()
	ts3 := c.Now()

	if ts1.Counter != 0 || ts2.Counter != 1 || ts3.Counter != 2 {
		t.Fatalf("expected counters 0,1,2 got %d,%d,%d",
			ts1.Counter, ts2.Counter, ts3.Counter)
	}
	if ts1.WallTime != ts2.WallTime || ts2.WallTime != ts3.WallTime {
		t.Fatal("wall times should match when clock is stalled")
	}
}

func TestCounter_ResetsWhenClockAdvances(t *testing.T) {
	phys := NewManualClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	c := NewClock(phys)

	c.Now()
	c.Now()
	c.Now()

	phys.Advance(time.Millisecond)
	ts := c.Now()
	if ts.Counter != 0 {
		t.Fatalf("counter should reset after clock advance, got %d", ts.Counter)
	}
}

// --- Timestamp Comparison ---

func TestTimestamp_Comparison(t *testing.T) {
	tests := []struct {
		name   string
		a, b   Timestamp
		before bool
		after  bool
		equal  bool
		cmp    int
	}{
		{
			name:   "a before b by wall time",
			a:      Timestamp{WallTime: 100, Counter: 5},
			b:      Timestamp{WallTime: 200, Counter: 0},
			before: true, after: false, equal: false, cmp: -1,
		},
		{
			name:   "a before b by counter",
			a:      Timestamp{WallTime: 100, Counter: 2},
			b:      Timestamp{WallTime: 100, Counter: 3},
			before: true, after: false, equal: false, cmp: -1,
		},
		{
			name:   "a after b by wall time",
			a:      Timestamp{WallTime: 300, Counter: 0},
			b:      Timestamp{WallTime: 200, Counter: 99},
			before: false, after: true, equal: false, cmp: 1,
		},
		{
			name:   "equal timestamps",
			a:      Timestamp{WallTime: 100, Counter: 5},
			b:      Timestamp{WallTime: 100, Counter: 5},
			before: false, after: false, equal: true, cmp: 0,
		},
		{
			name:   "zero timestamps are equal",
			a:      Timestamp{},
			b:      Timestamp{},
			before: false, after: false, equal: true, cmp: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.a.Before(tt.b); got != tt.before {
				t.Errorf("Before: got %v, want %v", got, tt.before)
			}
			if got := tt.a.After(tt.b); got != tt.after {
				t.Errorf("After: got %v, want %v", got, tt.after)
			}
			if got := tt.a.Equal(tt.b); got != tt.equal {
				t.Errorf("Equal: got %v, want %v", got, tt.equal)
			}
			if got := tt.a.Compare(tt.b); got != tt.cmp {
				t.Errorf("Compare: got %v, want %v", got, tt.cmp)
			}
		})
	}
}

func TestTimestamp_SortOrder(t *testing.T) {
	timestamps := []Timestamp{
		{WallTime: 300, Counter: 0},
		{WallTime: 100, Counter: 2},
		{WallTime: 100, Counter: 0},
		{WallTime: 200, Counter: 1},
		{WallTime: 100, Counter: 1},
	}

	sort.Slice(timestamps, func(i, j int) bool {
		return timestamps[i].Before(timestamps[j])
	})

	for i := 1; i < len(timestamps); i++ {
		if !timestamps[i-1].Before(timestamps[i]) {
			t.Fatalf("not sorted at index %d: %v >= %v",
				i, timestamps[i-1], timestamps[i])
		}
	}
}

// --- Send ---

func TestSend_BehavesLikeNow(t *testing.T) {
	phys := NewManualClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	c := NewClock(phys)

	ts1 := c.Now()
	ts2 := c.Send()

	if !ts2.After(ts1) {
		t.Fatalf("Send should advance past Now: ts1=%v ts2=%v", ts1, ts2)
	}
}

// --- Receive ---

func TestReceive_RemoteAheadOfLocal(t *testing.T) {
	phys := NewManualClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	c := NewClock(phys)

	local := c.Now()
	remote := Timestamp{WallTime: local.WallTime + 5000, Counter: 3}
	result := c.Receive(remote)

	if !result.After(remote) {
		t.Fatalf("result should be > remote: result=%v remote=%v", result, remote)
	}
	if !result.After(local) {
		t.Fatalf("result should be > local: result=%v local=%v", result, local)
	}
	if result.WallTime != remote.WallTime {
		t.Fatalf("should adopt remote wall time: got %d, want %d",
			result.WallTime, remote.WallTime)
	}
	if result.Counter != remote.Counter+1 {
		t.Fatalf("counter should be remote.Counter+1: got %d, want %d",
			result.Counter, remote.Counter+1)
	}
}

func TestReceive_LocalAheadOfRemote(t *testing.T) {
	phys := NewManualClock(time.Date(2026, 1, 1, 0, 0, 5, 0, time.UTC))
	c := NewClock(phys)

	local := c.Now()
	remote := Timestamp{WallTime: local.WallTime - 3000, Counter: 10}
	result := c.Receive(remote)

	if !result.After(local) {
		t.Fatalf("should advance past local: result=%v local=%v", result, local)
	}
	if result.WallTime != local.WallTime {
		t.Fatalf("should keep local wall time: got %d, want %d",
			result.WallTime, local.WallTime)
	}
	if result.Counter != local.Counter+1 {
		t.Fatalf("counter should be local.Counter+1: got %d, want %d",
			result.Counter, local.Counter+1)
	}
}

func TestReceive_TiedWallTimes(t *testing.T) {
	phys := NewManualClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	c := NewClock(phys)

	c.Now() // counter=0
	c.Now() // counter=1
	c.Now() // counter=2

	remote := Timestamp{WallTime: uint64(phys.Now().UnixMilli()), Counter: 5}
	result := c.Receive(remote)

	if result.WallTime != remote.WallTime {
		t.Fatalf("wall times should match: got %d, want %d",
			result.WallTime, remote.WallTime)
	}
	// max(2, 5) + 1 = 6
	if result.Counter != 6 {
		t.Fatalf("counter should be max(2,5)+1=6, got %d", result.Counter)
	}
}

func TestReceive_PhysicalClockAheadOfBoth(t *testing.T) {
	phys := NewManualClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	c := NewClock(phys)

	c.Now()
	phys.Advance(10 * time.Second)

	remote := Timestamp{WallTime: uint64(phys.Now().UnixMilli()) - 5000, Counter: 99}
	result := c.Receive(remote)

	expectedWall := uint64(phys.Now().UnixMilli())
	if result.WallTime != expectedWall {
		t.Fatalf("should use physical time: got %d, want %d",
			result.WallTime, expectedWall)
	}
	if result.Counter != 0 {
		t.Fatalf("counter should be 0 when physical clock is ahead, got %d", result.Counter)
	}
}

// --- Causality ---
//
// If event A causally precedes B (via a send/receive chain),
// then TS(A) < TS(B).

func TestCausality_SendReceiveChain(t *testing.T) {
	// Alice -> Bob -> Carol. Timestamps must increase along the chain.
	base := time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC)

	alice := NewClock(NewManualClock(base))
	bob := NewClock(NewManualClock(base))
	carol := NewClock(NewManualClock(base))

	aliceTS := alice.Send()

	bobTS := bob.Receive(aliceTS)
	if !bobTS.After(aliceTS) {
		t.Fatalf("bob should be > alice: bob=%v alice=%v", bobTS, aliceTS)
	}

	bobSendTS := bob.Send()
	if !bobSendTS.After(bobTS) {
		t.Fatalf("bob send should be > bob receive: send=%v recv=%v", bobSendTS, bobTS)
	}

	carolTS := carol.Receive(bobSendTS)
	if !carolTS.After(bobSendTS) {
		t.Fatalf("carol should be > bob send: carol=%v bob=%v", carolTS, bobSendTS)
	}
	if !carolTS.After(aliceTS) {
		t.Fatalf("transitive causality violated: carol=%v alice=%v", carolTS, aliceTS)
	}
}

func TestCausality_WithClockSkew(t *testing.T) {
	// Alice's clock is 2s ahead. Causality must hold despite skew.
	base := time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC)

	alice := NewClock(NewManualClock(base.Add(2 * time.Second)))
	bob := NewClock(NewManualClock(base))

	aliceTS := alice.Send()
	bobTS := bob.Receive(aliceTS)
	if !bobTS.After(aliceTS) {
		t.Fatalf("causality violated with clock skew: bob=%v alice=%v", bobTS, aliceTS)
	}
}

func TestCausality_BidirectionalExchange(t *testing.T) {
	// Alice and Bob exchange messages back and forth.
	base := time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC)

	alice := NewClock(NewManualClock(base))
	bob := NewClock(NewManualClock(base.Add(100 * time.Millisecond)))

	var chain []Timestamp

	// Round 1: Alice -> Bob
	a1 := alice.Send()
	chain = append(chain, a1)
	b1 := bob.Receive(a1)
	chain = append(chain, b1)

	// Round 2: Bob -> Alice
	b2 := bob.Send()
	chain = append(chain, b2)
	a2 := alice.Receive(b2)
	chain = append(chain, a2)

	// Round 3: Alice -> Bob
	a3 := alice.Send()
	chain = append(chain, a3)
	b3 := bob.Receive(a3)
	chain = append(chain, b3)

	for i := 1; i < len(chain); i++ {
		if !chain[i].After(chain[i-1]) {
			t.Fatalf("causal ordering broken at step %d: %v not after %v",
				i, chain[i], chain[i-1])
		}
	}
}

// --- Clock Skew Scenarios ---

func TestClockSkew_LargeForwardJump(t *testing.T) {
	phys := NewManualClock(time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC))
	c := NewClock(phys)

	for i := 0; i < 10; i++ {
		c.Now()
	}

	phys.Advance(time.Hour)
	ts := c.Now()

	expectedWall := uint64(phys.Now().UnixMilli())
	if ts.WallTime != expectedWall {
		t.Fatalf("should adopt new physical time: got %d, want %d",
			ts.WallTime, expectedWall)
	}
	if ts.Counter != 0 {
		t.Fatalf("counter should reset after large jump, got %d", ts.Counter)
	}
}

func TestClockSkew_LargeBackwardJump(t *testing.T) {
	phys := NewManualClock(time.Date(2026, 1, 1, 13, 0, 0, 0, time.UTC))
	c := NewClock(phys)

	before := c.Now()
	phys.Rewind(time.Hour)
	after := c.Now()

	if !after.After(before) {
		t.Fatalf("went backward: before=%v after=%v", before, after)
	}
	if after.WallTime != before.WallTime {
		t.Fatalf("wall time should be preserved: got %d, want %d",
			after.WallTime, before.WallTime)
	}
}

func TestClockSkew_ReceiveFromFarFuture(t *testing.T) {
	phys := NewManualClock(time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC))
	c := NewClock(phys)

	local := c.Now()
	futureRemote := Timestamp{WallTime: local.WallTime + 10_000, Counter: 0}

	result := c.Receive(futureRemote)
	if !result.After(futureRemote) {
		t.Fatalf("must be > remote: result=%v remote=%v", result, futureRemote)
	}

	// Subsequent calls must still be monotonic.
	next := c.Now()
	if !next.After(result) {
		t.Fatalf("Now() after Receive should still advance: next=%v result=%v",
			next, result)
	}
}

// --- Concurrency ---

func TestConcurrency_ParallelNow(t *testing.T) {
	phys := NewManualClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	c := NewClock(phys)

	const goroutines = 50
	const opsPerGoroutine = 100

	results := make([][]Timestamp, goroutines)
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for g := 0; g < goroutines; g++ {
		g := g
		results[g] = make([]Timestamp, opsPerGoroutine)
		go func() {
			defer wg.Done()
			for i := 0; i < opsPerGoroutine; i++ {
				results[g][i] = c.Now()
			}
		}()
	}

	wg.Wait()

	// All timestamps must be unique.
	seen := make(map[Timestamp]bool)
	for g := 0; g < goroutines; g++ {
		for i := 0; i < opsPerGoroutine; i++ {
			ts := results[g][i]
			if seen[ts] {
				t.Fatalf("duplicate timestamp from concurrent Now: %v", ts)
			}
			seen[ts] = true
		}
	}

	// Per-goroutine sequences must be monotonic.
	for g := 0; g < goroutines; g++ {
		for i := 1; i < opsPerGoroutine; i++ {
			if !results[g][i].After(results[g][i-1]) {
				t.Fatalf("goroutine %d: non-monotonic at index %d: %v not after %v",
					g, i, results[g][i], results[g][i-1])
			}
		}
	}
}

func TestConcurrency_MixedOperations(t *testing.T) {
	phys := NewManualClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	c := NewClock(phys)

	const goroutines = 20
	const opsPerGoroutine = 50

	var wg sync.WaitGroup
	wg.Add(goroutines * 3)

	collector := struct {
		mu         sync.Mutex
		timestamps []Timestamp
	}{}

	collect := func(ts Timestamp) {
		collector.mu.Lock()
		collector.timestamps = append(collector.timestamps, ts)
		collector.mu.Unlock()
	}

	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < opsPerGoroutine; i++ {
				collect(c.Now())
			}
		}()
	}

	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < opsPerGoroutine; i++ {
				collect(c.Send())
			}
		}()
	}

	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < opsPerGoroutine; i++ {
				remote := Timestamp{
					WallTime: uint64(phys.Now().UnixMilli()),
					Counter:  uint32(i),
				}
				collect(c.Receive(remote))
			}
		}()
	}

	wg.Wait()

	seen := make(map[Timestamp]bool)
	for _, ts := range collector.timestamps {
		if seen[ts] {
			t.Fatalf("duplicate timestamp from concurrent mixed ops: %v", ts)
		}
		seen[ts] = true
	}
}

// --- Edge Cases ---

func TestReceive_ZeroRemoteTimestamp(t *testing.T) {
	phys := NewManualClock(time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC))
	c := NewClock(phys)

	before := c.Now()
	result := c.Receive(Timestamp{})
	if !result.After(before) {
		t.Fatalf("Receive(zero) should advance: result=%v before=%v", result, before)
	}
}

func TestReceive_IdenticalRemoteTimestamp(t *testing.T) {
	phys := NewManualClock(time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC))
	c := NewClock(phys)

	local := c.Now()
	result := c.Receive(local)
	if !result.After(local) {
		t.Fatalf("Receive(self) should advance: result=%v local=%v", result, local)
	}
}

func TestTimestamp_String(t *testing.T) {
	ts := Timestamp{WallTime: 12345, Counter: 7}
	s := ts.String()
	if s != "{wall:12345 counter:7}" {
		t.Fatalf("unexpected String output: %s", s)
	}
}

// --- Multi-Node Scenario ---

func TestScenario_ThreeNodeConvergence(t *testing.T) {
	// Three nodes with different clock skews exchange messages.
	base := time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC)

	physA := NewManualClock(base)                              // on time
	physB := NewManualClock(base.Add(500 * time.Millisecond))  // 500ms ahead
	physC := NewManualClock(base.Add(-300 * time.Millisecond)) // 300ms behind

	a := NewClock(physA)
	b := NewClock(physB)
	c := NewClock(physC)

	a1 := a.Now()
	a2 := a.Now()

	a3 := a.Send()
	b1 := b.Receive(a3)

	b2 := b.Now()
	b3 := b.Send()
	c1 := c.Receive(b3)

	c2 := c.Send()
	a4 := a.Receive(c2)

	// Within-node subsequences must be ordered.
	for _, seq := range [][]Timestamp{
		{a1, a2, a3, a4},
		{b1, b2, b3},
		{c1, c2},
	} {
		for i := 1; i < len(seq); i++ {
			if !seq[i].After(seq[i-1]) {
				t.Fatalf("within-node ordering violated: %v not after %v",
					seq[i], seq[i-1])
			}
		}
	}

	// Cross-node causal pairs: send < receive.
	for _, pair := range [][2]Timestamp{
		{a3, b1},
		{b3, c1},
		{c2, a4},
	} {
		if !pair[1].After(pair[0]) {
			t.Fatalf("cross-node causality violated: %v not after %v",
				pair[1], pair[0])
		}
	}
}

func TestClock_LastAndSeed(t *testing.T) {
	phys := NewManualClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	c := NewClock(phys)

	if got := c.Last(); got != (Timestamp{}) {
		t.Fatalf("expected zero timestamp initially, got %v", got)
	}

	ts1 := c.Now()
	if got := c.Last(); got != ts1 {
		t.Fatalf("expected Last to return %v, got %v", ts1, got)
	}

	// Seed with an older timestamp should be a no-op
	older := Timestamp{WallTime: ts1.WallTime - 1000, Counter: 0}
	c.Seed(older)
	if got := c.Last(); got != ts1 {
		t.Fatalf("expected Seed with older timestamp to be no-op, got %v", got)
	}

	// Seed with a newer timestamp should advance Last
	future := Timestamp{WallTime: ts1.WallTime + 5000, Counter: 42}
	c.Seed(future)
	if got := c.Last(); got != future {
		t.Fatalf("expected Last to match seeded future timestamp, got %v", got)
	}

	// Next Now() should advance beyond the seeded timestamp even if physical clock is behind
	next := c.Now()
	if !next.After(future) {
		t.Fatalf("expected next timestamp %v to be after seeded future timestamp %v", next, future)
	}
}
