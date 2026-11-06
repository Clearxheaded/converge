package crdt

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/Clearxheaded/converge/internal/hlc"
)

// manualClock provides a deterministic time source for CRDT tests.
type manualClock struct {
	mu  sync.Mutex
	now time.Time
}

func newManualClock(t time.Time) *manualClock {
	return &manualClock{now: t}
}

func (c *manualClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *manualClock) Set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = t
}

func (c *manualClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func baseTime() time.Time {
	return time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
}

// --- Total Ordering & Tie-Breaking ---

func TestCompareRecords_TimestampOrdering(t *testing.T) {
	tests := []struct {
		name     string
		a        Record
		b        Record
		expected int
	}{
		{
			name:     "higher wall time wins",
			a:        Record{Key: "k", Value: "v1", Timestamp: hlc.Timestamp{WallTime: 200, Counter: 0}, ReplicaID: "node-1"},
			b:        Record{Key: "k", Value: "v2", Timestamp: hlc.Timestamp{WallTime: 100, Counter: 0}, ReplicaID: "node-2"},
			expected: 1,
		},
		{
			name:     "lower wall time loses",
			a:        Record{Key: "k", Value: "v1", Timestamp: hlc.Timestamp{WallTime: 100, Counter: 0}, ReplicaID: "node-2"},
			b:        Record{Key: "k", Value: "v2", Timestamp: hlc.Timestamp{WallTime: 200, Counter: 0}, ReplicaID: "node-1"},
			expected: -1,
		},
		{
			name:     "same wall time, higher counter wins",
			a:        Record{Key: "k", Value: "v1", Timestamp: hlc.Timestamp{WallTime: 100, Counter: 5}, ReplicaID: "node-1"},
			b:        Record{Key: "k", Value: "v2", Timestamp: hlc.Timestamp{WallTime: 100, Counter: 2}, ReplicaID: "node-2"},
			expected: 1,
		},
		{
			name:     "same wall time, lower counter loses",
			a:        Record{Key: "k", Value: "v1", Timestamp: hlc.Timestamp{WallTime: 100, Counter: 2}, ReplicaID: "node-1"},
			b:        Record{Key: "k", Value: "v2", Timestamp: hlc.Timestamp{WallTime: 100, Counter: 5}, ReplicaID: "node-2"},
			expected: -1,
		},
		{
			name:     "same timestamp, higher replica ID wins",
			a:        Record{Key: "k", Value: "v1", Timestamp: hlc.Timestamp{WallTime: 100, Counter: 1}, ReplicaID: "node-b"},
			b:        Record{Key: "k", Value: "v2", Timestamp: hlc.Timestamp{WallTime: 100, Counter: 1}, ReplicaID: "node-a"},
			expected: 1,
		},
		{
			name:     "same timestamp, lower replica ID loses",
			a:        Record{Key: "k", Value: "v1", Timestamp: hlc.Timestamp{WallTime: 100, Counter: 1}, ReplicaID: "node-a"},
			b:        Record{Key: "k", Value: "v2", Timestamp: hlc.Timestamp{WallTime: 100, Counter: 1}, ReplicaID: "node-b"},
			expected: -1,
		},
		{
			name:     "same timestamp and replica ID, delete wins over set",
			a:        Record{Key: "k", Value: "", Timestamp: hlc.Timestamp{WallTime: 100, Counter: 1}, ReplicaID: "node-a", Deleted: true},
			b:        Record{Key: "k", Value: "v2", Timestamp: hlc.Timestamp{WallTime: 100, Counter: 1}, ReplicaID: "node-a", Deleted: false},
			expected: 1,
		},
		{
			name:     "same timestamp and replica ID, set loses to delete",
			a:        Record{Key: "k", Value: "v1", Timestamp: hlc.Timestamp{WallTime: 100, Counter: 1}, ReplicaID: "node-a", Deleted: false},
			b:        Record{Key: "k", Value: "", Timestamp: hlc.Timestamp{WallTime: 100, Counter: 1}, ReplicaID: "node-a", Deleted: true},
			expected: -1,
		},
		{
			name:     "identical records tie",
			a:        Record{Key: "k", Value: "v1", Timestamp: hlc.Timestamp{WallTime: 100, Counter: 1}, ReplicaID: "node-a", Deleted: false},
			b:        Record{Key: "k", Value: "v1", Timestamp: hlc.Timestamp{WallTime: 100, Counter: 1}, ReplicaID: "node-a", Deleted: false},
			expected: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := CompareRecords(tt.a, tt.b)
			if got != tt.expected {
				t.Fatalf("CompareRecords got %d, expected %d", got, tt.expected)
			}
			// Strict anti-symmetry check
			sym := CompareRecords(tt.b, tt.a)
			if sym != -tt.expected {
				t.Fatalf("Anti-symmetry violation: CompareRecords(b, a) = %d, expected %d", sym, -tt.expected)
			}
		})
	}
}

func TestCompareRecords_Transitivity(t *testing.T) {
	// a > b and b > c implies a > c
	a := Record{Key: "k", Value: "v3", Timestamp: hlc.Timestamp{WallTime: 300, Counter: 0}, ReplicaID: "node-1"}
	b := Record{Key: "k", Value: "v2", Timestamp: hlc.Timestamp{WallTime: 200, Counter: 0}, ReplicaID: "node-2"}
	c := Record{Key: "k", Value: "v1", Timestamp: hlc.Timestamp{WallTime: 100, Counter: 0}, ReplicaID: "node-3"}

	if CompareRecords(a, b) <= 0 {
		t.Fatalf("expected a > b")
	}
	if CompareRecords(b, c) <= 0 {
		t.Fatalf("expected b > c")
	}
	if CompareRecords(a, c) <= 0 {
		t.Fatalf("transitivity failed: expected a > c")
	}
}

// --- Basic Operations ---

func TestNewLWWMap_Validations(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected panic on empty replicaID")
		}
	}()
	NewLWWMap("", nil)
}

func TestLWWMap_BasicSetGet(t *testing.T) {
	clock := hlc.NewClock(newManualClock(baseTime()))
	m := NewLWWMap("node-1", clock)

	if m.ReplicaID() != "node-1" {
		t.Fatalf("expected replicaID 'node-1', got %s", m.ReplicaID())
	}
	if m.Clock() != clock {
		t.Fatalf("expected clock pointer match")
	}
	if m.Len() != 0 {
		t.Fatalf("expected initial Len 0, got %d", m.Len())
	}

	// Non-existent key
	val, ok := m.Get("name")
	if ok || val != "" {
		t.Fatalf("expected non-existent key to return ('', false)")
	}
	if m.Has("name") {
		t.Fatalf("expected Has('name') to be false")
	}

	// Insert key
	rec := m.Set("name", "Alice")
	if rec.Key != "name" || rec.Value != "Alice" || rec.Deleted {
		t.Fatalf("unexpected record returned by Set: %+v", rec)
	}
	if m.Len() != 1 {
		t.Fatalf("expected Len 1, got %d", m.Len())
	}

	val, ok = m.Get("name")
	if !ok || val != "Alice" {
		t.Fatalf("expected ('Alice', true), got ('%s', %v)", val, ok)
	}
	if !m.Has("name") {
		t.Fatalf("expected Has('name') to be true")
	}

	// Update existing key
	rec2 := m.Set("name", "Bob")
	if rec2.Value != "Bob" {
		t.Fatalf("expected value 'Bob', got %s", rec2.Value)
	}
	if !rec2.Timestamp.After(rec.Timestamp) {
		t.Fatalf("expected new timestamp %v to be after %v", rec2.Timestamp, rec.Timestamp)
	}
	val, ok = m.Get("name")
	if !ok || val != "Bob" {
		t.Fatalf("expected ('Bob', true), got ('%s', %v)", val, ok)
	}
}

func TestLWWMap_KeysAndValues(t *testing.T) {
	clock := hlc.NewClock(newManualClock(baseTime()))
	m := NewLWWMap("node-1", clock)

	m.Set("zebra", "1")
	m.Set("apple", "2")
	m.Set("mango", "3")

	keys := m.Keys()
	expectedKeys := []string{"apple", "mango", "zebra"}
	if !reflect.DeepEqual(keys, expectedKeys) {
		t.Fatalf("Keys sorted mismatch: got %v, expected %v", keys, expectedKeys)
	}

	values := m.Values()
	expectedValues := map[string]string{
		"zebra": "1",
		"apple": "2",
		"mango": "3",
	}
	if !reflect.DeepEqual(values, expectedValues) {
		t.Fatalf("Values mismatch: got %v, expected %v", values, expectedValues)
	}
}

// --- Tombstone Handling and Deletion ---

func TestLWWMap_DeleteExistingKey(t *testing.T) {
	clock := hlc.NewClock(newManualClock(baseTime()))
	m := NewLWWMap("node-1", clock)

	setRec := m.Set("key1", "val1")
	delRec, wasActive := m.Delete("key1")

	if !wasActive {
		t.Fatalf("expected wasActive true when deleting active key")
	}
	if !delRec.Deleted {
		t.Fatalf("expected Deleted to be true on tombstone record")
	}
	if delRec.Value != "" {
		t.Fatalf("expected empty value on tombstone record, got %s", delRec.Value)
	}
	if !delRec.Timestamp.After(setRec.Timestamp) {
		t.Fatalf("expected tombstone timestamp %v to be after set timestamp %v", delRec.Timestamp, setRec.Timestamp)
	}

	// Active inspection
	if m.Len() != 0 {
		t.Fatalf("expected active Len 0, got %d", m.Len())
	}
	if m.Has("key1") {
		t.Fatalf("expected Has('key1') to be false after deletion")
	}
	val, ok := m.Get("key1")
	if ok || val != "" {
		t.Fatalf("expected Get('key1') to return ('', false)")
	}

	// Internal inspection
	if m.TombstoneCount() != 1 {
		t.Fatalf("expected TombstoneCount 1, got %d", m.TombstoneCount())
	}
	if m.RecordCount() != 1 {
		t.Fatalf("expected RecordCount 1, got %d", m.RecordCount())
	}

	rec, exists := m.GetRecord("key1")
	if !exists || !rec.Deleted {
		t.Fatalf("expected raw record with Deleted=true")
	}
}

func TestLWWMap_DeleteNonExistentKey(t *testing.T) {
	// In distributed systems, deleting a key not yet observed locally must still
	// record a tombstone to prevent resurrection when older remote writes arrive.
	clock := hlc.NewClock(newManualClock(baseTime()))
	m := NewLWWMap("node-1", clock)

	delRec, wasActive := m.Delete("ghost")
	if wasActive {
		t.Fatalf("expected wasActive false for non-existent key")
	}
	if !delRec.Deleted {
		t.Fatalf("expected Deleted to be true")
	}
	if m.TombstoneCount() != 1 {
		t.Fatalf("expected TombstoneCount 1, got %d", m.TombstoneCount())
	}
	if m.Len() != 0 {
		t.Fatalf("expected Len 0, got %d", m.Len())
	}
}

func TestLWWMap_ResurrectionAfterDeletion(t *testing.T) {
	// A subsequent Set with a higher timestamp should overwrite the tombstone.
	clock := hlc.NewClock(newManualClock(baseTime()))
	m := NewLWWMap("node-1", clock)

	m.Set("k", "v1")
	m.Delete("k")
	if m.Len() != 0 {
		t.Fatalf("expected Len 0 after delete")
	}

	m.Set("k", "v2")
	if m.Len() != 1 {
		t.Fatalf("expected Len 1 after resurrection")
	}
	val, ok := m.Get("k")
	if !ok || val != "v2" {
		t.Fatalf("expected resurrected value 'v2', got '%s'", val)
	}
	if m.TombstoneCount() != 0 {
		t.Fatalf("expected 0 tombstones after resurrection, got %d", m.TombstoneCount())
	}
}

// --- CRDT Mathematical Properties ---

func TestCRDT_Commutativity(t *testing.T) {
	// Commutativity: Merge(A, B) produces the exact same state as Merge(B, A).
	physA := newManualClock(baseTime())
	clockA := hlc.NewClock(physA)
	nodeA := NewLWWMap("node-a", clockA)

	physB := newManualClock(baseTime().Add(10 * time.Millisecond))
	clockB := hlc.NewClock(physB)
	nodeB := NewLWWMap("node-b", clockB)

	// Divergent writes
	nodeA.Set("common", "valA")
	nodeA.Set("onlyA", "hello")
	nodeB.Set("common", "valB")
	nodeB.Set("onlyB", "world")

	// Branch 1: Clone A, merge B into A
	branch1 := nodeA.Clone()
	branch1.Merge(nodeB)

	// Branch 2: Clone B, merge A into B
	branch2 := nodeB.Clone()
	branch2.Merge(nodeA)

	// States must be identical
	if !branch1.EqualState(branch2) {
		t.Fatalf("Commutativity violation: branch1 != branch2\nBranch 1: %+v\nBranch 2: %+v",
			branch1.Records(), branch2.Records())
	}
}

func TestCRDT_Associativity(t *testing.T) {
	// Associativity: (A ⊔ B) ⊔ C == A ⊔ (B ⊔ C)
	physA := newManualClock(baseTime())
	nodeA := NewLWWMap("node-a", hlc.NewClock(physA))

	physB := newManualClock(baseTime().Add(5 * time.Millisecond))
	nodeB := NewLWWMap("node-b", hlc.NewClock(physB))

	physC := newManualClock(baseTime().Add(10 * time.Millisecond))
	nodeC := NewLWWMap("node-c", hlc.NewClock(physC))

	// Replicas perform distinct and overlapping operations
	nodeA.Set("shared", "valA")
	nodeA.Set("keyA", "1")

	nodeB.Set("shared", "valB")
	nodeB.Delete("shared") // B deletes shared
	nodeB.Set("keyB", "2")

	nodeC.Set("shared", "valC") // C writes shared with later timestamp
	nodeC.Set("keyC", "3")

	// Grouping 1: (A ⊔ B) ⊔ C
	left := nodeA.Clone()
	left.Merge(nodeB)
	left.Merge(nodeC)

	// Grouping 2: A ⊔ (B ⊔ C)
	rightInner := nodeB.Clone()
	rightInner.Merge(nodeC)
	right := nodeA.Clone()
	right.Merge(rightInner)

	if !left.EqualState(right) {
		t.Fatalf("Associativity violation: (A ⊔ B) ⊔ C != A ⊔ (B ⊔ C)\nLeft: %+v\nRight: %+v",
			left.Records(), right.Records())
	}

	// All 6 permutations of merging [A, B, C] must yield identical final states
	replicas := []*LWWMap{nodeA, nodeB, nodeC}
	perms := [][]int{
		{0, 1, 2},
		{0, 2, 1},
		{1, 0, 2},
		{1, 2, 0},
		{2, 0, 1},
		{2, 1, 0},
	}

	var canonical *LWWMap
	for i, p := range perms {
		m := NewLWWMap(fmt.Sprintf("perm-%d", i), nil)
		m.Merge(replicas[p[0]])
		m.Merge(replicas[p[1]])
		m.Merge(replicas[p[2]])

		if canonical == nil {
			canonical = m
		} else if !canonical.EqualState(m) {
			t.Fatalf("Permutation %v produced different state than canonical permutation", p)
		}
	}
}

func TestCRDT_Idempotence(t *testing.T) {
	// Idempotence: A ⊔ A == A, and repeated merges do not change state.
	clock := hlc.NewClock(newManualClock(baseTime()))
	nodeA := NewLWWMap("node-a", clock)
	nodeA.Set("k1", "v1")
	nodeA.Set("k2", "v2")
	nodeA.Delete("k1")

	snapshot := nodeA.Clone()

	// Merge with self
	updated := nodeA.Merge(nodeA)
	if updated != 0 {
		t.Fatalf("expected 0 updates on self merge, got %d", updated)
	}
	if !nodeA.EqualState(snapshot) {
		t.Fatalf("state changed after self merge")
	}

	// Repeated merge of another map
	nodeB := NewLWWMap("node-b", hlc.NewClock(newManualClock(baseTime())))
	nodeB.Set("k3", "v3")

	firstUpdate := nodeA.Merge(nodeB)
	if firstUpdate != 1 {
		t.Fatalf("expected 1 update on first merge, got %d", firstUpdate)
	}
	secondUpdate := nodeA.Merge(nodeB)
	if secondUpdate != 0 {
		t.Fatalf("expected 0 updates on second identical merge, got %d", secondUpdate)
	}
	thirdUpdate := nodeA.Merge(nodeB)
	if thirdUpdate != 0 {
		t.Fatalf("expected 0 updates on third identical merge, got %d", thirdUpdate)
	}
}

// --- Deterministic Conflict Resolution (Tie-Breaking) ---

func TestCRDT_DeterministicTieBreaking(t *testing.T) {
	// When two replicas perform concurrent writes with identical physical timestamps
	// and identical logical counters, the replica ID tie-breaker must guarantee that
	// all replicas converge to the exact same winner.
	fixedTime := baseTime()
	clockA := hlc.NewClock(newManualClock(fixedTime))
	clockB := hlc.NewClock(newManualClock(fixedTime))

	nodeA := NewLWWMap("node-alpha", clockA)
	nodeB := NewLWWMap("node-beta", clockB)

	// Both write at exact same wall time and counter 0
	nodeA.Set("key", "value-from-alpha")
	nodeB.Set("key", "value-from-beta")

	recA, _ := nodeA.GetRecord("key")
	recB, _ := nodeB.GetRecord("key")

	if !recA.Timestamp.Equal(recB.Timestamp) {
		t.Fatalf("test prerequisite failed: timestamps must be equal, got %v and %v", recA.Timestamp, recB.Timestamp)
	}

	// node-beta > node-alpha lexicographically, so node-beta must win on both nodes
	nodeA.Merge(nodeB)
	nodeB.Merge(nodeA)

	if !nodeA.EqualState(nodeB) {
		t.Fatalf("nodes failed to converge under tie breaking")
	}

	valA, _ := nodeA.Get("key")
	valB, _ := nodeB.Get("key")
	if valA != "value-from-beta" || valB != "value-from-beta" {
		t.Fatalf("expected 'value-from-beta' to win tie, got valA='%s', valB='%s'", valA, valB)
	}
}

// --- Tombstone vs Older Write Convergence ---

func TestCRDT_TombstonePrecedenceOverOlderWrite(t *testing.T) {
	// If Replica A writes a key at t=100, and Replica B deletes that key at t=200,
	// merging A into B should NOT restore the key.
	// Merging B into A must delete the key on A.
	physA := newManualClock(baseTime())
	nodeA := NewLWWMap("node-a", hlc.NewClock(physA))

	physB := newManualClock(baseTime().Add(100 * time.Millisecond))
	nodeB := NewLWWMap("node-b", hlc.NewClock(physB))

	nodeA.Set("item", "created-at-100")
	// B learns about it, then deletes it at t=200
	nodeB.Merge(nodeA)
	nodeB.Delete("item")

	// Now A (still at t=100) merges B's delete
	nodeA.Merge(nodeB)

	valA, okA := nodeA.Get("item")
	if okA || valA != "" {
		t.Fatalf("nodeA failed to apply remote delete: got val='%s', ok=%v", valA, okA)
	}
	if nodeA.TombstoneCount() != 1 {
		t.Fatalf("expected tombstone on nodeA")
	}
	if !nodeA.EqualState(nodeB) {
		t.Fatalf("nodeA and nodeB diverged on tombstone precedence")
	}
}

func TestCRDT_StaleWriteDoesNotOverturnTombstone(t *testing.T) {
	// Replica A is offline and wrote "stale" at t=100.
	// Replica B already deleted "stale" at t=200.
	// When A re-connects and attempts to send its t=100 write to B, B rejects it.
	physA := newManualClock(baseTime())
	nodeA := NewLWWMap("node-a", hlc.NewClock(physA))
	nodeA.Set("x", "stale-value")

	physB := newManualClock(baseTime().Add(100 * time.Millisecond))
	nodeB := NewLWWMap("node-b", hlc.NewClock(physB))
	nodeB.Delete("x")

	updated := nodeB.Merge(nodeA)
	if updated != 0 {
		t.Fatalf("stale write overturned tombstone; updated=%d", updated)
	}

	val, ok := nodeB.Get("x")
	if ok || val != "" {
		t.Fatalf("resurrected stale write: got '%s'", val)
	}
}

// --- HLC Causal Ordering Integration ---

func TestCRDT_HLCClockCausality(t *testing.T) {
	// Node A physical clock is in the future (skewed ahead: +500ms).
	// Node B physical clock is at normal time (0ms).
	// 1. Node A writes "doc" = "v1" with WallTime = 500.
	// 2. Node B merges from Node A.
	// 3. Node B modifies "doc" = "v2".
	// Because Node B called Receive during Merge, Node B's HLC clock advances
	// past 500ms (to counter+1).
	// Therefore, Node B's subsequent write beats Node A's earlier write,
	// preserving causality despite Node B's lagged physical clock.
	physA := newManualClock(baseTime().Add(500 * time.Millisecond))
	nodeA := NewLWWMap("node-a", hlc.NewClock(physA))

	physB := newManualClock(baseTime())
	nodeB := NewLWWMap("node-b", hlc.NewClock(physB))

	// Node A writes
	nodeA.Set("doc", "v1")

	// Node B receives Node A's state
	nodeB.Merge(nodeA)

	// Node B now causally edits "doc"
	recB := nodeB.Set("doc", "v2")

	recA, _ := nodeA.GetRecord("doc")
	if !recB.Timestamp.After(recA.Timestamp) {
		t.Fatalf("causality broken by clock skew: recB %v not after recA %v", recB.Timestamp, recA.Timestamp)
	}

	// Node A receives Node B's edit
	nodeA.Merge(nodeB)

	valA, _ := nodeA.Get("doc")
	valB, _ := nodeB.Get("doc")
	if valA != "v2" || valB != "v2" {
		t.Fatalf("expected 'v2' on both nodes after causal edit, got valA='%s', valB='%s'", valA, valB)
	}
}

// --- Multi-Replica Convergence ---

func TestCRDT_MultiReplicaConvergence(t *testing.T) {
	// 4 replicas perform overlapping mutations across 10 keys.
	// Synchronize them via random pairwise gossip until all have merged each other.
	numReplicas := 4
	replicas := make([]*LWWMap, numReplicas)
	for i := 0; i < numReplicas; i++ {
		phys := newManualClock(baseTime().Add(time.Duration(i*10) * time.Millisecond))
		replicas[i] = NewLWWMap(fmt.Sprintf("replica-%d", i), hlc.NewClock(phys))
	}

	// Deterministic seed for reproducible test run
	rng := rand.New(rand.NewSource(42))

	keys := []string{"k0", "k1", "k2", "k3", "k4", "k5", "k6", "k7", "k8", "k9"}

	// Replicas perform 100 random local operations (sets and deletes)
	for op := 0; op < 100; op++ {
		repIdx := rng.Intn(numReplicas)
		k := keys[rng.Intn(len(keys))]
		if rng.Float64() < 0.25 {
			replicas[repIdx].Delete(k)
		} else {
			replicas[repIdx].Set(k, fmt.Sprintf("val-%d-%d", repIdx, op))
		}
	}

	// Network synchronization rounds: pair-wise gossip
	for round := 0; round < 20; round++ {
		for i := 0; i < numReplicas; i++ {
			j := (i + 1) % numReplicas
			replicas[j].Merge(replicas[i])
		}
	}

	// Verify all replicas converged to identical state
	canonical := replicas[0]
	for i := 1; i < numReplicas; i++ {
		if !canonical.EqualState(replicas[i]) {
			t.Fatalf("Replica %d did not converge with Replica 0\nRep 0: %+v\nRep %d: %+v",
				i, canonical.Records(), i, replicas[i].Records())
		}
	}
}

// --- Concurrent Thread Safety ---

func TestCRDT_ConcurrentAccess(t *testing.T) {
	clock := hlc.NewClock(hlc.WallClock{})
	m := NewLWWMap("node-concurrent", clock)

	var wg sync.WaitGroup
	workers := 16
	opsPerWorker := 100

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for i := 0; i < opsPerWorker; i++ {
				key := fmt.Sprintf("key-%d", (workerID*10+i)%20)
				switch i % 5 {
				case 0, 1:
					m.Set(key, fmt.Sprintf("w%d-v%d", workerID, i))
				case 2:
					m.Delete(key)
				case 3:
					m.Get(key)
					m.Has(key)
				case 4:
					m.Keys()
					m.Values()
					m.AllRecords()
				}
			}
		}(w)
	}

	wg.Wait()
}

func TestCRDT_ConcurrentMergeDeadlockFreedom(t *testing.T) {
	// Two goroutines continuously merging A->B and B->A while writes occur.
	// Must not deadlock.
	clockA := hlc.NewClock(hlc.WallClock{})
	clockB := hlc.NewClock(hlc.WallClock{})
	nodeA := NewLWWMap("node-a", clockA)
	nodeB := NewLWWMap("node-b", clockB)

	var wg sync.WaitGroup
	iterations := 200

	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			nodeA.Set(fmt.Sprintf("ka-%d", i%10), fmt.Sprintf("val-%d", i))
			nodeA.Merge(nodeB)
		}
	}()

	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			nodeB.Set(fmt.Sprintf("kb-%d", i%10), fmt.Sprintf("val-%d", i))
			nodeB.Merge(nodeA)
		}
	}()

	wg.Wait()
}

// --- Snapshot and Clone Isolation ---

func TestLWWMap_CloneIsolation(t *testing.T) {
	clock := hlc.NewClock(newManualClock(baseTime()))
	m := NewLWWMap("node-1", clock)
	m.Set("orig", "v1")

	clone := m.Clone()

	// Modifying m must not alter clone
	m.Set("orig", "v2")
	m.Set("newKey", "v3")

	valClone, ok := clone.Get("orig")
	if !ok || valClone != "v1" {
		t.Fatalf("expected clone to retain 'v1', got '%s'", valClone)
	}
	if clone.Has("newKey") {
		t.Fatalf("clone saw newKey written to original map")
	}
}

// --- JSON Serialization ---

func TestRecord_JSONSerialization(t *testing.T) {
	rec := Record{
		Key:       "user:100",
		Value:     "{\"name\":\"Carol\"}",
		Timestamp: hlc.Timestamp{WallTime: 1735732800000, Counter: 4},
		ReplicaID: "node-xyz",
		Deleted:   false,
	}

	bytes, err := json.Marshal(rec)
	if err != nil {
		t.Fatalf("failed to marshal Record: %v", err)
	}

	var decoded Record
	if err := json.Unmarshal(bytes, &decoded); err != nil {
		t.Fatalf("failed to unmarshal Record: %v", err)
	}

	if decoded != rec {
		t.Fatalf("decoded record mismatch: got %+v, expected %+v", decoded, rec)
	}
}

func TestCRDT_SetRecordApplyRecordAndLoadRecords(t *testing.T) {
	m := NewLWWMap("node-1", nil)

	rec1 := Record{
		Key:       "k1",
		Value:     "v1",
		Timestamp: hlc.Timestamp{WallTime: 100, Counter: 0},
		ReplicaID: "node-1",
	}
	m.SetRecord(rec1)

	got, ok := m.Get("k1")
	if !ok || got != "v1" {
		t.Fatalf("expected v1, got %v (ok=%v)", got, ok)
	}

	// Apply older record should fail
	older := Record{
		Key:       "k1",
		Value:     "v0",
		Timestamp: hlc.Timestamp{WallTime: 50, Counter: 0},
		ReplicaID: "node-2",
	}
	if m.ApplyRecord(older) {
		t.Fatalf("older record should not have applied")
	}

	// Apply newer record should succeed
	newer := Record{
		Key:       "k1",
		Value:     "v2",
		Timestamp: hlc.Timestamp{WallTime: 200, Counter: 0},
		ReplicaID: "node-2",
	}
	if !m.ApplyRecord(newer) {
		t.Fatalf("newer record should have applied")
	}
	if val, _ := m.Get("k1"); val != "v2" {
		t.Fatalf("expected v2, got %v", val)
	}

	// LoadRecords batch load
	m2 := NewLWWMap("node-2", nil)
	batch := []Record{
		{Key: "a", Value: "val-a", Timestamp: hlc.Timestamp{WallTime: 100, Counter: 0}, ReplicaID: "node-1"},
		{Key: "b", Value: "val-b", Timestamp: hlc.Timestamp{WallTime: 100, Counter: 0}, ReplicaID: "node-1"},
		{Key: "a", Value: "val-a-newer", Timestamp: hlc.Timestamp{WallTime: 150, Counter: 0}, ReplicaID: "node-2"},
	}
	m2.LoadRecords(batch)

	if m2.Len() != 2 {
		t.Fatalf("expected 2 active records, got %d", m2.Len())
	}
	if val, _ := m2.Get("a"); val != "val-a-newer" {
		t.Fatalf("expected val-a-newer, got %s", val)
	}
	if val, _ := m2.Get("b"); val != "val-b" {
		t.Fatalf("expected val-b, got %s", val)
	}
}

func TestCRDT_PruneTombstones(t *testing.T) {
	m := NewLWWMap("node-1", nil)

	// Add 2 active records and 2 tombstones
	m.ApplyRecord(Record{Key: "active1", Value: "v1", Timestamp: hlc.Timestamp{WallTime: 50, Counter: 0}, ReplicaID: "node-1", Deleted: false})
	m.ApplyRecord(Record{Key: "tomb1", Value: "", Timestamp: hlc.Timestamp{WallTime: 100, Counter: 0}, ReplicaID: "node-1", Deleted: true})
	m.ApplyRecord(Record{Key: "tomb2", Value: "", Timestamp: hlc.Timestamp{WallTime: 200, Counter: 0}, ReplicaID: "node-1", Deleted: true})
	m.ApplyRecord(Record{Key: "active2", Value: "v2", Timestamp: hlc.Timestamp{WallTime: 250, Counter: 0}, ReplicaID: "node-1", Deleted: false})

	if m.TombstoneCount() != 2 {
		t.Fatalf("expected 2 tombstones, got %d", m.TombstoneCount())
	}
	if m.RecordCount() != 4 {
		t.Fatalf("expected 4 total records, got %d", m.RecordCount())
	}

	// Prune with cutoff 100: should only prune tomb1
	pruned := m.PruneTombstones(hlc.Timestamp{WallTime: 100, Counter: 0})
	if pruned != 1 {
		t.Fatalf("expected 1 pruned tombstone, got %d", pruned)
	}
	if m.TombstoneCount() != 1 {
		t.Fatalf("expected 1 remaining tombstone, got %d", m.TombstoneCount())
	}
	if m.Len() != 2 {
		t.Fatalf("expected active records to remain intact (len 2), got %d", m.Len())
	}

	// Active record with older timestamp should never be pruned
	if val, exists := m.Get("active1"); !exists || val != "v1" {
		t.Fatalf("expected active1 to remain intact, got exists=%v val=%v", exists, val)
	}

	// Prune with cutoff 300: should prune tomb2
	pruned2 := m.PruneTombstones(hlc.Timestamp{WallTime: 300, Counter: 0})
	if pruned2 != 1 {
		t.Fatalf("expected 1 pruned tombstone, got %d", pruned2)
	}
	if m.TombstoneCount() != 0 {
		t.Fatalf("expected 0 tombstones, got %d", m.TombstoneCount())
	}
	if m.Len() != 2 {
		t.Fatalf("expected active records to remain intact (len 2), got %d", m.Len())
	}
}
