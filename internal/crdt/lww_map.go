package crdt

import (
	"sort"
	"sync"

	"github.com/Clearxheaded/converge/internal/hlc"
)

// Record represents a single register entry in the LWW map,
// including its value, HLC timestamp, replica identifier, and deletion status.
type Record struct {
	Key       string        `json:"key"`
	Value     string        `json:"value"`
	Timestamp hlc.Timestamp `json:"timestamp"`
	ReplicaID string        `json:"replica_id"`
	Deleted   bool          `json:"deleted"`
}

// CompareRecords establishes a strict total ordering over records:
// 1. Higher Timestamp wins (WallTime first, then Counter).
// 2. If Timestamps tie, higher ReplicaID wins (lexicographical comparison).
// 3. If ReplicaIDs tie, Deleted true wins over Deleted false.
// 4. If Deleted flags tie, higher Value wins.
// 5. If Values tie, higher Key wins.
//
// Returns 1 if a > b, -1 if a < b, and 0 if a and b are identical.
func CompareRecords(a, b Record) int {
	if a.Timestamp.After(b.Timestamp) {
		return 1
	}
	if a.Timestamp.Before(b.Timestamp) {
		return -1
	}

	// Tie-break identical timestamps using globally unique replica identifiers.
	if a.ReplicaID > b.ReplicaID {
		return 1
	}
	if a.ReplicaID < b.ReplicaID {
		return -1
	}

	// For identical timestamps from the same replica: deterministic fallbacks.
	if a.Deleted && !b.Deleted {
		return 1
	}
	if !a.Deleted && b.Deleted {
		return -1
	}

	if a.Value > b.Value {
		return 1
	}
	if a.Value < b.Value {
		return -1
	}

	if a.Key > b.Key {
		return 1
	}
	if a.Key < b.Key {
		return -1
	}

	return 0
}

// LWWMap is a state-based Conflict-free Replicated Data Type (CRDT) map
// where each key acts as an independent Last-Write-Wins register.
// Safe for concurrent use across multiple goroutines.
type LWWMap struct {
	mu        sync.RWMutex
	replicaID string
	clock     *hlc.Clock
	records   map[string]Record
}

// NewLWWMap creates an LWWMap associated with a unique replicaID and an HLC clock.
func NewLWWMap(replicaID string, clock *hlc.Clock) *LWWMap {
	if replicaID == "" {
		panic("crdt: replicaID must not be empty")
	}
	return &LWWMap{
		replicaID: replicaID,
		clock:     clock,
		records:   make(map[string]Record),
	}
}

// ReplicaID returns the identifier of the local replica.
func (m *LWWMap) ReplicaID() string {
	return m.replicaID
}

// Clock returns the clock driving this map, or nil if none was provided.
func (m *LWWMap) Clock() *hlc.Clock {
	return m.clock
}

// Get returns the value of an active key and true.
// Returns empty string and false if the key does not exist or has been deleted.
func (m *LWWMap) Get(key string) (string, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	rec, exists := m.records[key]
	if !exists || rec.Deleted {
		return "", false
	}
	return rec.Value, true
}

// Has reports whether the key exists and has not been tombstoned.
func (m *LWWMap) Has(key string) bool {
	_, ok := m.Get(key)
	return ok
}

// GetRecord returns the raw Record for key (including tombstones) and true if present.
func (m *LWWMap) GetRecord(key string) (Record, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	rec, exists := m.records[key]
	return rec, exists
}

// Set writes a key-value pair using a fresh timestamp from the local HLC clock.
// Returns the written Record.
func (m *LWWMap) Set(key, value string) Record {
	m.mu.Lock()
	defer m.mu.Unlock()

	var ts hlc.Timestamp
	if m.clock != nil {
		ts = m.clock.Now()
	}

	rec := Record{
		Key:       key,
		Value:     value,
		Timestamp: ts,
		ReplicaID: m.replicaID,
		Deleted:   false,
	}
	m.records[key] = rec
	return rec
}

// Delete marks a key as deleted by recording a tombstone with a fresh timestamp.
// Returns the tombstone Record and whether the key was actively present before deletion.
func (m *LWWMap) Delete(key string) (Record, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	existing, exists := m.records[key]
	wasActive := exists && !existing.Deleted

	var ts hlc.Timestamp
	if m.clock != nil {
		ts = m.clock.Now()
	}

	rec := Record{
		Key:       key,
		Value:     "",
		Timestamp: ts,
		ReplicaID: m.replicaID,
		Deleted:   true,
	}
	m.records[key] = rec
	return rec, wasActive
}

// Apply merges a single incoming record into the map.
// If the incoming record wins under LWW total ordering, the map is updated and true is returned.
// Always advances the local clock past incoming.Timestamp if a clock is configured.
func (m *LWWMap) Apply(incoming Record) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.clock != nil {
		m.clock.Receive(incoming.Timestamp)
	}

	existing, exists := m.records[incoming.Key]
	if !exists || CompareRecords(incoming, existing) > 0 {
		m.records[incoming.Key] = incoming
		return true
	}
	return false
}

// SetRecord directly inserts or overwrites a record without altering the clock.
// This is used by storage engines where transactional persistence and clock
// progression are managed externally.
func (m *LWWMap) SetRecord(rec Record) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.records[rec.Key] = rec
}

// ApplyRecord merges a single record without advancing the clock.
// Returns true if the incoming record won under LWW total ordering.
func (m *LWWMap) ApplyRecord(incoming Record) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	existing, exists := m.records[incoming.Key]
	if !exists || CompareRecords(incoming, existing) > 0 {
		m.records[incoming.Key] = incoming
		return true
	}
	return false
}

// LoadRecords populates the map from a slice of records, keeping the winning
// record for each key, without advancing the clock. This is used during crash recovery.
func (m *LWWMap) LoadRecords(records []Record) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, rec := range records {
		existing, exists := m.records[rec.Key]
		if !exists || CompareRecords(rec, existing) > 0 {
			m.records[rec.Key] = rec
		}
	}
}

// Merge incorporates all records from another LWWMap into this map.
// Snapshots the remote records first to prevent nested locking and deadlocks.
// Returns the number of keys updated.
func (m *LWWMap) Merge(other *LWWMap) int {
	if other == nil {
		return 0
	}
	return m.MergeRecords(other.AllRecords())
}

// MergeRecords merges a slice of records into this map.
// Returns the number of keys updated.
func (m *LWWMap) MergeRecords(records []Record) int {
	m.mu.Lock()
	defer m.mu.Unlock()

	updated := 0
	for _, incoming := range records {
		if m.clock != nil {
			m.clock.Receive(incoming.Timestamp)
		}

		existing, exists := m.records[incoming.Key]
		if !exists || CompareRecords(incoming, existing) > 0 {
			m.records[incoming.Key] = incoming
			updated++
		}
	}
	return updated
}

// Len returns the count of active (non-deleted) keys.
func (m *LWWMap) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	count := 0
	for _, rec := range m.records {
		if !rec.Deleted {
			count++
		}
	}
	return count
}

// TombstoneCount returns the count of deleted keys retained in memory.
func (m *LWWMap) TombstoneCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	count := 0
	for _, rec := range m.records {
		if rec.Deleted {
			count++
		}
	}
	return count
}

// RecordCount returns the total number of records, active and tombstones combined.
func (m *LWWMap) RecordCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return len(m.records)
}

// Keys returns a sorted slice of active (non-deleted) keys.
func (m *LWWMap) Keys() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	keys := make([]string, 0, len(m.records))
	for k, rec := range m.records {
		if !rec.Deleted {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	return keys
}

// Values returns a copy of all active key-value pairs.
func (m *LWWMap) Values() map[string]string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	res := make(map[string]string, len(m.records))
	for k, rec := range m.records {
		if !rec.Deleted {
			res[k] = rec.Value
		}
	}
	return res
}

// Records returns a copy of all records indexed by key, including tombstones.
func (m *LWWMap) Records() map[string]Record {
	m.mu.RLock()
	defer m.mu.RUnlock()

	res := make(map[string]Record, len(m.records))
	for k, rec := range m.records {
		res[k] = rec
	}
	return res
}

// AllRecords returns a slice containing a snapshot of all records, including tombstones.
func (m *LWWMap) AllRecords() []Record {
	m.mu.RLock()
	defer m.mu.RUnlock()

	res := make([]Record, 0, len(m.records))
	for _, rec := range m.records {
		res = append(res, rec)
	}
	return res
}

// Clone returns an independent deep copy of the map state.
func (m *LWWMap) Clone() *LWWMap {
	m.mu.RLock()
	defer m.mu.RUnlock()

	copied := make(map[string]Record, len(m.records))
	for k, rec := range m.records {
		copied[k] = rec
	}

	return &LWWMap{
		replicaID: m.replicaID,
		clock:     m.clock,
		records:   copied,
	}
}

// EqualState reports whether m and other hold identical records, including tombstones.
// Takes snapshots under read locks sequentially to prevent lock inversion.
func (m *LWWMap) EqualState(other *LWWMap) bool {
	if m == other {
		return true
	}
	if m == nil || other == nil {
		return false
	}

	recsA := m.Records()
	recsB := other.Records()

	if len(recsA) != len(recsB) {
		return false
	}
	for k, a := range recsA {
		b, exists := recsB[k]
		if !exists || a != b {
			return false
		}
	}
	return true
}
