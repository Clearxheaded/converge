package hlc

import "sync"

// VectorClock maps replica IDs to their highest observed HLC timestamps.
// It is used to track cluster-wide causal frontiers and compute the stable
// global watermark for tombstone garbage collection.
type VectorClock struct {
	mu     sync.RWMutex
	clocks map[string]Timestamp
}

// NewVectorClock creates an empty, thread-safe vector clock.
func NewVectorClock() *VectorClock {
	return &VectorClock{
		clocks: make(map[string]Timestamp),
	}
}

// Get returns the highest known timestamp for the given replica.
// If the replica is unknown, returns the zero timestamp.
func (vc *VectorClock) Get(replicaID string) Timestamp {
	vc.mu.RLock()
	defer vc.mu.RUnlock()
	return vc.clocks[replicaID]
}

// Set unconditionally stores the timestamp for the given replica.
func (vc *VectorClock) Set(replicaID string, ts Timestamp) {
	vc.mu.Lock()
	defer vc.mu.Unlock()
	vc.clocks[replicaID] = ts
}

// Update records an observed timestamp for replicaID if ts is greater
// than the currently stored timestamp for that replica.
// Returns true if the clock was updated.
func (vc *VectorClock) Update(replicaID string, ts Timestamp) bool {
	vc.mu.Lock()
	defer vc.mu.Unlock()

	cur, exists := vc.clocks[replicaID]
	if !exists || ts.After(cur) {
		vc.clocks[replicaID] = ts
		return true
	}
	return false
}

// Merge incorporates all entries from other, taking the pointwise maximum
// timestamp for each replica.
func (vc *VectorClock) Merge(other *VectorClock) {
	if other == nil {
		return
	}

	other.mu.RLock()
	entries := make(map[string]Timestamp, len(other.clocks))
	for k, v := range other.clocks {
		entries[k] = v
	}
	other.mu.RUnlock()

	vc.mu.Lock()
	defer vc.mu.Unlock()
	for id, ts := range entries {
		cur, exists := vc.clocks[id]
		if !exists || ts.After(cur) {
			vc.clocks[id] = ts
		}
	}
}

// Clone returns an independent deep copy of the vector clock.
func (vc *VectorClock) Clone() *VectorClock {
	vc.mu.RLock()
	defer vc.mu.RUnlock()

	clone := NewVectorClock()
	for k, v := range vc.clocks {
		clone.clocks[k] = v
	}
	return clone
}

// Replicas returns a slice of all known replica IDs in the vector clock.
func (vc *VectorClock) Replicas() []string {
	vc.mu.RLock()
	defer vc.mu.RUnlock()

	reps := make([]string, 0, len(vc.clocks))
	for k := range vc.clocks {
		reps = append(reps, k)
	}
	return reps
}

// Len returns the number of tracked replicas.
func (vc *VectorClock) Len() int {
	vc.mu.RLock()
	defer vc.mu.RUnlock()
	return len(vc.clocks)
}

// StableWatermark calculates the minimum timestamp observed across all required replicas.
//
// In an offline-first CRDT, a tombstone with timestamp T can only be purged
// if every replica in the cluster has acknowledged state up to or beyond T.
// If any required replica is missing from the vector clock, the stable watermark
// is the zero timestamp (meaning nothing can be safely garbage collected).
//
// If replicas is empty, the calculation considers all currently tracked replicas.
// If no replicas are tracked, returns the zero timestamp.
func (vc *VectorClock) StableWatermark(requiredReplicas []string) Timestamp {
	vc.mu.RLock()
	defer vc.mu.RUnlock()

	if len(requiredReplicas) == 0 {
		if len(vc.clocks) == 0 {
			return Timestamp{}
		}
		var minTS Timestamp
		first := true
		for _, ts := range vc.clocks {
			if first || ts.Before(minTS) {
				minTS = ts
				first = false
			}
		}
		return minTS
	}

	var minTS Timestamp
	first := true
	for _, reqID := range requiredReplicas {
		ts, exists := vc.clocks[reqID]
		if !exists || ts.IsZero() {
			// A required replica has not reported or is at zero; no safe watermark
			return Timestamp{}
		}
		if first || ts.Before(minTS) {
			minTS = ts
			first = false
		}
	}
	return minTS
}

