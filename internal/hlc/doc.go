// Package hlc implements a Hybrid Logical Clock (HLC) for causality tracking
// across distributed replicas with unsynchronized physical clocks.
//
// An HLC combines a physical wall-clock timestamp with a logical counter to
// produce monotonically increasing timestamps that respect both physical time
// and causal ordering. This allows independent replicas to generate timestamps
// that are globally comparable without requiring perfectly synchronized clocks.
//
// The clock is safe for concurrent use by multiple goroutines.
package hlc
