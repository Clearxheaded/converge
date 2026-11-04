package crdt

import (
	"fmt"
	"testing"

	"github.com/Clearxheaded/converge/internal/hlc"
)

func BenchmarkLWWMap_Set(b *testing.B) {
	clock := hlc.NewClock(hlc.WallClock{})
	m := NewLWWMap("node-1", clock)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key_%d", i%1000)
		m.Set(key, "value")
	}
}

func BenchmarkLWWMap_Get(b *testing.B) {
	clock := hlc.NewClock(hlc.WallClock{})
	m := NewLWWMap("node-1", clock)
	for i := 0; i < 1000; i++ {
		m.Set(fmt.Sprintf("key_%d", i), "value")
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key_%d", i%1000)
		_, _ = m.Get(key)
	}
}

func BenchmarkCompareRecords(b *testing.B) {
	r1 := Record{
		Key:       "user:100",
		Value:     "alpha_state",
		Timestamp: hlc.Timestamp{WallTime: 1700000000000, Counter: 5},
		ReplicaID: "node-a",
		Deleted:   false,
	}
	r2 := Record{
		Key:       "user:100",
		Value:     "beta_state",
		Timestamp: hlc.Timestamp{WallTime: 1700000000000, Counter: 6},
		ReplicaID: "node-b",
		Deleted:   false,
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = CompareRecords(r1, r2)
	}
}

func BenchmarkLWWMap_MergeDisjoint_100(b *testing.B) {
	benchmarkMergeDisjoint(b, 100)
}

func BenchmarkLWWMap_MergeDisjoint_1000(b *testing.B) {
	benchmarkMergeDisjoint(b, 1000)
}

func benchmarkMergeDisjoint(b *testing.B, recordCount int) {
	records := make([]Record, recordCount)
	for i := 0; i < recordCount; i++ {
		records[i] = Record{
			Key:       fmt.Sprintf("remote_key_%d", i),
			Value:     "remote_val",
			Timestamp: hlc.Timestamp{WallTime: uint64(10000 + i), Counter: 0},
			ReplicaID: "node-remote",
			Deleted:   false,
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		clock := hlc.NewClock(hlc.WallClock{})
		m := NewLWWMap("node-local", clock)
		b.StartTimer()

		m.MergeRecords(records)
	}
}

func BenchmarkLWWMap_MergeConflicting_1000(b *testing.B) {
	records := make([]Record, 1000)
	for i := 0; i < 1000; i++ {
		records[i] = Record{
			Key:       fmt.Sprintf("key_%d", i),
			Value:     "remote_winner",
			Timestamp: hlc.Timestamp{WallTime: 20000, Counter: uint32(i)},
			ReplicaID: "node-remote",
			Deleted:   false,
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		clock := hlc.NewClock(hlc.WallClock{})
		m := NewLWWMap("node-local", clock)
		for j := 0; j < 1000; j++ {
			_ = m.Apply(Record{
				Key:       fmt.Sprintf("key_%d", j),
				Value:     "local_loser",
				Timestamp: hlc.Timestamp{WallTime: 10000, Counter: uint32(j)},
				ReplicaID: "node-local",
				Deleted:   false,
			})
		}
		b.StartTimer()

		m.MergeRecords(records)
	}
}

func BenchmarkLWWMap_MergeTombstones_1000(b *testing.B) {
	records := make([]Record, 1000)
	for i := 0; i < 1000; i++ {
		records[i] = Record{
			Key:       fmt.Sprintf("key_%d", i),
			Value:     "",
			Timestamp: hlc.Timestamp{WallTime: 20000, Counter: uint32(i)},
			ReplicaID: "node-remote",
			Deleted:   true,
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		clock := hlc.NewClock(hlc.WallClock{})
		m := NewLWWMap("node-local", clock)
		for j := 0; j < 1000; j++ {
			_ = m.Apply(Record{
				Key:       fmt.Sprintf("key_%d", j),
				Value:     "active_data",
				Timestamp: hlc.Timestamp{WallTime: 10000, Counter: uint32(j)},
				ReplicaID: "node-local",
				Deleted:   false,
			})
		}
		b.StartTimer()

		m.MergeRecords(records)
	}
}
