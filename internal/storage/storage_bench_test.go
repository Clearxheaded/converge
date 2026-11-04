package storage

import (
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	"github.com/Clearxheaded/converge/internal/crdt"
	"github.com/Clearxheaded/converge/internal/hlc"
)

func BenchmarkStorage_SetSingle(b *testing.B) {
	tempDir := b.TempDir()
	dbPath := filepath.Join(tempDir, "bench_set.db")
	clock := hlc.NewClock(hlc.WallClock{})

	s, err := Open(dbPath, "bench-node", clock)
	if err != nil {
		b.Fatalf("open storage: %v", err)
	}
	defer s.Close()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key_%d", i%500)
		_, err := s.Set(key, "bench_value")
		if err != nil {
			b.Fatalf("set failed: %v", err)
		}
	}
}

func BenchmarkStorage_Get(b *testing.B) {
	tempDir := b.TempDir()
	dbPath := filepath.Join(tempDir, "bench_get.db")
	clock := hlc.NewClock(hlc.WallClock{})

	s, err := Open(dbPath, "bench-node", clock)
	if err != nil {
		b.Fatalf("open storage: %v", err)
	}
	defer s.Close()

	for i := 0; i < 500; i++ {
		_, _ = s.Set(fmt.Sprintf("key_%d", i), "bench_value")
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key_%d", i%500)
		_, _ = s.Get(key)
	}
}

func BenchmarkStorage_MergeBatch_100(b *testing.B) {
	benchmarkStorageMergeBatch(b, 100)
}

func BenchmarkStorage_MergeBatch_1000(b *testing.B) {
	benchmarkStorageMergeBatch(b, 1000)
}

func benchmarkStorageMergeBatch(b *testing.B, batchSize int) {
	tempDir := b.TempDir()
	dbPath := filepath.Join(tempDir, "bench_merge.db")
	clock := hlc.NewClock(hlc.WallClock{})

	s, err := Open(dbPath, "bench-node", clock)
	if err != nil {
		b.Fatalf("open storage: %v", err)
	}
	defer s.Close()

	records := make([]crdt.Record, batchSize)
	for i := 0; i < batchSize; i++ {
		records[i] = crdt.Record{
			Key:       fmt.Sprintf("batch_key_%d", i),
			Value:     "batch_val",
			Timestamp: hlc.Timestamp{WallTime: uint64(20000 + i), Counter: 0},
			ReplicaID: "remote-node",
			Deleted:   false,
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Increment timestamp for next iteration so writes are winners
		for j := range records {
			records[j].Timestamp = hlc.Timestamp{WallTime: uint64(30000 + i*batchSize + j), Counter: 0}
		}

		_, err := s.MergeRecords(records)
		if err != nil {
			b.Fatalf("merge records: %v", err)
		}
	}
}

func BenchmarkStorage_ConcurrentSet(b *testing.B) {
	tempDir := b.TempDir()
	dbPath := filepath.Join(tempDir, "bench_concurrent.db")
	clock := hlc.NewClock(hlc.WallClock{})

	s, err := Open(dbPath, "bench-node", clock)
	if err != nil {
		b.Fatalf("open storage: %v", err)
	}
	defer s.Close()

	b.ResetTimer()
	b.ReportAllocs()

	concurrency := 8
	itemsPerWorker := b.N / concurrency
	if itemsPerWorker == 0 {
		itemsPerWorker = 1
	}

	var wg sync.WaitGroup
	wg.Add(concurrency)

	for w := 0; w < concurrency; w++ {
		workerID := w
		go func() {
			defer wg.Done()
			for i := 0; i < itemsPerWorker; i++ {
				key := fmt.Sprintf("w%d_key_%d", workerID, i%100)
				_, _ = s.Set(key, "concurrent_val")
			}
		}()
	}

	wg.Wait()
}

func BenchmarkStorage_RecordsSince(b *testing.B) {
	tempDir := b.TempDir()
	dbPath := filepath.Join(tempDir, "bench_watermark.db")
	clock := hlc.NewClock(hlc.WallClock{})

	s, err := Open(dbPath, "bench-node", clock)
	if err != nil {
		b.Fatalf("open storage: %v", err)
	}
	defer s.Close()

	for i := 0; i < 1000; i++ {
		_, _ = s.Set(fmt.Sprintf("key_%d", i), "bench_val")
	}

	midpoint := hlc.Timestamp{WallTime: clock.Now().WallTime - 500, Counter: 0}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = s.RecordsSince(midpoint)
	}
}
