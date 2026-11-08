package hlc_test

import (
	"sync"
	"testing"

	"github.com/Clearxheaded/converge/internal/hlc"
)

func TestVectorClock_UpdateAndGet(t *testing.T) {
	vc := hlc.NewVectorClock()

	ts1 := hlc.Timestamp{WallTime: 100, Counter: 0}
	ts2 := hlc.Timestamp{WallTime: 100, Counter: 1}
	tsOld := hlc.Timestamp{WallTime: 90, Counter: 5}

	if !vc.Update("node-a", ts1) {
		t.Fatalf("expected first update to succeed")
	}
	if got := vc.Get("node-a"); got != ts1 {
		t.Fatalf("expected %v, got %v", ts1, got)
	}

	// Monotonic advance
	if !vc.Update("node-a", ts2) {
		t.Fatalf("expected advance update to succeed")
	}
	if got := vc.Get("node-a"); got != ts2 {
		t.Fatalf("expected %v, got %v", ts2, got)
	}

	// Stale update should be rejected
	if vc.Update("node-a", tsOld) {
		t.Fatalf("expected older update to be rejected")
	}
	if got := vc.Get("node-a"); got != ts2 {
		t.Fatalf("expected %v, got %v", ts2, got)
	}
}

func TestVectorClock_Merge(t *testing.T) {
	vc1 := hlc.NewVectorClock()
	vc1.Set("node-a", hlc.Timestamp{WallTime: 100, Counter: 2})
	vc1.Set("node-b", hlc.Timestamp{WallTime: 50, Counter: 0})

	vc2 := hlc.NewVectorClock()
	vc2.Set("node-a", hlc.Timestamp{WallTime: 90, Counter: 10})
	vc2.Set("node-b", hlc.Timestamp{WallTime: 60, Counter: 0})
	vc2.Set("node-c", hlc.Timestamp{WallTime: 75, Counter: 1})

	vc1.Merge(vc2)

	expectedA := hlc.Timestamp{WallTime: 100, Counter: 2}
	expectedB := hlc.Timestamp{WallTime: 60, Counter: 0}
	expectedC := hlc.Timestamp{WallTime: 75, Counter: 1}

	if got := vc1.Get("node-a"); got != expectedA {
		t.Fatalf("node-a: expected %v, got %v", expectedA, got)
	}
	if got := vc1.Get("node-b"); got != expectedB {
		t.Fatalf("node-b: expected %v, got %v", expectedB, got)
	}
	if got := vc1.Get("node-c"); got != expectedC {
		t.Fatalf("node-c: expected %v, got %v", expectedC, got)
	}
}

func TestVectorClock_StableWatermark(t *testing.T) {
	vc := hlc.NewVectorClock()
	vc.Set("node-a", hlc.Timestamp{WallTime: 200, Counter: 0})
	vc.Set("node-b", hlc.Timestamp{WallTime: 150, Counter: 5})
	vc.Set("node-c", hlc.Timestamp{WallTime: 180, Counter: 1})

	cluster := []string{"node-a", "node-b", "node-c"}
	watermark := vc.StableWatermark(cluster)

	expected := hlc.Timestamp{WallTime: 150, Counter: 5}
	if watermark != expected {
		t.Fatalf("expected stable watermark %v, got %v", expected, watermark)
	}

	// Missing node in required set returns zero watermark
	watermarkWithMissing := vc.StableWatermark([]string{"node-a", "node-b", "node-d"})
	if !watermarkWithMissing.IsZero() {
		t.Fatalf("expected zero watermark when required node is missing, got %v", watermarkWithMissing)
	}
}

func TestVectorClock_ConcurrentStress(t *testing.T) {
	vc := hlc.NewVectorClock()
	var wg sync.WaitGroup

	nodes := []string{"node-a", "node-b", "node-c", "node-d"}
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 1; j <= 50; j++ {
				node := nodes[(workerID+j)%len(nodes)]
				ts := hlc.Timestamp{WallTime: uint64(j * 10), Counter: uint32(workerID)}
				vc.Update(node, ts)
				_ = vc.Get(node)
				_ = vc.Clone()
				_ = vc.StableWatermark(nodes)
			}
		}(i)
	}

	wg.Wait()
	if vc.Len() != len(nodes) {
		t.Fatalf("expected %d tracked replicas, got %d", len(nodes), vc.Len())
	}
}
