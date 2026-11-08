package sync

import (
	"fmt"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Clearxheaded/converge/internal/hlc"
	"github.com/Clearxheaded/converge/internal/storage"
)

func createBenchNode(b *testing.B, replicaID string) (*storage.Storage, *SyncManager, *httptest.Server) {
	b.Helper()
	dbPath := filepath.Join(b.TempDir(), replicaID+".db")
	clock := hlc.NewClock(hlc.WallClock{})
	store, err := storage.Open(dbPath, replicaID, clock)
	if err != nil {
		b.Fatalf("open storage %s: %v", replicaID, err)
	}

	mgr := NewManager(store, FastConfig())
	server := httptest.NewServer(mgr)
	return store, mgr, server
}

func BenchmarkSync_StreamingSingle(b *testing.B) {
	storeA, mgrA, srvA := createBenchNode(b, "bench-a")
	defer func() {
		_ = mgrA.Close()
		srvA.Close()
		_ = storeA.Close()
	}()

	storeB, mgrB, srvB := createBenchNode(b, "bench-b")
	defer func() {
		_ = mgrB.Close()
		srvB.Close()
		_ = storeB.Close()
	}()

	wsURL := "ws" + strings.TrimPrefix(srvB.URL, "http")
	peer, err := mgrA.Connect(wsURL)
	if err != nil {
		b.Fatalf("connect A -> B: %v", err)
	}
	if err := peer.WaitHandshake(2 * time.Second); err != nil {
		b.Fatalf("handshake A -> B: %v", err)
	}

	// Wait for B to register A as handshaked
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && mgrB.PeerCount() == 0 {
		time.Sleep(5 * time.Millisecond)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("stream_key_%d", i)
		val := fmt.Sprintf("stream_val_%d", i)

		_, err := storeA.Set(key, val)
		if err != nil {
			b.Fatalf("storeA set: %v", err)
		}

		// Wait until storeB receives and commits the update
		waitDeadline := time.Now().Add(2 * time.Second)
		converged := false
		for time.Now().Before(waitDeadline) {
			if v, ok := storeB.Get(key); ok && v == val {
				converged = true
				break
			}
			time.Sleep(1 * time.Millisecond)
		}
		if !converged {
			b.Fatalf("storeB did not converge on key %s", key)
		}
	}
}

func BenchmarkSync_CatchUpSync_500(b *testing.B) {
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		func() {
			b.StopTimer()

			// Prepare node A with 500 records
			storeA, mgrA, srvA := createBenchNode(b, fmt.Sprintf("node-a-%d", i))
			defer func() {
				_ = mgrA.Close()
				srvA.Close()
				_ = storeA.Close()
			}()

			for j := 0; j < 500; j++ {
				_, _ = storeA.Set(fmt.Sprintf("seed_key_%d", j), "seed_value")
			}

			// Prepare fresh node B
			storeB, mgrB, srvB := createBenchNode(b, fmt.Sprintf("node-b-%d", i))
			defer func() {
				_ = mgrB.Close()
				srvB.Close()
				_ = storeB.Close()
			}()

			wsURL := "ws" + strings.TrimPrefix(srvB.URL, "http")

			b.StartTimer()

			peer, err := mgrA.Connect(wsURL)
			if err != nil {
				b.Fatalf("connect: %v", err)
			}
			if err := peer.WaitHandshake(2 * time.Second); err != nil {
				b.Fatalf("handshake: %v", err)
			}

			// Wait until node B synchronizes all 500 records
			waitDeadline := time.Now().Add(5 * time.Second)
			converged := false
			for time.Now().Before(waitDeadline) {
				if storeB.Len() >= 500 {
					converged = true
					break
				}
				time.Sleep(5 * time.Millisecond)
			}

			b.StopTimer()

			if !converged {
				b.Fatalf("catch-up sync timed out: storeB has %d/500 records", storeB.Len())
			}
		}()
	}
}
