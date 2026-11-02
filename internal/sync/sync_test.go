package sync

import (
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Clearxheaded/converge/internal/hlc"
	"github.com/Clearxheaded/converge/internal/storage"
)

type manualClock struct {
	now time.Time
}

func (m *manualClock) Now() time.Time {
	return m.now
}

func (m *manualClock) Set(t time.Time) {
	m.now = t
}

func createTestNode(t *testing.T, replicaID string, clock *hlc.Clock) (*storage.Storage, *SyncManager) {
	dbPath := filepath.Join(t.TempDir(), replicaID+".db")
	store, err := storage.Open(dbPath, replicaID, clock)
	if err != nil {
		t.Fatalf("open storage for %s: %v", replicaID, err)
	}

	mgr := NewManager(store, FastConfig())
	return store, mgr
}

func waitForCondition(t *testing.T, timeout time.Duration, condition func() bool) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for condition after %v", timeout)
}

func toWSURL(httpURL string) string {
	return "ws" + strings.TrimPrefix(httpURL, "http")
}

func TestSync_LoopbackHandshakeAndCleanShutdown(t *testing.T) {
	storeA, mgrA := createTestNode(t, "node-a", nil)
	defer storeA.Close()
	defer mgrA.Close()

	storeB, mgrB := createTestNode(t, "node-b", nil)
	defer storeB.Close()
	defer mgrB.Close()

	serverB := httptest.NewServer(mgrB)
	defer serverB.Close()

	peerA, err := mgrA.Connect(toWSURL(serverB.URL))
	if err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer peerA.Close()

	if err := peerA.WaitHandshake(2 * time.Second); err != nil {
		t.Fatalf("handshake failed: %v", err)
	}

	waitForCondition(t, 2*time.Second, func() bool {
		return mgrA.PeerCount() == 1 && mgrB.PeerCount() == 1
	})

	if p, ok := mgrA.Peer("node-b"); !ok || p.RemoteID() != "node-b" {
		t.Fatalf("expected node-a to have handshaked peer node-b")
	}
	if p, ok := mgrB.Peer("node-a"); !ok || p.RemoteID() != "node-a" {
		t.Fatalf("expected node-b to have handshaked peer node-a")
	}
}

func TestSync_SelfConnectionRejected(t *testing.T) {
	storeA, mgrA := createTestNode(t, "node-a", nil)
	defer storeA.Close()
	defer mgrA.Close()

	serverA := httptest.NewServer(mgrA)
	defer serverA.Close()

	peerA, err := mgrA.Connect(toWSURL(serverA.URL))
	if err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer peerA.Close()

	// Wait for peer to be closed due to self connection rejection
	waitForCondition(t, 2*time.Second, func() bool {
		return mgrA.PeerCount() == 0
	})
}

func TestSync_BidirectionalInitialSync(t *testing.T) {
	storeA, mgrA := createTestNode(t, "node-a", nil)
	defer storeA.Close()
	defer mgrA.Close()

	storeB, mgrB := createTestNode(t, "node-b", nil)
	defer storeB.Close()
	defer mgrB.Close()

	// Offline writes on Node A
	if _, err := storeA.Set("title", "Distributed Systems"); err != nil {
		t.Fatalf("set title on A: %v", err)
	}
	if _, err := storeA.Set("author", "Lamport"); err != nil {
		t.Fatalf("set author on A: %v", err)
	}

	// Offline writes on Node B
	if _, err := storeB.Set("edition", "3rd"); err != nil {
		t.Fatalf("set edition on B: %v", err)
	}
	if _, err := storeB.Set("publisher", "ACM"); err != nil {
		t.Fatalf("set publisher on B: %v", err)
	}

	serverB := httptest.NewServer(mgrB)
	defer serverB.Close()

	peerA, err := mgrA.Connect(toWSURL(serverB.URL))
	if err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer peerA.Close()

	// Wait for both nodes to acquire all 4 records
	waitForCondition(t, 3*time.Second, func() bool {
		return storeA.Len() == 4 && storeB.Len() == 4
	})

	expected := map[string]string{
		"title":     "Distributed Systems",
		"author":    "Lamport",
		"edition":   "3rd",
		"publisher": "ACM",
	}

	for k, v := range expected {
		if val, ok := storeA.Get(k); !ok || val != v {
			t.Fatalf("node A key %s: expected %s, got %s", k, v, val)
		}
		if val, ok := storeB.Get(k); !ok || val != v {
			t.Fatalf("node B key %s: expected %s, got %s", k, v, val)
		}
	}

	// Verify durability in SQLite
	recordsA, err := storeA.LoadRecordsFromDB()
	if err != nil {
		t.Fatalf("load records from DB A: %v", err)
	}
	recordsB, err := storeB.LoadRecordsFromDB()
	if err != nil {
		t.Fatalf("load records from DB B: %v", err)
	}

	if len(recordsA) != 4 || len(recordsB) != 4 {
		t.Fatalf("sqlite record count mismatch: A has %d, B has %d, want 4", len(recordsA), len(recordsB))
	}
}

func TestSync_ConflictResolutionOverTheWire(t *testing.T) {
	clockA := hlc.NewClock(&manualClock{now: time.Date(2026, 3, 1, 10, 0, 0, 0, time.UTC)})
	clockB := hlc.NewClock(&manualClock{now: time.Date(2026, 3, 1, 10, 0, 0, 0, time.UTC)})

	storeA, mgrA := createTestNode(t, "node-a", clockA)
	defer storeA.Close()
	defer mgrA.Close()

	storeB, mgrB := createTestNode(t, "node-b", clockB)
	defer storeB.Close()
	defer mgrB.Close()

	// Both nodes write to the exact same key concurrently with equal wall times
	// Node B has replicaID "node-b" which is lexicographically greater than "node-a"
	if _, err := storeA.Set("status", "pending_a"); err != nil {
		t.Fatalf("set status A: %v", err)
	}
	if _, err := storeB.Set("status", "pending_b"); err != nil {
		t.Fatalf("set status B: %v", err)
	}

	serverB := httptest.NewServer(mgrB)
	defer serverB.Close()

	peerA, err := mgrA.Connect(toWSURL(serverB.URL))
	if err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer peerA.Close()

	waitForCondition(t, 3*time.Second, func() bool {
		valA, okA := storeA.Get("status")
		valB, okB := storeB.Get("status")
		return okA && okB && valA == valB
	})

	// "node-b" wins the deterministic tie-breaker over "node-a"
	valA, _ := storeA.Get("status")
	valB, _ := storeB.Get("status")
	if valA != "pending_b" || valB != "pending_b" {
		t.Fatalf("expected tie-breaker winner 'pending_b', got A: %s, B: %s", valA, valB)
	}
}

func TestSync_TombstoneReplication(t *testing.T) {
	storeA, mgrA := createTestNode(t, "node-a", nil)
	defer storeA.Close()
	defer mgrA.Close()

	storeB, mgrB := createTestNode(t, "node-b", nil)
	defer storeB.Close()
	defer mgrB.Close()

	serverB := httptest.NewServer(mgrB)
	defer serverB.Close()

	peerA, err := mgrA.Connect(toWSURL(serverB.URL))
	if err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer peerA.Close()

	if err := peerA.WaitHandshake(2 * time.Second); err != nil {
		t.Fatalf("handshake failed: %v", err)
	}

	// Live write on Node A
	if _, err := storeA.Set("temp_key", "temporary_value"); err != nil {
		t.Fatalf("set temp_key on A: %v", err)
	}

	waitForCondition(t, 2*time.Second, func() bool {
		val, ok := storeB.Get("temp_key")
		return ok && val == "temporary_value"
	})

	// Delete on Node A
	if _, _, err := storeA.Delete("temp_key"); err != nil {
		t.Fatalf("delete temp_key on A: %v", err)
	}

	// Verify tombstone propagates to Node B
	waitForCondition(t, 2*time.Second, func() bool {
		_, ok := storeB.Get("temp_key")
		return !ok && storeB.TombstoneCount() == 1
	})

	// Verify DB durability of tombstone on Node B
	recB, exists := storeB.GetRecord("temp_key")
	if !exists || !recB.Deleted {
		t.Fatalf("expected tombstoned record on node B, got %+v", recB)
	}
}

func TestSync_LiveStreamingBidirectional(t *testing.T) {
	storeA, mgrA := createTestNode(t, "node-a", nil)
	defer storeA.Close()
	defer mgrA.Close()

	storeB, mgrB := createTestNode(t, "node-b", nil)
	defer storeB.Close()
	defer mgrB.Close()

	serverB := httptest.NewServer(mgrB)
	defer serverB.Close()

	peerA, err := mgrA.Connect(toWSURL(serverB.URL))
	if err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer peerA.Close()

	if err := peerA.WaitHandshake(2 * time.Second); err != nil {
		t.Fatalf("handshake failed: %v", err)
	}

	// Stream from A to B
	if _, err := storeA.Set("stream_a", "val_a"); err != nil {
		t.Fatalf("set stream_a on A: %v", err)
	}

	waitForCondition(t, 2*time.Second, func() bool {
		v, ok := storeB.Get("stream_a")
		return ok && v == "val_a"
	})

	// Stream from B to A
	if _, err := storeB.Set("stream_b", "val_b"); err != nil {
		t.Fatalf("set stream_b on B: %v", err)
	}

	waitForCondition(t, 2*time.Second, func() bool {
		v, ok := storeA.Get("stream_b")
		return ok && v == "val_b"
	})

	// Verify total state equality
	snapA := storeA.Snapshot()
	snapB := storeB.Snapshot()
	if !snapA.EqualState(snapB) {
		t.Fatalf("state snapshot mismatch between node A and node B")
	}
}

func TestSync_ReconnectionAndIncrementalSync(t *testing.T) {
	storeA, mgrA := createTestNode(t, "node-a", nil)
	defer storeA.Close()
	defer mgrA.Close()

	storeB, mgrB := createTestNode(t, "node-b", nil)
	defer storeB.Close()
	defer mgrB.Close()

	serverB := httptest.NewServer(mgrB)
	defer serverB.Close()

	// 1. Initial connection and sync
	if _, err := storeA.Set("initial", "one"); err != nil {
		t.Fatalf("set initial on A: %v", err)
	}

	peerA, err := mgrA.Connect(toWSURL(serverB.URL))
	if err != nil {
		t.Fatalf("connect failed: %v", err)
	}

	waitForCondition(t, 2*time.Second, func() bool {
		v, ok := storeB.Get("initial")
		return ok && v == "one"
	})

	// 2. Disconnect peer
	_ = peerA.Close()
	waitForCondition(t, 2*time.Second, func() bool {
		return mgrA.PeerCount() == 0 && mgrB.PeerCount() == 0
	})

	// 3. Write offline mutations on Node A while disconnected
	if _, err := storeA.Set("offline_1", "alpha"); err != nil {
		t.Fatalf("set offline_1: %v", err)
	}
	if _, err := storeA.Set("offline_2", "beta"); err != nil {
		t.Fatalf("set offline_2: %v", err)
	}

	// 4. Reconnect
	peerA2, err := mgrA.Connect(toWSURL(serverB.URL))
	if err != nil {
		t.Fatalf("reconnect failed: %v", err)
	}
	defer peerA2.Close()

	// 5. Verify incremental sync delivers offline changes
	waitForCondition(t, 2*time.Second, func() bool {
		v1, ok1 := storeB.Get("offline_1")
		v2, ok2 := storeB.Get("offline_2")
		return ok1 && ok2 && v1 == "alpha" && v2 == "beta"
	})

	if storeB.Len() != 3 {
		t.Fatalf("expected 3 keys on node B, got %d", storeB.Len())
	}
}

func TestSync_HeartbeatPingPong(t *testing.T) {
	storeA, mgrA := createTestNode(t, "node-a", nil)
	defer storeA.Close()
	defer mgrA.Close()

	storeB, mgrB := createTestNode(t, "node-b", nil)
	defer storeB.Close()
	defer mgrB.Close()

	serverB := httptest.NewServer(mgrB)
	defer serverB.Close()

	peerA, err := mgrA.Connect(toWSURL(serverB.URL))
	if err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer peerA.Close()

	if err := peerA.WaitHandshake(2 * time.Second); err != nil {
		t.Fatalf("handshake failed: %v", err)
	}

	// Wait across multiple heartbeat periods (FastConfig ping period is 500ms)
	time.Sleep(1200 * time.Millisecond)

	// Verify connection is still open and healthy
	if !peerA.IsHandshaked() || mgrA.PeerCount() != 1 || mgrB.PeerCount() != 1 {
		t.Fatalf("connection died or failed heartbeat")
	}
}
