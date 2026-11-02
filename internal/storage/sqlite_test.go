package storage

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Clearxheaded/converge/internal/crdt"
	"github.com/Clearxheaded/converge/internal/hlc"
)

// manualClock provides controllable physical time for deterministic tests.
type manualClock struct {
	mu  sync.Mutex
	now time.Time
}

func newManualClock(t time.Time) *manualClock {
	return &manualClock{now: t}
}

func (m *manualClock) Now() time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.now
}

func (m *manualClock) Advance(d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.now = m.now.Add(d)
}

func (m *manualClock) Rewind(d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.now = m.now.Add(-d)
}

func baseTime() time.Time {
	return time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
}

func TestStorage_OpenAndSchema(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "schema_test.db")
	phys := newManualClock(baseTime())
	clock := hlc.NewClock(phys)

	s, err := Open(dbPath, "node-1", clock)
	if err != nil {
		t.Fatalf("failed to open storage: %v", err)
	}
	defer s.Close()

	if s.ReplicaID() != "node-1" {
		t.Fatalf("expected replicaID 'node-1', got '%s'", s.ReplicaID())
	}

	// Verify WAL mode
	var journalMode string
	err = s.DB().QueryRow("PRAGMA journal_mode;").Scan(&journalMode)
	if err != nil {
		t.Fatalf("failed to query journal_mode: %v", err)
	}
	if journalMode != "wal" {
		t.Fatalf("expected journal_mode 'wal', got '%s'", journalMode)
	}

	// Verify tables exist
	var tableName string
	err = s.DB().QueryRow("SELECT name FROM sqlite_master WHERE type='table' AND name='records';").Scan(&tableName)
	if err != nil || tableName != "records" {
		t.Fatalf("expected records table to exist: err=%v, table=%s", err, tableName)
	}

	err = s.DB().QueryRow("SELECT name FROM sqlite_master WHERE type='table' AND name='clock_state';").Scan(&tableName)
	if err != nil || tableName != "clock_state" {
		t.Fatalf("expected clock_state table to exist: err=%v, table=%s", err, tableName)
	}
}

func TestStorage_CRUDAndTombstones(t *testing.T) {
	phys := newManualClock(baseTime())
	clock := hlc.NewClock(phys)

	s, err := Open(":memory:", "node-1", clock)
	if err != nil {
		t.Fatalf("failed to open in-memory storage: %v", err)
	}
	defer s.Close()

	// Initial state
	if s.Len() != 0 || s.RecordCount() != 0 {
		t.Fatalf("expected empty storage initially, got len=%d, records=%d", s.Len(), s.RecordCount())
	}

	// Set key "user:1"
	rec1, err := s.Set("user:1", "Alice")
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}
	if rec1.Key != "user:1" || rec1.Value != "Alice" || rec1.Deleted {
		t.Fatalf("unexpected record returned by Set: %+v", rec1)
	}

	// Get key "user:1"
	val, ok := s.Get("user:1")
	if !ok || val != "Alice" {
		t.Fatalf("expected 'Alice', got val='%s', ok=%v", val, ok)
	}
	if !s.Has("user:1") {
		t.Fatalf("expected Has('user:1') to be true")
	}

	// Check underlying DB table
	dbRecords, err := s.LoadRecordsFromDB()
	if err != nil {
		t.Fatalf("failed to load records from DB: %v", err)
	}
	if len(dbRecords) != 1 || dbRecords[0] != rec1 {
		t.Fatalf("persisted DB record mismatch: got %+v, expected [%+v]", dbRecords, rec1)
	}

	// Check DB clock state
	dbClock, err := s.LoadClockFromDB()
	if err != nil {
		t.Fatalf("failed to load clock from DB: %v", err)
	}
	if dbClock != rec1.Timestamp {
		t.Fatalf("persisted clock mismatch: got %v, expected %v", dbClock, rec1.Timestamp)
	}

	// Update key "user:1"
	rec2, err := s.Set("user:1", "Alice Updated")
	if err != nil {
		t.Fatalf("Set update failed: %v", err)
	}
	if !rec2.Timestamp.After(rec1.Timestamp) {
		t.Fatalf("expected updated record timestamp to advance: rec2=%v, rec1=%v", rec2.Timestamp, rec1.Timestamp)
	}
	val, _ = s.Get("user:1")
	if val != "Alice Updated" {
		t.Fatalf("expected updated value 'Alice Updated', got '%s'", val)
	}

	// Delete key "user:1" -> creates tombstone
	recDel, wasActive, err := s.Delete("user:1")
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	if !wasActive {
		t.Fatalf("expected wasActive=true when deleting active key")
	}
	if !recDel.Deleted || recDel.Value != "" {
		t.Fatalf("expected tombstone record, got %+v", recDel)
	}

	// Verify key is inactive in memory
	val, ok = s.Get("user:1")
	if ok || val != "" {
		t.Fatalf("expected deleted key to return false and empty string, got '%s', ok=%v", val, ok)
	}
	if s.Has("user:1") {
		t.Fatalf("expected Has('user:1') to be false after deletion")
	}
	if s.Len() != 0 {
		t.Fatalf("expected Len() to be 0 after deleting only key, got %d", s.Len())
	}
	if s.TombstoneCount() != 1 {
		t.Fatalf("expected TombstoneCount() to be 1, got %d", s.TombstoneCount())
	}
	if s.RecordCount() != 1 {
		t.Fatalf("expected RecordCount() to be 1, got %d", s.RecordCount())
	}

	// Verify tombstone in DB
	dbRecords, err = s.LoadRecordsFromDB()
	if err != nil {
		t.Fatalf("failed to read DB records: %v", err)
	}
	if len(dbRecords) != 1 || !dbRecords[0].Deleted || dbRecords[0].Key != "user:1" {
		t.Fatalf("expected tombstone record persisted on disk: %+v", dbRecords)
	}

	// Delete non-existent key: creates tombstone directly
	tombstoneNew, wasActive, err := s.Delete("non_existent")
	if err != nil {
		t.Fatalf("Delete non_existent failed: %v", err)
	}
	if wasActive {
		t.Fatalf("expected wasActive=false when deleting non-existent key")
	}
	if !tombstoneNew.Deleted || tombstoneNew.Key != "non_existent" {
		t.Fatalf("unexpected tombstone record: %+v", tombstoneNew)
	}
	if s.TombstoneCount() != 2 {
		t.Fatalf("expected 2 tombstones, got %d", s.TombstoneCount())
	}
}

func TestStorage_MergeRecords(t *testing.T) {
	phys := newManualClock(baseTime())
	clock := hlc.NewClock(phys)

	s, err := Open(":memory:", "node-local", clock)
	if err != nil {
		t.Fatalf("failed to open storage: %v", err)
	}
	defer s.Close()

	// Local write at physical time = baseTime (wall = baseMs, counter = 0)
	localRec, err := s.Set("key1", "local_val")
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// Remote incoming batch:
	// - "key1" older timestamp: should lose
	// - "key2" newer timestamp: should win
	// - "key3" incoming twice in same batch (intra-batch deduplication): higher counter should win
	olderRemote := crdt.Record{
		Key:       "key1",
		Value:     "remote_stale",
		Timestamp: hlc.Timestamp{WallTime: localRec.Timestamp.WallTime - 100, Counter: 0},
		ReplicaID: "node-remote",
		Deleted:   false,
	}
	newerRemote := crdt.Record{
		Key:       "key2",
		Value:     "remote_val2",
		Timestamp: hlc.Timestamp{WallTime: localRec.Timestamp.WallTime + 500, Counter: 0},
		ReplicaID: "node-remote",
		Deleted:   false,
	}
	key3First := crdt.Record{
		Key:       "key3",
		Value:     "key3_v1",
		Timestamp: hlc.Timestamp{WallTime: localRec.Timestamp.WallTime + 200, Counter: 1},
		ReplicaID: "node-remote",
		Deleted:   false,
	}
	key3Second := crdt.Record{
		Key:       "key3",
		Value:     "key3_v2",
		Timestamp: hlc.Timestamp{WallTime: localRec.Timestamp.WallTime + 200, Counter: 5},
		ReplicaID: "node-remote",
		Deleted:   false,
	}

	updated, err := s.MergeRecords([]crdt.Record{olderRemote, newerRemote, key3First, key3Second})
	if err != nil {
		t.Fatalf("MergeRecords failed: %v", err)
	}
	if updated != 2 {
		t.Fatalf("expected 2 keys updated (key2 and key3), got %d", updated)
	}

	// Verify key1 retained local value
	val1, _ := s.Get("key1")
	if val1 != "local_val" {
		t.Fatalf("key1 should have retained 'local_val', got '%s'", val1)
	}

	// Verify key2 adopted remote value
	val2, _ := s.Get("key2")
	if val2 != "remote_val2" {
		t.Fatalf("key2 should have adopted 'remote_val2', got '%s'", val2)
	}

	// Verify key3 adopted the intra-batch winner (counter=5)
	rec3, _ := s.GetRecord("key3")
	if rec3.Value != "key3_v2" || rec3.Timestamp.Counter != 5 {
		t.Fatalf("key3 intra-batch deduplication failed: got %+v", rec3)
	}

	// Verify clock advanced past the highest incoming timestamp (key2: wall+500)
	if !s.Clock().Last().After(newerRemote.Timestamp) {
		t.Fatalf("expected local clock to advance beyond %v, got %v", newerRemote.Timestamp, s.Clock().Last())
	}

	// Verify DB matches in-memory records
	dbRecords, err := s.LoadRecordsFromDB()
	if err != nil {
		t.Fatalf("failed to load records from DB: %v", err)
	}
	if len(dbRecords) != 3 {
		t.Fatalf("expected 3 records in DB, got %d", len(dbRecords))
	}
}

func TestStorage_CrashRecoveryAndMonotonicity(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "crash_recovery_test.db")
	phys1 := newManualClock(baseTime())
	clock1 := hlc.NewClock(phys1)

	// Step 1: Initialize Storage 1, perform writes and deletions
	s1, err := Open(dbPath, "replica-A", clock1)
	if err != nil {
		t.Fatalf("failed to open s1: %v", err)
	}

	_, err = s1.Set("active_doc", "initial content")
	if err != nil {
		t.Fatalf("s1 Set active_doc failed: %v", err)
	}

	_, err = s1.Set("deleted_doc", "temporary content")
	if err != nil {
		t.Fatalf("s1 Set deleted_doc failed: %v", err)
	}

	// Delete deleted_doc
	tombstone, _, err := s1.Delete("deleted_doc")
	if err != nil {
		t.Fatalf("s1 Delete deleted_doc failed: %v", err)
	}

	preCrashClock := s1.Clock().Last()

	// Step 2: Simulate process crash.
	// We close s1 abruptly, release locks, and discard all in-memory structures.
	if err := s1.Close(); err != nil {
		t.Fatalf("failed to close s1: %v", err)
	}

	// Step 3: Simulate backward clock jump on recovery (e.g. NTP regression or CMOS battery issue)
	// Physical clock on the restarted node is 2 hours BEFORE baseTime!
	skewedPhys := newManualClock(baseTime().Add(-2 * time.Hour))
	restartedClock := hlc.NewClock(skewedPhys)

	// Step 4: Open new Storage instance on the same DB file
	s2, err := Open(dbPath, "replica-A", restartedClock)
	if err != nil {
		t.Fatalf("failed to open s2 after simulated crash: %v", err)
	}
	defer s2.Close()

	// Step 5: Verify in-memory state restoration
	val, ok := s2.Get("active_doc")
	if !ok || val != "initial content" {
		t.Fatalf("expected active_doc to be recovered as 'initial content', got '%s', ok=%v", val, ok)
	}

	val, ok = s2.Get("deleted_doc")
	if ok || val != "" {
		t.Fatalf("expected deleted_doc to remain tombstoned after restart, got '%s', ok=%v", val, ok)
	}

	recTomb, exists := s2.GetRecord("deleted_doc")
	if !exists || !recTomb.Deleted || recTomb.Timestamp != tombstone.Timestamp {
		t.Fatalf("recovered tombstone mismatch: got %+v, expected %+v", recTomb, tombstone)
	}

	if s2.Len() != 1 {
		t.Fatalf("expected Len()=1 after recovery, got %d", s2.Len())
	}
	if s2.TombstoneCount() != 1 {
		t.Fatalf("expected TombstoneCount()=1 after recovery, got %d", s2.TombstoneCount())
	}

	// Step 6: Verify clock monotonicity across crash
	// Despite the physical clock being 2 hours in the past, the seeded clock must not regress.
	recoveredClockTs := s2.Clock().Last()
	if recoveredClockTs.Before(preCrashClock) {
		t.Fatalf("monotonicity violated: recovered clock %v is before pre-crash clock %v", recoveredClockTs, preCrashClock)
	}

	// A new post-recovery write must have a timestamp strictly greater than pre-crash timestamp.
	postCrashRec, err := s2.Set("new_doc", "post crash write")
	if err != nil {
		t.Fatalf("post-crash Set failed: %v", err)
	}
	if !postCrashRec.Timestamp.After(preCrashClock) {
		t.Fatalf("post-crash write timestamp %v must be strictly after pre-crash timestamp %v", postCrashRec.Timestamp, preCrashClock)
	}
}

func TestStorage_ConcurrentReadWriteStress(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "concurrent_stress.db")
	phys := newManualClock(baseTime())
	clock := hlc.NewClock(phys)

	s, err := Open(dbPath, "replica-concurrent", clock)
	if err != nil {
		t.Fatalf("failed to open storage: %v", err)
	}
	defer s.Close()

	numGoroutines := 16
	opsPerGoroutine := 50
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for g := 0; g < numGoroutines; g++ {
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < opsPerGoroutine; i++ {
				key := fmt.Sprintf("key_%d", (gid*7+i)%20)

				switch i % 4 {
				case 0, 1:
					// Set
					val := fmt.Sprintf("val_g%d_i%d", gid, i)
					_, err := s.Set(key, val)
					if err != nil {
						t.Errorf("concurrent Set error: %v", err)
					}
				case 2:
					// Delete
					_, _, err := s.Delete(key)
					if err != nil {
						t.Errorf("concurrent Delete error: %v", err)
					}
				case 3:
					// Read queries
					_ = s.Has(key)
					_, _ = s.Get(key)
					_ = s.Keys()
					_ = s.Values()
				}
			}
		}(g)
	}

	wg.Wait()

	// Verify DB state and in-memory state are strictly consistent
	memRecords := s.Records()
	dbRecords, err := s.LoadRecordsFromDB()
	if err != nil {
		t.Fatalf("failed to load records from DB: %v", err)
	}

	if len(memRecords) != len(dbRecords) {
		t.Fatalf("record count mismatch between memory (%d) and DB (%d)", len(memRecords), len(dbRecords))
	}

	for _, dbRec := range dbRecords {
		memRec, exists := memRecords[dbRec.Key]
		if !exists {
			t.Fatalf("key '%s' found in DB but missing in memory", dbRec.Key)
		}
		if dbRec != memRec {
			t.Fatalf("record mismatch for key '%s': db=%+v, mem=%+v", dbRec.Key, dbRec, memRec)
		}
	}
}

func TestStorage_TransactionalAtomicity(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "atomicity_test.db")
	phys := newManualClock(baseTime())
	clock := hlc.NewClock(phys)

	s, err := Open(dbPath, "node-atomic", clock)
	if err != nil {
		t.Fatalf("failed to open storage: %v", err)
	}
	defer s.Close()

	// Initial write
	_, err = s.Set("key1", "val1")
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// Begin an explicit manual transaction that fails/aborts to verify rollback
	tx, err := s.DB().Begin()
	if err != nil {
		t.Fatalf("failed to begin tx: %v", err)
	}
	_, err = tx.Exec("INSERT INTO records (key, value, wall_time, counter, replica_id, deleted) VALUES (?, ?, ?, ?, ?, ?)",
		"uncommitted_key", "uncommitted_val", 999999, 0, "node-atomic", 0)
	if err != nil {
		t.Fatalf("insert failed: %v", err)
	}
	// Explicit Rollback
	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback failed: %v", err)
	}

	// Verify uncommitted record does not exist on disk
	var count int
	err = s.DB().QueryRow("SELECT count(*) FROM records WHERE key = 'uncommitted_key'").Scan(&count)
	if err != nil && err != sql.ErrNoRows {
		t.Fatalf("query failed: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected rolled back key to not exist in database, count=%d", count)
	}
}
