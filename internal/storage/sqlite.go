package storage

import (
	"database/sql"
	"fmt"
	"sync"

	_ "github.com/mattn/go-sqlite3"

	"github.com/Clearxheaded/converge/internal/crdt"
	"github.com/Clearxheaded/converge/internal/hlc"
)

const schemaSQL = `
CREATE TABLE IF NOT EXISTS records (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    wall_time INTEGER NOT NULL,
    counter INTEGER NOT NULL,
    replica_id TEXT NOT NULL,
    deleted INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS clock_state (
    replica_id TEXT PRIMARY KEY,
    wall_time INTEGER NOT NULL,
    counter INTEGER NOT NULL
);
`

// ChangeListener is called when records are committed to storage.
// isRemote indicates whether the write originated from a remote peer merge.
type ChangeListener func(records []crdt.Record, isRemote bool)

// Storage provides durable, transactionally consistent SQLite persistence
// for the LWW register map and Hybrid Logical Clock state.
type Storage struct {
	db          *sql.DB
	replicaID   string
	clock       *hlc.Clock
	memMap      *crdt.LWWMap
	writeMu     sync.Mutex
	subsMu      sync.RWMutex
	subscribers map[uint64]ChangeListener
	nextSubID   uint64
}

// Open opens an SQLite database at path, enables WAL mode, applies schema migrations,
// recovers persisted records and clock state, and seeds the clock.
func Open(path string, replicaID string, clock *hlc.Clock) (*Storage, error) {
	if replicaID == "" {
		return nil, fmt.Errorf("storage: replicaID must not be empty")
	}

	dsn := path
	if path != ":memory:" {
		dsn = fmt.Sprintf("%s?_journal_mode=WAL&_busy_timeout=5000&_synchronous=NORMAL&_foreign_keys=ON", path)
	}

	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, fmt.Errorf("storage: open sqlite db: %w", err)
	}

	// Apply PRAGMAs explicitly for maximum portability across connection strings
	pragmas := []string{
		"PRAGMA journal_mode = WAL;",
		"PRAGMA busy_timeout = 5000;",
		"PRAGMA synchronous = NORMAL;",
		"PRAGMA foreign_keys = ON;",
	}
	for _, p := range pragmas {
		_, _ = db.Exec(p)
	}

	return New(db, replicaID, clock)
}

// New initializes Storage around an existing sql.DB handle, runs schema migrations,
// and executes the crash recovery routine.
func New(db *sql.DB, replicaID string, clock *hlc.Clock) (*Storage, error) {
	if replicaID == "" {
		return nil, fmt.Errorf("storage: replicaID must not be empty")
	}
	if clock == nil {
		clock = hlc.NewClock(hlc.WallClock{})
	}

	if _, err := db.Exec(schemaSQL); err != nil {
		return nil, fmt.Errorf("storage: init schema: %w", err)
	}

	// 1. Recover clock state for this replica
	var clockTs hlc.Timestamp
	row := db.QueryRow("SELECT wall_time, counter FROM clock_state WHERE replica_id = ?", replicaID)
	var wt, cnt int64
	if err := row.Scan(&wt, &cnt); err == nil {
		clockTs = hlc.Timestamp{WallTime: uint64(wt), Counter: uint32(cnt)}
	} else if err != sql.ErrNoRows {
		return nil, fmt.Errorf("storage: read clock state: %w", err)
	}

	// 2. Recover all records (including tombstones)
	rows, err := db.Query("SELECT key, value, wall_time, counter, replica_id, deleted FROM records")
	if err != nil {
		return nil, fmt.Errorf("storage: read records: %w", err)
	}
	defer rows.Close()

	var records []crdt.Record
	maxRecordTs := clockTs
	for rows.Next() {
		var key, val, repID string
		var rwt, rcnt, del int64
		if err := rows.Scan(&key, &val, &rwt, &rcnt, &repID, &del); err != nil {
			return nil, fmt.Errorf("storage: scan record: %w", err)
		}
		ts := hlc.Timestamp{WallTime: uint64(rwt), Counter: uint32(rcnt)}
		rec := crdt.Record{
			Key:       key,
			Value:     val,
			Timestamp: ts,
			ReplicaID: repID,
			Deleted:   del == 1,
		}
		records = append(records, rec)
		if ts.After(maxRecordTs) {
			maxRecordTs = ts
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("storage: iterate records: %w", err)
	}

	// 3. Seed clock with the maximum observed timestamp across clock state and records.
	// This guarantees strict monotonicity across process restarts even if the physical clock jumps backward.
	clock.Seed(maxRecordTs)

	// 4. Populate in-memory map without triggering clock events.
	memMap := crdt.NewLWWMap(replicaID, nil)
	memMap.LoadRecords(records)

	return &Storage{
		db:          db,
		replicaID:   replicaID,
		clock:       clock,
		memMap:      memMap,
		subscribers: make(map[uint64]ChangeListener),
	}, nil
}

// DB returns the underlying sql.DB instance.
func (s *Storage) DB() *sql.DB {
	return s.db
}

// ReplicaID returns the replica identifier for this storage engine.
func (s *Storage) ReplicaID() string {
	return s.replicaID
}

// Clock returns the HLC clock driving this storage instance.
func (s *Storage) Clock() *hlc.Clock {
	return s.clock
}

// Snapshot returns an independent copy of the current in-memory CRDT map.
func (s *Storage) Snapshot() *crdt.LWWMap {
	return s.memMap.Clone()
}

// Close closes the underlying SQLite database connection.
func (s *Storage) Close() error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return s.db.Close()
}

// Set commits a key-value write transactionally to SQLite and updates the in-memory map.
// Both the record and the updated HLC clock state are persisted in the same transaction.
func (s *Storage) Set(key, value string) (crdt.Record, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	ts := s.clock.Now()
	rec := crdt.Record{
		Key:       key,
		Value:     value,
		Timestamp: ts,
		ReplicaID: s.replicaID,
		Deleted:   false,
	}

	tx, err := s.db.Begin()
	if err != nil {
		return crdt.Record{}, fmt.Errorf("storage: begin transaction: %w", err)
	}
	defer tx.Rollback()

	const upsertRecord = `
INSERT INTO records (key, value, wall_time, counter, replica_id, deleted)
VALUES (?, ?, ?, ?, ?, 0)
ON CONFLICT(key) DO UPDATE SET
    value = excluded.value,
    wall_time = excluded.wall_time,
    counter = excluded.counter,
    replica_id = excluded.replica_id,
    deleted = 0;
`
	if _, err := tx.Exec(upsertRecord, rec.Key, rec.Value, int64(rec.Timestamp.WallTime), int64(rec.Timestamp.Counter), rec.ReplicaID); err != nil {
		return crdt.Record{}, fmt.Errorf("storage: upsert record: %w", err)
	}

	const upsertClock = `
INSERT INTO clock_state (replica_id, wall_time, counter)
VALUES (?, ?, ?)
ON CONFLICT(replica_id) DO UPDATE SET
    wall_time = excluded.wall_time,
    counter = excluded.counter;
`
	if _, err := tx.Exec(upsertClock, s.replicaID, int64(ts.WallTime), int64(ts.Counter)); err != nil {
		return crdt.Record{}, fmt.Errorf("storage: upsert clock: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return crdt.Record{}, fmt.Errorf("storage: commit transaction: %w", err)
	}

	s.memMap.SetRecord(rec)
	s.notifyListeners([]crdt.Record{rec}, false)
	return rec, nil
}

// Delete commits a deletion tombstone transactionally to SQLite and updates the in-memory map.
// Returns the written tombstone record and whether the key was actively present before deletion.
func (s *Storage) Delete(key string) (crdt.Record, bool, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	existing, exists := s.memMap.GetRecord(key)
	wasActive := exists && !existing.Deleted

	ts := s.clock.Now()
	rec := crdt.Record{
		Key:       key,
		Value:     "",
		Timestamp: ts,
		ReplicaID: s.replicaID,
		Deleted:   true,
	}

	tx, err := s.db.Begin()
	if err != nil {
		return crdt.Record{}, false, fmt.Errorf("storage: begin transaction: %w", err)
	}
	defer tx.Rollback()

	const upsertRecord = `
INSERT INTO records (key, value, wall_time, counter, replica_id, deleted)
VALUES (?, ?, ?, ?, ?, 1)
ON CONFLICT(key) DO UPDATE SET
    value = excluded.value,
    wall_time = excluded.wall_time,
    counter = excluded.counter,
    replica_id = excluded.replica_id,
    deleted = 1;
`
	if _, err := tx.Exec(upsertRecord, rec.Key, rec.Value, int64(rec.Timestamp.WallTime), int64(rec.Timestamp.Counter), rec.ReplicaID); err != nil {
		return crdt.Record{}, false, fmt.Errorf("storage: upsert tombstone: %w", err)
	}

	const upsertClock = `
INSERT INTO clock_state (replica_id, wall_time, counter)
VALUES (?, ?, ?)
ON CONFLICT(replica_id) DO UPDATE SET
    wall_time = excluded.wall_time,
    counter = excluded.counter;
`
	if _, err := tx.Exec(upsertClock, s.replicaID, int64(ts.WallTime), int64(ts.Counter)); err != nil {
		return crdt.Record{}, false, fmt.Errorf("storage: upsert clock: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return crdt.Record{}, false, fmt.Errorf("storage: commit transaction: %w", err)
	}

	s.memMap.SetRecord(rec)
	s.notifyListeners([]crdt.Record{rec}, false)
	return rec, wasActive, nil
}

// MergeRecords merges a batch of incoming records transactionally.
// It advances the local clock past all incoming timestamps, identifies records that win
// under CRDT total ordering, commits winners and the advanced clock state to SQLite,
// and updates the in-memory map. Returns the number of winning records applied.
func (s *Storage) MergeRecords(records []crdt.Record) (int, error) {
	if len(records) == 0 {
		return 0, nil
	}

	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	// 1. Advance clock past all incoming timestamps to preserve Lamport causality.
	for _, rec := range records {
		s.clock.Receive(rec.Timestamp)
	}
	latestClock := s.clock.Last()

	// 2. Intra-batch deduplication: resolve any duplicate keys within the incoming batch.
	batchWinners := make(map[string]crdt.Record)
	for _, incoming := range records {
		current, exists := batchWinners[incoming.Key]
		if !exists || crdt.CompareRecords(incoming, current) > 0 {
			batchWinners[incoming.Key] = incoming
		}
	}

	// 3. Determine which batch winners beat current persisted state.
	var winning []crdt.Record
	for _, incoming := range batchWinners {
		existing, exists := s.memMap.GetRecord(incoming.Key)
		if !exists || crdt.CompareRecords(incoming, existing) > 0 {
			winning = append(winning, incoming)
		}
	}

	// 4. Execute transaction: persist winning records and updated clock state.
	tx, err := s.db.Begin()
	if err != nil {
		return 0, fmt.Errorf("storage: begin transaction: %w", err)
	}
	defer tx.Rollback()

	if len(winning) > 0 {
		stmt, err := tx.Prepare(`
INSERT INTO records (key, value, wall_time, counter, replica_id, deleted)
VALUES (?, ?, ?, ?, ?, ?)
ON CONFLICT(key) DO UPDATE SET
    value = excluded.value,
    wall_time = excluded.wall_time,
    counter = excluded.counter,
    replica_id = excluded.replica_id,
    deleted = excluded.deleted;
`)
		if err != nil {
			return 0, fmt.Errorf("storage: prepare upsert record: %w", err)
		}
		defer stmt.Close()

		for _, rec := range winning {
			delInt := 0
			if rec.Deleted {
				delInt = 1
			}
			if _, err := stmt.Exec(rec.Key, rec.Value, int64(rec.Timestamp.WallTime), int64(rec.Timestamp.Counter), rec.ReplicaID, delInt); err != nil {
				return 0, fmt.Errorf("storage: exec upsert record: %w", err)
			}
		}
	}

	const upsertClock = `
INSERT INTO clock_state (replica_id, wall_time, counter)
VALUES (?, ?, ?)
ON CONFLICT(replica_id) DO UPDATE SET
    wall_time = excluded.wall_time,
    counter = excluded.counter;
`
	if _, err := tx.Exec(upsertClock, s.replicaID, int64(latestClock.WallTime), int64(latestClock.Counter)); err != nil {
		return 0, fmt.Errorf("storage: upsert clock: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("storage: commit transaction: %w", err)
	}

	// 5. Update in-memory state for winning records only.
	for _, rec := range winning {
		s.memMap.SetRecord(rec)
	}

	if len(winning) > 0 {
		s.notifyListeners(winning, true)
	}

	return len(winning), nil
}

// Get returns the value of an active key and true, or empty string and false if absent or deleted.
func (s *Storage) Get(key string) (string, bool) {
	return s.memMap.Get(key)
}

// Has reports whether the key exists and has not been tombstoned.
func (s *Storage) Has(key string) bool {
	return s.memMap.Has(key)
}

// GetRecord returns the raw Record for key, including tombstones.
func (s *Storage) GetRecord(key string) (crdt.Record, bool) {
	return s.memMap.GetRecord(key)
}

// Keys returns a sorted slice of active (non-deleted) keys.
func (s *Storage) Keys() []string {
	return s.memMap.Keys()
}

// Values returns a copy of all active key-value pairs.
func (s *Storage) Values() map[string]string {
	return s.memMap.Values()
}

// Records returns a copy of all records indexed by key, including tombstones.
func (s *Storage) Records() map[string]crdt.Record {
	return s.memMap.Records()
}

// AllRecords returns a slice containing a snapshot of all records, including tombstones.
func (s *Storage) AllRecords() []crdt.Record {
	return s.memMap.AllRecords()
}

// Len returns the count of active (non-deleted) keys.
func (s *Storage) Len() int {
	return s.memMap.Len()
}

// TombstoneCount returns the count of deleted keys retained.
func (s *Storage) TombstoneCount() int {
	return s.memMap.TombstoneCount()
}

// RecordCount returns the total number of records, active and tombstones combined.
func (s *Storage) RecordCount() int {
	return s.memMap.RecordCount()
}

// Subscribe registers a callback invoked whenever records are committed to storage.
// Returns an unsubscribe function.
func (s *Storage) Subscribe(listener ChangeListener) func() {
	s.subsMu.Lock()
	defer s.subsMu.Unlock()
	s.nextSubID++
	id := s.nextSubID
	s.subscribers[id] = listener

	return func() {
		s.subsMu.Lock()
		defer s.subsMu.Unlock()
		delete(s.subscribers, id)
	}
}

func (s *Storage) notifyListeners(records []crdt.Record, isRemote bool) {
	s.subsMu.RLock()
	if len(s.subscribers) == 0 {
		s.subsMu.RUnlock()
		return
	}
	listeners := make([]ChangeListener, 0, len(s.subscribers))
	for _, l := range s.subscribers {
		listeners = append(listeners, l)
	}
	s.subsMu.RUnlock()

	for _, l := range listeners {
		l(records, isRemote)
	}
}

// RecordsSince returns all records modified strictly after the given HLC timestamp.
// If since is the zero timestamp, all records are returned.
func (s *Storage) RecordsSince(since hlc.Timestamp) []crdt.Record {
	all := s.memMap.AllRecords()
	if since.IsZero() {
		return all
	}

	var results []crdt.Record
	for _, rec := range all {
		if rec.Timestamp.After(since) {
			results = append(results, rec)
		}
	}
	return results
}

// LoadRecordsFromDB reads all records directly from the database table.
func (s *Storage) LoadRecordsFromDB() ([]crdt.Record, error) {
	rows, err := s.db.Query("SELECT key, value, wall_time, counter, replica_id, deleted FROM records ORDER BY key ASC")
	if err != nil {
		return nil, fmt.Errorf("storage: query records: %w", err)
	}
	defer rows.Close()

	var records []crdt.Record
	for rows.Next() {
		var key, val, repID string
		var wt, cnt, del int64
		if err := rows.Scan(&key, &val, &wt, &cnt, &repID, &del); err != nil {
			return nil, fmt.Errorf("storage: scan record: %w", err)
		}
		records = append(records, crdt.Record{
			Key:       key,
			Value:     val,
			Timestamp: hlc.Timestamp{WallTime: uint64(wt), Counter: uint32(cnt)},
			ReplicaID: repID,
			Deleted:   del == 1,
		})
	}
	return records, rows.Err()
}

// LoadClockFromDB reads the clock state directly from the database table for this replica.
func (s *Storage) LoadClockFromDB() (hlc.Timestamp, error) {
	row := s.db.QueryRow("SELECT wall_time, counter FROM clock_state WHERE replica_id = ?", s.replicaID)
	var wt, cnt int64
	err := row.Scan(&wt, &cnt)
	if err == sql.ErrNoRows {
		return hlc.Timestamp{}, nil
	}
	if err != nil {
		return hlc.Timestamp{}, fmt.Errorf("storage: query clock: %w", err)
	}
	return hlc.Timestamp{WallTime: uint64(wt), Counter: uint32(cnt)}, nil
}

