package simulation

import (
	"fmt"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/Clearxheaded/converge/internal/crdt"
	"github.com/Clearxheaded/converge/internal/hlc"
	"github.com/Clearxheaded/converge/internal/storage"
	csync "github.com/Clearxheaded/converge/internal/sync"
)

// ClusterNode represents a single independent node in the simulated cluster,
// containing its own durable SQLite storage engine, HLC clock, and WebSocket sync manager.
type ClusterNode struct {
	ID      string
	Store   *storage.Storage
	Manager *csync.SyncManager
	Server  *httptest.Server
	DBPath  string
	Clock   *hlc.Clock
}

// Cluster manages a set of interconnected ClusterNodes attached to a simulated Network.
type Cluster struct {
	t        *testing.T
	Network  *Network
	nodes    map[string]*ClusterNode
	nodeList []*ClusterNode
	mu       sync.Mutex
	closed   bool
}

// NewCluster spins up an in-process cluster with independent SQLite stores and WebSocket endpoints.
func NewCluster(t *testing.T, nodeIDs ...string) *Cluster {
	net := NewNetwork()

	c := &Cluster{
		t:       t,
		Network: net,
		nodes:   make(map[string]*ClusterNode),
	}

	tempDir := t.TempDir()

	for _, id := range nodeIDs {
		clock := hlc.NewClock(hlc.WallClock{})
		dbPath := filepath.Join(tempDir, id+".db")

		store, err := storage.Open(dbPath, id, clock)
		if err != nil {
			t.Fatalf("cluster: open storage for node %s: %v", id, err)
		}

		mgr := csync.NewManager(store, csync.FastConfig())
		server := httptest.NewServer(mgr)

		net.RegisterNode(id, server.URL)

		node := &ClusterNode{
			ID:      id,
			Store:   store,
			Manager: mgr,
			Server:  server,
			DBPath:  dbPath,
			Clock:   clock,
		}

		c.nodes[id] = node
		c.nodeList = append(c.nodeList, node)
	}

	return c
}

// Node returns the ClusterNode with the specified replica ID.
func (c *Cluster) Node(id string) *ClusterNode {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.nodes[id]
}

// Nodes returns a slice of all nodes in the cluster.
func (c *Cluster) Nodes() []*ClusterNode {
	c.mu.Lock()
	defer c.mu.Unlock()
	res := make([]*ClusterNode, len(c.nodeList))
	copy(res, c.nodeList)
	return res
}

// Connect establishes a bidirectional WebSocket replication link from 'fromID' to 'toID'
// through the simulated network proxy.
func (c *Cluster) Connect(fromID, toID string) (*csync.Peer, error) {
	nodeFrom := c.Node(fromID)
	if nodeFrom == nil {
		return nil, fmt.Errorf("unknown node: %s", fromID)
	}
	if c.Node(toID) == nil {
		return nil, fmt.Errorf("unknown node: %s", toID)
	}

	if p, exists := nodeFrom.Manager.Peer(toID); exists {
		return p, nil
	}

	dialURL := c.Network.DialURL(fromID, toID)
	peer, err := nodeFrom.Manager.Connect(dialURL)
	if err != nil {
		return nil, fmt.Errorf("cluster connect %s -> %s: %w", fromID, toID, err)
	}

	if err := peer.WaitHandshake(2 * time.Second); err != nil {
		return nil, fmt.Errorf("handshake %s -> %s: %w", fromID, toID, err)
	}

	return peer, nil
}

// ConnectAll forms a full mesh topology by connecting every pair of nodes
// that is allowed to communicate under current network partition rules.
func (c *Cluster) ConnectAll() {
	nodes := c.Nodes()
	for i := 0; i < len(nodes); i++ {
		for j := i + 1; j < len(nodes); j++ {
			from := nodes[i].ID
			to := nodes[j].ID
			if c.Network.CanCommunicate(from, to) {
				_, _ = c.Connect(from, to)
			}
		}
	}
}

// Disconnect severs the connection between two nodes.
func (c *Cluster) Disconnect(fromID, toID string) {
	c.Network.Disconnect(fromID, toID)
}

// Partition divides the cluster into mutually isolated network partitions.
func (c *Cluster) Partition(subsets ...[]string) {
	c.Network.Partition(subsets...)
}

// Isolate disconnects a single node from all others.
func (c *Cluster) Isolate(nodeID string) {
	c.Network.Isolate(nodeID)
}

// Heal restores full connectivity across the entire network.
func (c *Cluster) Heal() {
	c.Network.Heal()
}

// HealAndConnect heals all partitions and re-establishes a full mesh across all nodes.
func (c *Cluster) HealAndConnect() {
	c.Heal()
	c.ConnectAll()
}

// SyncAll triggers an incremental sync request across all handshaked peers in the cluster.
func (c *Cluster) SyncAll() {
	nodes := c.Nodes()
	for _, n := range nodes {
		n.Manager.SyncAll()
	}
}

// AssertConverged polls the cluster until all nodes have converged to bit-for-bit identical
// states in memory (values and tombstones) and in SQLite disk records, or fails the test.
func (c *Cluster) AssertConverged(timeout time.Duration) {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	var lastErr error

	for time.Now().Before(deadline) {
		lastErr = c.checkConvergence()
		if lastErr == nil {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}

	c.t.Fatalf("cluster failed to converge after %v: %v", timeout, lastErr)
}

func (c *Cluster) checkConvergence() error {
	nodes := c.Nodes()
	if len(nodes) <= 1 {
		return nil
	}

	ref := nodes[0]
	refValues := ref.Store.Values()
	refRecords := ref.Store.Records()
	refDBRecords, err := ref.Store.LoadRecordsFromDB()
	if err != nil {
		return fmt.Errorf("load db records from %s: %w", ref.ID, err)
	}

	sortRecordsByKey(refDBRecords)

	for i := 1; i < len(nodes); i++ {
		other := nodes[i]

		// 1. Compare in-memory active key-value pairs
		otherValues := other.Store.Values()
		if !reflect.DeepEqual(refValues, otherValues) {
			return fmt.Errorf("active values mismatch between %s and %s (ref: %v, other: %v)",
				ref.ID, other.ID, refValues, otherValues)
		}

		// 2. Compare in-memory full records including tombstones
		otherRecords := other.Store.Records()
		if len(refRecords) != len(otherRecords) {
			return fmt.Errorf("total record count mismatch between %s (%d) and %s (%d)",
				ref.ID, len(refRecords), other.ID, len(otherRecords))
		}
		for k, refRec := range refRecords {
			otherRec, ok := otherRecords[k]
			if !ok {
				return fmt.Errorf("node %s missing record key %s present in %s", other.ID, k, ref.ID)
			}
			if refRec != otherRec {
				return fmt.Errorf("record mismatch on key %s between %s and %s (ref: %+v, other: %+v)",
					k, ref.ID, other.ID, refRec, otherRec)
			}
		}

		// 3. Compare direct SQLite database records
		otherDBRecords, err := other.Store.LoadRecordsFromDB()
		if err != nil {
			return fmt.Errorf("load db records from %s: %w", other.ID, err)
		}
		sortRecordsByKey(otherDBRecords)

		if len(refDBRecords) != len(otherDBRecords) {
			return fmt.Errorf("db record count mismatch between %s (%d) and %s (%d)",
				ref.ID, len(refDBRecords), other.ID, len(otherDBRecords))
		}
		for idx := range refDBRecords {
			if refDBRecords[idx] != otherDBRecords[idx] {
				return fmt.Errorf("db record mismatch at index %d between %s and %s (ref: %+v, other: %+v)",
					idx, ref.ID, other.ID, refDBRecords[idx], otherDBRecords[idx])
			}
		}
	}

	return nil
}

func sortRecordsByKey(recs []crdt.Record) {
	sort.Slice(recs, func(i, j int) bool {
		return recs[i].Key < recs[j].Key
	})
}

// Close gracefully closes all cluster nodes and the simulated network.
func (c *Cluster) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil
	}
	c.closed = true

	for _, n := range c.nodeList {
		_ = n.Manager.Close()
		n.Server.Close()
		_ = n.Store.Close()
	}

	_ = c.Network.Close()
	return nil
}
