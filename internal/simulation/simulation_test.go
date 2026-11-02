package simulation

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/Clearxheaded/converge/internal/crdt"
)

func TestSimulation_SplitBrainConcurrentWrites(t *testing.T) {
	cluster := NewCluster(t, "node-a", "node-b", "node-c")
	defer cluster.Close()

	cluster.ConnectAll()

	// Initial sync baseline.
	nodeA := cluster.Node("node-a")
	nodeB := cluster.Node("node-b")
	nodeC := cluster.Node("node-c")

	if _, err := nodeA.Store.Set("init", "genesis"); err != nil {
		t.Fatalf("set init: %v", err)
	}
	cluster.AssertConverged(2 * time.Second)

	// Partition cluster into {node-a, node-b} and {node-c}.
	cluster.Partition([]string{"node-a", "node-b"}, []string{"node-c"})

	// Wait for connections across the partition to be severed.
	time.Sleep(50 * time.Millisecond)

	// Concurrent split-brain writes on both partitions.
	// 1. Conflict on common key.
	recA, err := nodeA.Store.Set("common_key", "val_from_A")
	if err != nil {
		t.Fatalf("nodeA set common_key: %v", err)
	}

	recC, err := nodeC.Store.Set("common_key", "val_from_C")
	if err != nil {
		t.Fatalf("nodeC set common_key: %v", err)
	}

	// 2. Disjoint writes in subset 1.
	if _, err := nodeB.Store.Set("subset1_key", "val_subset1"); err != nil {
		t.Fatalf("nodeB set subset1_key: %v", err)
	}

	// 3. Disjoint writes in subset 2.
	if _, err := nodeC.Store.Set("subset2_key", "val_subset2"); err != nil {
		t.Fatalf("nodeC set subset2_key: %v", err)
	}

	// Wait for in-partition propagation.
	time.Sleep(100 * time.Millisecond)

	// Assert partition isolation: node-c must not see subset1_key; node-a/b must not see subset2_key.
	if nodeC.Store.Has("subset1_key") {
		t.Fatalf("node-c received subset1_key across active partition")
	}
	if nodeA.Store.Has("subset2_key") || nodeB.Store.Has("subset2_key") {
		t.Fatalf("node-a or node-b received subset2_key across active partition")
	}

	// Verify that within partition 1, node-a and node-b converged on val_from_A and val_subset1.
	valBCommon, _ := nodeB.Store.Get("common_key")
	if valBCommon != "val_from_A" {
		t.Fatalf("expected node-b to have 'val_from_A', got '%s'", valBCommon)
	}

	// Heal partition and reconnect all nodes.
	cluster.HealAndConnect()

	// Assert full convergence across all 3 nodes in memory and SQLite.
	cluster.AssertConverged(3 * time.Second)

	// Verify deterministic tie-breaker on common_key.
	expectedWinner := recA
	if crdt.CompareRecords(recC, recA) > 0 {
		expectedWinner = recC
	}

	finalValA, _ := nodeA.Store.Get("common_key")
	finalValB, _ := nodeB.Store.Get("common_key")
	finalValC, _ := nodeC.Store.Get("common_key")

	if finalValA != expectedWinner.Value || finalValB != expectedWinner.Value || finalValC != expectedWinner.Value {
		t.Fatalf("conflict resolution mismatch: expected %s, got A=%s, B=%s, C=%s",
			expectedWinner.Value, finalValA, finalValB, finalValC)
	}
}

func TestSimulation_TransitiveReplicationMultiHop(t *testing.T) {
	cluster := NewCluster(t, "node-1", "node-2", "node-3", "node-4")
	defer cluster.Close()

	// Establish a strict line topology: 1 <-> 2 <-> 3 <-> 4.
	// Nodes 1 and 4 have no direct connection.
	if _, err := cluster.Connect("node-1", "node-2"); err != nil {
		t.Fatalf("connect 1-2: %v", err)
	}
	if _, err := cluster.Connect("node-2", "node-3"); err != nil {
		t.Fatalf("connect 2-3: %v", err)
	}
	if _, err := cluster.Connect("node-3", "node-4"); err != nil {
		t.Fatalf("connect 3-4: %v", err)
	}

	node1 := cluster.Node("node-1")
	node2 := cluster.Node("node-2")
	node4 := cluster.Node("node-4")

	// Node 1 writes. It must transitively reach Node 4 via Nodes 2 and 3.
	if _, err := node1.Store.Set("source_node_1", "propagated_to_node_4"); err != nil {
		t.Fatalf("node 1 set: %v", err)
	}

	cluster.AssertConverged(3 * time.Second)

	val4, ok := node4.Store.Get("source_node_1")
	if !ok || val4 != "propagated_to_node_4" {
		t.Fatalf("node 4 did not receive multi-hop write from node 1: ok=%v, val=%s", ok, val4)
	}

	// Node 4 writes. It must transitively reach Node 1.
	if _, err := node4.Store.Set("source_node_4", "propagated_to_node_1"); err != nil {
		t.Fatalf("node 4 set: %v", err)
	}

	cluster.AssertConverged(3 * time.Second)

	val1, ok := node1.Store.Get("source_node_4")
	if !ok || val1 != "propagated_to_node_1" {
		t.Fatalf("node 1 did not receive multi-hop write from node 4: ok=%v, val=%s", ok, val1)
	}

	// Node 2 deletes a key. The deletion tombstone must transitively propagate in both directions.
	if _, _, err := node2.Store.Delete("source_node_1"); err != nil {
		t.Fatalf("node 2 delete: %v", err)
	}

	cluster.AssertConverged(3 * time.Second)

	if node1.Store.Has("source_node_1") || node4.Store.Has("source_node_1") {
		t.Fatalf("tombstone failed to propagate transitively across chain")
	}
}

func TestSimulation_ChurnAndIntermittentDisconnections(t *testing.T) {
	cluster := NewCluster(t, "churn-1", "churn-2", "churn-3", "churn-4")
	defer cluster.Close()

	cluster.ConnectAll()

	done := make(chan struct{})
	var wg sync.WaitGroup

	// Background churn worker: randomly disconnects and reconnects links.
	wg.Add(1)
	go func() {
		defer wg.Done()
		r := rand.New(rand.NewSource(42))
		nodes := []string{"churn-1", "churn-2", "churn-3", "churn-4"}

		for {
			select {
			case <-done:
				return
			case <-time.After(25 * time.Millisecond):
				idxA := r.Intn(len(nodes))
				idxB := r.Intn(len(nodes))
				if idxA == idxB {
					continue
				}
				nA, nB := nodes[idxA], nodes[idxB]

				if cluster.Network.CanCommunicate(nA, nB) {
					cluster.Disconnect(nA, nB)
				} else {
					cluster.Network.Heal()
					_, _ = cluster.Connect(nA, nB)
				}
			}
		}
	}()

	// Parallel writers performing concurrent writes across the churn.
	nodes := cluster.Nodes()
	for i := 0; i < len(nodes); i++ {
		wg.Add(1)
		go func(workerID int, node *ClusterNode) {
			defer wg.Done()
			for j := 0; j < 15; j++ {
				key := fmt.Sprintf("churn_key_%d", j%5)
				val := fmt.Sprintf("worker_%d_iter_%d", workerID, j)
				_, _ = node.Store.Set(key, val)
				time.Sleep(10 * time.Millisecond)
			}
		}(i, nodes[i])
	}

	// Let writes run under churn.
	time.Sleep(300 * time.Millisecond)
	close(done)
	wg.Wait()

	// Stop churn, heal all partitions, reconnect, and run reconciliation.
	cluster.HealAndConnect()
	cluster.SyncAll()

	// Verify all 4 nodes converge to identical state in SQLite.
	cluster.AssertConverged(4 * time.Second)
}

func TestSimulation_PacketDelayAndReordering(t *testing.T) {
	cluster := NewCluster(t, "order-a", "order-b")
	defer cluster.Close()

	// Inject 40ms max jitter on traffic from order-a to order-b.
	cluster.Network.SetReorder("order-a", "order-b", 40*time.Millisecond)

	if _, err := cluster.Connect("order-a", "order-b"); err != nil {
		t.Fatalf("connect: %v", err)
	}

	nodeA := cluster.Node("order-a")
	nodeB := cluster.Node("order-b")

	// Rapidly write multiple sequential versions of the same key on node-a.
	for i := 1; i <= 10; i++ {
		val := fmt.Sprintf("version_%02d", i)
		if _, err := nodeA.Store.Set("sequenced_key", val); err != nil {
			t.Fatalf("set seq %d: %v", i, err)
		}
		time.Sleep(5 * time.Millisecond)
	}

	// Even though packets may arrive out of order, the highest HLC timestamp must win.
	cluster.AssertConverged(3 * time.Second)

	finalVal, ok := nodeB.Store.Get("sequenced_key")
	if !ok || finalVal != "version_10" {
		t.Fatalf("expected final version 'version_10', got ok=%v, val='%s'", ok, finalVal)
	}
}

func TestSimulation_PacketDropAndHeal(t *testing.T) {
	cluster := NewCluster(t, "drop-a", "drop-b")
	defer cluster.Close()

	if _, err := cluster.Connect("drop-a", "drop-b"); err != nil {
		t.Fatalf("connect: %v", err)
	}

	nodeA := cluster.Node("drop-a")
	nodeB := cluster.Node("drop-b")

	// Initial write works.
	if _, err := nodeA.Store.Set("key1", "val1"); err != nil {
		t.Fatalf("set key1: %v", err)
	}
	cluster.AssertConverged(2 * time.Second)

	// Set 100% drop rate from A to B (black hole).
	cluster.Network.SetDropRate("drop-a", "drop-b", 1.0)

	// Write key2 on A while packets are dropped.
	if _, err := nodeA.Store.Set("key2", "dropped_update"); err != nil {
		t.Fatalf("set key2: %v", err)
	}

	// Wait briefly: B should not have key2 because UpdatePush was dropped.
	time.Sleep(100 * time.Millisecond)
	if nodeB.Store.Has("key2") {
		t.Fatalf("node-b received key2 despite 100%% drop rate")
	}

	// Restore packet delivery.
	cluster.Network.SetDropRate("drop-a", "drop-b", 0.0)

	// Trigger sync catch-up via watermark.
	if err := nodeB.Manager.Sync("drop-a"); err != nil {
		t.Fatalf("sync catch-up: %v", err)
	}

	cluster.AssertConverged(2 * time.Second)

	val2, ok := nodeB.Store.Get("key2")
	if !ok || val2 != "dropped_update" {
		t.Fatalf("expected node-b to have 'dropped_update', got ok=%v, val='%s'", ok, val2)
	}
}

func TestSimulation_AsymmetricCourierReplication(t *testing.T) {
	cluster := NewCluster(t, "courier-a", "courier-b", "courier-c")
	defer cluster.Close()

	// Topology: Node B acts as courier between isolated nodes A and C.
	// A and C can never communicate directly.
	cluster.Network.Disconnect("courier-a", "courier-c")

	nodeA := cluster.Node("courier-a")
	nodeB := cluster.Node("courier-b")
	nodeC := cluster.Node("courier-c")

	// Phase 1: Courier B visits A (connected A <-> B, disconnected B <-> C).
	cluster.Network.Disconnect("courier-b", "courier-c")
	if _, err := cluster.Connect("courier-a", "courier-b"); err != nil {
		t.Fatalf("connect A-B: %v", err)
	}

	if _, err := nodeA.Store.Set("courier_msg", "payload_from_A"); err != nil {
		t.Fatalf("set A: %v", err)
	}

	// Wait for B to receive from A.
	time.Sleep(100 * time.Millisecond)
	valB, ok := nodeB.Store.Get("courier_msg")
	if !ok || valB != "payload_from_A" {
		t.Fatalf("courier B failed to receive from A: ok=%v, val=%s", ok, valB)
	}

	// Phase 2: Courier B departs from A and visits C (disconnected A <-> B, connected B <-> C).
	cluster.Disconnect("courier-a", "courier-b")
	time.Sleep(50 * time.Millisecond)

	cluster.Network.Heal()
	cluster.Network.Disconnect("courier-a", "courier-c")
	cluster.Network.Disconnect("courier-a", "courier-b")

	if _, err := cluster.Connect("courier-b", "courier-c"); err != nil {
		t.Fatalf("connect B-C: %v", err)
	}

	// B syncs with C, delivering A's payload.
	nodeC.Manager.SyncAll()

	// Wait for C to receive payload from B.
	time.Sleep(150 * time.Millisecond)
	valC, ok := nodeC.Store.Get("courier_msg")
	if !ok || valC != "payload_from_A" {
		t.Fatalf("courier C failed to receive from B: ok=%v, val=%s", ok, valC)
	}
}

