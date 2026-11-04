package main

import (
	"bufio"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/Clearxheaded/converge/internal/hlc"
	"github.com/Clearxheaded/converge/internal/simulation"
	"github.com/Clearxheaded/converge/internal/storage"
	csync "github.com/Clearxheaded/converge/internal/sync"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	command := os.Args[1]
	switch command {
	case "demo":
		runDemo(os.Args[2:])
	case "node":
		runNode(os.Args[2:])
	case "help", "-h", "--help":
		printUsage()
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n\n", command)
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println("Converge - Local-First Distributed Synchronization Engine")
	fmt.Println()
	fmt.Println("Usage:")
	fmt.Println("  converge demo [options]   Run multi-node offline/partition/convergence demo")
	fmt.Println("  converge node [options]   Run a standalone node with interactive REPL")
	fmt.Println("  converge help             Display this help message")
	fmt.Println()
	fmt.Println("Demo Options:")
	fmt.Println("  --auto          Run automatically without prompting between steps")
	fmt.Println("  --delay <dur>   Delay between steps in auto mode (default: 1200ms)")
	fmt.Println()
	fmt.Println("Node Options:")
	fmt.Println("  --id <string>       Replica identifier (default: node-1)")
	fmt.Println("  --db <path>         SQLite database path (default: node-1.db)")
	fmt.Println("  --listen <addr>     Listen address for WebSockets (default: :8001)")
	fmt.Println("  --peers <urls>      Comma-separated peer WebSocket URLs to dial")
}

// -----------------------------------------------------------------------------
// Interactive / Automated Multi-Node Demo
// -----------------------------------------------------------------------------

func runDemo(args []string) {
	fs := flag.NewFlagSet("demo", flag.ExitOnError)
	autoMode := fs.Bool("auto", false, "Run automatically without interactive pauses")
	stepDelay := fs.Duration("delay", 1200*time.Millisecond, "Step delay in auto mode")
	_ = fs.Parse(args)

	scanner := bufio.NewScanner(os.Stdin)
	pause := func(stepName string) {
		if *autoMode {
			time.Sleep(*stepDelay)
			return
		}
		fmt.Printf("\n[Press Enter to continue: %s] ", stepName)
		scanner.Scan()
		fmt.Println()
	}

	printBanner("CONVERGE DISTRIBUTED ENGINE DEMO", "=")
	fmt.Println("This demo shows 3 independent nodes communicating over WebSockets,")
	fmt.Println("handling concurrent offline writes during a split-brain network partition,")
	fmt.Println("and deterministically converging to identical state upon network healing.")
	fmt.Println()

	tempDir, err := os.MkdirTemp("", "converge-demo-*")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create temp dir: %v\n", err)
		os.Exit(1)
	}
	defer os.RemoveAll(tempDir)

	nodes := []string{"us-east", "eu-west", "ap-south"}
	cluster, err := simulation.NewStandaloneCluster(tempDir, nodes...)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize demo cluster: %v\n", err)
		os.Exit(1)
	}
	defer cluster.Close()

	// Phase 1: Cluster Topology Initialization
	printSection("Phase 1: Cluster Initialization")
	fmt.Printf("Created 3 independent replicas with isolated SQLite WAL stores in:\n  %s\n", tempDir)
	for _, id := range nodes {
		node := cluster.Node(id)
		fmt.Printf("  - Node [%s] listening on %s (DB: %s.db)\n", id, node.Server.URL, id)
	}

	cluster.ConnectAll()
	time.Sleep(100 * time.Millisecond)
	fmt.Println("\nConnected all nodes into a full mesh topology.")

	pause("Proceed to Online Replication")

	// Phase 2: Online Replication
	printSection("Phase 2: Real-Time Replication Across Mesh")
	fmt.Println("1. Node [us-east] writes key 'user:alice' -> '{\"name\": \"Alice\", \"role\": \"admin\"}'")
	_, err = cluster.Node("us-east").Store.Set("user:alice", `{"name": "Alice", "role": "admin"}`)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Set failed: %v\n", err)
		return
	}

	fmt.Println("2. Node [eu-west] writes key 'config:theme' -> 'dark_mode'")
	_, err = cluster.Node("eu-west").Store.Set("config:theme", "dark_mode")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Set failed: %v\n", err)
		return
	}

	// Wait for gossip propagation
	time.Sleep(200 * time.Millisecond)

	fmt.Println("\nState across all nodes after online streaming:")
	printClusterState(cluster, nodes)

	pause("Proceed to Split-Brain Network Partition")

	// Phase 3: Split-Brain Network Partition
	printSection("Phase 3: Network Partition & Split-Brain Writes")
	fmt.Println("Injecting symmetrical network partition:")
	fmt.Println("  Partition Island 1: { us-east, eu-west }")
	fmt.Println("  Partition Island 2: { ap-south } (isolated)")

	cluster.Partition([]string{"us-east", "eu-west"}, []string{"ap-south"})
	time.Sleep(100 * time.Millisecond)

	fmt.Println("\nWriting concurrently to both isolated sides of the partition...")

	// Writes on Partition 1
	fmt.Println("  [us-east]  sets 'cluster:mode' = 'maintenance'")
	_, _ = cluster.Node("us-east").Store.Set("cluster:mode", "maintenance")

	fmt.Println("  [eu-west]  sets 'service:status' = 'degraded'")
	_, _ = cluster.Node("eu-west").Store.Set("service:status", "degraded")

	// Concurrent conflicting write on Partition 2 (ap-south)
	time.Sleep(10 * time.Millisecond)
	fmt.Println("  [ap-south] sets 'cluster:mode' = 'emergency_override' (conflicts with us-east!)")
	_, _ = cluster.Node("ap-south").Store.Set("cluster:mode", "emergency_override")

	fmt.Println("  [ap-south] deletes 'user:alice' (creates a deletion tombstone)")
	_, _, _ = cluster.Node("ap-south").Store.Delete("user:alice")

	time.Sleep(150 * time.Millisecond)

	fmt.Println("\nDivergent states during network partition:")
	printClusterState(cluster, nodes)

	fmt.Println("Notice:")
	fmt.Println("  - Island 1 has 'cluster:mode = maintenance' and retains 'user:alice'")
	fmt.Println("  - Island 2 has 'cluster:mode = emergency_override' and tombstoned 'user:alice'")
	fmt.Println("  - Island 2 never received 'service:status = degraded'")

	pause("Proceed to Network Healing and Automatic Convergence")

	// Phase 4: Network Healing & Convergence
	printSection("Phase 4: Network Healing & Automatic CRDT Convergence")
	fmt.Println("Healing network partition and restoring full mesh communication...")
	cluster.HealAndConnect()
	cluster.SyncAll()

	err = cluster.WaitForConvergence(4 * time.Second)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Convergence check failed: %v\n", err)
		return
	}

	fmt.Println("All nodes synchronized and converged deterministically!")
	fmt.Println("\nConverged state across all nodes:")
	printClusterState(cluster, nodes)

	// Phase 5: Deep Storage Inspection
	printSection("Phase 5: Direct SQLite Disk Verification")
	fmt.Println("Inspecting raw records directly from each node's SQLite database file:")
	for _, id := range nodes {
		node := cluster.Node(id)
		records, err := node.Store.LoadRecordsFromDB()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to load db records from %s: %v\n", id, err)
			continue
		}
		fmt.Printf("\n--- Node [%s] SQLite Records (%d total) ---\n", id, len(records))
		for _, r := range records {
			status := "ACTIVE"
			if r.Deleted {
				status = "TOMBSTONE"
			}
			fmt.Printf("  Key: %-15s | Val: %-22s | HLC: %d:%-3d | Replica: %-8s | %s\n",
				r.Key, r.Value, r.Timestamp.WallTime, r.Timestamp.Counter, r.ReplicaID, status)
		}
	}

	printBanner("DEMO COMPLETE - 100% BIT-FOR-BIT CONVERGENCE ACHIEVED", "=")
	fmt.Println("Key takeaways:")
	fmt.Println("1. Local mutations commit immediately to SQLite without waiting for network acknowledgements.")
	fmt.Println("2. Hybrid Logical Clocks preserve causal Lamport ordering across independent machines.")
	fmt.Println("3. LWW register CRDT rules deterministically resolve concurrent conflicting writes.")
	fmt.Println("4. Durable tombstones prevent resurrection of deleted items across partition boundaries.")
	fmt.Println()
}

func printClusterState(c *simulation.Cluster, nodeIDs []string) {
	for _, id := range nodeIDs {
		node := c.Node(id)
		values := node.Store.Values()
		fmt.Printf("  Node [%-8s] Active keys (%d): ", id, len(values))
		if len(values) == 0 {
			fmt.Println("(empty)")
			continue
		}
		pairs := make([]string, 0, len(values))
		for k, v := range values {
			pairs = append(pairs, fmt.Sprintf("%s=%q", k, v))
		}
		fmt.Println(strings.Join(pairs, ", "))
	}
}

func printBanner(title, borderChar string) {
	width := 72
	border := strings.Repeat(borderChar, width)
	fmt.Println(border)
	pad := (width - len(title) - 2) / 2
	if pad < 0 {
		pad = 0
	}
	fmt.Printf("%s %s\n", strings.Repeat(" ", pad), title)
	fmt.Println(border)
}

func printSection(title string) {
	fmt.Printf("\n=== %s ===\n\n", title)
}

// -----------------------------------------------------------------------------
// Standalone Node with Interactive REPL
// -----------------------------------------------------------------------------

func runNode(args []string) {
	fs := flag.NewFlagSet("node", flag.ExitOnError)
	replicaID := fs.String("id", "node-1", "Replica identifier")
	dbPath := fs.String("db", "", "SQLite database path (default: <id>.db)")
	listenAddr := fs.String("listen", ":8001", "Listen address for WebSocket server")
	peersList := fs.String("peers", "", "Comma-separated peer WebSocket URLs to dial")
	_ = fs.Parse(args)

	if *dbPath == "" {
		*dbPath = *replicaID + ".db"
	}

	// Ensure parent directory exists
	if dir := filepath.Dir(*dbPath); dir != "." && dir != "" {
		_ = os.MkdirAll(dir, 0755)
	}

	clock := hlc.NewClock(hlc.WallClock{})
	store, err := storage.Open(*dbPath, *replicaID, clock)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to open storage at %s: %v\n", *dbPath, err)
		os.Exit(1)
	}
	defer store.Close()

	cfg := csync.DefaultConfig()
	mgr := csync.NewManager(store, cfg)
	defer mgr.Close()

	// Start HTTP server for incoming peer connections
	httpServer := &http.Server{
		Addr:    *listenAddr,
		Handler: mgr,
	}

	go func() {
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Fprintf(os.Stderr, "HTTP server error: %v\n", err)
		}
	}()
	defer httpServer.Close()

	printBanner(fmt.Sprintf("CONVERGE NODE: %s", *replicaID), "=")
	fmt.Printf("Database: %s\n", *dbPath)
	fmt.Printf("Listening: %s\n", *listenAddr)
	fmt.Printf("HLC WallTime: %d ms\n", clock.Now().WallTime)
	fmt.Println("Type 'help' for available commands or 'exit' to quit.")
	fmt.Println()

	// Connect to initial peers if provided
	if *peersList != "" {
		urls := strings.Split(*peersList, ",")
		for _, u := range urls {
			u = strings.TrimSpace(u)
			if u == "" {
				continue
			}
			fmt.Printf("Connecting to peer %s... ", u)
			peer, err := mgr.Connect(u)
			if err != nil {
				fmt.Printf("failed: %v\n", err)
			} else {
				go func(p *csync.Peer) {
					if err := p.WaitHandshake(3 * time.Second); err != nil {
						fmt.Printf("\nHandshake with %s failed: %v\n> ", p.RemoteID(), err)
					}
				}(peer)
				fmt.Println("dialed (handshaking in background).")
			}
		}
	}

	// Handle graceful termination signals
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigCh
		fmt.Println("\nReceived interrupt. Shutting down gracefully...")
		_ = mgr.Close()
		_ = store.Close()
		_ = httpServer.Close()
		os.Exit(0)
	}()

	runRepl(store, mgr, clock)
}

func runRepl(store *storage.Storage, mgr *csync.SyncManager, clock *hlc.Clock) {
	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			break
		}
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		parts := strings.SplitN(line, " ", 3)
		cmd := strings.ToLower(parts[0])

		switch cmd {
		case "help":
			fmt.Println("Available commands:")
			fmt.Println("  set <key> <val>       Set a key-value pair")
			fmt.Println("  get <key>             Get value for key")
			fmt.Println("  delete <key>          Delete a key (create tombstone)")
			fmt.Println("  list                  List all active key-value pairs")
			fmt.Println("  dump                  Dump all records including tombstones")
			fmt.Println("  peers                 Show connected peers")
			fmt.Println("  connect <ws_url>      Connect to remote peer WebSocket URL")
			fmt.Println("  sync                  Trigger catch-up sync with all peers")
			fmt.Println("  info                  Show node info and clock state")
			fmt.Println("  exit, quit            Exit node")

		case "set":
			if len(parts) < 3 {
				fmt.Println("Usage: set <key> <value>")
				continue
			}
			key := parts[1]
			val := parts[2]
			rec, err := store.Set(key, val)
			if err != nil {
				fmt.Printf("Error: %v\n", err)
			} else {
				fmt.Printf("OK (HLC: %d:%d)\n", rec.Timestamp.WallTime, rec.Timestamp.Counter)
			}

		case "get":
			if len(parts) < 2 {
				fmt.Println("Usage: get <key>")
				continue
			}
			key := parts[1]
			val, ok := store.Get(key)
			if !ok {
				fmt.Println("(nil - key not found or deleted)")
			} else {
				fmt.Printf("%s\n", val)
			}

		case "delete", "del":
			if len(parts) < 2 {
				fmt.Println("Usage: delete <key>")
				continue
			}
			key := parts[1]
			rec, wasActive, err := store.Delete(key)
			if err != nil {
				fmt.Printf("Error: %v\n", err)
			} else {
				fmt.Printf("OK (Tombstoned at HLC: %d:%d, wasActive: %v)\n", rec.Timestamp.WallTime, rec.Timestamp.Counter, wasActive)
			}

		case "list":
			values := store.Values()
			if len(values) == 0 {
				fmt.Println("(no active keys)")
				continue
			}
			for k, v := range values {
				fmt.Printf("  %-20s = %s\n", k, v)
			}

		case "dump":
			records, err := store.LoadRecordsFromDB()
			if err != nil {
				fmt.Printf("Error reading database: %v\n", err)
				continue
			}
			if len(records) == 0 {
				fmt.Println("(empty database)")
				continue
			}
			fmt.Printf("Total records in SQLite: %d\n", len(records))
			for _, r := range records {
				status := "ACTIVE"
				if r.Deleted {
					status = "TOMBSTONE"
				}
				fmt.Printf("  Key: %-15s | Val: %-20s | HLC: %d:%-3d | Replica: %-8s | %s\n",
					r.Key, r.Value, r.Timestamp.WallTime, r.Timestamp.Counter, r.ReplicaID, status)
			}

		case "peers":
			peers := mgr.Peers()
			if len(peers) == 0 {
				fmt.Println("No connected peers.")
				continue
			}
			fmt.Printf("Connected peers (%d):\n", len(peers))
			for _, p := range peers {
				wm := mgr.GetWatermark(p.RemoteID())
				fmt.Printf("  - %-12s (Watermark: %d:%d, Inbound: %v)\n",
					p.RemoteID(), wm.WallTime, wm.Counter, !p.IsOutbound())
			}

		case "connect":
			if len(parts) < 2 {
				fmt.Println("Usage: connect <ws_url>")
				continue
			}
			url := parts[1]
			fmt.Printf("Connecting to %s... ", url)
			peer, err := mgr.Connect(url)
			if err != nil {
				fmt.Printf("failed: %v\n", err)
			} else {
				go func(p *csync.Peer) {
					if err := p.WaitHandshake(3 * time.Second); err != nil {
						fmt.Printf("\nHandshake failed with %s: %v\n> ", url, err)
					} else {
						fmt.Printf("\nConnected and handshaked with %s!\n> ", p.RemoteID())
					}
				}(peer)
				fmt.Println("dialing...")
			}

		case "sync":
			mgr.SyncAll()
			fmt.Println("Sent SyncRequest to all handshaked peers.")

		case "info":
			ts := clock.Now()
			fmt.Printf("Replica ID:   %s\n", store.ReplicaID())
			fmt.Printf("HLC Clock:    %d ms (Counter: %d)\n", ts.WallTime, ts.Counter)
			fmt.Printf("Active Keys:  %d\n", store.Len())
			fmt.Printf("Peers:        %d\n", mgr.PeerCount())

		case "exit", "quit":
			fmt.Println("Goodbye.")
			return

		default:
			fmt.Printf("Unknown command: %s. Type 'help' for instructions.\n", cmd)
		}
	}
}
