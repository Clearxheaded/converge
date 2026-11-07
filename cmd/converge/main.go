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
	fmt.Println("  --no-color      Disable ANSI color escape formatting")
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
	noColor := fs.Bool("no-color", false, "Disable ANSI color formatting")
	_ = fs.Parse(args)

	ui := NewUI(*noColor)
	scanner := bufio.NewScanner(os.Stdin)
	pause := func(stepName string) {
		if *autoMode {
			time.Sleep(*stepDelay)
			return
		}
		fmt.Printf("\n  %s %s [Press Enter to continue: %s] ", ui.Cyan("▶"), ui.Bold("PAUSED"), ui.Dim(stepName))
		scanner.Scan()
		fmt.Println()
	}

	ui.Banner("CONVERGE DISTRIBUTED ENGINE DEMO", "Deterministic Local-First Synchronization · 3-Node Cluster Mesh")

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
	ui.PhaseHeader(1, 5, "Cluster Bootstrap & Mesh Topology")
	ui.CardLine("Initializing 3 independent replicas with isolated SQLite WAL stores in:")
	ui.CardLine("  %s", ui.Dim(tempDir))
	ui.CardLine("")
	for _, id := range nodes {
		node := cluster.Node(id)
		colorFn := ui.Cyan
		if id == "eu-west" {
			colorFn = ui.Magenta
		} else if id == "ap-south" {
			colorFn = ui.Green
		}
		ui.Bullet(colorFn, id, "Listen: %s  [DB: %s.db (WAL)]", ui.Dim(node.Server.URL), id)
	}

	cluster.ConnectAll()
	time.Sleep(100 * time.Millisecond)
	ui.CardLine("")
	ui.CardLine("Topology Status: %s", ui.Bold(ui.Green("FULL MESH [3 bidirectional WebSocket links established]")))
	ui.PhaseEnd()

	pause("Proceed to Online Replication")

	// Phase 2: Online Replication
	ui.PhaseHeader(2, 5, "Real-Time Mesh Replication")
	ui.StepAction("us-east", "writes key %s -> %s", ui.Bold("user:alice"), ui.Cyan(`{"name": "Alice", "role": "admin"}`))
	_, err = cluster.Node("us-east").Store.Set("user:alice", `{"name": "Alice", "role": "admin"}`)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Set failed: %v\n", err)
		return
	}

	ui.StepAction("eu-west", "writes key %s -> %s", ui.Bold("config:theme"), ui.Cyan("dark_mode"))
	_, err = cluster.Node("eu-west").Store.Set("config:theme", "dark_mode")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Set failed: %v\n", err)
		return
	}

	// Wait for gossip propagation
	time.Sleep(200 * time.Millisecond)

	ui.CardLine("")
	ui.CardLine("State across all replicas after live gossip:")
	printClusterStateTable(ui, cluster, nodes)
	ui.PhaseEnd()

	pause("Proceed to Split-Brain Network Partition")

	// Phase 3: Split-Brain Network Partition
	ui.PhaseHeader(3, 5, "Split-Brain Network Partition & Offline Writes")
	ui.PartitionDiagram([]string{"us-east", "eu-west"}, []string{"ap-south"})

	cluster.Partition([]string{"us-east", "eu-west"}, []string{"ap-south"})
	time.Sleep(100 * time.Millisecond)

	ui.CardLine("Writing concurrently to both isolated sides of the network boundary:")

	// Writes on Partition 1
	ui.StepAction("us-east", "sets %s = %s %s", ui.Bold("cluster:mode"), ui.Cyan("'maintenance'"), ui.Dim("(Island A)"))
	_, _ = cluster.Node("us-east").Store.Set("cluster:mode", "maintenance")

	ui.StepAction("eu-west", "sets %s = %s %s", ui.Bold("service:status"), ui.Cyan("'degraded'"), ui.Dim("(Island A)"))
	_, _ = cluster.Node("eu-west").Store.Set("service:status", "degraded")

	// Concurrent conflicting write on Partition 2 (ap-south)
	time.Sleep(10 * time.Millisecond)
	ui.StepAction("ap-south", "sets %s = %s %s", ui.Bold("cluster:mode"), ui.Yellow("'emergency_override'"), ui.Bold(ui.Red("[CONFLICT!]")))
	_, _ = cluster.Node("ap-south").Store.Set("cluster:mode", "emergency_override")

	ui.StepAction("ap-south", "deletes %s %s", ui.Bold("user:alice"), ui.Bold(ui.Red("[DURABLE TOMBSTONE]")))
	_, _, _ = cluster.Node("ap-south").Store.Delete("user:alice")

	time.Sleep(150 * time.Millisecond)

	ui.CardLine("")
	ui.CardLine("Divergent states across partition boundary:")
	printClusterStateTable(ui, cluster, nodes)
	ui.PhaseEnd()

	pause("Proceed to Network Healing and Automatic Convergence")

	// Phase 4: Network Healing & Convergence
	ui.PhaseHeader(4, 5, "Network Healing & Automatic CRDT Convergence")
	ui.HealingDiagram()

	cluster.HealAndConnect()
	cluster.SyncAll()

	err = cluster.WaitForConvergence(4 * time.Second)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Convergence check failed: %v\n", err)
		return
	}

	ui.CardLine("All replicas synchronized and converged deterministically!")
	ui.CardLine("Reconciled cluster state:")
	printClusterStateTable(ui, cluster, nodes)
	ui.PhaseEnd()

	pause("Proceed to SQLite Disk Storage Verification")

	// Phase 5: Deep Storage Inspection
	ui.PhaseHeader(5, 5, "Direct SQLite Disk Verification")
	ui.CardLine("Querying raw records directly from each replica's SQLite database file:")
	printDiskRecordsTable(ui, cluster, nodes)
	ui.PhaseEnd()

	ui.SummaryBox("BIT-FOR-BIT CONVERGENCE ACHIEVED ACROSS ALL NODES", []string{
		"Local mutations commit immediately to SQLite WAL without blocking for network acknowledgements.",
		"Hybrid Logical Clocks preserve strict causal Lamport ordering across independent physical nodes.",
		"LWW-Element-Set CRDT total ordering deterministically resolves concurrent conflicting edits.",
		"Durable tombstones prevent resurrection of deleted items across network partitions.",
	})
}

func printClusterStateTable(u *UI, c *simulation.Cluster, nodeIDs []string) {
	headers := []string{"REPLICA", "ACTIVE KEYS", "SQLITE COMMITTED STATE"}
	rows := make([][]string, 0, len(nodeIDs))

	for _, id := range nodeIDs {
		node := c.Node(id)
		values := node.Store.Values()
		pairs := make([]string, 0, len(values))
		for k, v := range values {
			pairs = append(pairs, fmt.Sprintf("%s=%q", k, v))
		}
		valStr := strings.Join(pairs, ", ")
		if len(pairs) == 0 {
			valStr = u.Dim("(empty)")
		}
		rows = append(rows, []string{
			u.Bold(id),
			fmt.Sprintf("%d keys", len(values)),
			valStr,
		})
	}
	u.Table(headers, rows)
}

func printDiskRecordsTable(u *UI, c *simulation.Cluster, nodeIDs []string) {
	headers := []string{"REPLICA", "KEY", "VALUE", "HLC TIMESTAMP", "ORIGIN", "STATUS"}
	var rows [][]string

	for _, id := range nodeIDs {
		node := c.Node(id)
		records, err := node.Store.LoadRecordsFromDB()
		if err != nil {
			continue
		}
		for _, r := range records {
			status := u.Green("● ACTIVE")
			if r.Deleted {
				status = u.Red("⚰ TOMBSTONE")
			}
			rows = append(rows, []string{
				u.Bold(id),
				r.Key,
				r.Value,
				fmt.Sprintf("%d:%d", r.Timestamp.WallTime, r.Timestamp.Counter),
				r.ReplicaID,
				status,
			})
		}
	}
	u.Table(headers, rows)
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

	ui := NewUI(false)
	ui.Banner(fmt.Sprintf("CONVERGE REPLICA: %s", *replicaID), fmt.Sprintf("Listening on %s · SQLite WAL: %s", *listenAddr, *dbPath))

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

	runRepl(store, mgr, clock, ui)
}

func runRepl(store *storage.Storage, mgr *csync.SyncManager, clock *hlc.Clock, ui *UI) {
	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print(ui.Cyan("> "))
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
				fmt.Printf("%s: %v\n", ui.Red("Error"), err)
			} else {
				fmt.Printf("%s (HLC: %s)\n", ui.Green("OK"), ui.Cyan(fmt.Sprintf("%d:%d", rec.Timestamp.WallTime, rec.Timestamp.Counter)))
			}

		case "get":
			if len(parts) < 2 {
				fmt.Println("Usage: get <key>")
				continue
			}
			key := parts[1]
			val, ok := store.Get(key)
			if !ok {
				fmt.Println(ui.Dim("(nil - key not found or deleted)"))
			} else {
				fmt.Println(val)
			}

		case "delete", "del":
			if len(parts) < 2 {
				fmt.Println("Usage: delete <key>")
				continue
			}
			key := parts[1]
			rec, wasActive, err := store.Delete(key)
			if err != nil {
				fmt.Printf("%s: %v\n", ui.Red("Error"), err)
			} else {
				fmt.Printf("%s (Tombstoned at HLC: %s, wasActive: %v)\n",
					ui.Yellow("OK"),
					ui.Cyan(fmt.Sprintf("%d:%d", rec.Timestamp.WallTime, rec.Timestamp.Counter)),
					wasActive,
				)
			}

		case "list":
			values := store.Values()
			if len(values) == 0 {
				fmt.Println(ui.Dim("(no active keys)"))
				continue
			}
			headers := []string{"KEY", "COMMITTED VALUE"}
			var rows [][]string
			for k, v := range values {
				rows = append(rows, []string{k, v})
			}
			ui.Table(headers, rows)

		case "dump":
			records, err := store.LoadRecordsFromDB()
			if err != nil {
				fmt.Printf("%s reading database: %v\n", ui.Red("Error"), err)
				continue
			}
			if len(records) == 0 {
				fmt.Println(ui.Dim("(empty database)"))
				continue
			}
			headers := []string{"KEY", "VALUE", "HLC TIMESTAMP", "REPLICA", "STATUS"}
			var rows [][]string
			for _, r := range records {
				status := ui.Green("● ACTIVE")
				if r.Deleted {
					status = ui.Red("⚰ TOMBSTONE")
				}
				rows = append(rows, []string{
					r.Key,
					r.Value,
					fmt.Sprintf("%d:%d", r.Timestamp.WallTime, r.Timestamp.Counter),
					r.ReplicaID,
					status,
				})
			}
			ui.Table(headers, rows)

		case "peers":
			peers := mgr.Peers()
			if len(peers) == 0 {
				fmt.Println(ui.Dim("No connected peers."))
				continue
			}
			headers := []string{"PEER ID", "HLC WATERMARK", "DIRECTION"}
			var rows [][]string
			for _, p := range peers {
				wm := mgr.GetWatermark(p.RemoteID())
				dir := ui.Cyan("Inbound")
				if p.IsOutbound() {
					dir = ui.Magenta("Outbound")
				}
				rows = append(rows, []string{
					ui.Bold(p.RemoteID()),
					fmt.Sprintf("%d:%d", wm.WallTime, wm.Counter),
					dir,
				})
			}
			ui.Table(headers, rows)

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
