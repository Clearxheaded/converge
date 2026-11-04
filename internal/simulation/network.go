package simulation

import (
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

func toWSURL(httpURL string) string {
	return "ws" + strings.TrimPrefix(httpURL, "http")
}

// Bridge represents an active, bidirectional proxied WebSocket link between two nodes.
type Bridge struct {
	from       string
	to         string
	clientConn *websocket.Conn
	targetConn *websocket.Conn
	clientMu   sync.Mutex
	targetMu   sync.Mutex
	net        *Network
	closed     chan struct{}
	closeOnce  sync.Once
}

// Close terminates both legs of the proxied connection.
func (b *Bridge) Close() error {
	var err error
	b.closeOnce.Do(func() {
		close(b.closed)
		err1 := b.clientConn.Close()
		err2 := b.targetConn.Close()
		if err1 != nil {
			err = err1
		} else {
			err = err2
		}
		b.net.removeBridge(b)
	})
	return err
}

func (b *Bridge) forward(src, dst *websocket.Conn, dstMu *sync.Mutex, dirFrom, dirTo string) {
	defer func() {
		_ = b.Close()
	}()

	for {
		msgType, data, err := src.ReadMessage()
		if err != nil {
			return
		}

		select {
		case <-b.closed:
			return
		default:
		}

		// Partition check: abort forwarding if this link is blocked.
		if !b.net.CanCommunicate(dirFrom, dirTo) {
			return
		}

		// Packet drop check.
		if b.net.shouldDrop(dirFrom, dirTo) {
			continue
		}

		// Delay injection.
		delay := b.net.getDelay(dirFrom, dirTo)
		if delay > 0 {
			time.Sleep(delay)
		}

		// Message reordering jitter.
		reorderJitter := b.net.getReorder(dirFrom, dirTo)
		if reorderJitter > 0 {
			go func(mt int, payload []byte) {
				extra := time.Duration(b.net.randomFloat() * float64(reorderJitter))
				time.Sleep(extra)
				select {
				case <-b.closed:
					return
				default:
					dstMu.Lock()
					_ = dst.WriteMessage(mt, payload)
					dstMu.Unlock()
				}
			}(msgType, data)
			continue
		}

		dstMu.Lock()
		err = dst.WriteMessage(msgType, data)
		dstMu.Unlock()
		if err != nil {
			return
		}
	}
}

// Network simulates a distributed network with programmatic topology control,
// partitions, packet dropping, message delays, and reordering.
type Network struct {
	mu        sync.RWMutex
	nodes     map[string]string          // replicaID -> target HTTP URL
	blocked   map[string]map[string]bool // [from][to] -> true if communication is blocked
	bridges   map[*Bridge]struct{}
	delays    map[string]time.Duration // "from->to" -> delay
	dropRates map[string]float64       // "from->to" -> drop probability [0.0, 1.0]
	reorders  map[string]time.Duration // "from->to" -> max jitter
	server    *httptest.Server
	upgrader  websocket.Upgrader
	rnd       *rand.Rand
	rndMu     sync.Mutex
}

// NewNetwork creates a new simulated network router.
func NewNetwork() *Network {
	n := &Network{
		nodes:     make(map[string]string),
		blocked:   make(map[string]map[string]bool),
		bridges:   make(map[*Bridge]struct{}),
		delays:    make(map[string]time.Duration),
		dropRates: make(map[string]float64),
		reorders:  make(map[string]time.Duration),
		rnd:       rand.New(rand.NewSource(time.Now().UnixNano())),
		upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool {
				return true
			},
		},
	}
	n.server = httptest.NewServer(http.HandlerFunc(n.handleBridge))
	return n
}

// RegisterNode registers a node ID with its physical HTTP server address.
func (n *Network) RegisterNode(id string, targetURL string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.nodes[id] = targetURL
}

// DialURL returns the simulated proxy WebSocket URL for a connection from node 'from' to node 'to'.
func (n *Network) DialURL(from, to string) string {
	wsBase := toWSURL(n.server.URL)
	return fmt.Sprintf("%s/bridge?from=%s&to=%s", wsBase, from, to)
}

// CanCommunicate reports whether node 'from' can send traffic to node 'to'.
func (n *Network) CanCommunicate(from, to string) bool {
	n.mu.RLock()
	defer n.mu.RUnlock()

	if toMap, exists := n.blocked[from]; exists && toMap[to] {
		return false
	}
	return true
}

// Partition divides nodes into mutually isolated subsets. Nodes in the same subset
// can communicate, but traffic between different subsets is completely blocked.
// Any existing active connections crossing the partition boundary are severed immediately.
func (n *Network) Partition(subsets ...[]string) {
	n.mu.Lock()
	defer n.mu.Unlock()

	subsetMap := make(map[string]int)
	for i, set := range subsets {
		for _, id := range set {
			subsetMap[id] = i
		}
	}

	for _, setA := range subsets {
		for _, idA := range setA {
			if n.blocked[idA] == nil {
				n.blocked[idA] = make(map[string]bool)
			}
			for _, setB := range subsets {
				for _, idB := range setB {
					if subsetMap[idA] != subsetMap[idB] {
						n.blocked[idA][idB] = true
						if n.blocked[idB] == nil {
							n.blocked[idB] = make(map[string]bool)
						}
						n.blocked[idB][idA] = true
					}
				}
			}
		}
	}

	// Sever existing bridges across the partition.
	for b := range n.bridges {
		subA, okA := subsetMap[b.from]
		subB, okB := subsetMap[b.to]
		if okA && okB && subA != subB {
			go func(bridge *Bridge) {
				_ = bridge.Close()
			}(b)
		}
	}
}

// Disconnect cuts bidirectional communication between two specific nodes.
func (n *Network) Disconnect(nodeA, nodeB string) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.blocked[nodeA] == nil {
		n.blocked[nodeA] = make(map[string]bool)
	}
	if n.blocked[nodeB] == nil {
		n.blocked[nodeB] = make(map[string]bool)
	}
	n.blocked[nodeA][nodeB] = true
	n.blocked[nodeB][nodeA] = true

	for b := range n.bridges {
		if (b.from == nodeA && b.to == nodeB) || (b.from == nodeB && b.to == nodeA) {
			go func(bridge *Bridge) {
				_ = bridge.Close()
			}(b)
		}
	}
}

// Isolate cuts a node off from all other nodes in the network.
func (n *Network) Isolate(nodeID string) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.blocked[nodeID] == nil {
		n.blocked[nodeID] = make(map[string]bool)
	}
	for other := range n.nodes {
		if other != nodeID {
			n.blocked[nodeID][other] = true
			if n.blocked[other] == nil {
				n.blocked[other] = make(map[string]bool)
			}
			n.blocked[other][nodeID] = true
		}
	}

	for b := range n.bridges {
		if b.from == nodeID || b.to == nodeID {
			go func(bridge *Bridge) {
				_ = bridge.Close()
			}(b)
		}
	}
}

// Heal removes all partition rules and restores full connectivity between all nodes.
func (n *Network) Heal() {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.blocked = make(map[string]map[string]bool)
	n.delays = make(map[string]time.Duration)
	n.dropRates = make(map[string]float64)
	n.reorders = make(map[string]time.Duration)
}

// SetDelay configures an artificial latency delay for traffic from node 'from' to node 'to'.
func (n *Network) SetDelay(from, to string, d time.Duration) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.delays[fmt.Sprintf("%s->%s", from, to)] = d
}

// SetDropRate configures packet drop probability (0.0 to 1.0) for traffic from 'from' to 'to'.
func (n *Network) SetDropRate(from, to string, rate float64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.dropRates[fmt.Sprintf("%s->%s", from, to)] = rate
}

// SetReorder configures message reordering with random jitter up to maxJitter.
func (n *Network) SetReorder(from, to string, maxJitter time.Duration) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.reorders[fmt.Sprintf("%s->%s", from, to)] = maxJitter
}

func (n *Network) getDelay(from, to string) time.Duration {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.delays[fmt.Sprintf("%s->%s", from, to)]
}

func (n *Network) getReorder(from, to string) time.Duration {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.reorders[fmt.Sprintf("%s->%s", from, to)]
}

func (n *Network) shouldDrop(from, to string) bool {
	n.mu.RLock()
	rate := n.dropRates[fmt.Sprintf("%s->%s", from, to)]
	n.mu.RUnlock()

	if rate <= 0 {
		return false
	}
	if rate >= 1.0 {
		return true
	}
	return n.randomFloat() < rate
}

func (n *Network) randomFloat() float64 {
	n.rndMu.Lock()
	defer n.rndMu.Unlock()
	return n.rnd.Float64()
}

func (n *Network) removeBridge(b *Bridge) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.bridges, b)
}

func (n *Network) handleBridge(w http.ResponseWriter, r *http.Request) {
	from := r.URL.Query().Get("from")
	to := r.URL.Query().Get("to")

	if from == "" || to == "" {
		http.Error(w, "missing from or to query parameters", http.StatusBadRequest)
		return
	}

	if !n.CanCommunicate(from, to) {
		http.Error(w, "network: partitioned", http.StatusServiceUnavailable)
		return
	}

	n.mu.RLock()
	targetURL, exists := n.nodes[to]
	n.mu.RUnlock()

	if !exists {
		http.Error(w, fmt.Sprintf("target node %s not found", to), http.StatusNotFound)
		return
	}

	clientConn, err := n.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}

	targetConn, _, err := websocket.DefaultDialer.Dial(toWSURL(targetURL), nil)
	if err != nil {
		_ = clientConn.Close()
		return
	}

	bridge := &Bridge{
		from:       from,
		to:         to,
		clientConn: clientConn,
		targetConn: targetConn,
		net:        n,
		closed:     make(chan struct{}),
	}

	n.mu.Lock()
	n.bridges[bridge] = struct{}{}
	n.mu.Unlock()

	go bridge.forward(clientConn, targetConn, &bridge.targetMu, from, to)
	go bridge.forward(targetConn, clientConn, &bridge.clientMu, to, from)
}

// Close shuts down the simulated network proxy server and closes all active bridges.
func (n *Network) Close() error {
	n.mu.Lock()
	bridges := make([]*Bridge, 0, len(n.bridges))
	for b := range n.bridges {
		bridges = append(bridges, b)
	}
	n.mu.Unlock()

	for _, b := range bridges {
		_ = b.Close()
	}

	n.server.Close()
	return nil
}
