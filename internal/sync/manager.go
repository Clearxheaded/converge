package sync

import (
	"fmt"
	"net/http"
	"sync"

	"github.com/gorilla/websocket"

	"github.com/Clearxheaded/converge/internal/crdt"
	"github.com/Clearxheaded/converge/internal/hlc"
	"github.com/Clearxheaded/converge/internal/protocol"
	"github.com/Clearxheaded/converge/internal/storage"
)

// SyncManager coordinates peer replication sessions, incremental synchronization,
// and live mutation broadcasting over WebSockets.
type SyncManager struct {
	store        *storage.Storage
	cfg          Config
	upgrader     websocket.Upgrader
	peersMu      sync.RWMutex
	peers        map[string]*Peer
	watermarksMu sync.RWMutex
	watermarks   map[string]hlc.Timestamp
	unsub        func()
	closeMu      sync.Mutex
	closed       bool
}

// NewManager creates a replication manager backed by the provided Storage instance.
func NewManager(store *storage.Storage, cfg Config) *SyncManager {
	m := &SyncManager{
		store:      store,
		cfg:        cfg,
		peers:      make(map[string]*Peer),
		watermarks: make(map[string]hlc.Timestamp),
		upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool {
				return true
			},
		},
	}

	// Subscribe to local storage mutations to broadcast live updates to connected peers.
	m.unsub = store.Subscribe(func(records []crdt.Record, isRemote bool) {
		if isRemote || len(records) == 0 {
			return
		}
		m.Broadcast(protocol.NewUpdatePush(records...))
	})

	return m
}

// Storage returns the underlying Storage engine.
func (m *SyncManager) Storage() *storage.Storage {
	return m.store
}

// ServeHTTP handles incoming HTTP requests and upgrades them to WebSocket replication sessions.
func (m *SyncManager) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	conn, err := m.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}

	peer := newPeer(m, conn, false, m.cfg)
	peer.Start()
}

// Connect initiates an outbound WebSocket connection to a remote replica.
func (m *SyncManager) Connect(wsURL string) (*Peer, error) {
	dialer := websocket.DefaultDialer
	conn, _, err := dialer.Dial(wsURL, nil)
	if err != nil {
		return nil, fmt.Errorf("sync: dial %s: %w", wsURL, err)
	}

	peer := newPeer(m, conn, true, m.cfg)
	peer.Start()
	return peer, nil
}

// registerPeer stores the peer in the active peers map.
func (m *SyncManager) registerPeer(p *Peer) {
	id := p.RemoteID()
	if id == "" {
		return
	}

	m.peersMu.Lock()
	defer m.peersMu.Unlock()

	// If an existing session for this peer is present, close the old one.
	if old, exists := m.peers[id]; exists && old != p {
		_ = old.Close()
	}
	m.peers[id] = p
}

// unregisterPeer removes the peer from the active peers map.
func (m *SyncManager) unregisterPeer(p *Peer) {
	id := p.RemoteID()
	if id == "" {
		return
	}

	m.peersMu.Lock()
	defer m.peersMu.Unlock()

	if cur, exists := m.peers[id]; exists && cur == p {
		delete(m.peers, id)
	}
}

// Peers returns a snapshot list of currently active, handshaked peers.
func (m *SyncManager) Peers() []*Peer {
	m.peersMu.RLock()
	defer m.peersMu.RUnlock()

	result := make([]*Peer, 0, len(m.peers))
	for _, p := range m.peers {
		if p.IsHandshaked() {
			result = append(result, p)
		}
	}
	return result
}

// Peer returns the active peer for the given replicaID, if present.
func (m *SyncManager) Peer(replicaID string) (*Peer, bool) {
	m.peersMu.RLock()
	defer m.peersMu.RUnlock()

	p, exists := m.peers[replicaID]
	return p, exists && p.IsHandshaked()
}

// PeerCount returns the count of currently handshaked peers.
func (m *SyncManager) PeerCount() int {
	return len(m.Peers())
}

// GetWatermark returns the highest observed timestamp for the given replica.
func (m *SyncManager) GetWatermark(replicaID string) hlc.Timestamp {
	m.watermarksMu.RLock()
	defer m.watermarksMu.RUnlock()
	return m.watermarks[replicaID]
}

// SetWatermark records or advances the watermark timestamp for the given replica.
func (m *SyncManager) SetWatermark(replicaID string, ts hlc.Timestamp) {
	m.watermarksMu.Lock()
	defer m.watermarksMu.Unlock()

	current := m.watermarks[replicaID]
	if ts.After(current) {
		m.watermarks[replicaID] = ts
	}
}

// Broadcast sends a replication message to all active, handshaked peers.
func (m *SyncManager) Broadcast(msg *protocol.Message) {
	peers := m.Peers()
	for _, p := range peers {
		_ = p.Send(msg)
	}
}

// Close gracefully shuts down the sync manager, unregisters from storage,
// and terminates all active peer sessions.
func (m *SyncManager) Close() error {
	m.closeMu.Lock()
	defer m.closeMu.Unlock()

	if m.closed {
		return nil
	}
	m.closed = true

	if m.unsub != nil {
		m.unsub()
		m.unsub = nil
	}

	m.peersMu.Lock()
	peers := make([]*Peer, 0, len(m.peers))
	for _, p := range m.peers {
		peers = append(peers, p)
	}
	m.peers = make(map[string]*Peer)
	m.peersMu.Unlock()

	for _, p := range peers {
		_ = p.Close()
	}

	return nil
}
