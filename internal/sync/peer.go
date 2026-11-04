package sync

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"github.com/Clearxheaded/converge/internal/protocol"
)

// Peer represents an active WebSocket replication connection with a remote replica.
type Peer struct {
	manager     *SyncManager
	conn        *websocket.Conn
	isOutbound  bool
	cfg         Config
	send        chan []byte
	closed      chan struct{}
	closeOnce   sync.Once
	mu          sync.RWMutex
	remoteID    string
	handshaked  bool
	handshakeCh chan struct{}
	lastError   string
}

func newPeer(manager *SyncManager, conn *websocket.Conn, isOutbound bool, cfg Config) *Peer {
	return &Peer{
		manager:     manager,
		conn:        conn,
		isOutbound:  isOutbound,
		cfg:         cfg,
		send:        make(chan []byte, 128),
		closed:      make(chan struct{}),
		handshakeCh: make(chan struct{}),
	}
}

// Start launches the read and write pumps.
func (p *Peer) Start() {
	go p.writePump()
	go p.readPump()

	if p.isOutbound {
		_ = p.Send(protocol.NewHandshake(p.manager.store.ReplicaID()))
	}
}

// RemoteID returns the peer replica identifier, or empty string if not yet handshaked.
func (p *Peer) RemoteID() string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.remoteID
}

// IsOutbound returns whether this peer connection was initiated locally.
func (p *Peer) IsOutbound() bool {
	return p.isOutbound
}

func (p *Peer) setRemoteID(id string) {
	p.mu.Lock()
	p.remoteID = id
	p.mu.Unlock()
}

// IsHandshaked returns true if the handshake has completed.
func (p *Peer) IsHandshaked() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.handshaked
}

func (p *Peer) markHandshaked() {
	p.mu.Lock()
	if !p.handshaked {
		p.handshaked = true
		close(p.handshakeCh)
	}
	p.mu.Unlock()
}

// WaitHandshake blocks until the handshake completes or timeout expires.
func (p *Peer) WaitHandshake(timeout time.Duration) error {
	select {
	case <-p.handshakeCh:
		return nil
	case <-p.closed:
		return errors.New("peer: connection closed before handshake completed")
	case <-time.After(timeout):
		return errors.New("peer: handshake timeout")
	}
}

// LastError returns the most recent error message received from the peer.
func (p *Peer) LastError() string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.lastError
}

func (p *Peer) setLastError(err string) {
	p.mu.Lock()
	p.lastError = err
	p.mu.Unlock()
}

// Send encodes and queues a message for transmission to the remote peer.
func (p *Peer) Send(msg *protocol.Message) error {
	data, err := protocol.Encode(msg)
	if err != nil {
		return err
	}

	select {
	case <-p.closed:
		return errors.New("peer: connection closed")
	default:
	}

	select {
	case p.send <- data:
		return nil
	case <-p.closed:
		return errors.New("peer: connection closed")
	default:
		return errors.New("peer: send queue full")
	}
}

// Close closes the WebSocket connection and terminates background pumps.
func (p *Peer) Close() error {
	var err error
	p.closeOnce.Do(func() {
		close(p.closed)
		err = p.conn.Close()
		p.manager.unregisterPeer(p)
	})
	return err
}

func (p *Peer) writePump() {
	ticker := time.NewTicker(p.cfg.PingPeriod)
	defer func() {
		ticker.Stop()
		_ = p.Close()
	}()

	for {
		select {
		case data, ok := <-p.send:
			_ = p.conn.SetWriteDeadline(time.Now().Add(p.cfg.WriteWait))
			if !ok {
				_ = p.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			if err := p.conn.WriteMessage(websocket.TextMessage, data); err != nil {
				return
			}

		case <-ticker.C:
			_ = p.conn.SetWriteDeadline(time.Now().Add(p.cfg.WriteWait))
			if err := p.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}

		case <-p.closed:
			return
		}
	}
}

func (p *Peer) readPump() {
	defer func() {
		_ = p.Close()
	}()

	p.conn.SetReadLimit(p.cfg.MaxMessageSize)
	_ = p.conn.SetReadDeadline(time.Now().Add(p.cfg.PongWait))
	p.conn.SetPongHandler(func(string) error {
		_ = p.conn.SetReadDeadline(time.Now().Add(p.cfg.PongWait))
		return nil
	})

	for {
		_, data, err := p.conn.ReadMessage()
		if err != nil {
			return
		}

		msg, err := protocol.Decode(data)
		if err != nil {
			_ = p.Send(protocol.NewError(protocol.ErrCodeInvalidMessage, err.Error()))
			continue
		}

		if err := p.handleMessage(msg); err != nil {
			return
		}
	}
}

func (p *Peer) handleMessage(msg *protocol.Message) error {
	switch msg.Type {
	case protocol.TypeHandshake:
		if msg.Handshake.ProtocolVersion != protocol.CurrentProtocolVersion {
			errStr := fmt.Sprintf("unsupported protocol version %d", msg.Handshake.ProtocolVersion)
			_ = p.Send(protocol.NewError(protocol.ErrCodeVersionMismatch, errStr))
			p.setLastError(errStr)
			_ = p.Close()
			return errors.New(errStr)
		}

		if msg.Handshake.ReplicaID == p.manager.store.ReplicaID() {
			errStr := "cannot peer with self"
			_ = p.Send(protocol.NewError(protocol.ErrCodeSelfConnection, errStr))
			p.setLastError(errStr)
			_ = p.Close()
			return errors.New(errStr)
		}

		p.setRemoteID(msg.Handshake.ReplicaID)

		if !p.isOutbound {
			// Inbound peer: send handshake response
			if err := p.Send(protocol.NewHandshake(p.manager.store.ReplicaID())); err != nil {
				return err
			}
		}

		p.markHandshaked()
		p.manager.registerPeer(p)

		// Trigger initial sync using watermark for this peer
		watermark := p.manager.GetWatermark(p.remoteID)
		_ = p.Send(protocol.NewSyncRequest(watermark))
		return nil

	case protocol.TypeSyncRequest:
		if !p.IsHandshaked() {
			_ = p.Send(protocol.NewError(protocol.ErrCodeNotHandshaked, "session not handshaked"))
			return nil
		}

		records := p.manager.store.RecordsSince(msg.SyncRequest.Since)
		latestClock := p.manager.store.Clock().Last()
		return p.Send(protocol.NewSyncResponse(records, latestClock))

	case protocol.TypeSyncResponse:
		if !p.IsHandshaked() {
			return nil
		}

		if len(msg.SyncResponse.Records) > 0 {
			winners, err := p.manager.store.MergeRecords(msg.SyncResponse.Records)
			if err != nil {
				return err
			}

			// Transitive gossip: relay winning records to other peers.
			if len(winners) > 0 {
				p.manager.BroadcastExcept(p.RemoteID(), protocol.NewUpdatePush(winners...))
			}
		}

		// Advance watermark for this peer
		maxTs := msg.SyncResponse.LatestClock
		for _, rec := range msg.SyncResponse.Records {
			if rec.Timestamp.After(maxTs) {
				maxTs = rec.Timestamp
			}
		}
		p.manager.SetWatermark(p.RemoteID(), maxTs)
		return nil

	case protocol.TypeUpdatePush:
		if !p.IsHandshaked() {
			return nil
		}

		if len(msg.UpdatePush.Records) > 0 {
			winners, err := p.manager.store.MergeRecords(msg.UpdatePush.Records)
			if err != nil {
				return err
			}

			maxTs := p.manager.GetWatermark(p.RemoteID())
			for _, rec := range msg.UpdatePush.Records {
				if rec.Timestamp.After(maxTs) {
					maxTs = rec.Timestamp
				}
			}
			p.manager.SetWatermark(p.RemoteID(), maxTs)

			// Transitive gossip: relay winning records to all other peers.
			if len(winners) > 0 {
				p.manager.BroadcastExcept(p.RemoteID(), protocol.NewUpdatePush(winners...))
			}
		}
		return nil

	case protocol.TypeError:
		p.setLastError(msg.Error.Message)
		return nil

	default:
		return fmt.Errorf("unexpected message type: %s", msg.Type)
	}
}
