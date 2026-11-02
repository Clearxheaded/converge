package protocol

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/Clearxheaded/converge/internal/crdt"
	"github.com/Clearxheaded/converge/internal/hlc"
)

// CurrentProtocolVersion represents the active protocol wire format version.
const CurrentProtocolVersion = 1

// Error codes for replication protocol failures.
const (
	ErrCodeInvalidMessage  = "INVALID_MESSAGE"
	ErrCodeVersionMismatch = "VERSION_MISMATCH"
	ErrCodeSelfConnection  = "SELF_CONNECTION"
	ErrCodeNotHandshaked   = "NOT_HANDSHAKED"
	ErrCodeInternalError   = "INTERNAL_ERROR"
)

// MessageType indicates the type of replication frame.
type MessageType string

const (
	TypeHandshake    MessageType = "handshake"
	TypeSyncRequest  MessageType = "sync_request"
	TypeSyncResponse MessageType = "sync_response"
	TypeUpdatePush   MessageType = "update_push"
	TypeError        MessageType = "error"
)

// HandshakePayload is exchanged during session establishment.
type HandshakePayload struct {
	ProtocolVersion int    `json:"protocol_version"`
	ReplicaID       string `json:"replica_id"`
}

// SyncRequestPayload initiates catch-up or incremental replication.
// Since specifies the watermark timestamp: records modified strictly after Since will be returned.
// A zero timestamp requests a full state synchronization.
type SyncRequestPayload struct {
	Since hlc.Timestamp `json:"since"`
}

// SyncResponsePayload transmits records in response to a SyncRequest.
type SyncResponsePayload struct {
	Records     []crdt.Record `json:"records"`
	LatestClock hlc.Timestamp `json:"latest_clock"`
}

// UpdatePushPayload streams live mutations to connected peers.
type UpdatePushPayload struct {
	Records []crdt.Record `json:"records"`
}

// ErrorPayload describes a protocol violation or processing error.
type ErrorPayload struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// Message is the top-level wire envelope for all replication messages.
type Message struct {
	Type         MessageType          `json:"type"`
	Handshake    *HandshakePayload    `json:"handshake,omitempty"`
	SyncRequest  *SyncRequestPayload  `json:"sync_request,omitempty"`
	SyncResponse *SyncResponsePayload `json:"sync_response,omitempty"`
	UpdatePush   *UpdatePushPayload   `json:"update_push,omitempty"`
	Error        *ErrorPayload        `json:"error,omitempty"`
}

// NewHandshake creates a handshake message.
func NewHandshake(replicaID string) *Message {
	return &Message{
		Type: TypeHandshake,
		Handshake: &HandshakePayload{
			ProtocolVersion: CurrentProtocolVersion,
			ReplicaID:       replicaID,
		},
	}
}

// NewSyncRequest creates a synchronization request starting from watermark since.
func NewSyncRequest(since hlc.Timestamp) *Message {
	return &Message{
		Type: TypeSyncRequest,
		SyncRequest: &SyncRequestPayload{
			Since: since,
		},
	}
}

// NewSyncResponse creates a synchronization response containing records and clock state.
func NewSyncResponse(records []crdt.Record, latestClock hlc.Timestamp) *Message {
	if records == nil {
		records = []crdt.Record{}
	}
	return &Message{
		Type: TypeSyncResponse,
		SyncResponse: &SyncResponsePayload{
			Records:     records,
			LatestClock: latestClock,
		},
	}
}

// NewUpdatePush creates a live update push containing one or more records.
func NewUpdatePush(records ...crdt.Record) *Message {
	if records == nil {
		records = []crdt.Record{}
	}
	return &Message{
		Type: TypeUpdatePush,
		UpdatePush: &UpdatePushPayload{
			Records: records,
		},
	}
}

// NewError creates a protocol error message.
func NewError(code, message string) *Message {
	return &Message{
		Type: TypeError,
		Error: &ErrorPayload{
			Code:    code,
			Message: message,
		},
	}
}

// Validate verifies protocol invariants for the message.
func (m *Message) Validate() error {
	switch m.Type {
	case TypeHandshake:
		if m.Handshake == nil {
			return errors.New("protocol: missing handshake payload")
		}
		if m.Handshake.ReplicaID == "" {
			return errors.New("protocol: empty replica_id in handshake")
		}
		if m.Handshake.ProtocolVersion <= 0 {
			return fmt.Errorf("protocol: invalid protocol version %d", m.Handshake.ProtocolVersion)
		}
		return nil

	case TypeSyncRequest:
		if m.SyncRequest == nil {
			return errors.New("protocol: missing sync_request payload")
		}
		return nil

	case TypeSyncResponse:
		if m.SyncResponse == nil {
			return errors.New("protocol: missing sync_response payload")
		}
		if m.SyncResponse.Records == nil {
			m.SyncResponse.Records = []crdt.Record{}
		}
		return nil

	case TypeUpdatePush:
		if m.UpdatePush == nil {
			return errors.New("protocol: missing update_push payload")
		}
		if len(m.UpdatePush.Records) == 0 {
			return errors.New("protocol: update_push must contain at least one record")
		}
		return nil

	case TypeError:
		if m.Error == nil {
			return errors.New("protocol: missing error payload")
		}
		if m.Error.Code == "" {
			return errors.New("protocol: empty error code")
		}
		return nil

	default:
		return fmt.Errorf("protocol: unknown message type %q", m.Type)
	}
}

// Encode serializes a validated message to JSON bytes.
func Encode(msg *Message) ([]byte, error) {
	if msg == nil {
		return nil, errors.New("protocol: cannot encode nil message")
	}
	if err := msg.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(msg)
}

// Decode deserializes and validates a JSON message.
func Decode(data []byte) (*Message, error) {
	var msg Message
	if err := json.Unmarshal(data, &msg); err != nil {
		return nil, fmt.Errorf("protocol: unmarshal json: %w", err)
	}
	if err := msg.Validate(); err != nil {
		return nil, err
	}
	return &msg, nil
}

