package protocol

import (
	"reflect"
	"testing"

	"github.com/Clearxheaded/converge/internal/crdt"
	"github.com/Clearxheaded/converge/internal/hlc"
)

func TestProtocol_HandshakeRoundTrip(t *testing.T) {
	orig := NewHandshake("replica-alpha")

	data, err := Encode(orig)
	if err != nil {
		t.Fatalf("encode handshake: %v", err)
	}

	decoded, err := Decode(data)
	if err != nil {
		t.Fatalf("decode handshake: %v", err)
	}

	if decoded.Type != TypeHandshake {
		t.Fatalf("expected type %s, got %s", TypeHandshake, decoded.Type)
	}
	if !reflect.DeepEqual(decoded.Handshake, orig.Handshake) {
		t.Fatalf("mismatch: got %+v, want %+v", decoded.Handshake, orig.Handshake)
	}
}

func TestProtocol_SyncRequestRoundTrip(t *testing.T) {
	orig := NewSyncRequest(hlc.Timestamp{WallTime: 1234567, Counter: 42})

	data, err := Encode(orig)
	if err != nil {
		t.Fatalf("encode sync request: %v", err)
	}

	decoded, err := Decode(data)
	if err != nil {
		t.Fatalf("decode sync request: %v", err)
	}

	if decoded.Type != TypeSyncRequest {
		t.Fatalf("expected type %s, got %s", TypeSyncRequest, decoded.Type)
	}
	if decoded.SyncRequest.Since != orig.SyncRequest.Since {
		t.Fatalf("timestamp mismatch: got %v, want %v", decoded.SyncRequest.Since, orig.SyncRequest.Since)
	}
}

func TestProtocol_SyncResponseRoundTrip(t *testing.T) {
	records := []crdt.Record{
		{
			Key:       "user:1",
			Value:     "Alice",
			Timestamp: hlc.Timestamp{WallTime: 1000, Counter: 1},
			ReplicaID: "node-1",
			Deleted:   false,
		},
		{
			Key:       "user:2",
			Value:     "",
			Timestamp: hlc.Timestamp{WallTime: 1005, Counter: 0},
			ReplicaID: "node-2",
			Deleted:   true,
		},
	}
	latestClock := hlc.Timestamp{WallTime: 1010, Counter: 3}
	orig := NewSyncResponse(records, latestClock)

	data, err := Encode(orig)
	if err != nil {
		t.Fatalf("encode sync response: %v", err)
	}

	decoded, err := Decode(data)
	if err != nil {
		t.Fatalf("decode sync response: %v", err)
	}

	if decoded.Type != TypeSyncResponse {
		t.Fatalf("expected type %s, got %s", TypeSyncResponse, decoded.Type)
	}
	if len(decoded.SyncResponse.Records) != 2 {
		t.Fatalf("expected 2 records, got %d", len(decoded.SyncResponse.Records))
	}
	if !reflect.DeepEqual(decoded.SyncResponse.Records, orig.SyncResponse.Records) {
		t.Fatalf("records mismatch: got %+v, want %+v", decoded.SyncResponse.Records, orig.SyncResponse.Records)
	}
	if decoded.SyncResponse.LatestClock != latestClock {
		t.Fatalf("clock mismatch: got %v, want %v", decoded.SyncResponse.LatestClock, latestClock)
	}
}

func TestProtocol_UpdatePushRoundTrip(t *testing.T) {
	rec := crdt.Record{
		Key:       "setting:theme",
		Value:     "dark",
		Timestamp: hlc.Timestamp{WallTime: 2000, Counter: 0},
		ReplicaID: "node-a",
		Deleted:   false,
	}
	orig := NewUpdatePush(rec)

	data, err := Encode(orig)
	if err != nil {
		t.Fatalf("encode update push: %v", err)
	}

	decoded, err := Decode(data)
	if err != nil {
		t.Fatalf("decode update push: %v", err)
	}

	if decoded.Type != TypeUpdatePush {
		t.Fatalf("expected type %s, got %s", TypeUpdatePush, decoded.Type)
	}
	if len(decoded.UpdatePush.Records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(decoded.UpdatePush.Records))
	}
	if !reflect.DeepEqual(decoded.UpdatePush.Records[0], rec) {
		t.Fatalf("record mismatch: got %+v, want %+v", decoded.UpdatePush.Records[0], rec)
	}
}

func TestProtocol_ErrorRoundTrip(t *testing.T) {
	orig := NewError(ErrCodeVersionMismatch, "unsupported version 99")

	data, err := Encode(orig)
	if err != nil {
		t.Fatalf("encode error: %v", err)
	}

	decoded, err := Decode(data)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}

	if decoded.Type != TypeError {
		t.Fatalf("expected type %s, got %s", TypeError, decoded.Type)
	}
	if !reflect.DeepEqual(decoded.Error, orig.Error) {
		t.Fatalf("error mismatch: got %+v, want %+v", decoded.Error, orig.Error)
	}
}

func TestProtocol_ValidationFailures(t *testing.T) {
	tests := []struct {
		name string
		msg  *Message
	}{
		{
			name: "unknown message type",
			msg:  &Message{Type: "unknown_type"},
		},
		{
			name: "handshake missing payload",
			msg:  &Message{Type: TypeHandshake},
		},
		{
			name: "handshake empty replica id",
			msg: &Message{
				Type: TypeHandshake,
				Handshake: &HandshakePayload{
					ProtocolVersion: 1,
					ReplicaID:       "",
				},
			},
		},
		{
			name: "handshake invalid version",
			msg: &Message{
				Type: TypeHandshake,
				Handshake: &HandshakePayload{
					ProtocolVersion: 0,
					ReplicaID:       "node-1",
				},
			},
		},
		{
			name: "sync request missing payload",
			msg:  &Message{Type: TypeSyncRequest},
		},
		{
			name: "sync response missing payload",
			msg:  &Message{Type: TypeSyncResponse},
		},
		{
			name: "update push missing payload",
			msg:  &Message{Type: TypeUpdatePush},
		},
		{
			name: "update push empty records",
			msg: &Message{
				Type:       TypeUpdatePush,
				UpdatePush: &UpdatePushPayload{Records: []crdt.Record{}},
			},
		},
		{
			name: "error missing payload",
			msg:  &Message{Type: TypeError},
		},
		{
			name: "error empty code",
			msg: &Message{
				Type:  TypeError,
				Error: &ErrorPayload{Code: "", Message: "failed"},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.msg.Validate(); err == nil {
				t.Fatalf("expected validation error for %s, got nil", tc.name)
			}
			if _, err := Encode(tc.msg); err == nil {
				t.Fatalf("expected encode error for %s, got nil", tc.name)
			}
		})
	}
}

func TestProtocol_DecodeErrors(t *testing.T) {
	// Malformed JSON
	if _, err := Decode([]byte("{invalid-json")); err == nil {
		t.Fatal("expected error decoding malformed JSON, got nil")
	}

	// Valid JSON but invalid protocol frame
	if _, err := Decode([]byte(`{"type":"handshake"}`)); err == nil {
		t.Fatal("expected error decoding handshake with nil payload, got nil")
	}

	// Nil encode
	if _, err := Encode(nil); err == nil {
		t.Fatal("expected error encoding nil message, got nil")
	}
}

