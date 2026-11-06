package protocol_test

import (
	"reflect"
	"testing"

	"github.com/Clearxheaded/converge/internal/crdt"
	"github.com/Clearxheaded/converge/internal/hlc"
	"github.com/Clearxheaded/converge/internal/protocol"
)

func TestBinaryRoundTrip_AllTypes(t *testing.T) {
	records := []crdt.Record{
		{
			Key:       "user:101",
			Value:     `{"name":"Alice","role":"admin"}`,
			Timestamp: hlc.Timestamp{WallTime: 1793661642561, Counter: 4},
			ReplicaID: "node-a",
			Deleted:   false,
		},
		{
			Key:       "user:102",
			Value:     "",
			Timestamp: hlc.Timestamp{WallTime: 1793661642570, Counter: 0},
			ReplicaID: "node-b",
			Deleted:   true,
		},
	}

	testCases := []struct {
		name string
		msg  *protocol.Message
	}{
		{
			name: "Handshake",
			msg:  protocol.NewHandshake("replica-xyz-123"),
		},
		{
			name: "SyncRequest",
			msg:  protocol.NewSyncRequest(hlc.Timestamp{WallTime: 1793661642000, Counter: 12}),
		},
		{
			name: "SyncResponse",
			msg:  protocol.NewSyncResponse(records, hlc.Timestamp{WallTime: 1793661642570, Counter: 0}),
		},
		{
			name: "UpdatePush",
			msg:  protocol.NewUpdatePush(records...),
		},
		{
			name: "Error",
			msg:  protocol.NewError(protocol.ErrCodeVersionMismatch, "unsupported wire version 99"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			data, err := protocol.EncodeBinary(tc.msg)
			if err != nil {
				t.Fatalf("EncodeBinary failed: %v", err)
			}
			if len(data) == 0 {
				t.Fatalf("expected non-empty binary payload")
			}

			decoded, err := protocol.DecodeBinary(data)
			if err != nil {
				t.Fatalf("DecodeBinary failed: %v", err)
			}

			if !reflect.DeepEqual(tc.msg, decoded) {
				t.Fatalf("round-trip mismatch:\nexpected: %+v\ngot:      %+v", tc.msg, decoded)
			}
		})
	}
}

func TestBinaryDecode_Truncated(t *testing.T) {
	msg := protocol.NewHandshake("node-test")
	data, err := protocol.EncodeBinary(msg)
	if err != nil {
		t.Fatalf("EncodeBinary failed: %v", err)
	}

	// Truncate payload
	for i := 0; i < len(data)-1; i++ {
		_, err := protocol.DecodeBinary(data[:i])
		if err == nil {
			t.Fatalf("expected error on truncated binary payload of length %d", i)
		}
	}
}
