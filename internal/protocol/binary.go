package protocol

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/Clearxheaded/converge/internal/crdt"
	"github.com/Clearxheaded/converge/internal/hlc"
)

const (
	binaryTagHandshake    byte = 1
	binaryTagSyncRequest  byte = 2
	binaryTagSyncResponse byte = 3
	binaryTagUpdatePush   byte = 4
	binaryTagError        byte = 5
)

// EncodeBinary serializes a Message into a compact binary representation
// without external dependencies.
func EncodeBinary(msg *Message) ([]byte, error) {
	if msg == nil {
		return nil, errors.New("protocol: cannot encode nil message")
	}
	if err := msg.Validate(); err != nil {
		return nil, fmt.Errorf("protocol: invalid message: %w", err)
	}

	switch msg.Type {
	case TypeHandshake:
		p := msg.Handshake
		repBytes := []byte(p.ReplicaID)
		buf := make([]byte, 1+2+2+len(repBytes))
		buf[0] = binaryTagHandshake
		binary.BigEndian.PutUint16(buf[1:3], uint16(p.ProtocolVersion))
		binary.BigEndian.PutUint16(buf[3:5], uint16(len(repBytes)))
		copy(buf[5:], repBytes)
		return buf, nil

	case TypeSyncRequest:
		p := msg.SyncRequest
		buf := make([]byte, 1+8+4)
		buf[0] = binaryTagSyncRequest
		binary.BigEndian.PutUint64(buf[1:9], p.Since.WallTime)
		binary.BigEndian.PutUint32(buf[9:13], p.Since.Counter)
		return buf, nil

	case TypeSyncResponse:
		p := msg.SyncResponse
		// Calculate total buffer size
		total := 1 + 8 + 4 + 4
		for i := range p.Records {
			total += 2 + len(p.Records[i].Key) + 4 + len(p.Records[i].Value) + 8 + 4 + 2 + len(p.Records[i].ReplicaID) + 1
		}
		buf := make([]byte, total)
		buf[0] = binaryTagSyncResponse
		binary.BigEndian.PutUint64(buf[1:9], p.LatestClock.WallTime)
		binary.BigEndian.PutUint32(buf[9:13], p.LatestClock.Counter)
		binary.BigEndian.PutUint32(buf[13:17], uint32(len(p.Records)))

		offset := 17
		for i := range p.Records {
			offset = writeBinaryRecord(buf, offset, &p.Records[i])
		}
		return buf, nil

	case TypeUpdatePush:
		p := msg.UpdatePush
		total := 1 + 4
		for i := range p.Records {
			total += 2 + len(p.Records[i].Key) + 4 + len(p.Records[i].Value) + 8 + 4 + 2 + len(p.Records[i].ReplicaID) + 1
		}
		buf := make([]byte, total)
		buf[0] = binaryTagUpdatePush
		binary.BigEndian.PutUint32(buf[1:5], uint32(len(p.Records)))

		offset := 5
		for i := range p.Records {
			offset = writeBinaryRecord(buf, offset, &p.Records[i])
		}
		return buf, nil

	case TypeError:
		p := msg.Error
		codeBytes := []byte(p.Code)
		msgBytes := []byte(p.Message)
		buf := make([]byte, 1+2+len(codeBytes)+4+len(msgBytes))
		buf[0] = binaryTagError
		binary.BigEndian.PutUint16(buf[1:3], uint16(len(codeBytes)))
		copy(buf[3:3+len(codeBytes)], codeBytes)
		offset := 3 + len(codeBytes)
		binary.BigEndian.PutUint32(buf[offset:offset+4], uint32(len(msgBytes)))
		copy(buf[offset+4:], msgBytes)
		return buf, nil

	default:
		return nil, fmt.Errorf("protocol: unknown message type %s", msg.Type)
	}
}

func writeBinaryRecord(buf []byte, offset int, rec *crdt.Record) int {
	kBytes := []byte(rec.Key)
	vBytes := []byte(rec.Value)
	rBytes := []byte(rec.ReplicaID)

	binary.BigEndian.PutUint16(buf[offset:offset+2], uint16(len(kBytes)))
	offset += 2
	copy(buf[offset:offset+len(kBytes)], kBytes)
	offset += len(kBytes)

	binary.BigEndian.PutUint32(buf[offset:offset+4], uint32(len(vBytes)))
	offset += 4
	copy(buf[offset:offset+len(vBytes)], vBytes)
	offset += len(vBytes)

	binary.BigEndian.PutUint64(buf[offset:offset+8], rec.Timestamp.WallTime)
	offset += 8
	binary.BigEndian.PutUint32(buf[offset:offset+4], rec.Timestamp.Counter)
	offset += 4

	binary.BigEndian.PutUint16(buf[offset:offset+2], uint16(len(rBytes)))
	offset += 2
	copy(buf[offset:offset+len(rBytes)], rBytes)
	offset += len(rBytes)

	if rec.Deleted {
		buf[offset] = 1
	} else {
		buf[offset] = 0
	}
	offset++

	return offset
}

// DecodeBinary parses a Message from compact binary wire format.
func DecodeBinary(data []byte) (*Message, error) {
	if len(data) == 0 {
		return nil, errors.New("protocol: empty binary payload")
	}

	tag := data[0]
	offset := 1

	switch tag {
	case binaryTagHandshake:
		if len(data) < 5 {
			return nil, errors.New("protocol: truncated binary handshake")
		}
		version := int(binary.BigEndian.Uint16(data[offset : offset+2]))
		offset += 2
		repLen := int(binary.BigEndian.Uint16(data[offset : offset+2]))
		offset += 2
		if len(data) < offset+repLen {
			return nil, errors.New("protocol: truncated binary replica_id")
		}
		replicaID := string(data[offset : offset+repLen])
		msg := &Message{
			Type: TypeHandshake,
			Handshake: &HandshakePayload{
				ProtocolVersion: version,
				ReplicaID:       replicaID,
			},
		}
		return msg, msg.Validate()

	case binaryTagSyncRequest:
		if len(data) < 13 {
			return nil, errors.New("protocol: truncated binary sync_request")
		}
		wt := binary.BigEndian.Uint64(data[offset : offset+8])
		offset += 8
		cnt := binary.BigEndian.Uint32(data[offset : offset+4])
		msg := &Message{
			Type: TypeSyncRequest,
			SyncRequest: &SyncRequestPayload{
				Since: hlc.Timestamp{WallTime: wt, Counter: cnt},
			},
		}
		return msg, msg.Validate()

	case binaryTagSyncResponse:
		if len(data) < 17 {
			return nil, errors.New("protocol: truncated binary sync_response header")
		}
		wt := binary.BigEndian.Uint64(data[offset : offset+8])
		offset += 8
		cnt := binary.BigEndian.Uint32(data[offset : offset+4])
		offset += 4
		recCount := int(binary.BigEndian.Uint32(data[offset : offset+4]))
		offset += 4

		records, err := readBinaryRecords(data, offset, recCount)
		if err != nil {
			return nil, err
		}

		msg := &Message{
			Type: TypeSyncResponse,
			SyncResponse: &SyncResponsePayload{
				Records:     records,
				LatestClock: hlc.Timestamp{WallTime: wt, Counter: cnt},
			},
		}
		return msg, msg.Validate()

	case binaryTagUpdatePush:
		if len(data) < 5 {
			return nil, errors.New("protocol: truncated binary update_push header")
		}
		recCount := int(binary.BigEndian.Uint32(data[offset : offset+4]))
		offset += 4

		records, err := readBinaryRecords(data, offset, recCount)
		if err != nil {
			return nil, err
		}

		msg := &Message{
			Type: TypeUpdatePush,
			UpdatePush: &UpdatePushPayload{
				Records: records,
			},
		}
		return msg, msg.Validate()

	case binaryTagError:
		if len(data) < 7 {
			return nil, errors.New("protocol: truncated binary error payload")
		}
		codeLen := int(binary.BigEndian.Uint16(data[offset : offset+2]))
		offset += 2
		if len(data) < offset+codeLen+4 {
			return nil, errors.New("protocol: truncated binary error code")
		}
		code := string(data[offset : offset+codeLen])
		offset += codeLen

		msgLen := int(binary.BigEndian.Uint32(data[offset : offset+4]))
		offset += 4
		if len(data) < offset+msgLen {
			return nil, errors.New("protocol: truncated binary error message")
		}
		errMsg := string(data[offset : offset+msgLen])

		msg := &Message{
			Type: TypeError,
			Error: &ErrorPayload{
				Code:    code,
				Message: errMsg,
			},
		}
		return msg, msg.Validate()

	default:
		return nil, fmt.Errorf("protocol: unknown binary tag %d", tag)
	}
}

func readBinaryRecords(data []byte, offset int, count int) ([]crdt.Record, error) {
	records := make([]crdt.Record, 0, count)
	for i := 0; i < count; i++ {
		if len(data) < offset+2 {
			return nil, errors.New("protocol: truncated record key length")
		}
		kLen := int(binary.BigEndian.Uint16(data[offset : offset+2]))
		offset += 2
		if len(data) < offset+kLen+4 {
			return nil, errors.New("protocol: truncated record key or val length")
		}
		key := string(data[offset : offset+kLen])
		offset += kLen

		vLen := int(binary.BigEndian.Uint32(data[offset : offset+4]))
		offset += 4
		if len(data) < offset+vLen+8+4+2 {
			return nil, errors.New("protocol: truncated record value or metadata")
		}
		value := string(data[offset : offset+vLen])
		offset += vLen

		wt := binary.BigEndian.Uint64(data[offset : offset+8])
		offset += 8
		cnt := binary.BigEndian.Uint32(data[offset : offset+4])
		offset += 4

		rLen := int(binary.BigEndian.Uint16(data[offset : offset+2]))
		offset += 2
		if len(data) < offset+rLen+1 {
			return nil, errors.New("protocol: truncated record replica_id or deleted flag")
		}
		repID := string(data[offset : offset+rLen])
		offset += rLen

		deleted := data[offset] == 1
		offset++

		records = append(records, crdt.Record{
			Key:       key,
			Value:     value,
			Timestamp: hlc.Timestamp{WallTime: wt, Counter: cnt},
			ReplicaID: repID,
			Deleted:   deleted,
		})
	}
	return records, nil
}
