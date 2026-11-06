package protocol_test

import (
	"fmt"
	"testing"

	"github.com/Clearxheaded/converge/internal/crdt"
	"github.com/Clearxheaded/converge/internal/hlc"
	"github.com/Clearxheaded/converge/internal/protocol"
)

func makeBenchmarkRecords(n int) []crdt.Record {
	records := make([]crdt.Record, n)
	for i := 0; i < n; i++ {
		records[i] = crdt.Record{
			Key:       fmt.Sprintf("user:account:%06d", i),
			Value:     fmt.Sprintf(`{"name":"User_%d","email":"user%d@cluster.local","active":true,"balance":%d}`, i, i, i*100),
			Timestamp: hlc.Timestamp{WallTime: 1793661642500 + uint64(i), Counter: uint32(i % 5)},
			ReplicaID: fmt.Sprintf("node-%02d", i%8),
			Deleted:   i%10 == 0, // 10% tombstones
		}
	}
	return records
}

func BenchmarkSerialization_Encode_JSON_100(b *testing.B) {
	records := makeBenchmarkRecords(100)
	msg := protocol.NewSyncResponse(records, hlc.Timestamp{WallTime: 1793661642500, Counter: 0})

	data, err := protocol.Encode(msg)
	if err != nil {
		b.Fatalf("Encode failed: %v", err)
	}
	b.ReportMetric(float64(len(data)), "bytes/payload")
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _ = protocol.Encode(msg)
	}
}

func BenchmarkSerialization_Encode_Binary_100(b *testing.B) {
	records := makeBenchmarkRecords(100)
	msg := protocol.NewSyncResponse(records, hlc.Timestamp{WallTime: 1793661642500, Counter: 0})

	data, err := protocol.EncodeBinary(msg)
	if err != nil {
		b.Fatalf("EncodeBinary failed: %v", err)
	}
	b.ReportMetric(float64(len(data)), "bytes/payload")
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _ = protocol.EncodeBinary(msg)
	}
}

func BenchmarkSerialization_Decode_JSON_100(b *testing.B) {
	records := makeBenchmarkRecords(100)
	msg := protocol.NewSyncResponse(records, hlc.Timestamp{WallTime: 1793661642500, Counter: 0})
	data, _ := protocol.Encode(msg)
	b.ReportMetric(float64(len(data)), "bytes/payload")
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _ = protocol.Decode(data)
	}
}

func BenchmarkSerialization_Decode_Binary_100(b *testing.B) {
	records := makeBenchmarkRecords(100)
	msg := protocol.NewSyncResponse(records, hlc.Timestamp{WallTime: 1793661642500, Counter: 0})
	data, _ := protocol.EncodeBinary(msg)
	b.ReportMetric(float64(len(data)), "bytes/payload")
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _ = protocol.DecodeBinary(data)
	}
}

func BenchmarkSerialization_Encode_JSON_1000(b *testing.B) {
	records := makeBenchmarkRecords(1000)
	msg := protocol.NewSyncResponse(records, hlc.Timestamp{WallTime: 1793661642500, Counter: 0})

	data, err := protocol.Encode(msg)
	if err != nil {
		b.Fatalf("Encode failed: %v", err)
	}
	b.ReportMetric(float64(len(data)), "bytes/payload")
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _ = protocol.Encode(msg)
	}
}

func BenchmarkSerialization_Encode_Binary_1000(b *testing.B) {
	records := makeBenchmarkRecords(1000)
	msg := protocol.NewSyncResponse(records, hlc.Timestamp{WallTime: 1793661642500, Counter: 0})

	data, err := protocol.EncodeBinary(msg)
	if err != nil {
		b.Fatalf("EncodeBinary failed: %v", err)
	}
	b.ReportMetric(float64(len(data)), "bytes/payload")
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _ = protocol.EncodeBinary(msg)
	}
}

func BenchmarkSerialization_Decode_JSON_1000(b *testing.B) {
	records := makeBenchmarkRecords(1000)
	msg := protocol.NewSyncResponse(records, hlc.Timestamp{WallTime: 1793661642500, Counter: 0})
	data, _ := protocol.Encode(msg)
	b.ReportMetric(float64(len(data)), "bytes/payload")
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _ = protocol.Decode(data)
	}
}

func BenchmarkSerialization_Decode_Binary_1000(b *testing.B) {
	records := makeBenchmarkRecords(1000)
	msg := protocol.NewSyncResponse(records, hlc.Timestamp{WallTime: 1793661642500, Counter: 0})
	data, _ := protocol.EncodeBinary(msg)
	b.ReportMetric(float64(len(data)), "bytes/payload")
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _ = protocol.DecodeBinary(data)
	}
}

