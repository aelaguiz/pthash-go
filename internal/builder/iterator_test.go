// pthash-go/internal/builder/iterator_test.go
package builder

import (
	"pthashgo/internal/core"
	"testing"
)

// Minimal mock sub-builder for iterator test
type mockSubBuilder struct {
	pilotsData []uint64
}

func (m *mockSubBuilder) Pilots() []uint64   { return m.pilotsData }
func (m *mockSubBuilder) NumBuckets() uint64 { return uint64(len(m.pilotsData)) }

// Add other methods needed by the builder interface if any (like Seed, Bucketer etc returning zero values)
func (m *mockSubBuilder) Seed() uint64                 { return 0 }
func (m *mockSubBuilder) Bucketer() *core.SkewBucketer { return nil } // Placeholder
// ... add stubs for other methods used by the iterator's creator if necessary ...

func TestInterleavingPilotsIterator(t *testing.T) {
	numPartitions := uint64(3)
	numBuckets := uint64(4)

	// Create mock builders with distinct pilot values
	mockBuilders := make([]*InternalMemoryBuilderSinglePHF[uint64, core.XXHash128Hasher[uint64], *core.SkewBucketer], numPartitions)
	for p := uint64(0); p < numPartitions; p++ {
		mockBuilders[p] = &InternalMemoryBuilderSinglePHF[uint64, core.XXHash128Hasher[uint64], *core.SkewBucketer]{
			pilots:     make([]uint64, numBuckets),
			numBuckets: numBuckets, // Set NumBuckets field
		}
		for b := uint64(0); b < numBuckets; b++ {
			mockBuilders[p].pilots[b] = 100*p + b // Unique value: PBB (Partition, Bucket)
		}
	}

	// Create a mock partitioned builder containing these mocks
	mockPB := &InternalMemoryBuilderPartitionedPHF[uint64, core.XXHash128Hasher[uint64], *core.SkewBucketer]{
		subBuilders:            mockBuilders,
		numPartitions:          numPartitions,
		numBucketsPerPartition: numBuckets, // Set this field
	}

	iter := NewInterleavingPilotsIterator(mockPB)

	var results []uint64
	for iter.HasNext() {
		results = append(results, iter.Next())
	}

	// Expected sequence: P0B0, P1B0, P2B0, P0B1, P1B1, P2B1, P0B2, ...
	expected := []uint64{
		000, 100, 200, // Bucket 0
		001, 101, 201, // Bucket 1
		002, 102, 202, // Bucket 2
		003, 103, 203, // Bucket 3
	}

	if len(results) != len(expected) {
		t.Fatalf("Iterator produced %d values, want %d", len(results), len(expected))
	}

	for i := 0; i < len(expected); i++ {
		if results[i] != expected[i] {
			t.Errorf("Mismatch at index %d: got %d, want %d", i, results[i], expected[i])
		}
	}

	// Test HasNext after finishing
	if iter.HasNext() {
		t.Error("HasNext should be false after iteration")
	}
}
