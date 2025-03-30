// pthash-go/internal/core/dense_encoders_test.go
package core

import (
	"testing"
)

// MockEncoder for testing dense wrappers AccessDense logic.
type MockEncoder struct {
	Data []uint64
}

func (m *MockEncoder) Encode(pilots []uint64) error      { m.Data = pilots; return nil }
func (m *MockEncoder) Access(i uint64) uint64            { return m.Data[i] }
func (m *MockEncoder) NumBits() uint64                   { return uint64(len(m.Data) * 8 * 8) } // Rough estimate
func (m *MockEncoder) Size() uint64                      { return uint64(len(m.Data)) }
func (m *MockEncoder) Name() string                      { return "Mock" }
func (m *MockEncoder) MarshalBinary() ([]byte, error)    { return nil, nil } // Placeholder
func (m *MockEncoder) UnmarshalBinary(data []byte) error { return nil }      // Placeholder

func TestDenseMonoAccess(t *testing.T) {
	numPartitions := uint64(10)
	numBuckets := uint64(5)
	totalPilots := numPartitions * numBuckets

	// Create flattened pilot data: p0b0, p1b0,..., p9b0, p0b1, p1b1,...
	pilots := make([]uint64, totalPilots)
	for b := uint64(0); b < numBuckets; b++ {
		for p := uint64(0); p < numPartitions; p++ {
			idx := b*numPartitions + p
			// Assign a unique value based on partition and bucket for verification
			pilots[idx] = 1000*b + p
		}
	}

	var denseEnc DenseMono[*MockEncoder]
	denseEnc.NumPartitions = numPartitions
	denseEnc.Encoder = &MockEncoder{} // Initialize with pointer
	err := denseEnc.Encoder.Encode(pilots)
	if err != nil {
		t.Fatalf("MockEncoder Encode failed: %v", err)
	}

	// Verify AccessDense maps correctly to flattened index
	for b := uint64(0); b < numBuckets; b++ {
		for p := uint64(0); p < numPartitions; p++ {
			expectedValue := 1000*b + p
			gotValue := denseEnc.AccessDense(p, b)
			if gotValue != expectedValue {
				t.Errorf("AccessDense(part=%d, bucket=%d): got %d, want %d", p, b, gotValue, expectedValue)
			}
		}
	}
}

func TestDenseMonoSerialization(t *testing.T) {
	type E = RiceEncoder // Use Rice as underlying for test
	dm1 := DenseMono[*E]{}
	// Simulate encoding
	dm1.NumPartitions = 5
	pilots := make([]uint64, 5*3) // 5 partitions, 3 buckets
	for i := range pilots {
		pilots[i] = uint64(i * 10)
	}
	dm1.Encoder = &E{} // Allocate pointer
	err := dm1.Encoder.Encode(pilots)
	if err != nil {
		if IsD1ArraySelectStubbed() { t.Skip("Skipping: D1Array stubbed"); return }
		t.Fatalf("Encode failed: %v", err)
	}

	data, err := dm1.MarshalBinary()
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	dm2 := DenseMono[*E]{}
	err = dm2.UnmarshalBinary(data)
	if err != nil {
		if IsD1ArraySelectStubbed() { t.Skip("Skipping: D1Array stubbed"); return }
		t.Fatalf("Unmarshal failed: %v", err)
	}

	if dm1.NumPartitions != dm2.NumPartitions {
		t.Errorf("NumPartitions mismatch: %d != %d", dm1.NumPartitions, dm2.NumPartitions)
	}
	if dm1.Encoder == nil || dm2.Encoder == nil {
		t.Fatalf("Encoder is nil after unmarshal")
	}
	if dm1.Encoder.Size() != dm2.Encoder.Size() {
		t.Errorf("Encoder size mismatch: %d != %d", dm1.Encoder.Size(), dm2.Encoder.Size())
	}
	if dm1.AccessDense(2, 1) != dm2.AccessDense(2, 1) {
		t.Errorf("AccessDense mismatch")
	}
}

func TestDenseInterleavedAccess(t *testing.T) {
	numPartitions := uint64(10)
	numBuckets := uint64(5)

	var denseEnc DenseInterleaved[*MockEncoder]
	// E is *MockEncoder, so []*E is []**MockEncoder
	denseEnc.Encoders = make([]**MockEncoder, numBuckets)

	// Create and encode data for each bucket's encoder separately
	for b := uint64(0); b < numBuckets; b++ {
		bucketPilots := make([]uint64, numPartitions)
		for p := uint64(0); p < numPartitions; p++ {
			// Assign unique value (same formula as Mono for comparison)
			bucketPilots[p] = 1000*b + p
		}
		// encoderPtr is *MockEncoder
		var encoderPtr *MockEncoder = &MockEncoder{}
		err := encoderPtr.Encode(bucketPilots)
		if err != nil {
			t.Fatalf("MockEncoder Encode for bucket %d failed: %v", b, err)
		}
		// Assign the address of the pointer to the slice element (**MockEncoder)
		denseEnc.Encoders[b] = &encoderPtr
	}

	// Verify AccessDense maps correctly to the right encoder and index within it
	for b := uint64(0); b < numBuckets; b++ {
		for p := uint64(0); p < numPartitions; p++ {
			expectedValue := 1000*b + p
			gotValue := denseEnc.AccessDense(p, b)
			if gotValue != expectedValue {
				t.Errorf("AccessDense(part=%d, bucket=%d): got %d, want %d", p, b, gotValue, expectedValue)
			}
		}
	}
}
