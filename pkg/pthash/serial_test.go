package pthash_test

import (
	"pthashgo/internal/core"
	"pthashgo/internal/serial"
	"testing"
)

func TestMarshalUnmarshalBitVector(t *testing.T) {
	bv1 := core.NewBitVector(100)
	bv1.Set(10)
	bv1.Set(65)
	bv1.Set(99)

	data, err := serial.TryMarshal(bv1)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}
	if len(data) == 0 {
		t.Fatalf("Marshal returned empty data")
	}

	bv2 := core.NewBitVector(0) // Create empty one
	err = serial.TryUnmarshal(bv2, data)
	if err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	// Verify
	if bv2.Size() != bv1.Size() {
		t.Errorf("Size mismatch: got %d, want %d", bv2.Size(), bv1.Size())
	}
	if len(bv2.Words()) != len(bv1.Words()) {
		t.Errorf("Word count mismatch: got %d, want %d", len(bv2.Words()), len(bv1.Words()))
	}
	for i := uint64(0); i < bv1.Size(); i++ {
		if bv2.Get(i) != bv1.Get(i) {
			t.Errorf("Bit mismatch at pos %d: got %t, want %t", i, bv2.Get(i), bv1.Get(i))
		}
	}
}

func TestMarshalUnmarshalRangeBucketer(t *testing.T) {
	rb1 := &core.RangeBucketer{}
	err := rb1.Init(12345, 0, 0, 0) // Other args ignored
	if err != nil {
		t.Fatalf("Init failed: %v", err)
	}

	data, err := serial.TryMarshal(rb1)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}
	if len(data) != 8 { // Expecting only numBuckets (uint64)
		t.Fatalf("Marshal returned unexpected data length: %d, want 8", len(data))
	}

	rb2 := &core.RangeBucketer{} // Create empty one
	err = serial.TryUnmarshal(rb2, data)
	if err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	// Verify
	if rb2.NumBuckets() != rb1.NumBuckets() {
		t.Errorf("NumBuckets mismatch: got %d, want %d", rb2.NumBuckets(), rb1.NumBuckets())
	}
}

func TestTryMarshalUnmarshalErrors(t *testing.T) {
	// Test marshaling non-marshaler type
	var i int = 5
	_, err := serial.TryMarshal(i)
	if err == nil {
		t.Error("TryMarshal should fail for int")
	}

	// Test unmarshaling into non-unmarshaler type
	err = serial.TryUnmarshal(&i, []byte{1, 2, 3})
	if err == nil {
		t.Error("TryUnmarshal should fail for *int")
	}

	// Test unmarshaling with bad data (too short)
	bv := core.NewBitVector(0)
	err = serial.TryUnmarshal(bv, []byte{1, 2, 3}) // Too short
	if err == nil {
		t.Error("TryUnmarshal(BitVector) should fail with short data")
	}
}
