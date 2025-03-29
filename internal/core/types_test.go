// pthash-go/internal/core/types_test.go
package core

import (
	"testing"
)

func TestHash128Methods(t *testing.T) {
	h := Hash128{High: 0x0123456789ABCDEF, Low: 0xFEDCBA9876543210}
	if h.First() != 0x0123456789ABCDEF {
		t.Errorf("First() failed: got %x", h.First())
	}
	if h.Second() != 0xFEDCBA9876543210 {
		t.Errorf("Second() failed: got %x", h.Second())
	}
	expectedMix := uint64(0x0123456789ABCDEF ^ 0xFEDCBA9876543210)
	if h.Mix() != expectedMix {
		t.Errorf("Mix() failed: got %x, want %x", h.Mix(), expectedMix)
	}
}

func TestBucketPayloadPairLess(t *testing.T) {
	tests := []struct {
		a, b     BucketPayloadPair
		expected bool // a < b
	}{
		{BucketPayloadPair{10, 100}, BucketPayloadPair{20, 50}, true},
		{BucketPayloadPair{20, 50}, BucketPayloadPair{10, 100}, false},
		{BucketPayloadPair{10, 100}, BucketPayloadPair{10, 200}, true},
		{BucketPayloadPair{10, 200}, BucketPayloadPair{10, 100}, false},
		{BucketPayloadPair{10, 100}, BucketPayloadPair{10, 100}, false},
	}

	for _, tt := range tests {
		if got := tt.a.Less(tt.b); got != tt.expected {
			t.Errorf("%v.Less(%v) = %t, want %t", tt.a, tt.b, got, tt.expected)
		}
		// Test symmetry (b < a)
		if got := tt.b.Less(tt.a); got != !tt.expected && tt.a != tt.b {
			t.Errorf("%v.Less(%v) = %t, want %t", tt.b, tt.a, got, !tt.expected)
		}
	}
}

func TestBucketTMethods(t *testing.T) {
	data1 := []uint64{10, 101}           // size 1
	data3 := []uint64{20, 201, 202, 203} // size 3
	data0 := []uint64{30}                // size 0 (only ID)
	emptyData := []uint64{}              // Invalid state

	b1 := NewBucketT(data1, 1)
	if b1.ID() != 10 {
		t.Errorf("b1.ID() = %d, want 10", b1.ID())
	}
	if b1.Size() != 1 {
		t.Errorf("b1.Size() = %d, want 1", b1.Size())
	}
	p1 := b1.Payloads()
	if len(p1) != 1 || p1[0] != 101 {
		t.Errorf("b1.Payloads() = %v, want [101]", p1)
	}

	b3 := NewBucketT(data3, 3)
	if b3.ID() != 20 {
		t.Errorf("b3.ID() = %d, want 20", b3.ID())
	}
	if b3.Size() != 3 {
		t.Errorf("b3.Size() = %d, want 3", b3.Size())
	}
	p3 := b3.Payloads()
	if len(p3) != 3 || p3[0] != 201 || p3[1] != 202 || p3[2] != 203 {
		t.Errorf("b3.Payloads() = %v, want [201 202 203]", p3)
	}

	b0 := NewBucketT(data0, 0)
	if b0.ID() != 30 {
		t.Errorf("b0.ID() = %d, want 30", b0.ID())
	}
	if b0.Size() != 0 {
		t.Errorf("b0.Size() = %d, want 0", b0.Size())
	}
	p0 := b0.Payloads()
	if len(p0) != 0 {
		t.Errorf("b0.Payloads() = %v, want []", p0)
	}

	// Test panic cases
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("NewBucketT with mismatching size/data len should panic")
		}
	}()
	_ = NewBucketT(data3, 2) // Should panic

	defer func() {
		if r := recover(); r == nil {
			t.Errorf("BucketT.ID() on empty data should panic")
		}
	}()
	bEmpty := BucketT{data: emptyData, size: 0}
	_ = bEmpty.ID() // Should panic
}
