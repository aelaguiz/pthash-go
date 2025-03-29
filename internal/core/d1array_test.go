// pthashgo/internal/core/d1array_test.go
package core

import (
	"reflect"
	"testing"
)

func TestD1ArrayCorrectness(t *testing.T) {
	bv := NewBitVector(d1SuperBlockSize + d1BlockSize*2 + 100) // Test across boundaries
	// Set bits at specific locations for testing rank/select
	setPositions := []uint64{0, 5, 63, 64, 127, d1BlockSize - 1, d1BlockSize, d1BlockSize + 1,
		d1SuperBlockSize - 1, d1SuperBlockSize, d1SuperBlockSize + 65, bv.Size() - 1}
	posMap := make(map[uint64]bool)
	for _, p := range setPositions {
		bv.Set(p)
		posMap[p] = true
	}

	// Manually calculate expected ranks and select positions
	expectedRanks := make([]uint64, bv.Size()+1)
	expectedSelects := []uint64{}
	count := uint64(0)
	for i := uint64(0); i < bv.Size(); i++ {
		expectedRanks[i] = count
		if posMap[i] {
			expectedSelects = append(expectedSelects, i)
			count++
		}
	}
	expectedRanks[bv.Size()] = count

	// Build D1Array
	d1 := NewD1Array(bv)
	if d1.numSetBits != count {
		t.Fatalf("Expected %d set bits, D1Array computed %d", count, d1.numSetBits)
	}

	// Test Rank1
	for pos, wantRank := range expectedRanks {
		gotRank := d1.Rank1(uint64(pos))
		if gotRank != wantRank {
			t.Errorf("Rank1(%d): got %d, want %d", pos, gotRank, wantRank)
		}
	}
	// Test Rank1 past end
	if gotRank := d1.Rank1(bv.Size() + 10); gotRank != count {
		t.Errorf("Rank1(%d): got %d, want %d (total count)", bv.Size()+10, gotRank, count)
	}

	// Test Select
	for rank, wantPos := range expectedSelects {
		gotPos := d1.Select(uint64(rank))
		if gotPos != wantPos {
			t.Errorf("Select(%d): got %d, want %d", rank, gotPos, wantPos)
		}
	}
	// Test Select out of bounds
	if gotPos := d1.Select(count); gotPos != bv.Size() {
		t.Errorf("Select(%d) (out of bounds): got %d, want %d (size)", count, gotPos, bv.Size())
	}

}

func TestD1ArrayEmpty(t *testing.T) {
	bv := NewBitVector(100)
	d1 := NewD1Array(bv)

	if d1.numSetBits != 0 {
		t.Errorf("Expected 0 set bits, got %d", d1.numSetBits)
	}
	if r := d1.Rank1(50); r != 0 {
		t.Errorf("Rank1(50) on empty: got %d, want 0", r)
	}
	if s := d1.Select(0); s != 100 {
		t.Errorf("Select(0) on empty: got %d, want 100 (size)", s)
	}
}

// Test Serialization
func TestD1ArraySerialization(t *testing.T) {
	bv1 := NewBitVector(200)
	bv1.Set(10)
	bv1.Set(150)
	bv1.Set(199)
	d1 := NewD1Array(bv1)

	data, err := d1.MarshalBinary()
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	d2 := &D1Array{}
	err = d2.UnmarshalBinary(data)
	if err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	// Compare fields
	if d1.size != d2.size || d1.numSetBits != d2.numSetBits || len(d1.superBlockRanks) != len(d2.superBlockRanks) || len(d1.blockRanks) != len(d2.blockRanks) {
		t.Errorf("Metadata mismatch after unmarshal")
	}
	if !reflect.DeepEqual(d1.superBlockRanks, d2.superBlockRanks) {
		t.Errorf("Superblock ranks mismatch")
	}
	if !reflect.DeepEqual(d1.blockRanks, d2.blockRanks) {
		t.Errorf("Block ranks mismatch")
	}
	if d1.bv.Size() != d2.bv.Size() || !reflect.DeepEqual(d1.bv.Words(), d2.bv.Words()) {
		t.Errorf("BitVector mismatch")
	}

	// Check functionality
	if d1.Rank1(151) != d2.Rank1(151) {
		t.Errorf("Rank mismatch after unmarshal")
	}
	if d1.Select(1) != d2.Select(1) {
		t.Errorf("Select mismatch after unmarshal")
	}

}
