// pthashgo/internal/core/d1array_test.go
package core

import (
	"fmt"
	"testing"
)

func TestD1ArrayPlaceholderLogic(t *testing.T) {
	bv := NewBitVector(100)
	// Set some bits: 0, 3, 4, 10, 31, 63, 64, 65, 99
	setPositions := []uint64{0, 3, 4, 10, 31, 63, 64, 65, 99}
	for _, pos := range setPositions {
		bv.Set(pos)
	}
	numSet := uint64(len(setPositions))

	d1 := NewD1Array(bv) // Using placeholder implementation

	// Test Rank1
	rankTests := []struct {
		pos  uint64
		want uint64
	}{
		{0, 0}, {1, 1}, {3, 1}, {4, 2}, {5, 3}, {10, 3}, {11, 4},
		{31, 4}, {32, 5}, {63, 5}, {64, 6}, {65, 7}, {66, 8},
		{99, 8}, {100, numSet}, {101, numSet}, // Check bounds
	}
	for _, tc := range rankTests {
		t.Run(fmt.Sprintf("Rank1(%d)", tc.pos), func(t *testing.T) {
			got := d1.Rank1(tc.pos)
			if got != tc.want {
				t.Errorf("Rank1(%d) got %d, want %d", tc.pos, got, tc.want)
			}
		})
	}

	// Test Select (select0 in C++ often means select for 0s, select1 for 1s)
	// Our D1Array Select is for 1s (placeholder logic finds the (rank+1)-th set bit)
	selectTests := []struct {
		rank uint64 // 0-based rank
		want uint64 // Expected position
	}{
		{0, 0}, {1, 3}, {2, 4}, {3, 10}, {4, 31},
		{5, 63}, {6, 64}, {7, 65}, {8, 99},
		{numSet, 100},     // Rank out of bounds -> bv.Size()
		{numSet + 1, 100}, // Rank out of bounds -> bv.Size()
	}
	for _, tc := range selectTests {
		t.Run(fmt.Sprintf("Select(%d)", tc.rank), func(t *testing.T) {
			got := d1.Select(tc.rank)
			if got != tc.want {
				t.Errorf("Select(%d) got %d, want %d", tc.rank, got, tc.want)
			}
		})
	}

	// Test empty BitVector
	bvEmpty := NewBitVector(10)
	d1Empty := NewD1Array(bvEmpty)
	if got := d1Empty.Rank1(5); got != 0 {
		t.Errorf("Rank1(5) on empty got %d, want 0", got)
	}
	if got := d1Empty.Select(0); got != 10 { // Should return size if rank 0 doesn't exist
		t.Errorf("Select(0) on empty got %d, want 10", got)
	}
}
