package core

import (
	"fmt"
	"math/bits"
	"reflect"
	"testing"
)

// buildBitVectorFromString creates a BitVector from a string of '0's and '1's.
// The string represents the bits from LSB (index 0) to MSB.
func buildBitVectorFromString(t *testing.T, pattern string) *BitVector {
	t.Helper()
	size := uint64(len(pattern))
	bv := NewBitVector(size)
	for i, char := range pattern {
		if char == '1' {
			bv.Set(uint64(i))
		} else if char != '0' {
			t.Fatalf("Invalid character '%c' in bit pattern string", char)
		}
	}
	return bv
}

// Helper: Linear scan select-in-word for verification/debugging. Slower but simpler.
func selectInWordLinear(word uint64, k uint8) uint8 {
	count := uint8(0)
	for i := uint8(0); i < 64; i++ {
		if (word>>i)&1 == 1 {
			if count == k {
				return i
			}
			count++
		}
	}
	return 64 // Not found
}

// Helper to repeat string (Go 1.16+ has strings.Repeat)
func repeatString(s string, count int) string {
	if count <= 0 {
		return ""
	}
	// Consider using strings.Builder for efficiency if count is large
	res := ""
	for i := 0; i < count; i++ {
		res += s
	}
	return res
}

// Helper to truncate long strings for logging
func truncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}

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

// TestSelect64 compares the optimized Select64 against the linear scan version.
func TestSelect64(t *testing.T) {
	testWords := []uint64{
		0,
		1,
		0x8000000000000000,
		0xFFFFFFFFFFFFFFFF,
		0xAAAAAAAAAAAAAAAA, // Alternating 1010...
		0x5555555555555555, // Alternating 0101...
		0x000000000000000F, // Low bits set
		0xF000000000000000, // High bits set
		0x0101010101010101,
		0x1000000000000001,
	}

	for _, word := range testWords {
		t.Run(fmt.Sprintf("Word=0x%016x", word), func(t *testing.T) {
			popCount := uint8(bits.OnesCount64(word))
			for k := uint8(0); k < popCount; k++ {
				want := selectInWordLinear(word, k)
				got := Select64(word, k) // Call the function under test
				if got != want {
					t.Errorf("Select64(0x%016x, %d): got %d, want %d (linear)", word, k, got, want)
				}
			}
			// Test rank out of bounds
			want := uint8(64)
			got := Select64(word, popCount)
			if got != want {
				t.Errorf("Select64(0x%016x, %d) (out of bounds): got %d, want %d", word, popCount, got, want)
			}
		})
	}
}

// TestD1ArraySelect_EliasFanoPatterns tests Select on patterns resembling Elias-Fano upperBits.
func TestD1ArraySelect_EliasFanoPatterns(t *testing.T) {
	tests := []struct {
		name            string
		pattern         string            // LSB first string representation
		expectedSelects map[uint64]uint64 // rank -> expected position
	}{
		{
			name:    "SimpleShort",
			pattern: "1001011000101", // Represents highs derived from e.g., l=1, values like [1, 7, 11, 13, 23, 27] -> highs [0, 3, 5, 6, 11, 13] -> deltas [0, 3, 2, 1, 5, 2]
			// Ranks:   0  1  2  3   4  5
			expectedSelects: map[uint64]uint64{
				0: 0,  // 1st '1' is at index 0
				1: 3,  // 2nd '1' is at index 3
				2: 5,  // 3rd '1' is at index 5
				3: 6,  // 4th '1' is at index 6
				4: 10, // 5th '1' is at index 10
				5: 12, // 6th '1' is at index 12
			},
		},
		{
			name:    "ConsecutiveOnes",
			pattern: "11100101", // Represents e.g., l=0, values [0, 1, 2, 5, 7] -> highs [0, 1, 2, 5, 7] -> deltas [0, 1, 1, 3, 2]
			// Ranks:   0 1 2   3  4
			expectedSelects: map[uint64]uint64{
				0: 0,
				1: 1,
				2: 2,
				3: 5,
				4: 7,
			},
		},
		{
			name:    "LongZeroRun",
			pattern: "1" + repeatString("0", 100) + "1001", // A '1', 100 '0's, '1', '0', '0', '1'
			// Ranks:   0                       1    2
			expectedSelects: map[uint64]uint64{
				0: 0,   // 1st '1' at index 0
				1: 101, // 2nd '1' at index 101
				2: 104, // 3rd '1' at index 104
			},
		},
		{
			name:    "BoundaryCrossingWord",
			pattern: repeatString("0", 60) + "1001" + "1100" + repeatString("0", 56), // Crosses word boundary at 64
			// Bits: ...000[60]=1 [61]=0 [62]=0 [63]=1 | [64]=1 [65]=1 [66]=0 [67]=0 ...0
			// Ranks:         0                 1       2    3
			expectedSelects: map[uint64]uint64{
				0: 60,
				1: 63,
				2: 64,
				3: 65,
			},
		},
		{
			name:    "BoundaryCrossingBlock",                                           // Assuming BlockSize=4096
			pattern: "1" + repeatString("0", 4094) + "1" + "1" + repeatString("0", 10), // '1' at 0, 4095, 4096
			// Ranks:   0                       1    2
			expectedSelects: map[uint64]uint64{
				0: 0,
				1: 4095,
				2: 4096,
			},
		},
		{
			name:    "BoundaryCrossingSuperBlock",                                        // Assuming SuperBlockSize=262144
			pattern: "1" + repeatString("0", 262142) + "1" + "1" + repeatString("0", 10), // '1' at 0, 262143, 262144
			// Ranks:   0                          1     2
			expectedSelects: map[uint64]uint64{
				0: 0,
				1: 262143,
				2: 262144,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bv := buildBitVectorFromString(t, tt.pattern)
			d1 := NewD1Array(bv) // Build the structure

			// Verify numSetBits first
			if d1.numSetBits != uint64(len(tt.expectedSelects)) {
				t.Fatalf("numSetBits mismatch: D1Array calculated %d, expected %d", d1.numSetBits, len(tt.expectedSelects))
			}
			if d1.numSetBits == 0 {
				// Check Select(0) on empty returns size
				if sel0 := d1.Select(0); sel0 != bv.Size() {
					t.Errorf("Select(0) on empty pattern: got %d, want %d (size)", sel0, bv.Size())
				}
				return // No more selects to check
			}

			// Verify Select results
			for rank, wantPos := range tt.expectedSelects {
				var gotPos uint64
				var panicked bool
				func() {
					defer func() {
						if r := recover(); r != nil {
							panicked = true
							t.Logf("PANIC during Select(%d): %v", rank, r)
						}
					}()
					gotPos = d1.Select(rank)
				}()

				if panicked {
					t.Errorf("Select(%d): panicked, want %d. Pattern: %s", rank, wantPos, truncateString(tt.pattern, 100))
				} else if gotPos != wantPos {
					t.Errorf("Select(%d): got %d, want %d. Pattern: %s", rank, gotPos, wantPos, truncateString(tt.pattern, 100))

					// Add diagnostic info if Select fails
					wordIdx := gotPos / 64
					targetRankInWord := uint8(rank) // This isn't quite right, need rank within word
					if wordIdx < uint64(len(bv.Words())) {
						word := bv.Words()[wordIdx]
						t.Logf("  Word[%d] = 0x%016x", wordIdx, word)
						// Calculate actual rank within word for more precise debugging
						rankAtWordStart := d1.Rank1(wordIdx * 64)
						targetRankInWord = uint8(rank - rankAtWordStart)
						linearSel := selectInWordLinear(word, targetRankInWord)
						t.Logf("  Target rank in word: %d. Linear select gives: %d", targetRankInWord, linearSel)
					} else {
						t.Logf("  Got position %d is beyond available words (%d)", gotPos, len(bv.Words()))
					}
				}
			}

			// Verify Select out of bounds
			rankOOB := uint64(len(tt.expectedSelects))
			wantPosOOB := bv.Size()
			gotPosOOB := d1.Select(rankOOB)
			if gotPosOOB != wantPosOOB {
				t.Errorf("Select(%d) (out of bounds): got %d, want %d (size)", rankOOB, gotPosOOB, wantPosOOB)
			}
		})
	}
}
