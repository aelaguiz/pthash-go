// pthash-go/internal/core/bitvector_test.go
package core

import (
	"math/rand"
	"testing"
	"time"
)

func TestBitVectorBasic(t *testing.T) {
	size := uint64(100)
	bv := NewBitVector(size)

	if bv.Size() != size {
		t.Fatalf("Expected size %d, got %d", size, bv.Size())
	}

	// Check initial state (all zeros)
	for i := uint64(0); i < size; i++ {
		if bv.Get(i) {
			t.Errorf("Bit %d should be 0 initially", i)
		}
	}

	// Set some bits
	bv.Set(0)
	bv.Set(10)
	bv.Set(63)
	bv.Set(64)
	bv.Set(99)

	if !bv.Get(0) {
		t.Errorf("Bit 0 should be set")
	}
	if bv.Get(1) {
		t.Errorf("Bit 1 should not be set")
	}
	if !bv.Get(10) {
		t.Errorf("Bit 10 should be set")
	}
	if !bv.Get(63) {
		t.Errorf("Bit 63 should be set")
	}
	if !bv.Get(64) {
		t.Errorf("Bit 64 should be set")
	}
	if bv.Get(65) {
		t.Errorf("Bit 65 should not be set")
	}
	if !bv.Get(99) {
		t.Errorf("Bit 99 should be set")
	}

	// Unset a bit
	bv.Unset(10)
	if bv.Get(10) {
		t.Errorf("Bit 10 should be unset")
	}

	// Test out of bounds (should panic)
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("Get out of bounds should panic")
		}
	}()
	_ = bv.Get(100)

	defer func() {
		if r := recover(); r == nil {
			t.Errorf("Set out of bounds should panic")
		}
	}()
	bv.Set(100)
}

func TestBitVectorGetBits(t *testing.T) {
	size := uint64(130)
	bv := NewBitVector(size)

	// Set pattern: 10110 at pos 5
	bv.Set(5)
	bv.Set(7)
	bv.Set(8)
	// Set pattern: 11001 at pos 62
	bv.Set(62)
	bv.Set(63)
	// word boundary
	bv.Set(64 + 2) // bit 66

	// Read within word 0
	if got := bv.GetBits(5, 5); got != 0b01101 { // Read 10110 -> LSB first means 01101
		t.Errorf("GetBits(5, 5) = %05b, want 01101", got)
	}
	if got := bv.GetBits(6, 3); got != 0b110 { // Read 011 -> LSB first means 110
		t.Errorf("GetBits(6, 3) = %03b, want 110", got)
	}

	// Read across word boundary (pos 62, len 5: 11001)
	// Bits: ...[61]=0 [62]=1 [63]=1 | [64]=0 [65]=0 [66]=1 ...
	// Read 5 bits starting at 62: reads bits 62, 63, 64, 65, 66
	// Values: 1, 1, 0, 0, 1
	// LSB first: 10011
	if got := bv.GetBits(62, 5); got != 0b10011 {
		t.Errorf("GetBits(62, 5) across boundary = %05b, want 10011", got)
	}

	// Read edge cases
	if got := bv.GetBits(0, 1); got != 0 {
		t.Errorf("GetBits(0, 1) = %d, want 0", got)
	}
	bv.Set(0)
	if got := bv.GetBits(0, 1); got != 1 {
		t.Errorf("GetBits(0, 1) after set = %d, want 1", got)
	}
	if got := bv.GetBits(127, 3); got != 0 {
		t.Errorf("GetBits(127, 3) = %d, want 0 (near end)", got)
	}

	// Test panic on reading past end
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("GetBits reading past end should panic")
		}
	}()
	_ = bv.GetBits(128, 3) // pos 128, read 3 bits -> reads up to 130 (exclusive), size is 130, OK
	_ = bv.GetBits(129, 2) // reads up to 131 (exclusive) -> Panic

}

func TestBitVectorBuilder(t *testing.T) {
	b := NewBitVectorBuilder(10) // Initial capacity

	b.PushBack(true)  // 1
	b.PushBack(false) // 0
	b.PushBack(true)  // 1 -> 101

	if b.size != 3 {
		t.Errorf("Builder size after PushBack = %d, want 3", b.size)
	}
	if !b.Get(0) {
		t.Errorf("Builder Get(0) failed")
	}
	if b.Get(1) {
		t.Errorf("Builder Get(1) failed")
	}
	if !b.Get(2) {
		t.Errorf("Builder Get(2) failed")
	}

	// Append 5 bits: 11010 (LSB first) -> val = 0x1A
	b.AppendBits(0x1A, 5) // Current: 101 11010
	// Expected size = 3 + 5 = 8

	if b.size != 8 {
		t.Errorf("Builder size after AppendBits = %d, want 8", b.size)
	}
	// Check appended bits
	if b.Get(3) {
		t.Errorf("Builder Get(3) failed (0)")
	}
	if !b.Get(4) {
		t.Errorf("Builder Get(4) failed (1)")
	}
	if b.Get(5) {
		t.Errorf("Builder Get(5) failed (0)")
	}
	if !b.Get(6) {
		t.Errorf("Builder Get(6) failed (1)")
	}
	if !b.Get(7) {
		t.Errorf("Builder Get(7) failed (1)")
	}

	// Append across word boundary
	b = NewBitVectorBuilder(60)
	for i := 0; i < 60; i++ {
		b.PushBack(false)
	} // Fill first 60 bits with 0
	// Append 10 bits: 1010101010 (LSB first) -> val = 0x2AA
	b.AppendBits(0x2AA, 10)
	// Expected size = 60 + 10 = 70
	if b.size != 70 {
		t.Errorf("Builder size after cross-boundary append = %d, want 70", b.size)
	}
	// Check bits around boundary
	if b.Get(59) {
		t.Errorf("Bit 59 should be 0")
	}
	if b.Get(60) {
		t.Errorf("Bit 60 should be 0 (LSB of 0x2AA)")
	}
	if !b.Get(61) {
		t.Errorf("Bit 61 should be 1")
	}
	if b.Get(62) {
		t.Errorf("Bit 62 should be 0")
	}
	if !b.Get(63) {
		t.Errorf("Bit 63 should be 1")
	} // End of word 0
	if b.Get(64) {
		t.Errorf("Bit 64 should be 0")
	} // Start of word 1
	if !b.Get(65) {
		t.Errorf("Bit 65 should be 1")
	}
	// ... check up to bit 69
	if !b.Get(69) {
		t.Errorf("Bit 69 should be 1 (MSB of 0x2AA)")
	}
	if b.Get(70) {
		t.Errorf("Bit 70 should be 0 (out of bounds conceptually)")
	} // relies on Get checking size

	// Test Set beyond size
	b.Set(75)
	if b.size != 76 {
		t.Errorf("Builder size after Set beyond size = %d, want 76", b.size)
	}
	if !b.Get(75) {
		t.Errorf("Bit 75 should be set")
	}
	if b.Get(74) {
		t.Errorf("Bit 74 should not be set")
	}

	// Build the final vector
	bv := b.Build()
	if bv.Size() != 76 {
		t.Errorf("Final BV size = %d, want 76", bv.Size())
	}
	if bv.NumWords() != 2 {
		t.Errorf("Final BV words = %d, want 2", bv.NumWords())
	}
	if !bv.Get(75) {
		t.Errorf("Final BV Get(75) failed")
	}
	if !bv.Get(69) {
		t.Errorf("Final BV Get(69) failed")
	}

	// Builder should be reset
	if b.size != 0 || len(b.words) != 0 {
		t.Errorf("Builder not reset after Build()")
	}
}

func TestBitVectorSerialization(t *testing.T) {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	size := uint64(100 + rng.Intn(200)) // Random size around 100-300
	bv1 := NewBitVector(size)
	for i := 0; i < int(size)/3; i++ {
		bv1.Set(uint64(rng.Intn(int(size))))
	}

	data, err := bv1.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary failed: %v", err)
	}

	bv2 := &BitVector{} // Need pointer for UnmarshalBinary
	err = bv2.UnmarshalBinary(data)
	if err != nil {
		t.Fatalf("UnmarshalBinary failed: %v", err)
	}

	if bv1.Size() != bv2.Size() {
		t.Errorf("Size mismatch after marshal/unmarshal: %d != %d", bv1.Size(), bv2.Size())
	}
	if bv1.NumWords() != bv2.NumWords() {
		t.Errorf("NumWords mismatch after marshal/unmarshal: %d != %d", bv1.NumWords(), bv2.NumWords())
	}
	if len(bv1.bits) != len(bv2.bits) {
		// Should be caught by NumWords check, but explicit check
		t.Errorf("Slice length mismatch: %d != %d", len(bv1.bits), len(bv2.bits))
	}

	// Compare content
	equal := true
	for i := uint64(0); i < bv1.Size(); i++ {
		if bv1.Get(i) != bv2.Get(i) {
			equal = false
			t.Errorf("Data mismatch at bit %d", i)
			// break // Optional: stop on first mismatch
		}
	}
	if !equal {
		t.Errorf("BitVector content differs after serialization.")
		// Optionally dump words for comparison:
		// t.Logf("BV1 words: %v", bv1.bits)
		// t.Logf("BV2 words: %v", bv2.bits)
	}

}

// TestBitVectorBuilderPushBackCrossBoundary verifies PushBack across word boundaries,
// specifically testing the clearing of bits when pushing false.
func TestBitVectorBuilderPushBackCrossBoundary(t *testing.T) {
	b := NewBitVectorBuilder(60) // Start with capacity slightly less than 64

	// 1. Fill the first ~60 bits with alternating 1s
	// This ensures the word isn't all zeros initially.
	for i := uint64(0); i < 60; i++ {
		b.PushBack(i%2 != 0) // Push 0, 1, 0, 1, ...
	}
	if b.Size() != 60 {
		t.Fatalf("Setup failed: size should be 60, got %d", b.Size())
	}
	t.Logf("After initial fill (60 bits): size=%d, words[0]=0x%x", b.Size(), b.words[0])

	// 2. Push 'false' bits to fill up to bit 63
	// We expect bits 60, 61, 62, 63 to become 0.
	t.Logf("Pushing 4 'false' bits (60 to 63)...")
	for i := 0; i < 4; i++ {
		b.PushBack(false)
	}
	if b.Size() != 64 {
		t.Fatalf("After pushing false: size should be 64, got %d", b.Size())
	}
	// Verify bits 60-63 are now 0 in word 0
	mask60_63 := uint64(0xF) << 60
	if (b.words[0] & mask60_63) != 0 {
		t.Errorf("Bits 60-63 should be 0 after pushing false, word[0]=0x%x", b.words[0])
	}
	t.Logf("After pushing 4 false: size=%d, words[0]=0x%x", b.Size(), b.words[0])

	// 3. Push 'true' bits across the boundary (bits 64, 65)
	t.Logf("Pushing 2 'true' bits (64, 65)...")
	b.PushBack(true) // Should go into words[1], bit 0
	b.PushBack(true) // Should go into words[1], bit 1
	if b.Size() != 66 {
		t.Fatalf("After pushing true: size should be 66, got %d", b.Size())
	}
	if len(b.words) != 2 {
		t.Fatalf("Should have 2 words, got %d", len(b.words))
	}
	// Expected: words[1] should have bits 0 and 1 set -> 0x...0011 = 3
	if b.words[1] != 3 {
		t.Errorf("Word 1 has incorrect value after pushing true: got 0x%x, want 0x3", b.words[1])
	}
	t.Logf("After pushing 2 true: size=%d, words[0]=0x%x, words[1]=0x%x", b.Size(), b.words[0], b.words[1])

	// 4. Push 'false' bits into the second word (bits 66, 67)
	t.Logf("Pushing 2 'false' bits (66, 67)...")
	b.PushBack(false) // Should clear bit 2 in words[1]
	b.PushBack(false) // Should clear bit 3 in words[1]
	if b.Size() != 68 {
		t.Fatalf("After pushing false again: size should be 68, got %d", b.Size())
	}
	// Expected: words[1] should *still* be 3 (bits 0, 1 set), assuming bits 2,3 were 0.
	// If the original PushBack(false) wasn't clearing, this wouldn't change anything.
	// The crucial test was step 2. Let's re-verify word 1 just in case.
	if b.words[1] != 3 {
		t.Errorf("Word 1 has incorrect value after pushing false again: got 0x%x, want 0x3", b.words[1])
	}
	t.Logf("After pushing 2 false: size=%d, words[0]=0x%x, words[1]=0x%x", b.Size(), b.words[0], b.words[1])

	// 5. Build and verify final BitVector
	bv := b.Build()
	if bv.Size() != 68 {
		t.Fatalf("Final BV size mismatch: got %d, want 68", bv.Size())
	}
	// Check some boundary bits in the final vector
	if bv.Get(60) || bv.Get(61) || bv.Get(62) || bv.Get(63) {
		t.Errorf("Bits 60-63 should be false in final BV")
	}
	if !bv.Get(64) || !bv.Get(65) {
		t.Errorf("Bits 64, 65 should be true in final BV")
	}
	if bv.Get(66) || bv.Get(67) {
		t.Errorf("Bits 66, 67 should be false in final BV")
	}
}
