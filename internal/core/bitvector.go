package core

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"sync"
)

// BitVector provides basic bit manipulation capabilities.
type BitVector struct {
	bits []uint64
	size uint64 // Number of bits stored
}

// NewBitVector creates a bit vector initialized to zero for a given size.
func NewBitVector(size uint64) *BitVector {
	numWords := (size + 63) / 64
	return &BitVector{
		bits: make([]uint64, numWords),
		size: size,
	}
}

// Size returns the number of bits the vector conceptually holds.
func (bv *BitVector) Size() uint64 {
	return bv.size
}

// Set sets the bit at the given position to 1.
func (bv *BitVector) Set(pos uint64) {
	if pos >= bv.size {
		panic("BitVector.Set: position out of bounds")
	}
	wordIndex := pos / 64
	bitIndex := pos % 64
	bv.bits[wordIndex] |= (1 << bitIndex)
}

// Unset sets the bit at the given position to 0.
func (bv *BitVector) Unset(pos uint64) {
	if pos >= bv.size {
		panic("BitVector.Unset: position out of bounds")
	}
	wordIndex := pos / 64
	bitIndex := pos % 64
	bv.bits[wordIndex] &= ^(1 << bitIndex)
}

// Get returns true if the bit at the given position is 1.
func (bv *BitVector) Get(pos uint64) bool {
	if pos >= bv.size {
		panic("BitVector.Get: position out of bounds")
	}
	wordIndex := pos / 64
	bitIndex := pos % 64
	return (bv.bits[wordIndex] & (1 << bitIndex)) != 0
}

// GetBits reads 'numBits' starting from 'pos'. numBits must be <= 64.
func (bv *BitVector) GetBits(pos uint64, numBits uint8) uint64 {
	if numBits == 0 {
		return 0
	}
	if numBits > 64 {
		panic("BitVector.GetBits: numBits must be <= 64")
	}
	if pos+uint64(numBits) > bv.size {
		panic("BitVector.GetBits: reading past end of vector")
	}

	startWord := pos / 64
	startBit := pos % 64
	endWord := (pos + uint64(numBits) - 1) / 64

	val := bv.bits[startWord] >> startBit

	if startWord == endWord {
		// All bits are within the same word
		// Mask out bits beyond numBits
		if startBit+uint64(numBits) < 64 {
			val &= (1 << numBits) - 1
		}
	} else {
		// Bits span across two words
		bitsInFirstWord := 64 - startBit
		bitsInSecondWord := numBits - uint8(bitsInFirstWord)
		if endWord < uint64(len(bv.bits)) { // Check if endWord is valid
			secondWordVal := bv.bits[endWord]
			// Mask out bits beyond numBits in the second word
			secondWordVal &= (1 << bitsInSecondWord) - 1
			val |= secondWordVal << bitsInFirstWord
		} else {
			// This case implies pos + numBits > bv.size, handled by initial check.
			panic("BitVector.GetBits: internal logic error, endWord out of bounds")
		}
	}
	return val
}

// NumWords returns the number of 64-bit words used for storage.
func (bv *BitVector) NumWords() int {
	return len(bv.bits)
}

// Words returns the underlying word slice. Use with caution.
func (bv *BitVector) Words() []uint64 {
	return bv.bits
}

// NumBitsStored returns the storage size in bits (for the underlying slice).
func (bv *BitVector) NumBitsStored() uint64 {
	return uint64(len(bv.bits) * 64)
}

// MarshalBinary implements encoding.BinaryMarshaler.
func (bv *BitVector) MarshalBinary() ([]byte, error) {
	buf := make([]byte, 8+8+len(bv.bits)*8) // size + numWords + data
	binary.LittleEndian.PutUint64(buf[0:8], bv.size)
	binary.LittleEndian.PutUint64(buf[8:16], uint64(len(bv.bits)))
	for i, w := range bv.bits {
		binary.LittleEndian.PutUint64(buf[16+i*8:16+(i+1)*8], w)
	}
	return buf, nil
}

// UnmarshalBinary implements encoding.BinaryUnmarshaler.
func (bv *BitVector) UnmarshalBinary(data []byte) error {
	if len(data) < 16 {
		return io.ErrUnexpectedEOF
	}
	bv.size = binary.LittleEndian.Uint64(data[0:8])
	numWords := binary.LittleEndian.Uint64(data[8:16])
	expectedLen := 16 + numWords*8
	if uint64(len(data)) < expectedLen {
		return io.ErrUnexpectedEOF
	}
	if bv.bits == nil || uint64(len(bv.bits)) != numWords {
		bv.bits = make([]uint64, numWords)
	}
	for i := uint64(0); i < numWords; i++ {
		bv.bits[i] = binary.LittleEndian.Uint64(data[16+i*8 : 16+(i+1)*8])
	}
	// Verify consistency: size should fit within numWords*64
	if bv.size > numWords*64 {
		return fmt.Errorf("invalid BitVector data: size %d exceeds capacity %d", bv.size, numWords*64)
	}
	return nil
}

// --- BitVector Builder ---

// BitVectorBuilder helps construct a BitVector incrementally.
// Thread-safe for concurrent Get/Set operations.
type BitVectorBuilder struct {
	mu       sync.Mutex // Mutex to protect words and size
	words    []uint64
	capacity uint64 // Capacity in bits
	size     uint64 // Current size in bits
}

// NewBitVectorBuilder creates a builder, optionally reserving capacity.
func NewBitVectorBuilder(initialCapacity uint64) *BitVectorBuilder {
	numWords := (initialCapacity + 63) / 64
	return &BitVectorBuilder{
		words:    make([]uint64, numWords), // Allocate directly to avoid races during grow checks
		capacity: numWords * 64,           // Capacity based on allocated words
		size:     0,                       // Start with size 0 conceptually
	}
}

// grow ensures the builder has space for at least 'targetBitIndex' + 1 bits.
// MUST be called with the lock held.
func (b *BitVectorBuilder) grow(targetBitIndex uint64) {
    targetCap := targetBitIndex + 1
    if targetCap <= b.capacity {
        return // Already have enough capacity
    }

    // Determine new capacity (e.g., double or target, whichever is larger)
    newCapacity := b.capacity * 2
    if newCapacity < targetCap {
        newCapacity = targetCap
    }
    numWords := (newCapacity + 63) / 64
    log.Printf("[DEBUG BVBuilder.grow Locked] Growing: targetBit=%d, newCapacity=%d, newNumWords=%d", targetBitIndex, newCapacity, numWords)

    if numWords > uint64(cap(b.words)) {
        // This allocation is tricky under lock, might be better to pre-allocate larger
        // For now, create new slice and copy
        newWords := make([]uint64, numWords)
        copy(newWords, b.words)
        b.words = newWords
    } else if numWords > uint64(len(b.words)) {
        // Extend length if within capacity
        b.words = b.words[:numWords]
    }
    b.capacity = numWords * 64
}

// Get returns the bit value at the given position. Thread-safe.
// This is used during search to check for collisions.
func (b *BitVectorBuilder) Get(pos uint64) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Check against conceptual size first
	if pos >= b.size {
		return false
	}

	wordIndex := pos / 64
	bitIndex := pos % 64

	// Check against actual allocated words (should be consistent if Set maintains size)
	if wordIndex >= uint64(len(b.words)) {
		// This implies an internal inconsistency or race condition elsewhere
		log.Printf("[WARN BVBuilder.Get Locked] pos %d maps to word %d, but len(words) is %d (size=%d)", pos, wordIndex, len(b.words), b.size)
		return false // Treat as 0 if word doesn't exist yet
	}
	return (b.words[wordIndex] & (1 << bitIndex)) != 0
}

// Set sets the bit at the given position to 1. Thread-safe.
// This is used during search to mark taken positions.
func (b *BitVectorBuilder) Set(pos uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Grow if necessary BEFORE accessing words index
	b.grow(pos) // Grow to ensure capacity includes 'pos'

	wordIndex := pos / 64
	bitIndex := pos % 64

	// Double check wordIndex after grow (should be fine)
	if wordIndex >= uint64(len(b.words)) {
		log.Printf("[ERROR BVBuilder.Set Locked] pos %d maps to word %d, but len(words) is %d even after grow (cap=%d, size=%d)", pos, wordIndex, len(b.words), b.capacity, b.size)
		// This indicates a potential issue in grow() or concurrent modification outside locks
		panic(fmt.Sprintf("BitVectorBuilder.Set inconsistency: cannot access word %d", wordIndex))
	}

	b.words[wordIndex] |= (1 << bitIndex)

	// Update conceptual size if this position extends it
	if pos >= b.size {
		b.size = pos + 1
	}
}

// PushBack appends a single bit (true=1, false=0). Thread-safe.
// NOT Recommended for Concurrent Use (Append requires strict order)
func (b *BitVectorBuilder) PushBack(bit bool) {
    b.mu.Lock()
    defer b.mu.Unlock()
    
    oldSize := b.size
    log.Printf("[DEBUG BVBuilder.PushBack] ENTER: bit=%t, oldSize=%d, capacity=%d, len(words)=%d",
        bit, oldSize, b.capacity, len(b.words))

    b.grow(b.size) // Ensure space for the next bit
    wordIndex := b.size / 64
    bitIndex := b.size % 64
    
    if wordIndex >= uint64(len(b.words)) {
        panic("grow failed in PushBack") // Should not happen
    }
    
    if bit {
        b.words[wordIndex] |= (1 << bitIndex)
        log.Printf("[DEBUG BVBuilder.PushBack]   Set bit to 1")
    } else {
        b.words[wordIndex] &= ^(1 << bitIndex) // Explicitly clear
        log.Printf("[DEBUG BVBuilder.PushBack]   Set bit to 0")
    }
    
    // Log word state AFTER modification
    log.Printf("[DEBUG BVBuilder.PushBack]   Word[%d] AFTER:  0x%016x", wordIndex, b.words[wordIndex])
    
    b.size++ // Increment size
    log.Printf("[DEBUG BVBuilder.PushBack] EXIT: newSize=%d", b.size)
}

// AppendBits appends the lowest 'numBits' of 'val'. Thread-safe.
// NOT Recommended for Concurrent Use (Append requires strict order)
func (b *BitVectorBuilder) AppendBits(val uint64, numBits uint8) {
	b.mu.Lock()
	defer b.mu.Unlock()
	
	if numBits == 0 {
		return
	}
	if numBits > 64 {
		panic("BitVectorBuilder.AppendBits: numBits must be <= 64")
	}
	
	b.grow(b.size + uint64(numBits) - 1) // Grow to include last bit

	startBit := b.size % 64
	wordIndex := b.size / 64

	if wordIndex >= uint64(len(b.words)) {
		panic("grow failed in AppendBits")
	}

	// Mask val to keep only the lowest numBits
	if numBits < 64 {
		val &= (1 << numBits) - 1
	}

	b.words[wordIndex] |= (val << startBit)

	// If the value spans across word boundary
	bitsWrittenInFirst := 64 - startBit
	if uint64(numBits) > bitsWrittenInFirst {
		wordIndex++
		if wordIndex >= uint64(len(b.words)) {
			panic("grow failed in AppendBits cross-boundary")
		}
		b.words[wordIndex] |= (val >> bitsWrittenInFirst)
	}
	b.size += uint64(numBits)
}

// Build creates the final BitVector. The builder is reset.
// Not thread-safe relative to other ops.
// Should only be called after all concurrent Sets/Gets are done.
func (b *BitVectorBuilder) Build() *BitVector {
	b.mu.Lock() // Lock during build to prevent races if called prematurely
	defer b.mu.Unlock()

	numWords := (b.size + 63) / 64
	// Trim potentially overallocated words slice
	finalBits := make([]uint64, numWords)
	copy(finalBits, b.words[:numWords]) // Copy only the words needed for final size

	bv := &BitVector{
		bits: finalBits,
		size: b.size,
	}
	// Reset builder state
	b.size = 0
	b.words = nil // Allow GC
	b.capacity = 0
	return bv
}

// Size returns the current conceptual size (number of bits set or pushed). Thread-safe.
func (b *BitVectorBuilder) Size() uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.size
}

func (b *BitVectorBuilder) Capacity() uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.capacity
}
