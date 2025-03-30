package core

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
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
type BitVectorBuilder struct {
	words    []uint64
	capacity uint64 // Capacity in bits
	size     uint64 // Current size in bits
}

// NewBitVectorBuilder creates a builder, optionally reserving capacity.
func NewBitVectorBuilder(initialCapacity uint64) *BitVectorBuilder {
	numWords := (initialCapacity + 63) / 64
	return &BitVectorBuilder{
		words:    make([]uint64, 0, numWords), // Start with 0 length, but reserve capacity
		capacity: initialCapacity,
		size:     0,
	}
}

// grow ensures the builder has space for at least 'needed' more bits.
func (b *BitVectorBuilder) grow(needed uint64) {
    oldCap := b.capacity
    oldLen := len(b.words)
    newSize := b.size + needed
    shouldGrow := newSize > b.capacity

    log.Printf("[DEBUG BVBuilder.grow] ENTER: needed=%d, currentSize=%d, currentCap=%d, currentLen=%d -> newSize=%d",
        needed, b.size, oldCap, oldLen, newSize)

    if shouldGrow {
        // Double the capacity or increase by needed, whichever is larger
        newCapacity := b.capacity * 2
        if newCapacity < newSize {
            newCapacity = newSize
        }
        numWords := (newCapacity + 63) / 64
        log.Printf("[DEBUG BVBuilder.grow]   Growing: newCapacity=%d, newNumWords=%d", newCapacity, numWords)
        if uint64(cap(b.words)) < numWords {
            newWords := make([]uint64, len(b.words), numWords)
            copy(newWords, b.words)
            b.words = newWords
            log.Printf("[DEBUG BVBuilder.grow]     Allocated new words slice: len=%d, cap=%d", len(b.words), cap(b.words))
        }
        b.capacity = newCapacity
    }

    // Ensure words slice length covers current size (potentially needed even if capacity didn't change)
    currentNumWords := (b.size + 63) / 64
    requiredNumWords := (newSize + 63) / 64
    if requiredNumWords > currentNumWords && requiredNumWords > uint64(len(b.words)) {
        if requiredNumWords <= uint64(cap(b.words)) {
            log.Printf("[DEBUG BVBuilder.grow]   Extending slice length from %d to %d (within cap %d)", len(b.words), requiredNumWords, cap(b.words))
            b.words = b.words[:requiredNumWords] // Extend length within capacity
        } else {
            // This should not happen if grow calculation is correct
            log.Printf("[ERROR BVBuilder.grow]   Cannot extend words slice: requiredLen=%d > cap=%d", requiredNumWords, cap(b.words))
            panic("grow calculation error")
        }
    }
    log.Printf("[DEBUG BVBuilder.grow] EXIT: finalCap=%d, finalLen=%d", b.capacity, len(b.words))
}

// Get returns the bit value at the given position.
// This is used during search to check for collisions.
// NOTE: Not safe for concurrent use without external synchronization!
func (b *BitVectorBuilder) Get(pos uint64) bool {
	if pos >= b.size {
		// Reading beyond current size conceptually returns 0/false
		return false
	}
	wordIndex := pos / 64
	bitIndex := pos % 64
	if wordIndex >= uint64(len(b.words)) {
		// Accessing word that hasn't been allocated yet
		return false
	}
	return (b.words[wordIndex] & (1 << bitIndex)) != 0
}

// Set sets the bit at the given position to 1.
// This is used during search to mark taken positions.
func (b *BitVectorBuilder) Set(pos uint64) {
	if pos >= b.capacity {
		b.grow(pos - b.size + 1) // Grow enough to include pos
	}
	wordIndex := pos / 64
	bitIndex := pos % 64
	if wordIndex >= uint64(len(b.words)) { // Ensure word exists after potential grow
		neededLen := wordIndex + 1
		if neededLen > uint64(cap(b.words)) {
			// Should not happen if grow worked correctly
			panic("Set after grow failed")
		}
		b.words = b.words[:neededLen]
	}
	b.words[wordIndex] |= (1 << bitIndex)
	if pos >= b.size { // Update size if we set a bit beyond the current end
		b.size = pos + 1
	}
}

// PushBack appends a single bit (true=1, false=0).
func (b *BitVectorBuilder) PushBack(bit bool) {
    // *** ADD DETAILED LOGGING ***
    oldSize := b.size
    log.Printf("[DEBUG BVBuilder.PushBack] ENTER: bit=%t, oldSize=%d, capacity=%d, len(words)=%d",
        bit, oldSize, b.capacity, len(b.words))

    b.grow(1) // Ensure space for 1 more bit

    wordIndex := b.size / 64
    bitIndex := b.size % 64

    // Ensure words slice is long enough AFTER potential grow
    requiredLen := wordIndex + 1
    if requiredLen > uint64(len(b.words)) {
         // Extend slice length up to its capacity if needed
         if requiredLen <= uint64(cap(b.words)) {
             b.words = b.words[:requiredLen]
             log.Printf("[DEBUG BVBuilder.PushBack]   Extended words slice length to %d", requiredLen)
         } else {
             // This indicates a problem in grow() logic
             log.Printf("[ERROR BVBuilder.PushBack]   Cannot extend words slice: requiredLen=%d > cap=%d", requiredLen, cap(b.words))
             panic("cannot extend words slice in PushBack")
         }
    }

    log.Printf("[DEBUG BVBuilder.PushBack]   Accessing wordIndex=%d, bitIndex=%d", wordIndex, bitIndex)
    // Log word state BEFORE modification
    log.Printf("[DEBUG BVBuilder.PushBack]   Word[%d] BEFORE: 0x%016x", wordIndex, b.words[wordIndex])

    if bit {
        b.words[wordIndex] |= (1 << bitIndex)
        log.Printf("[DEBUG BVBuilder.PushBack]   Set bit to 1")
    } else {
         // Explicitly clear the bit? Not strictly necessary if starting from 0, but safer.
         // b.words[wordIndex] &= ^(1 << bitIndex)
         log.Printf("[DEBUG BVBuilder.PushBack]   Set bit to 0 (no change if already 0)")
    }

    // Log word state AFTER modification
    log.Printf("[DEBUG BVBuilder.PushBack]   Word[%d] AFTER:  0x%016x", wordIndex, b.words[wordIndex])

    b.size++ // Increment size AFTER accessing/setting the bit at the original size index
    log.Printf("[DEBUG BVBuilder.PushBack] EXIT: newSize=%d", b.size)
}

// AppendBits appends the lowest 'numBits' of 'val'. numBits must be <= 64.
func (b *BitVectorBuilder) AppendBits(val uint64, numBits uint8) {
	if numBits == 0 {
		return
	}
	if numBits > 64 {
		panic("BitVectorBuilder.AppendBits: numBits must be <= 64")
	}
	b.grow(uint64(numBits))

	startBit := b.size % 64
	wordIndex := b.size / 64

	if wordIndex >= uint64(len(b.words)) { // Ensure word exists
		b.words = append(b.words, 0)
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
		if wordIndex >= uint64(len(b.words)) { // Ensure next word exists
			b.words = append(b.words, 0)
		}
		b.words[wordIndex] |= (val >> bitsWrittenInFirst)
	}
	b.size += uint64(numBits)
}

// Build creates the final BitVector. The builder is reset.
func (b *BitVectorBuilder) Build() *BitVector {
	numWords := (b.size + 63) / 64
	// Trim potentially overallocated words slice
	finalBits := make([]uint64, numWords)
	copy(finalBits, b.words)

	// Clear the builder
	bv := &BitVector{
		bits: finalBits,
		size: b.size,
	}
	*b = BitVectorBuilder{} // Reset builder state
	return bv
}

func (b *BitVectorBuilder) Size() uint64 {
	return b.size
}

func (b *BitVectorBuilder) Capacity() uint64 {
	return b.capacity
}
