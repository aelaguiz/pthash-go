// File: internal/core/compact_vector.go
package core

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
)

// CompactVector stores integers using a fixed number of bits per element.
type CompactVector struct {
	data  *BitVector // Underlying bit storage
	width uint8      // Bits per element
	size  uint64     // Number of elements
	mask  uint64     // Mask for extracting a single value (precomputed)
}

// NewCompactVector creates a new CompactVector with the given size and bit width.
func NewCompactVector(n uint64, w uint8) *CompactVector {
	if w > 64 {
		panic("CompactVector: width must be <= 64 bits")
	}

	totalBits := uint64(0)
	if w == 0 {
		// For width 0, we don't need any bits in the BitVector
	} else if n > 0 {
		totalBits = n * uint64(w)
	}

	mask := uint64(0)
	if w > 0 && w < 64 {
		mask = (uint64(1) << w) - 1
	} else if w == 64 {
		mask = ^uint64(0) // All bits set
	}

	underlyingBV := NewBitVector(totalBits)
	
	cv := &CompactVector{
		data:  underlyingBV,
		width: w,
		size:  n,
		mask:  mask,
	}
	return cv
}

// Access retrieves the value at the given index.
func (cv *CompactVector) Access(i uint64) uint64 {
	if i >= cv.size {
		panic(fmt.Sprintf("CompactVector.Access: index %d out of bounds (%d)", i, cv.size))
	}
	if cv.width == 0 {
		return 0 // All elements are 0 if width is 0
	}
	pos := i * uint64(cv.width)
	// Use the optimized GetBits from BitVector
	val := cv.data.GetBits(pos, cv.width)
	return val
}

// Set sets the value at the given index.
func (cv *CompactVector) Set(i uint64, val uint64) {
	if i >= cv.size {
		panic(fmt.Sprintf("CompactVector.Set: index %d out of bounds (%d)", i, cv.size))
	}

	// If width is 0, all elements must be 0
	if cv.width == 0 {
		if val != 0 {
			panic("CompactVector.Set: cannot store non-zero value with width 0")
		}
		return
	}

	// Check if val fits in width bits
	if cv.width < 64 && (val>>cv.width) > 0 { // Faster check than using mask
		panic(fmt.Sprintf("CompactVector.Set: value %d exceeds width %d", val, cv.width))
	}

	pos := i * uint64(cv.width)

	// Clear existing bits and set new value - needs careful bit manipulation
	wordIndex := pos / 64
	bitIndex := pos % 64

	// Mask the value to ensure only relevant bits are set (safety measure)
	maskedVal := val & cv.mask

	// Ensure data slice is large enough BEFORE writing
	requiredWords := (pos + uint64(cv.width) + 63) / 64
	if requiredWords > uint64(len(cv.data.bits)) {
		// This indicates an internal error, likely in NewCompactVector
		panic(fmt.Sprintf("CompactVector.Set: data slice too small (req %d words, have %d)", requiredWords, len(cv.data.bits)))
	}

	// --- Efficient Set Implementation ---
	// Write the lower part to the first word
	cv.data.bits[wordIndex] &= ^(cv.mask << bitIndex)  // Clear bits using shifted mask
	cv.data.bits[wordIndex] |= (maskedVal << bitIndex) // Set new bits

	// If the value spans across the word boundary
	bitsInFirst := 64 - bitIndex
	if bitsInFirst < uint64(cv.width) {
		wordIndex++
		// Ensure the next word exists (should be handled by BitVector allocation)
		if wordIndex >= uint64(len(cv.data.bits)) {
			panic("CompactVector.Set: data slice too small for cross-word write")
		}
		bitsInSecond := uint64(cv.width) - bitsInFirst
		secondWordMask := (uint64(1) << bitsInSecond) - 1     // Mask for bits in the second word
		cv.data.bits[wordIndex] &= ^secondWordMask            // Clear bits in the second word
		cv.data.bits[wordIndex] |= (maskedVal >> bitsInFirst) // Set new bits shifted correctly
	}
	// --- End Efficient Set ---
}

// Size returns the number of elements.
func (cv *CompactVector) Size() uint64 {
	return cv.size
}

// Width returns the number of bits per element.
func (cv *CompactVector) Width() uint8 {
	return cv.width
}

// NumBitsStored returns the total number of bits used for storage by the underlying BitVector.
func (cv *CompactVector) NumBitsStored() uint64 {
	if cv.data == nil {
		return 0
	}
	return cv.data.NumBitsStored()
}

// MarshalBinary implements encoding.BinaryMarshaler.
func (cv *CompactVector) MarshalBinary() ([]byte, error) {
	if cv.data == nil {
		return nil, fmt.Errorf("cannot marshal nil CompactVector data")
	}
	bvData, err := cv.data.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("failed to marshal underlying BitVector: %w", err)
	}

	// Format: size(8) + width(1) + reserved(7) + bvLen(8) + bvData
	totalSize := 8 + 1 + 7 + 8 + len(bvData)
	buf := make([]byte, totalSize)
	offset := 0

	binary.LittleEndian.PutUint64(buf[offset:offset+8], cv.size)
	offset += 8
	buf[offset] = cv.width
	offset += 1
	// copy(buf[offset:offset+7], make([]byte, 7)) // Zero out reserved bytes
	offset += 7 // Skip reserved bytes

	binary.LittleEndian.PutUint64(buf[offset:offset+8], uint64(len(bvData)))
	offset += 8
	copy(buf[offset:], bvData)
	offset += len(bvData)

	if offset != totalSize {
		return nil, fmt.Errorf("compact vector marshal size mismatch: %d != %d", offset, totalSize)
	}
	return buf, nil
}

// UnmarshalBinary implements encoding.BinaryUnmarshaler.
func (cv *CompactVector) UnmarshalBinary(data []byte) error {
	headerSize := 8 + 1 + 7 + 8 // size, width, reserved, bvLen
	if len(data) < headerSize {
		return io.ErrUnexpectedEOF
	}
	offset := 0

	cv.size = binary.LittleEndian.Uint64(data[offset : offset+8])
	offset += 8
	cv.width = data[offset]
	offset += 1 + 7 // width + reserved

	bvLen := binary.LittleEndian.Uint64(data[offset : offset+8])
	offset += 8
	expectedTotalLen := offset + int(bvLen)
	if len(data) < expectedTotalLen {
		return io.ErrUnexpectedEOF
	}

	cv.data = NewBitVector(0) // Create empty BitVector first
	err := cv.data.UnmarshalBinary(data[offset : offset+int(bvLen)])
	if err != nil {
		return fmt.Errorf("failed to unmarshal underlying BitVector: %w", err)
	}
	offset += int(bvLen)

	// Recalculate mask
	if cv.width == 0 {
		cv.mask = 0
	} else if cv.width == 64 {
		cv.mask = ^uint64(0)
	} else {
		cv.mask = (uint64(1) << cv.width) - 1
	}

	// Sanity check size/width consistency
	expectedBits := uint64(0)
	if cv.width > 0 && cv.size > 0 {
		expectedBits = cv.size * uint64(cv.width)
	}
	if cv.data.Size() != expectedBits {
		return fmt.Errorf("inconsistent CompactVector state after unmarshal: expected %d bits in BitVector, found %d (size=%d, width=%d)",
			expectedBits, cv.data.Size(), cv.size, cv.width)
	}
	if offset != len(data) {
		return fmt.Errorf("extra data detected after CompactVector unmarshal")
	}

	return nil
}

// --- CompactVectorBuilder ---

// CompactVectorBuilder helps construct a CompactVector.
type CompactVectorBuilder struct {
	vector *CompactVector
}

// NewCompactVectorBuilder creates a new builder for CompactVector.
func NewCompactVectorBuilder(n uint64, w uint8) *CompactVectorBuilder {
	b := &CompactVectorBuilder{
		vector: NewCompactVector(n, w),
	}
	return b
}

// Set sets the value at the given index.
func (b *CompactVectorBuilder) Set(i uint64, v uint64) {
	if b.vector == nil {
		panic("CompactVectorBuilder.Set called after Build")
	}
	b.vector.Set(i, v)
}

// Build finalizes and returns the CompactVector. The builder becomes unusable.
func (b *CompactVectorBuilder) Build() *CompactVector {
	if b.vector == nil {
		panic("CompactVectorBuilder.Build called multiple times")
	}
	
	// Special handling for width=0
	if b.vector.width == 0 {
		// Ensure underlying data is minimal
		if b.vector.data != nil && b.vector.data.Size() != 0 {
			// Reset underlying data if it's non-empty for width 0
			b.vector.data = NewBitVector(0)
		}
	}
	
	result := b.vector
	b.vector = nil // Prevent further modification
	return result
}
