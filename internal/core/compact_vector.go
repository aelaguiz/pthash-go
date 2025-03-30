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
	log.Printf("[DEBUG CV.New] ENTER: n=%d, w=%d", n, w)
	if w > 64 {
		panic("CompactVector: width must be <= 64 bits")
	}

	totalBits := uint64(0)
	if w == 0 {
		log.Printf("[DEBUG CV.New] Width is 0. totalBits remains 0.")
		// For width 0, we don't need any bits in the BitVector
	} else if n > 0 {
		totalBits = n * uint64(w)
		log.Printf("[DEBUG CV.New] Width > 0. totalBits calculated as %d", totalBits)
	} else {
		log.Printf("[DEBUG CV.New] Width > 0 but n=0. totalBits remains 0.")
	}

	mask := uint64(0)
	if w > 0 && w < 64 {
		mask = (uint64(1) << w) - 1
	} else if w == 64 {
		mask = ^uint64(0) // All bits set
	}
	log.Printf("[DEBUG CV.New] Mask calculated: 0x%x", mask)

	log.Printf("[DEBUG CV.New] Creating underlying BitVector with size %d", totalBits)
	underlyingBV := NewBitVector(totalBits)
	log.Printf("[DEBUG CV.New] Underlying BitVector created: size=%d, numWords=%d", underlyingBV.Size(), underlyingBV.NumWords())
	
	cv := &CompactVector{
		data:  underlyingBV,
		width: w,
		size:  n,
		mask:  mask,
	}
	log.Printf("[DEBUG CV.New] EXIT: Created CV: size=%d, width=%d, data.size=%d", cv.size, cv.width, cv.data.Size())
	return cv
}

// Access retrieves the value at the given index.
func (cv *CompactVector) Access(i uint64) uint64 {
	log.Printf("[DEBUG CV.Access] ENTER: index=%d, size=%d, width=%d", i, cv.size, cv.width)
	if i >= cv.size {
		panic(fmt.Sprintf("CompactVector.Access: index %d out of bounds (%d)", i, cv.size))
	}
	if cv.width == 0 {
		log.Printf("[DEBUG CV.Access] EXIT: width=0, returning 0")
		return 0 // All elements are 0 if width is 0
	}
	pos := i * uint64(cv.width)
	log.Printf("[DEBUG CV.Access]   Bit position pos=%d", pos)
	// Use the optimized GetBits from BitVector
	val := cv.data.GetBits(pos, cv.width)
	log.Printf("[DEBUG CV.Access]   GetBits(pos=%d, width=%d) -> val=%d (0x%x)", pos, cv.width, val, val)
	log.Printf("[DEBUG CV.Access] EXIT: Returning val %d", val)
	return val
}

// Set sets the value at the given index.
func (cv *CompactVector) Set(i uint64, val uint64) {
	log.Printf("[DEBUG CV.Set] ENTER: index=%d, val=%d, size=%d, width=%d", i, val, cv.size, cv.width)
	if i >= cv.size {
		panic(fmt.Sprintf("CompactVector.Set: index %d out of bounds (%d)", i, cv.size))
	}

	// If width is 0, all elements must be 0
	if cv.width == 0 {
		if val != 0 {
			log.Printf("[ERROR CV.Set] Attempted to set non-zero value (%d) with width 0.", val)
			panic("CompactVector.Set: cannot store non-zero value with width 0")
		}
		log.Printf("[DEBUG CV.Set] Width is 0 and value is 0. No action needed.")
		return
	}

	// Check if val fits in width bits
	if cv.width < 64 && (val>>cv.width) > 0 { // Faster check than using mask
		log.Printf("[ERROR CV.Set] Value %d exceeds width %d.", val, cv.width)
		panic(fmt.Sprintf("CompactVector.Set: value %d exceeds width %d", val, cv.width))
	}

	pos := i * uint64(cv.width)
	log.Printf("[DEBUG CV.Set]   Bit position pos=%d", pos)

	// Clear existing bits and set new value - needs careful bit manipulation
	wordIndex := pos / 64
	bitIndex := pos % 64

	// Mask the value to ensure only relevant bits are set (safety measure)
	maskedVal := val & cv.mask
	log.Printf("[DEBUG CV.Set]   WordIdx=%d, BitIdx=%d, MaskedVal=0x%x", wordIndex, bitIndex, maskedVal)

	// Ensure data slice is large enough BEFORE writing
	requiredWords := (pos + uint64(cv.width) + 63) / 64
	if requiredWords > uint64(len(cv.data.bits)) {
		// This indicates an internal error, likely in NewCompactVector
		log.Printf("[ERROR CV.Set] Underlying BitVector too small! requiredWords=%d, len(bits)=%d", requiredWords, len(cv.data.bits))
		panic(fmt.Sprintf("CompactVector.Set: data slice too small (req %d words, have %d)", requiredWords, len(cv.data.bits)))
	}

	// --- Efficient Set Implementation ---
	// Write the lower part to the first word
	log.Printf("[DEBUG CV.Set]   Before Word[%d]=0x%016x", wordIndex, cv.data.bits[wordIndex])
	cv.data.bits[wordIndex] &= ^(cv.mask << bitIndex)  // Clear bits using shifted mask
	cv.data.bits[wordIndex] |= (maskedVal << bitIndex) // Set new bits
	log.Printf("[DEBUG CV.Set]   After Word[%d]=0x%016x", wordIndex, cv.data.bits[wordIndex])

	// If the value spans across the word boundary
	bitsInFirst := 64 - bitIndex
	if bitsInFirst < uint64(cv.width) {
		wordIndex++
		log.Printf("[DEBUG CV.Set]   Cross-word boundary detected. Next WordIdx=%d", wordIndex)
		// Ensure the next word exists (should be handled by BitVector allocation)
		if wordIndex >= uint64(len(cv.data.bits)) {
			panic("CompactVector.Set: data slice too small for cross-word write")
		}
		bitsInSecond := uint64(cv.width) - bitsInFirst
		secondWordMask := (uint64(1) << bitsInSecond) - 1     // Mask for bits in the second word
		log.Printf("[DEBUG CV.Set]   BitsInSecond=%d, SecondWordMask=0x%x", bitsInSecond, secondWordMask)
		log.Printf("[DEBUG CV.Set]   Before Word[%d]=0x%016x", wordIndex, cv.data.bits[wordIndex])
		cv.data.bits[wordIndex] &= ^secondWordMask            // Clear bits in the second word
		cv.data.bits[wordIndex] |= (maskedVal >> bitsInFirst) // Set new bits shifted correctly
		log.Printf("[DEBUG CV.Set]   After Word[%d]=0x%016x", wordIndex, cv.data.bits[wordIndex])
	}
	// --- End Efficient Set ---
	log.Printf("[DEBUG CV.Set] EXIT")
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
	log.Printf("[DEBUG CVBuilder.New] ENTER: n=%d, w=%d", n, w)
	b := &CompactVectorBuilder{
		vector: NewCompactVector(n, w),
	}
	log.Printf("[DEBUG CVBuilder.New] EXIT")
	return b
}

// Set sets the value at the given index.
func (b *CompactVectorBuilder) Set(i uint64, v uint64) {
	log.Printf("[DEBUG CVBuilder.Set] ENTER: index=%d, val=%d", i, v)
	if b.vector == nil {
		panic("CompactVectorBuilder.Set called after Build")
	}
	b.vector.Set(i, v)
	log.Printf("[DEBUG CVBuilder.Set] EXIT")
}

// Build finalizes and returns the CompactVector. The builder becomes unusable.
func (b *CompactVectorBuilder) Build() *CompactVector {
	log.Printf("[DEBUG CVBuilder.Build] ENTER")
	if b.vector == nil {
		panic("CompactVectorBuilder.Build called multiple times")
	}
	
	// Special handling for width=0
	if b.vector.width == 0 {
		log.Printf("[DEBUG CVBuilder.Build] Width is 0. Finalizing.")
		// Ensure underlying data is minimal
		if b.vector.data != nil && b.vector.data.Size() != 0 {
			log.Printf("[WARN CVBuilder.Build] Width is 0, but underlying BitVector size is %d. Resetting.", b.vector.data.Size())
			// Reset underlying data if it's non-empty for width 0
			b.vector.data = NewBitVector(0)
		}
	} else {
		log.Printf("[DEBUG CVBuilder.Build] Width=%d. Finalizing.", b.vector.width)
	}
	
	result := b.vector
	b.vector = nil // Prevent further modification
	log.Printf("[DEBUG CVBuilder.Build] EXIT: Returning CV: size=%d, width=%d, data.size=%d", result.size, result.width, result.data.Size())
	return result
}
