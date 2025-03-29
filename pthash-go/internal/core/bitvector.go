package core

// BitVector provides basic bit manipulation capabilities.
// This is a very basic placeholder. A performant version is needed.
// Consider using existing libraries likeRoaringBitmap's underlying structures
// or github.com/willf/bitset, or implement efficiently.
type BitVector struct {
	bits []uint64
	size uint64
}

// NewBitVector creates a bit vector of a given size.
func NewBitVector(size uint64) *BitVector {
	numWords := (size + 63) / 64
	return &BitVector{
		bits: make([]uint64, numWords),
		size: size,
	}
}

// Set sets the bit at the given position.
func (bv *BitVector) Set(pos uint64) {
	if pos >= bv.size {
		return // Or panic
	}
	wordIndex := pos / 64
	bitIndex := pos % 64
	bv.bits[wordIndex] |= (1 << bitIndex)
}

// Get returns true if the bit at the given position is set.
func (bv *BitVector) Get(pos uint64) bool {
	if pos >= bv.size {
		return false // Or panic
	}
	wordIndex := pos / 64
	bitIndex := pos % 64
	return (bv.bits[wordIndex] & (1 << bitIndex)) != 0
}

// Size returns the number of bits the vector can hold.
func (bv *BitVector) Size() uint64 {
	return bv.size
}

// NumBits returns the storage size in bits (for the underlying slice).
func (bv *BitVector) NumBits() uint64 {
	return uint64(len(bv.bits) * 64)
}

// TODO: Add rank/select operations if needed by encoders like Elias-Fano.
// TODO: Add efficient serialization.
