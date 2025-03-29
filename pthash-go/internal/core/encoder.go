package core

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/bits"
)

// Encoder defines the interface for encoding/decoding pilot values.
type Encoder interface {
	// Encode takes a slice of pilot values and encodes them internally.
	Encode(pilots []uint64) error
	// Access retrieves the pilot value at the given index.
	Access(i uint64) uint64
	// NumBits returns the size of the encoded data in bits.
	NumBits() uint64
	// Size returns the number of elements encoded.
	Size() uint64
	// Name returns the string identifier of the encoder.
	Name() string
	// Add marshalling interfaces
	binary.BinaryMarshaler
	binary.BinaryUnmarshaler
}

// --- Rice Sequence Implementation ---

// RiceSequence implements Golomb-Rice coding for a sequence of uint64.
type RiceSequence struct {
	highBits      *BitVector // Unary part (using D1Array style select)
	highBitsD1    *D1Array   // Rank/Select structure for highBits
	lowBits       *CompactVector // Remainder part (fixed-width integers)
	optimalParamL uint8          // Optimal Rice parameter 'l' used for encoding
}

// optimalParameterKiely computes the optimal Rice parameter 'l'.
// Ported from C++ version's logic (using log2).
func optimalParameterKiely(values []uint64) uint8 {
	n := uint64(len(values))
	if n == 0 {
		return 0 // Default parameter if no values
	}

	sum := uint64(0)
	for _, v := range values {
		sum += v
	}

	// Handle sum == 0 case to avoid division by zero
	if sum == 0 {
		// If all values are 0, optimal l is technically 0, but Rice needs l >= 0.
		// Let's return 0. Encoding 0 with l=0 works (unary part is '1', low bits empty).
		return 0
	}

	// Calculate p = n / (sum + n)
	p := float64(n) / (float64(sum) + float64(n))

	// Avoid log(0) or log(negative) if p is 1 or invalid
	if p <= 0 || p >= 1 {
		// If p=1 (all values 0), l=0. If p<=0 (impossible for non-negative), handle?
		if p >= 1 {
			return 0
		}
		return 63 // Fallback for invalid p
	}

	// Eq. (8) from Kiely, "Selecting the Golomb Parameter in Rice Coding", 2004.
	// l = floor(log2(log(phi-1)/log(1-p))) + 1
	// phi = (sqrt(5) + 1) / 2
	// Use precomputed value for log_e(phi-1)
	log_e_phi_minus_1 := -0.48121182505960345

	log1MinusP := math.Log2(1.0 - p)
	if log1MinusP == 0 { // Avoid division by zero if p is very close to 0
		return 63 // Large parameter if p is tiny
	}

	val := logPhiMinus1 / log1MinusP
	l_float := math.Floor(val) + 1.0

	if l_float < 0 {
		return 0
	}
	if l_float >= 64 { // Clamp parameter L to be < 64 for uint64 lowBits
		return 63
	}
	return uint8(l_float)
}

// Encode encodes the values using Rice coding.
func (rs *RiceSequence) Encode(values []uint64) error {
	n := uint64(len(values))
	if n == 0 {
		rs.lowBits = NewCompactVector(0, 0) // Empty sequence
		rs.highBits = NewBitVector(0)
		rs.highBitsD1 = NewD1Array(rs.highBits)
		rs.optimalParamL = 0
		return nil
	}

	l := optimalParameterKiely(values)
	rs.optimalParamL = l

	// Estimate size needed for high bits (unary part)
	// Average value is sum/n. Average unary part length is roughly avg_val >> l.
	// Total high bits ~ n + sum >> l
	sum := uint64(0)
	for _, v := range values {
		sum += v
	}
	highBitsEstimate := n
	if l < 64 { // Avoid shift overflow
		highBitsEstimate += (sum >> l)
	} else { // If l >= 64, quotient is always 0, only terminator bit '1' remains
		highBitsEstimate = n
	}

	hbBuilder := NewBitVectorBuilder(highBitsEstimate) // Builder for unary codes + terminators
	lbBuilder := NewCompactVectorBuilder(n, l)         // Builder for low bits (remainders)

	lowMask := uint64(0)
	if l > 0 {
		if l == 64 { // Avoid overflow when creating mask
			lowMask = ^uint64(0)
		} else {
			lowMask = (uint64(1) << l) - 1
		}
	}

	for i, v := range values {
		low := uint64(0)
		if l > 0 {
			low = v & lowMask
		}
		lbBuilder.Set(uint64(i), low)

		high := uint64(0)
		if l < 64 { // Avoid shift overflow
			high = v >> l
		}

		// Append 'high' zeros for the unary part
		for j := uint64(0); j < high; j++ {
			hbBuilder.PushBack(false) // Append 0
		}
		// Append the terminator bit '1'
		hbBuilder.PushBack(true) // Append 1
	}

	rs.lowBits = lbBuilder.Build()
	rs.highBits = hbBuilder.Build()
	rs.highBitsD1 = NewD1Array(rs.highBits) // Build rank/select structure

	return nil
}

// Access retrieves the value at index i.
func (rs *RiceSequence) Access(i uint64) uint64 {
	if rs.lowBits == nil || i >= rs.lowBits.Size() {
		panic("RiceSequence.Access: index out of bounds")
	}

	// Find the start and end positions of the unary code for index i
	// start = select1(i-1) + 1 (or 0 if i=0)
	// end = select1(i)
	startPos := int64(-1) // Position *after* the (i-1)-th '1'
	if i > 0 {
		startPos = int64(rs.highBitsD1.Select(i - 1))
	}
	endPos := int64(rs.highBitsD1.Select(i)) // Position of the i-th '1'

	// The number of zeros (unary value) is endPos - (startPos + 1)
	high := uint64(endPos - (startPos + 1))

	// Get the low bits (remainder)
	low := rs.lowBits.Access(i)

	// Reconstruct the value: (high << l) | low
	val := low
	if rs.optimalParamL < 64 { // Avoid shift overflow
		val |= (high << rs.optimalParamL)
	} else if high > 0 {
		// This case (l=64, high > 0) should technically not happen if values fit in uint64
		panic("RiceSequence decoding error: high part > 0 with l=64")
	}

	return val
}

// Size returns the number of encoded values.
func (rs *RiceSequence) Size() uint64 {
	if rs.lowBits == nil {
		return 0
	}
	return rs.lowBits.Size()
}

// NumBits returns the total number of bits used for storage.
func (rs *RiceSequence) NumBits() uint64 {
	lbBits := uint64(0)
	hbBits := uint64(0)
	d1Bits := uint64(0)
	if rs.lowBits != nil {
		lbBits = rs.lowBits.NumBitsStored()
	}
	if rs.highBits != nil {
		hbBits = rs.highBits.NumBitsStored()
	}
	if rs.highBitsD1 != nil {
		d1Bits = rs.highBitsD1.NumBits()
	}
	return lbBits + hbBits + d1Bits
}

// --- Rice Encoder Implementation ---

// RiceEncoder uses RiceSequence to encode pilot values.
type RiceEncoder struct {
	values RiceSequence
}

func (e *RiceEncoder) Name() string { return "R" }

// Encode implements the Encoder interface.
func (e *RiceEncoder) Encode(pilots []uint64) error {
	return e.values.Encode(pilots)
}

// Access implements the Encoder interface.
func (e *RiceEncoder) Access(i uint64) uint64 {
	return e.values.Access(i)
}

// NumBits implements the Encoder interface.
func (e *RiceEncoder) NumBits() uint64 {
	return e.values.NumBits()
}

// Size implements the Encoder interface.
func (e *RiceEncoder) Size() uint64 {
	return e.values.Size()
}

// MarshalBinary implements binary.BinaryMarshaler
func (e *RiceEncoder) MarshalBinary() ([]byte, error) {
	return tryMarshal(&e.values)
}

// UnmarshalBinary implements binary.BinaryUnmarshaler
func (e *RiceEncoder) UnmarshalBinary(data []byte) error {
	return tryUnmarshal(&e.values, data)
}

// --- CompactVector Implementation ---

// CompactVector stores integers using a fixed number of bits per element.
type CompactVector struct {
	data   *BitVector
	width  uint8  // Bits per element
	size   uint64 // Number of elements
}

// NewCompactVector creates a new CompactVector with the given size and bit width.
func NewCompactVector(n uint64, w uint8) *CompactVector {
	if w > 64 {
		panic("CompactVector: width must be <= 64 bits")
	}
	
	totalBits := uint64(0)
	if w > 0 && n > 0 {
		totalBits = n * uint64(w)
	}
	
	return &CompactVector{
		data:   NewBitVector(totalBits),
		width:  w,
		size:   n,
	}
}

// Access retrieves the value at the given index.
func (cv *CompactVector) Access(i uint64) uint64 {
	if i >= cv.size {
		panic("CompactVector.Access: index out of bounds")
	}
	if cv.width == 0 {
		return 0 // All elements are 0 if width is 0
	}
	pos := i * uint64(cv.width)
	return cv.data.GetBits(pos, cv.width)
}

// Set sets the value at the given index.
func (cv *CompactVector) Set(i uint64, val uint64) {
	if i >= cv.size {
		panic("CompactVector.Set: index out of bounds")
	}
	
	// If width is 0, all elements must be 0
	if cv.width == 0 {
		if val != 0 {
			panic("CompactVector.Set: cannot store non-zero value with width 0")
		}
		return
	}
	
	// Check if val fits in width bits
	if cv.width < 64 && val >= (uint64(1) << cv.width) {
		panic(fmt.Sprintf("CompactVector.Set: value %d exceeds width %d", val, cv.width))
	}
	
	pos := i * uint64(cv.width)
	
	// Clear existing bits
	wordIdx := pos / 64
	bitIdx := pos % 64
	
	// If the value spans a single word
	if bitIdx + uint64(cv.width) <= 64 {
		mask := ((uint64(1) << cv.width) - 1) << bitIdx
		cv.data.bits[wordIdx] &= ^mask
		cv.data.bits[wordIdx] |= (val << bitIdx)
	} else {
		// Value spans two words
		firstWordBits := 64 - bitIdx
		secondWordBits := cv.width - uint8(firstWordBits)
		
		// Clear and set first word
		firstWordMask := ^uint64(0) << bitIdx
		cv.data.bits[wordIdx] &= ^firstWordMask
		cv.data.bits[wordIdx] |= (val << bitIdx)
		
		// Clear and set second word
		secondWordMask := (uint64(1) << secondWordBits) - 1
		cv.data.bits[wordIdx+1] &= ^secondWordMask
		cv.data.bits[wordIdx+1] |= (val >> firstWordBits)
	}
}

// Size returns the number of elements.
func (cv *CompactVector) Size() uint64 {
	return cv.size
}

// NumBitsStored returns the total number of bits used for storage.
func (cv *CompactVector) NumBitsStored() uint64 {
	if cv.data == nil {
		return 0
	}
	return cv.data.NumBitsStored()
}

// --- CompactVectorBuilder ---

// CompactVectorBuilder helps construct a CompactVector.
type CompactVectorBuilder struct {
	vector *CompactVector
}

// NewCompactVectorBuilder creates a new builder for CompactVector.
func NewCompactVectorBuilder(n uint64, w uint8) *CompactVectorBuilder {
	return &CompactVectorBuilder{
		vector: NewCompactVector(n, w),
	}
}

// Set sets the value at the given index.
func (b *CompactVectorBuilder) Set(i uint64, v uint64) {
	b.vector.Set(i, v)
}

// Build finalizes and returns the CompactVector.
func (b *CompactVectorBuilder) Build() *CompactVector {
	result := b.vector
	b.vector = nil // Prevent further modification
	return result
}

// --- CompactEncoder Implementation ---

// CompactEncoder stores values using minimal fixed-width integers.
type CompactEncoder struct {
	values *CompactVector // Pointer to allow nil for empty
}

func (e *CompactEncoder) Name() string { return "C" }

// Encode implements the Encoder interface.
func (e *CompactEncoder) Encode(pilots []uint64) error {
	if len(pilots) == 0 {
		e.values = NewCompactVector(0, 0)
		return nil
	}
	
	// Determine max value to find width 'w'
	maxVal := uint64(0)
	for _, p := range pilots {
		if p > maxVal {
			maxVal = p
		}
	}
	
	w := uint8(0)
	if maxVal > 0 {
		w = uint8(bits.Len64(maxVal))
	} else {
		w = 1 // Need at least 1 bit to store 0
	}
	
	cv := NewCompactVector(uint64(len(pilots)), w)
	for i, p := range pilots {
		cv.Set(uint64(i), p)
	}
	e.values = cv
	return nil
}

// Access implements the Encoder interface.
func (e *CompactEncoder) Access(i uint64) uint64 {
	if e.values == nil {
		panic("CompactEncoder.Access: index out of bounds on nil vector")
	}
	return e.values.Access(i)
}

// NumBits implements the Encoder interface.
func (e *CompactEncoder) NumBits() uint64 {
	if e.values == nil {
		return 0
	}
	return e.values.NumBitsStored()
}

// Size implements the Encoder interface.
func (e *CompactEncoder) Size() uint64 {
	if e.values == nil {
		return 0
	}
	return e.values.Size()
}

// MarshalBinary implements binary.BinaryMarshaler
func (e *CompactEncoder) MarshalBinary() ([]byte, error) {
	return tryMarshal(e.values)
}

// UnmarshalBinary implements binary.BinaryUnmarshaler
func (e *CompactEncoder) UnmarshalBinary(data []byte) error {
	// Need to reconstruct CompactVector first
	e.values = &CompactVector{} // Create empty one
	return tryUnmarshal(e.values, data)
}

// --- Helper for RiceSequence: Rank/Select on BitVector ---
// This is a simple implementation for rank/select structure.
// A real implementation needs efficient rank/select, often using precomputed tables.
type D1Array struct {
	bv *BitVector
	// Add precomputed data structures for rank/select here
}

func NewD1Array(bv *BitVector) *D1Array {
	// TODO: Build rank/select data structures
	return &D1Array{bv: bv}
}

// Rank1 returns the number of set bits up to position pos (exclusive).
func (d *D1Array) Rank1(pos uint64) uint64 {
	// Slow linear scan for placeholder
	if d.bv == nil {
		return 0
	}
	count := uint64(0)
	limit := pos
	if limit > d.bv.Size() {
		limit = d.bv.Size()
	}
	for i := uint64(0); i < limit; i++ {
		if d.bv.Get(i) {
			count++
		}
	}
	return count
}

// Select returns the position of the (rank+1)-th set bit.
func (d *D1Array) Select(rank uint64) uint64 {
	// Slow linear scan for placeholder
	if d.bv == nil {
		return 0
	}
	count := uint64(0)
	for i := uint64(0); i < d.bv.Size(); i++ {
		if d.bv.Get(i) {
			if count == rank {
				return i
			}
			count++
		}
	}
	return d.bv.Size() // Indicate not found
}

// NumBits returns the size of the D1Array structure itself.
func (d *D1Array) NumBits() uint64 {
	// Size of the pointer + precomputed structures (currently none)
	return 64 // Placeholder for pointer size
}

// --- Placeholder for other encoders ---

// EliasFano is needed for minimal PHF free slots. Placeholder.
type EliasFano struct { /* TODO */ }

// DiffCompactEncoder for dense partitioned offsets. Placeholder.
type DiffCompactEncoder struct {} // Empty placeholder

// EliasFano stores a monotone sequence compactly. Placeholder.
type EliasFano struct {
	// Internal data structures for lower/upper bits, rank/select
	numValues uint64
	// Placeholder fields
	lowerBits *BitVector
	upperBits *BitVector
	upperBitsSelect *D1Array // For select0 on upper bits
}

// NewEliasFano creates an empty EliasFano encoder.
func NewEliasFano() *EliasFano {
	// Initialize? Or wait for Encode.
	return &EliasFano{}
}

// Encode builds the Elias-Fano structure from a sorted slice.
func (ef *EliasFano) Encode(sortedValues []uint64) error {
	// TODO: Implement actual Elias-Fano encoding algorithm.
	// 1. Determine parameters (l, number of upper bits).
	// 2. Build lower bits bitvector.
	// 3. Build upper bits bitvector (unary code of high parts).
	// 4. Build rank/select structures (e.g., D1Array for select0).
	ef.numValues = uint64(len(sortedValues))
	return fmt.Errorf("EliasFano.Encode not implemented")
}

// Access retrieves the value with rank `i` (0-based).
func (ef *EliasFano) Access(rank uint64) uint64 {
	if rank >= ef.numValues {
		panic("EliasFano.Access: rank out of bounds")
	}
	// TODO: Implement Elias-Fano access algorithm.
	// 1. Use select0 on upperBits to find position range.
	// 2. Calculate high part from rank and position.
	// 3. Read low part from lowerBits at 'rank'.
	// 4. Combine high and low parts.
	// fmt.Println("Warning: EliasFano.Access returning placeholder 0")
	return 0 // Placeholder
}

// Size returns the number of values encoded.
func (ef *EliasFano) Size() uint64 {
	return ef.numValues
}

// NumBits returns the storage size. Placeholder.
func (ef *EliasFano) NumBits() uint64 {
	lbBits := uint64(0)
	ubBits := uint64(0)
	selBits := uint64(0)
	if ef.lowerBits != nil { lbBits = ef.lowerBits.NumBitsStored() }
	if ef.upperBits != nil { ubBits = ef.upperBits.NumBitsStored() }
	if ef.upperBitsSelect != nil { selBits = ef.upperBitsSelect.NumBits() }
	return lbBits + ubBits + selBits + 8*8 // + size field
}

// MarshalBinary placeholder
func (ef *EliasFano) MarshalBinary() ([]byte, error) {
	// TODO: Serialize ef.numValues, ef.lowerBits, ef.upperBits, ef.upperBitsSelect
	return nil, fmt.Errorf("EliasFano.MarshalBinary not implemented")
}

// UnmarshalBinary placeholder
func (ef *EliasFano) UnmarshalBinary(data []byte) error {
	// TODO: Deserialize fields
	return fmt.Errorf("EliasFano.UnmarshalBinary not implemented")
}

// Helper functions for marshaling/unmarshaling
func tryMarshal(v interface{}) ([]byte, error) {
	if marshaler, ok := v.(binary.BinaryMarshaler); ok {
		return marshaler.MarshalBinary()
	}
	return nil, fmt.Errorf("type %T does not implement binary.BinaryMarshaler", v)
}

func tryUnmarshal(v interface{}, data []byte) error {
	if unmarshaler, ok := v.(binary.BinaryUnmarshaler); ok {
		return unmarshaler.UnmarshalBinary(data)
	}
	return fmt.Errorf("type %T does not implement binary.BinaryUnmarshaler", v)
}
