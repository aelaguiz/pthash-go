package core

import (
	"encoding" // Add this import for BinaryMarshaler/Unmarshaler
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"math/bits"
	"reflect"

	"pthashgo/internal/serial"
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
	encoding.BinaryMarshaler
	encoding.BinaryUnmarshaler
}

// --- Rice Sequence Implementation ---

// RiceSequence implements Golomb-Rice coding for a sequence of uint64.
type RiceSequence struct {
	highBits      *BitVector     // Unary part (using D1Array style select)
	highBitsD1    *D1Array       // Rank/Select structure for highBits
	lowBits       *CompactVector // Remainder part (fixed-width integers)
	optimalParamL uint8          // Optimal Rice parameter 'l' used for encoding
}

// optimalParameterKiely computes the optimal Rice parameter 'l'.
// Ported from C++ version's logic (using log2).
func optimalParameterKiely(values []uint64) uint8 {
	n := uint64(len(values))
	fmt.Printf("[DEBUG] optimalParameterKiely: n=%d\n", n)
	if n == 0 {
		return 0 // Default parameter if no values
	}

	sum := uint64(0)
	for _, v := range values {
		sum += v
	}
	fmt.Printf("[DEBUG] optimalParameterKiely: sum=%d\n", sum)

	// Handle sum == 0 case to avoid division by zero
	if sum == 0 {
		// If all values are 0, optimal l is technically 0, but Rice needs l >= 0.
		// Let's return 0. Encoding 0 with l=0 works (unary part is '1', low bits empty).
		return 0
	}

	// Calculate p = n / (sum + n)
	p := float64(n) / (float64(sum) + float64(n))
	fmt.Printf("[DEBUG] optimalParameterKiely: p=%f\n", p)

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
	const logPhiMinus1 = -0.48121182505960345 // Precomputed natural log: log(phi - 1)

	log1MinusP := math.Log(1.0 - p) // Use natural log (Log) for the inner ratio
	fmt.Printf("[DEBUG] optimalParameterKiely: log(1-p)=%f\n", log1MinusP)
	if log1MinusP == 0 { // Avoid division by zero if p is very close to 0
		return 63 // Large parameter if p is tiny
	}

	val := logPhiMinus1 / log1MinusP
	fmt.Printf("[DEBUG] optimalParameterKiely: ratio val=%f\n", val)
	// Now take log2 of the ratio 'val' before floor+1
	if val <= 0 { // log2 is undefined for non-positive
		fmt.Printf("[DEBUG] optimalParameterKiely: ratio val <= 0, returning 0\n")
		return 0 // Or some other default if ratio is non-positive
	}
	log2Val := math.Log2(val)
	fmt.Printf("[DEBUG] optimalParameterKiely: log2(val)=%f\n", log2Val)
	l_float := math.Floor(log2Val) + 1.0
	fmt.Printf("[DEBUG] optimalParameterKiely: floor(log2(val))+1 = %f\n", l_float)

	if l_float < 0 {
		return 0
	}
	if l_float >= 64 { // Clamp parameter L to be < 64 for uint64 lowBits
		return 63
	}
	result := uint8(l_float)
	fmt.Printf("[DEBUG] optimalParameterKiely: Returning l=%d\n", result)
	return result
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

// MarshalBinary implements encoding.BinaryMarshaler.
func (rs *RiceSequence) MarshalBinary() ([]byte, error) {
	// TODO: Serialize fields optimalParamL, lowBits, highBits, highBitsD1
	return nil, fmt.Errorf("RiceSequence.MarshalBinary not implemented")
}

// UnmarshalBinary implements encoding.BinaryUnmarshaler.
func (rs *RiceSequence) UnmarshalBinary(data []byte) error {
	// TODO: Deserialize fields
	return fmt.Errorf("RiceSequence.UnmarshalBinary not implemented")
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
	return serial.TryMarshal(&e.values)
}

// UnmarshalBinary implements binary.BinaryUnmarshaler
func (e *RiceEncoder) UnmarshalBinary(data []byte) error {
	return serial.TryUnmarshal(&e.values, data)
}

// --- CompactVector Implementation ---

// CompactVector stores integers using a fixed number of bits per element.
type CompactVector struct {
	data  *BitVector
	width uint8  // Bits per element
	size  uint64 // Number of elements
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
		data:  NewBitVector(totalBits),
		width: w,
		size:  n,
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
	if cv.width < 64 && val >= (uint64(1)<<cv.width) {
		panic(fmt.Sprintf("CompactVector.Set: value %d exceeds width %d", val, cv.width))
	}

	pos := i * uint64(cv.width)

	// Clear existing bits
	wordIndex := pos / 64
	bitIndex := pos % 64

	// If the value spans a single word
	if bitIndex+uint64(cv.width) <= 64 {
		mask := ((uint64(1) << cv.width) - 1) << bitIndex
		cv.data.bits[wordIndex] &= ^mask
		cv.data.bits[wordIndex] |= (val << bitIndex)
	} else {
		// Value spans two words
		firstWordBits := 64 - bitIndex
		secondWordBits := cv.width - uint8(firstWordBits)

		// Clear and set first word
		firstWordMask := ^uint64(0) << bitIndex
		cv.data.bits[wordIndex] &= ^firstWordMask
		cv.data.bits[wordIndex] |= (val << bitIndex)

		// Clear and set second word
		secondWordMask := (uint64(1) << secondWordBits) - 1
		cv.data.bits[wordIndex+1] &= ^secondWordMask
		cv.data.bits[wordIndex+1] |= (val >> firstWordBits)
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

// MarshalBinary implements encoding.BinaryMarshaler.
func (cv *CompactVector) MarshalBinary() ([]byte, error) {
	// TODO: Serialize width, size, and data (BitVector)
	if cv.data == nil {
		// Handle case where data might be nil (e.g., after build error?)
		return nil, fmt.Errorf("CompactVector.MarshalBinary: data is nil")
	}
	return serial.TryMarshal(cv.data) // Example: delegate, but needs width/size too
}

// UnmarshalBinary implements encoding.BinaryUnmarshaler.
func (cv *CompactVector) UnmarshalBinary(data []byte) error {
	return fmt.Errorf("CompactVector.UnmarshalBinary not implemented")
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
	return serial.TryMarshal(e.values)
}

// UnmarshalBinary implements binary.BinaryUnmarshaler
func (e *CompactEncoder) UnmarshalBinary(data []byte) error {
	// Need to reconstruct CompactVector first
	e.values = &CompactVector{} // Create empty one
	return serial.TryUnmarshal(e.values, data)
}

// --- Helper for RiceSequence: Rank/Select on BitVector ---
// This is a simple implement

// --- Placeholder for other encoders ---

// EliasFano stores a monotone sequence compactly.
type EliasFano struct {
	numValues       uint64
	universe        uint64 // Max value + 1
	numLowBits      uint8
	lowerBits       *CompactVector // Stores lower l bits of each value
	upperBits       *BitVector     // Unary codes of upper parts
	upperBitsSelect *D1Array       // Rank/Select structure for upperBits
}

// DiffCompactEncoder for dense partitioned offsets. Placeholder.
type DiffCompactEncoder struct{} // Empty placeholder

// Constants for EliasFano and other encoders
const logPhiMinus1 = -0.48121182505960345 // Pre-computed log(phi-1) = log(1/phi) = -log(phi)

// NewEliasFano creates an empty EliasFano encoder.
func NewEliasFano() *EliasFano {
	return &EliasFano{}
}

// Encode builds the Elias-Fano structure from a sorted slice of DISTINCT values.
// Input values MUST be monotonically increasing and distinct.
func (ef *EliasFano) Encode(sortedValues []uint64) error {
	n := uint64(len(sortedValues))
	ef.numValues = n

	if n == 0 {
		ef.universe = 0
		ef.numLowBits = 0
		ef.lowerBits = NewCompactVector(0, 0)
		ef.upperBits = NewBitVector(0)
		ef.upperBitsSelect = NewD1Array(ef.upperBits)
		return nil
	}

	// Check monotonicity and find universe (max value + 1)
	u := uint64(0)
	if n > 0 {
		u = sortedValues[n-1] + 1
		for i := uint64(1); i < n; i++ {
			if sortedValues[i] < sortedValues[i-1] { // Allow duplicates for now? C++ EF might. Let's require monotonic.
				return fmt.Errorf("EliasFano.Encode input must be monotonically increasing (value[%d]=%d < value[%d]=%d)", i, sortedValues[i], i-1, sortedValues[i-1])
			}
			// if sortedValues[i] == sortedValues[i-1] {
			// 	return fmt.Errorf("EliasFano.Encode input must contain distinct values (value[%d]=%d == value[%d]=%d)", i, sortedValues[i], i-1, sortedValues[i-1])
			// }
		}
	}
	ef.universe = u

	// Calculate l = floor(log2(u/n)) or 0 if u/n is 0
	l := uint8(0)
	if n > 0 && u > n { // Avoid division by zero and log(<=1)
		ratio := float64(u) / float64(n)
		l = uint8(math.Floor(math.Log2(ratio)))
	}
	// Ensure l is within reasonable bounds (e.g., <= 64)
	if l > 64 {
		l = 64
	}
	ef.numLowBits = l

	// Estimate size for upper bits: n '1's + sum(v >> l) '0's
	// Approx sum(v >> l) = sum(v) >> l ? Not quite.
	// Safe upper bound: n * (u >> l) zeros + n ones
	upperBitsSizeEstimate := uint64(0)
	if u > 0 && l < 64 { // Avoid shift issues
		upperBitsSizeEstimate = n + n*(u>>l)
	} else {
		upperBitsSizeEstimate = n // Just the '1' terminators if l >= 64 or u=0
	}
	// Add some buffer
	upperBitsSizeEstimate += 100

	lbBuilder := NewCompactVectorBuilder(n, l)
	ubBuilder := NewBitVectorBuilder(upperBitsSizeEstimate)

	lastHighPart := uint64(0)
	mask := uint64(0)
	if l < 64 {
		mask = (uint64(1) << l) - 1
	} else if l == 64 {
		mask = ^uint64(0)
	}

	for i, v := range sortedValues {
		low := uint64(0)
		high := uint64(0)

		if l < 64 {
			low = v & mask
			high = v >> l
		} else { // l == 64
			low = v
			high = 0 // High part is always 0 if l=64
		}

		lbBuilder.Set(uint64(i), low)

		// Add unary code for high part delta
		delta := high - lastHighPart
		for k := uint64(0); k < delta; k++ {
			ubBuilder.PushBack(false) // 0
		}
		ubBuilder.PushBack(true) // 1 (terminator)
		lastHighPart = high
	}

	ef.lowerBits = lbBuilder.Build()
	ef.upperBits = ubBuilder.Build()
	ef.upperBitsSelect = NewD1Array(ef.upperBits) // Build rank/select on upper bits

	// Verify numSetBits matches n
	if ef.upperBitsSelect.numSetBits != n {
		return fmt.Errorf("internal EliasFano error: upperBitsSelect has %d set bits, expected %d", ef.upperBitsSelect.numSetBits, n)
	}

	return nil
}

// Access retrieves the value with rank `i` (0-based).
func (ef *EliasFano) Access(rank uint64) uint64 {
	if rank >= ef.numValues {
		panic(fmt.Sprintf("EliasFano.Access: rank %d out of bounds (%d)", rank, ef.numValues))
	}
	if ef.numValues == 0 {
		panic("EliasFano.Access: accessing empty structure")
	}

	// 1. Find position of (rank+1)-th '1' in upper bits -> gives end of unary code
	selectRank := rank // Select uses 0-based rank
	pos := ef.upperBitsSelect.Select(selectRank)

	// 2. Find high part. Need position of rank-th '1' as well.
	high := uint64(0)
	if rank == 0 {
		// High part is the number of zeros before the first '1', which is Select(0)
		high = pos
	} else {
		prevPos := ef.upperBitsSelect.Select(rank - 1)
		// High part is number of zeros between prevPos and pos
		// Count = pos - (prevPos + 1)
		high = pos - (prevPos + 1)
	}

	// 3. Read low part from lowerBits at 'rank'.
	low := uint64(0)
	if ef.numLowBits > 0 {
		low = ef.lowerBits.Access(rank)
	}

	// 4. Combine: (high << l) | low
	val := low
	if ef.numLowBits < 64 {
		val |= (high << ef.numLowBits)
	} else if high > 0 {
		// Should not happen if values fit in uint64
		panic("EliasFano decoding error: high part > 0 with l=64")
	}

	return val
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
	if ef.lowerBits != nil {
		lbBits = ef.lowerBits.NumBitsStored()
	}
	if ef.upperBits != nil {
		ubBits = ef.upperBits.NumBitsStored()
	}
	if ef.upperBitsSelect != nil {
		selBits = ef.upperBitsSelect.NumBits()
	}
	return 8*8 + 8*8 + 8 + lbBits + ubBits + selBits // numValues, universe, numLowBits + data
}

// MarshalBinary implements encoding.BinaryMarshaler.
func (ef *EliasFano) MarshalBinary() ([]byte, error) {
	// Serialize numValues, universe, numLowBits, lowerBits, upperBits, upperBitsSelect
	// Placeholder implementation
	lbData, err := serial.TryMarshal(ef.lowerBits)
	if err != nil {
		return nil, err
	}
	selData, err := serial.TryMarshal(ef.upperBitsSelect) // Marshals BV + Ranks
	if err != nil {
		return nil, err
	}

	totalSize := 8 + 8 + 1 + 8 + len(lbData) + 8 + len(selData)
	buf := make([]byte, totalSize)
	offset := 0
	binary.LittleEndian.PutUint64(buf[offset:], ef.numValues)
	offset += 8
	binary.LittleEndian.PutUint64(buf[offset:], ef.universe)
	offset += 8
	buf[offset] = ef.numLowBits
	offset += 1

	binary.LittleEndian.PutUint64(buf[offset:], uint64(len(lbData)))
	offset += 8
	copy(buf[offset:], lbData)
	offset += len(lbData)

	binary.LittleEndian.PutUint64(buf[offset:], uint64(len(selData)))
	offset += 8
	copy(buf[offset:], selData)
	offset += len(selData)

	if offset != totalSize {
		return nil, fmt.Errorf("EF marshal size mismatch")
	}
	return buf, nil
}

// UnmarshalBinary implements encoding.BinaryUnmarshaler.
func (ef *EliasFano) UnmarshalBinary(data []byte) error {
	// Deserialize fields
	if len(data) < 8+8+1 {
		return io.ErrUnexpectedEOF
	}
	offset := 0
	ef.numValues = binary.LittleEndian.Uint64(data[offset:])
	offset += 8
	ef.universe = binary.LittleEndian.Uint64(data[offset:])
	offset += 8
	ef.numLowBits = data[offset]
	offset += 1

	if offset+8 > len(data) {
		return io.ErrUnexpectedEOF
	}
	lbLen := binary.LittleEndian.Uint64(data[offset:])
	offset += 8
	if offset+int(lbLen) > len(data) {
		return io.ErrUnexpectedEOF
	}
	ef.lowerBits = NewCompactVector(0, 0) // Create empty before unmarshal
	err := serial.TryUnmarshal(ef.lowerBits, data[offset:offset+int(lbLen)])
	if err != nil {
		return err
	}
	offset += int(lbLen)

	if offset+8 > len(data) {
		return io.ErrUnexpectedEOF
	}
	selLen := binary.LittleEndian.Uint64(data[offset:])
	offset += 8
	if offset+int(selLen) > len(data) {
		return io.ErrUnexpectedEOF
	}
	ef.upperBitsSelect = &D1Array{} // Create empty before unmarshal
	err = serial.TryUnmarshal(ef.upperBitsSelect, data[offset:offset+int(selLen)])
	if err != nil {
		return err
	}
	ef.upperBits = ef.upperBitsSelect.bv // Link upperBitsSelect's bv
	offset += int(selLen)

	if offset != len(data) {
		return fmt.Errorf("extra data after EF unmarshal")
	}
	return nil
}
