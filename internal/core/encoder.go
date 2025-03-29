// File: internal/core/encoder.go
package core

import (
	"encoding" // Add this import for BinaryMarshaler/Unmarshaler
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"math/bits"

	"pthashgo/internal/serial"
)

const logPhiMinus1 = -0.48121182505960345 // Pre-computed natural log: log(phi - 1)

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
	highBits      *BitVector     // Unary part
	highBitsD1    *D1Array       // Rank/Select structure for highBits
	lowBits       *CompactVector // Remainder part (fixed-width integers)
	optimalParamL uint8          // Optimal Rice parameter 'l' used for encoding
}

// optimalParameterKiely computes the optimal Rice parameter 'l'.
// Ported from C++ version's logic (using log2).
func optimalParameterKiely(values []uint64) uint8 {
	n := uint64(len(values))
	if n == 0 {
		return 0
	}
	sum := uint64(0)
	for _, v := range values {
		sum += v
	}
	if sum == 0 {
		return 0
	}
	p := float64(n) / (float64(sum) + float64(n))
	if p <= 0 || p >= 1 {
		return 0
	} // Return 0 if p indicates all zeros or invalid

	log1MinusP := math.Log(1.0 - p)
	if log1MinusP == 0 {
		return 63
	} // Avoid division by zero, large L for tiny p

	val := logPhiMinus1 / log1MinusP
	if val <= 0 {
		return 0
	} // Log2 undefined

	l_float := math.Floor(math.Log2(val)) + 1.0
	if l_float < 0 {
		return 0
	}
	if l_float >= 64 {
		return 63
	} // Clamp L
	return uint8(l_float)
}

// Encode encodes the values using Rice coding.
func (rs *RiceSequence) Encode(values []uint64) error {
	n := uint64(len(values))
	l := optimalParameterKiely(values)
	rs.optimalParamL = l

	// Estimate size needed for high bits
	sum := uint64(0)
	for _, v := range values {
		sum += v
	}
	highBitsEstimate := n
	if l < 64 {
		highBitsEstimate += (sum >> l)
	} else {
		highBitsEstimate = n
	}
	highBitsEstimate += 100 // Add buffer

	hbBuilder := NewBitVectorBuilder(highBitsEstimate)
	lbBuilder := NewCompactVectorBuilder(n, l) // Use correct width 'l'

	lowMask := uint64(0)
	if l > 0 {
		lowMask = (uint64(1) << l) - 1
	}

	for i, v := range values {
		low := uint64(0)
		if l > 0 {
			low = v & lowMask
		}
		lbBuilder.Set(uint64(i), low) // Set low bits

		high := uint64(0)
		if l < 64 {
			high = v >> l
		}

		for j := uint64(0); j < high; j++ {
			hbBuilder.PushBack(false)
		}
		hbBuilder.PushBack(true)
	}

	rs.lowBits = lbBuilder.Build()
	rs.highBits = hbBuilder.Build()
	// *** CRITICAL: Build D1Array on the *final* highBits BitVector ***
	rs.highBitsD1 = NewD1Array(rs.highBits) // Use the built BitVector

	return nil
}

// Access retrieves the value at index i. Relies on efficient D1Array.Select.
func (rs *RiceSequence) Access(i uint64) uint64 {
	if rs.lowBits == nil || i >= rs.lowBits.Size() {
		panic(fmt.Sprintf("RiceSequence.Access: index %d out of bounds (%d)", i, rs.Size()))
	}
	if rs.highBitsD1 == nil {
		panic("RiceSequence.Access: D1Array not initialized")
	}

	startPos := int64(-1)
	if i > 0 {
		startPos = int64(rs.highBitsD1.Select(i - 1))
	}
	endPos := int64(rs.highBitsD1.Select(i))
	high := uint64(endPos - (startPos + 1))
	low := rs.lowBits.Access(i)

	val := low
	if rs.optimalParamL < 64 {
		val |= (high << rs.optimalParamL)
	} else if high > 0 {
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
	lbBits, hbBits, d1Bits := uint64(0), uint64(0), uint64(0)
	if rs.lowBits != nil {
		lbBits = rs.lowBits.NumBitsStored()
	}
	// High bits size comes from D1Array's BitVector
	if rs.highBitsD1 != nil && rs.highBitsD1.bv != nil {
		hbBits = rs.highBitsD1.bv.NumBitsStored()
		d1Bits = rs.highBitsD1.NumBits() // D1Array structure size itself
	}
	return lbBits + hbBits + d1Bits + 8 // Add 8 bits for optimalParamL
}

// MarshalBinary implements encoding.BinaryMarshaler.
func (rs *RiceSequence) MarshalBinary() ([]byte, error) {
	lbData, err := serial.TryMarshal(rs.lowBits)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal lowBits: %w", err)
	}
	d1Data, err := serial.TryMarshal(rs.highBitsD1)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal highBitsD1: %w", err)
	}

	// Size: L(1) + reserved(7) + lbLen(8) + lbData + d1Len(8) + d1Data
	totalSize := 1 + 7 + 8 + len(lbData) + 8 + len(d1Data)
	buf := make([]byte, totalSize)
	offset := 0

	buf[offset] = rs.optimalParamL
	offset += 1 + 7 // L + reserved

	binary.LittleEndian.PutUint64(buf[offset:], uint64(len(lbData)))
	offset += 8
	copy(buf[offset:], lbData)
	offset += len(lbData)

	binary.LittleEndian.PutUint64(buf[offset:], uint64(len(d1Data)))
	offset += 8
	copy(buf[offset:], d1Data)
	offset += len(d1Data)

	if offset != totalSize {
		return nil, fmt.Errorf("rice sequence marshal size mismatch")
	}
	return buf, nil
}

// UnmarshalBinary implements encoding.BinaryUnmarshaler.
func (rs *RiceSequence) UnmarshalBinary(data []byte) error {
	headerSize := 1 + 7 + 8 + 8 // L, reserved, lbLen, d1Len
	if len(data) < headerSize {
		return io.ErrUnexpectedEOF
	}
	offset := 0

	rs.optimalParamL = data[offset]
	offset += 1 + 7 // L + reserved

	lbLen := binary.LittleEndian.Uint64(data[offset:])
	offset += 8
	if uint64(offset)+lbLen > uint64(len(data)) {
		return io.ErrUnexpectedEOF
	}
	rs.lowBits = NewCompactVector(0, 0) // Create empty
	err := serial.TryUnmarshal(rs.lowBits, data[offset:offset+int(lbLen)])
	if err != nil {
		return fmt.Errorf("failed to unmarshal lowBits: %w", err)
	}
	offset += int(lbLen)

	d1Len := binary.LittleEndian.Uint64(data[offset:])
	offset += 8
	if uint64(offset)+d1Len > uint64(len(data)) {
		return io.ErrUnexpectedEOF
	}
	rs.highBitsD1 = &D1Array{} // Create empty D1Array
	err = serial.TryUnmarshal(rs.highBitsD1, data[offset:offset+int(d1Len)])
	if err != nil {
		return fmt.Errorf("failed to unmarshal highBitsD1: %w", err)
	}
	rs.highBits = rs.highBitsD1.bv // Link the underlying BitVector
	offset += int(d1Len)

	if offset != len(data) {
		return fmt.Errorf("extra data after rice sequence unmarshal")
	}
	return nil
}

// --- Rice Encoder Implementation ---

// RiceEncoder uses RiceSequence to encode pilot values.
type RiceEncoder struct {
	values RiceSequence
}

func (e *RiceEncoder) Name() string                      { return "R" }
func (e *RiceEncoder) Encode(pilots []uint64) error      { return e.values.Encode(pilots) }
func (e *RiceEncoder) Access(i uint64) uint64            { return e.values.Access(i) }
func (e *RiceEncoder) NumBits() uint64                   { return e.values.NumBits() }
func (e *RiceEncoder) Size() uint64                      { return e.values.Size() }
func (e *RiceEncoder) MarshalBinary() ([]byte, error)    { return serial.TryMarshal(&e.values) }
func (e *RiceEncoder) UnmarshalBinary(data []byte) error { return serial.TryUnmarshal(&e.values, data) }

// --- CompactEncoder Implementation ---

// CompactEncoder stores values using minimal fixed-width integers.
type CompactEncoder struct {
	values *CompactVector // Pointer to allow nil for empty
}

func (e *CompactEncoder) Name() string { return "C" }

// Encode implements the Encoder interface.
func (e *CompactEncoder) Encode(pilots []uint64) error {
	n := uint64(len(pilots))
	if n == 0 {
		e.values = NewCompactVector(0, 0)
		return nil
	}
	maxVal := uint64(0)
	for _, p := range pilots {
		if p > maxVal {
			maxVal = p
		}
	}
	w := uint8(1) // Min width is 1, even for all zeros
	if maxVal > 0 {
		w = uint8(bits.Len64(maxVal))
	}

	cvb := NewCompactVectorBuilder(n, w)
	for i, p := range pilots {
		cvb.Set(uint64(i), p)
	}
	e.values = cvb.Build()
	return nil
}

func (e *CompactEncoder) Access(i uint64) uint64 {
	if e.values == nil {
		panic("CompactEncoder.Access: accessing nil vector")
	}
	return e.values.Access(i)
}
func (e *CompactEncoder) NumBits() uint64 {
	if e.values == nil {
		return 0
	}
	return e.values.NumBitsStored()
}
func (e *CompactEncoder) Size() uint64 {
	if e.values == nil {
		return 0
	}
	return e.values.Size()
}
func (e *CompactEncoder) MarshalBinary() ([]byte, error) { return serial.TryMarshal(e.values) }
func (e *CompactEncoder) UnmarshalBinary(data []byte) error {
	// Need to reconstruct CompactVector first
	e.values = &CompactVector{} // Create empty one
	return serial.TryUnmarshal(e.values, data)
}

// --- EliasFano Encoder Implementation ---

// EliasFano stores a monotone sequence compactly.
type EliasFano struct {
	numValues       uint64
	universe        uint64 // Max value + 1
	numLowBits      uint8
	lowerBits       *CompactVector // Stores lower l bits of each value
	upperBitsSelect *D1Array       // Rank/Select structure for upperBits (contains bv)
}

// NewEliasFano creates an empty EliasFano encoder.
func NewEliasFano() *EliasFano {
	return &EliasFano{}
}

// Encode builds the Elias-Fano structure from a sorted slice of values.
// Input values MUST be monotonically increasing.
func (ef *EliasFano) Encode(sortedValues []uint64) error {
	n := uint64(len(sortedValues))
	ef.numValues = n

	if n == 0 {
		ef.universe = 0
		ef.numLowBits = 0
		ef.lowerBits = NewCompactVector(0, 0)
		// upperBitsSelect should contain an empty BitVector
		ef.upperBitsSelect = NewD1Array(NewBitVector(0))
		return nil
	}

	u := uint64(0)
	if n > 0 {
		u = sortedValues[n-1] + 1
		// Check monotonicity (optional, but good practice)
		for i := uint64(1); i < n; i++ {
			if sortedValues[i] < sortedValues[i-1] {
				return fmt.Errorf("EliasFano.Encode input must be monotonically increasing (value[%d]=%d < value[%d]=%d)", i, sortedValues[i], i-1, sortedValues[i-1])
			}
		}
	}
	ef.universe = u

	l := uint8(0)
	if n > 0 && u > n {
		ratio := float64(u) / float64(n)
		lFloat := math.Log2(ratio) // Use Log2 directly
		if lFloat >= 0 {           // Ensure non-negative before floor
			l = uint8(math.Floor(lFloat))
		}
	}
	if l > 64 {
		l = 64
	} // Clamp
	ef.numLowBits = l

	upperBitsSizeEstimate := uint64(0)
	if u > 0 && l < 64 {
		upperBitsSizeEstimate = n + n*(u>>l)
	} else {
		upperBitsSizeEstimate = n
	}
	upperBitsSizeEstimate += 100

	lbBuilder := NewCompactVectorBuilder(n, l)
	ubBuilder := NewBitVectorBuilder(upperBitsSizeEstimate)

	lastHighPart := uint64(0)
	mask := uint64(0)
	if l < 64 {
		mask = (uint64(1) << l) - 1
	} else {
		mask = ^uint64(0)
	}

	for i, v := range sortedValues {
		low := uint64(0)
		high := uint64(0)
		if l < 64 {
			low = v & mask
			high = v >> l
		} else {
			low = v
		}

		lbBuilder.Set(uint64(i), low)

		delta := high - lastHighPart
		for k := uint64(0); k < delta; k++ {
			ubBuilder.PushBack(false)
		} // 0
		ubBuilder.PushBack(true) // 1
		lastHighPart = high
	}

	ef.lowerBits = lbBuilder.Build()
	upperBitsBV := ubBuilder.Build()             // Build the final bit vector
	ef.upperBitsSelect = NewD1Array(upperBitsBV) // Build D1Array on the BV

	if ef.upperBitsSelect.numSetBits != n {
		return fmt.Errorf("internal EliasFano error: upperBitsSelect has %d set bits, expected %d", ef.upperBitsSelect.numSetBits, n)
	}

	return nil
}

// Access retrieves the value with rank `i` (0-based). Relies on efficient D1Array.Select.
func (ef *EliasFano) Access(rank uint64) uint64 {
	if rank >= ef.numValues {
		panic(fmt.Sprintf("EliasFano.Access: rank %d out of bounds (%d)", rank, ef.numValues))
	}
	if ef.numValues == 0 {
		panic("EliasFano.Access: accessing empty structure")
	}
	if ef.upperBitsSelect == nil {
		panic("EliasFano.Access: D1Array not initialized")
	}

	selectRank := rank
	pos := ef.upperBitsSelect.Select(selectRank) // Position of (rank+1)-th '1'

	high := uint64(0)
	if rank == 0 {
		high = pos
	} else {
		prevPos := ef.upperBitsSelect.Select(rank - 1)
		high = pos - (prevPos + 1)
	}
	low := uint64(0)
	if ef.numLowBits > 0 {
		low = ef.lowerBits.Access(rank)
	}

	val := low
	if ef.numLowBits < 64 {
		val |= (high << ef.numLowBits)
	} else if high > 0 {
		panic("EliasFano decoding error: high part > 0 with l=64")
	}
	return val
}

func (ef *EliasFano) Size() uint64 { return ef.numValues }
func (ef *EliasFano) NumBits() uint64 {
	lbBits, selBits := uint64(0), uint64(0)
	if ef.lowerBits != nil {
		lbBits = ef.lowerBits.NumBitsStored()
	}
	if ef.upperBitsSelect != nil {
		selBits = ef.upperBitsSelect.NumBits()
	} // D1Array size includes its internal BV size
	return 8*8 + 8*8 + 8 + lbBits + selBits // numValues, universe, numLowBits + data
}
func (ef *EliasFano) Name() string { return "EF" }

func (ef *EliasFano) MarshalBinary() ([]byte, error) {
	// Serialize numValues, universe, numLowBits, lowerBits, upperBitsSelect
	lbData, err := serial.TryMarshal(ef.lowerBits)
	if err != nil {
		return nil, err
	}
	selData, err := serial.TryMarshal(ef.upperBitsSelect)
	if err != nil {
		return nil, err
	}

	totalSize := 8 + 8 + 1 + 7 + 8 + len(lbData) + 8 + len(selData) // Added 7 reserved bytes
	buf := make([]byte, totalSize)
	offset := 0
	binary.LittleEndian.PutUint64(buf[offset:], ef.numValues)
	offset += 8
	binary.LittleEndian.PutUint64(buf[offset:], ef.universe)
	offset += 8
	buf[offset] = ef.numLowBits
	offset += 1 + 7 // L + reserved

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

func (ef *EliasFano) UnmarshalBinary(data []byte) error {
	headerSize := 8 + 8 + 1 + 7 + 8 + 8 // Fields before variable data
	if len(data) < headerSize {
		return io.ErrUnexpectedEOF
	}
	offset := 0
	ef.numValues = binary.LittleEndian.Uint64(data[offset:])
	offset += 8
	ef.universe = binary.LittleEndian.Uint64(data[offset:])
	offset += 8
	ef.numLowBits = data[offset]
	offset += 1 + 7 // L + reserved

	lbLen := binary.LittleEndian.Uint64(data[offset:])
	offset += 8
	if uint64(offset)+lbLen > uint64(len(data)) {
		return io.ErrUnexpectedEOF
	}
	ef.lowerBits = &CompactVector{} // Create empty
	err := serial.TryUnmarshal(ef.lowerBits, data[offset:offset+int(lbLen)])
	if err != nil {
		return fmt.Errorf("failed to unmarshal lowerBits: %w", err)
	}
	offset += int(lbLen)

	selLen := binary.LittleEndian.Uint64(data[offset:])
	offset += 8
	if uint64(offset)+selLen > uint64(len(data)) {
		return io.ErrUnexpectedEOF
	}
	ef.upperBitsSelect = &D1Array{} // Create empty
	err = serial.TryUnmarshal(ef.upperBitsSelect, data[offset:offset+int(selLen)])
	if err != nil {
		return fmt.Errorf("failed to unmarshal upperBitsSelect: %w", err)
	}
	offset += int(selLen)

	if offset != len(data) {
		return fmt.Errorf("extra data after EF unmarshal")
	}
	// Sanity check (optional)
	if ef.upperBitsSelect.numSetBits != ef.numValues {
		return fmt.Errorf("EF consistency error: numSetBits %d != numValues %d", ef.upperBitsSelect.numSetBits, ef.numValues)
	}
	return nil
}

// IsEliasFanoStubbed checks if the EliasFano implementation is just a stub.
// Useful for skipping tests that rely on its functionality.
func IsEliasFanoStubbed() bool {
	// Check if a key method like NumBits returns 0 or Access panics immediately.
	// If D1Array is a stub, EF will likely be too.
	ef := NewEliasFano()
	// A simple check: if NumBits is always 0 for a non-empty structure, it's likely a stub.
	// Let's encode a single value and check NumBits.
	err := ef.Encode([]uint64{10})
	return err != nil || ef.NumBits() < 64 // A real EF should use more than just base field sizes
}

// --- Placeholder Dictionary/SDC/Dual etc. ---
// Add stubs or full implementations later
