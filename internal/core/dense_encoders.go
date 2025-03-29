// File: internal/core/dense_encoders.go
package core

import (
	"encoding/binary"
	"fmt"
	"io"
	"pthashgo/internal/serial"

	// "reflect" // Keep commented unless needed for advanced generic handling
	"sync"
)

// DenseEncoder is a marker interface for encoders suitable for DensePartitionedPHF.
type DenseEncoder interface {
	Encoder // Inherits basic Encoder methods

	// EncodeDense encodes pilots partitioned across partitions and buckets.
	// iterator provides pilots sequentially: p0b0, p1b0, ..., pNb0, p0b1, p1b1, ...
	EncodeDense(iterator PilotIterator, numPartitions uint64, numBucketsPerPartition uint64, numThreads int) error

	// AccessDense retrieves the pilot for a specific partition and bucket.
	AccessDense(partition uint64, bucket uint64) uint64
}

// PilotIterator defines an interface to iterate over pilot values sequentially.
// This is needed because Go doesn't have direct C++ style iterators.
type PilotIterator interface {
	HasNext() bool
	Next() uint64
}

// --- DenseMono Wrapper ---

// DenseMono uses a single underlying encoder E for all pilots.
type DenseMono[E Encoder] struct {
	NumPartitions uint64 // Store for AccessDense calculation
	Encoder       E      // The underlying encoder instance
}

func (dm *DenseMono[E]) Name() string {
	var zeroE E // Get zero value to call Name()
	return "mono-" + zeroE.Name()
}
func (dm *DenseMono[E]) Size() uint64                 { return dm.Encoder.Size() }
func (dm *DenseMono[E]) NumBits() uint64              { return 8*8 + dm.Encoder.NumBits() } // Add NumPartitions size
func (dm *DenseMono[E]) Encode(pilots []uint64) error { return dm.Encoder.Encode(pilots) }
func (dm *DenseMono[E]) Access(i uint64) uint64       { return dm.Encoder.Access(i) }

func (dm *DenseMono[E]) EncodeDense(iterator PilotIterator, numPartitions uint64, numBucketsPerPartition uint64, numThreads int) error {
	dm.NumPartitions = numPartitions
	totalPilots := numPartitions * numBucketsPerPartition
	allPilots := make([]uint64, 0, totalPilots)
	for iterator.HasNext() {
		allPilots = append(allPilots, iterator.Next())
	}
	if uint64(len(allPilots)) != totalPilots {
		return fmt.Errorf("DenseMono.EncodeDense: iterator provided %d pilots, expected %d", len(allPilots), totalPilots)
	}
	// Create a new encoder instance to encode into
	var encoder E                       // Create zero value
	dm.Encoder = encoder                // Assign (potentially zero value)
	return dm.Encoder.Encode(allPilots) // Call encode on the instance
}
func (dm *DenseMono[E]) AccessDense(partition uint64, bucket uint64) uint64 {
	if dm.NumPartitions == 0 {
		return 0
	}
	idx := dm.NumPartitions*bucket + partition
	return dm.Encoder.Access(idx)
}
func (dm *DenseMono[E]) MarshalBinary() ([]byte, error) {
	encData, err := serial.TryMarshal(&dm.Encoder) // Marshal pointer if E is pointer type
	if err != nil {
		return nil, fmt.Errorf("failed to marshal underlying encoder: %w", err)
	}
	// Size: numPart(8) + encLen(8) + encData
	totalSize := 8 + 8 + len(encData)
	buf := make([]byte, totalSize)
	offset := 0
	binary.LittleEndian.PutUint64(buf[offset:], dm.NumPartitions)
	offset += 8
	binary.LittleEndian.PutUint64(buf[offset:], uint64(len(encData)))
	offset += 8
	copy(buf[offset:], encData)
	offset += len(encData)
	if offset != totalSize {
		return nil, fmt.Errorf("DenseMono marshal size mismatch")
	}
	return buf, nil
}
func (dm *DenseMono[E]) UnmarshalBinary(data []byte) error {
	if len(data) < 16 {
		return io.ErrUnexpectedEOF
	}
	offset := 0
	dm.NumPartitions = binary.LittleEndian.Uint64(data[offset:])
	offset += 8
	encLen := binary.LittleEndian.Uint64(data[offset:])
	offset += 8
	if uint64(offset)+encLen > uint64(len(data)) {
		return io.ErrUnexpectedEOF
	}
	// Create zero value encoder and unmarshal into it
	var encoder E
	err := serial.TryUnmarshal(&encoder, data[offset:offset+int(encLen)])
	if err != nil {
		return fmt.Errorf("failed to unmarshal underlying encoder: %w", err)
	}
	dm.Encoder = encoder
	offset += int(encLen)
	if offset != len(data) {
		return fmt.Errorf("extra data after DenseMono unmarshal")
	}
	return nil
}

// --- DenseInterleaved Wrapper ---

// DenseInterleaved uses a separate encoder E for each bucket index.
type DenseInterleaved[E Encoder] struct {
	Encoders []*E // Store pointers to encoders
}

func (di *DenseInterleaved[E]) Name() string { var zeroE E; return "inter-" + zeroE.Name() }

func (di *DenseInterleaved[E]) Size() uint64 {
	// Total size is sum of sizes? Or just total elements? Let's use total elements.
	if len(di.Encoders) == 0 || di.Encoders[0] == nil { // Add nil check for first element
		return 0
	}
	// return di.Encoders[0].Size() * uint64(len(di.Encoders)) // Assumes all sub-encoders have same Size (NumPartitions)
	// CHANGE: Dereference the pointer before calling Size()
	return (*di.Encoders[0]).Size() * uint64(len(di.Encoders))
}

// CHANGE THIS METHOD (NumBits):
func (di *DenseInterleaved[E]) NumBits() uint64 {
	bits := uint64(0)
	for _, encPtr := range di.Encoders { // Renamed loop variable for clarity
		if encPtr != nil { // Add nil check for safety
			// bits += enc.NumBits()
			bits += (*encPtr).NumBits() // CHANGE: Dereference pointer before calling NumBits
		}
	}
	return bits // Add slice overhead? Minimal compared to data.
}
func (di *DenseInterleaved[E]) Encode(pilots []uint64) error {
	return fmt.Errorf("DenseInterleaved.Encode not implemented; use EncodeDense")
}
func (di *DenseInterleaved[E]) Access(i uint64) uint64 {
	panic("DenseInterleaved.Access not applicable; use AccessDense")
}

func (di *DenseInterleaved[E]) EncodeDense(iterator PilotIterator, numPartitions uint64, numBucketsPerPartition uint64, numThreads int) error {
	di.Encoders = make([]*E, numBucketsPerPartition) // Slice of pointers
	totalPilots := numPartitions * numBucketsPerPartition
	allPilots := make([]uint64, 0, totalPilots)
	for iterator.HasNext() {
		allPilots = append(allPilots, iterator.Next())
	}
	if uint64(len(allPilots)) != totalPilots {
		return fmt.Errorf("DenseInterleaved.EncodeDense: iterator provided %d pilots, expected %d", len(allPilots), totalPilots)
	}

	encodeBucketRange := func(bucketStart, bucketEnd uint64) error {
		pilotsForBucket := make([]uint64, numPartitions)
		for b := bucketStart; b < bucketEnd; b++ {
			for p := uint64(0); p < numPartitions; p++ {
				idx := b*numPartitions + p
				pilotsForBucket[p] = allPilots[idx]
			}
			// Create a pointer to a new zero value
			encoderPtr := new(E)
			// Need to call Encode on the pointer receiver
			err := (*encoderPtr).Encode(pilotsForBucket)
			if err != nil {
				return fmt.Errorf("encoding bucket %d failed: %w", b, err)
			}
			di.Encoders[b] = encoderPtr // Store the pointer
		}
		return nil
	}
	if numThreads > 1 && numBucketsPerPartition > uint64(numThreads) {
		var wg sync.WaitGroup
		errChan := make(chan error, numThreads)
		bucketsPerThread := (numBucketsPerPartition + uint64(numThreads) - 1) / uint64(numThreads)
		for t := 0; t < numThreads; t++ {
			start, end := uint64(t)*bucketsPerThread, (uint64(t)+1)*bucketsPerThread
			if end > numBucketsPerPartition {
				end = numBucketsPerPartition
			}
			if start >= end {
				continue
			}
			wg.Add(1)
			go func(bStart, bEnd uint64) { defer wg.Done(); errChan <- encodeBucketRange(bStart, bEnd) }(start, end)
		}
		wg.Wait()
		close(errChan)
		for err := range errChan {
			if err != nil {
				return err
			}
		}
	} else {
		if err := encodeBucketRange(0, numBucketsPerPartition); err != nil {
			return err
		}
	}
	return nil
}

func (di *DenseInterleaved[E]) AccessDense(partition uint64, bucket uint64) uint64 {
	if bucket >= uint64(len(di.Encoders)) || di.Encoders[bucket] == nil {
		panic(fmt.Sprintf("DenseInterleaved.AccessDense: bucket index %d out of bounds or nil encoder (%d)", bucket, len(di.Encoders)))
	}
	return (*di.Encoders[bucket]).Access(partition) // Access via pointer
}
func (di *DenseInterleaved[E]) MarshalBinary() ([]byte, error) {
	numEncoders := uint64(len(di.Encoders))
	encodedData := make([][]byte, numEncoders)
	totalDataLen := 0
	for i, encPtr := range di.Encoders {
		if encPtr == nil {
			// Handle nil pointers - serialize as zero length
			encodedData[i] = []byte{}
			continue
		}
		data, err := serial.TryMarshal(encPtr) // Marshal the pointer itself
		if err != nil {
			return nil, fmt.Errorf("failed to marshal encoder %d: %w", i, err)
		}
		encodedData[i] = data
		totalDataLen += len(data)
	}
	// Size: numEncoders(8) + loop( encLen(8) + encData )
	totalSize := 8 + int(numEncoders*8) + totalDataLen
	buf := make([]byte, totalSize)
	offset := 0
	binary.LittleEndian.PutUint64(buf[offset:], numEncoders)
	offset += 8
	for _, data := range encodedData {
		binary.LittleEndian.PutUint64(buf[offset:], uint64(len(data)))
		offset += 8
		copy(buf[offset:], data)
		offset += len(data)
	}
	if offset != totalSize {
		return nil, fmt.Errorf("DenseInterleaved marshal size mismatch")
	}
	return buf, nil
}
func (di *DenseInterleaved[E]) UnmarshalBinary(data []byte) error {
	if len(data) < 8 {
		return io.ErrUnexpectedEOF
	}
	offset := 0
	numEncoders := binary.LittleEndian.Uint64(data[offset:])
	offset += 8
	di.Encoders = make([]*E, numEncoders) // Slice of pointers
	for i := uint64(0); i < numEncoders; i++ {
		if offset+8 > len(data) {
			return io.ErrUnexpectedEOF
		}
		encLen := binary.LittleEndian.Uint64(data[offset:])
		offset += 8
		if uint64(offset)+encLen > uint64(len(data)) {
			return io.ErrUnexpectedEOF
		}
		// Skip empty encoders (nil pointers)
		if encLen == 0 {
			di.Encoders[i] = nil
			continue
		}
		// Create zero value pointer and unmarshal into it
		encoderPtr := new(E)
		err := serial.TryUnmarshal(encoderPtr, data[offset:offset+int(encLen)])
		if err != nil {
			return fmt.Errorf("failed to unmarshal encoder %d: %w", i, err)
		}
		di.Encoders[i] = encoderPtr // Assign pointer
		offset += int(encLen)
	}
	if offset != len(data) {
		return fmt.Errorf("extra data after DenseInterleaved unmarshal")
	}
	return nil
}

// --- DiffEncoder ---

// DiffEncoder encodes differences between consecutive elements assuming a fixed increment.
// Relies on the underlying Encoder E (typically CompactEncoder).
type DiffEncoder[E Encoder] struct {
	Increment uint64
	Encoder   E // Stores the encoded differences (zigzag encoded absolute values)
}

func (de *DiffEncoder[E]) Name() string { var zeroE E; return "Diff" + zeroE.Name() }

func (de *DiffEncoder[E]) Encode(values []uint64, increment uint64) error {
	de.Increment = increment
	n := uint64(len(values))

	// Initialize the underlying encoder (create zero value)
	var encoder E
	de.Encoder = encoder // Assign zero value

	if n == 0 {
		return de.Encoder.Encode([]uint64{})
	} // Encode empty slice

	diffValues := make([]uint64, n)
	expected := int64(0)
	for i, val := range values {
		toEncode := int64(val) - expected
		absToEncode := uint64(toEncode)
		if toEncode < 0 {
			absToEncode = uint64(-toEncode)
		}
		signBit := uint64(1)
		if toEncode < 0 {
			signBit = 0
		}
		diffValues[i] = (absToEncode << 1) | signBit
		expected += int64(increment)
	}
	return de.Encoder.Encode(diffValues) // Encode diffs using the initialized Encoder
}
func (de *DiffEncoder[E]) Access(i uint64) uint64 {
	encodedDiff := de.Encoder.Access(i)
	expected := int64(i * de.Increment)
	absDiff := encodedDiff >> 1
	signBit := encodedDiff & 1
	diff := int64(absDiff)
	if signBit == 0 {
		diff = -diff
	}
	result := expected + diff
	return uint64(result)
}
func (de *DiffEncoder[E]) Size() uint64    { return de.Encoder.Size() }
func (de *DiffEncoder[E]) NumBits() uint64 { return 8*8 + de.Encoder.NumBits() } // Increment + Encoder bits

func (de *DiffEncoder[E]) MarshalBinary() ([]byte, error) {
	encData, err := serial.TryMarshal(&de.Encoder)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal underlying encoder: %w", err)
	}
	// Size: increment(8) + encLen(8) + encData
	totalSize := 8 + 8 + len(encData)
	buf := make([]byte, totalSize)
	offset := 0
	binary.LittleEndian.PutUint64(buf[offset:], de.Increment)
	offset += 8
	binary.LittleEndian.PutUint64(buf[offset:], uint64(len(encData)))
	offset += 8
	copy(buf[offset:], encData)
	offset += len(encData)
	if offset != totalSize {
		return nil, fmt.Errorf("DiffEncoder marshal size mismatch")
	}
	return buf, nil
}
func (de *DiffEncoder[E]) UnmarshalBinary(data []byte) error {
	if len(data) < 16 {
		return io.ErrUnexpectedEOF
	}
	offset := 0
	de.Increment = binary.LittleEndian.Uint64(data[offset:])
	offset += 8
	encLen := binary.LittleEndian.Uint64(data[offset:])
	offset += 8
	if uint64(offset)+encLen > uint64(len(data)) {
		return io.ErrUnexpectedEOF
	}
	// Create zero value encoder and unmarshal into it
	var encoder E
	err := serial.TryUnmarshal(&encoder, data[offset:offset+int(encLen)])
	if err != nil {
		return fmt.Errorf("failed to unmarshal underlying encoder: %w", err)
	}
	de.Encoder = encoder
	offset += int(encLen)
	if offset != len(data) {
		return fmt.Errorf("extra data after DiffEncoder unmarshal")
	}
	return nil
}

// --- Convenience Type Aliases ---
// Make these pointers consistent with how they might be used in PHF structs
type MonoR = DenseMono[*RiceEncoder]
type InterR = DenseInterleaved[*RiceEncoder] // Pointers for interleaved
type MonoC = DenseMono[*CompactEncoder]
type InterC = DenseInterleaved[*CompactEncoder] // Pointers for interleaved
// Add more aliases (MonoD, InterD, MonoEF, InterEF, Dual variants) when base types exist
