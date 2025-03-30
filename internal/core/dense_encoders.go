// File: internal/core/dense_encoders.go
package core

import (
	"encoding/binary"
	"fmt"
	"io"
	"pthashgo/internal/serial"
	"reflect"
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

	// Initialize dm.Encoder correctly before use
	var zeroEncoder E // Get the type E (e.g., *RiceEncoder)
	typeE := reflect.TypeOf(zeroEncoder)

	// Check if we need to allocate a new encoder instance
	shouldAllocate := false
	if typeE != nil && typeE.Kind() == reflect.Ptr {
		// If E is a pointer type, check if the current dm.Encoder is nil
		if reflect.ValueOf(dm.Encoder).IsNil() {
			shouldAllocate = true
		}
	} else if typeE != nil {
		// If E is a value type, ensure dm.Encoder holds at least the zero value
		dm.Encoder = zeroEncoder
	}

	if shouldAllocate {
		// E is a pointer type (e.g., *RiceEncoder), and dm.Encoder is nil.
		// Allocate the underlying type.
		elemType := typeE.Elem()               // e.g., RiceEncoder
		newInstance := reflect.New(elemType)   // Create *RiceEncoder as reflect.Value
		// Assign the created pointer (*RiceEncoder) to dm.Encoder
		if newInstance.CanInterface() {
			dm.Encoder = newInstance.Interface().(E)
		} else {
			panic(fmt.Sprintf("Cannot interface allocated pointer encoder for type %v", elemType))
		}
		// Verify it's not nil after assignment
		if reflect.ValueOf(dm.Encoder).IsNil() {
			panic(fmt.Sprintf("Failed to allocate non-nil pointer encoder for type %v", typeE))
		}
	}

	// Now call Encode on the (guaranteed non-nil if pointer type) dm.Encoder
	return dm.Encoder.Encode(allPilots)
}
func (dm *DenseMono[E]) AccessDense(partition uint64, bucket uint64) uint64 {
	if dm.NumPartitions == 0 {
		return 0
	}
	idx := dm.NumPartitions*bucket + partition
	return dm.Encoder.Access(idx)
}
func (dm *DenseMono[E]) MarshalBinary() ([]byte, error) {
	// Marshal the Encoder field directly, not its address
	// This avoids double pointer issues when E is already a pointer type
	encData, err := serial.TryMarshal(dm.Encoder)
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

	// Handle allocation if E is a pointer type and is currently nil
	var zeroE E
	typeE := reflect.TypeOf(zeroE)
	if typeE != nil && typeE.Kind() == reflect.Ptr {
		if reflect.ValueOf(dm.Encoder).IsNil() { // Check if pointer field is nil
			elemType := typeE.Elem()
			newInstance := reflect.New(elemType)
			dm.Encoder = newInstance.Interface().(E)
		}
	}
	
	// Now unmarshal into the address of dm.Encoder
	err := serial.TryUnmarshal(&dm.Encoder, data[offset:offset+int(encLen)])
	if err != nil {
		return fmt.Errorf("failed to unmarshal underlying encoder: %w", err)
	}
	offset += int(encLen)
	if offset != len(data) {
		return fmt.Errorf("extra data after DenseMono unmarshal")
	}
	return nil
}

// --- DenseInterleaved Wrapper ---

// DenseInterleaved uses a separate encoder E for each bucket index.
type DenseInterleaved[E Encoder] struct {
	Encoders []E // Store E directly (could be values or pointers)
}

func (di *DenseInterleaved[E]) Name() string { var zeroE E; return "inter-" + zeroE.Name() }

func (di *DenseInterleaved[E]) Size() uint64 {
	// Total size is sum of sizes? Or just total elements? Let's use total elements.
	if len(di.Encoders) == 0 {
		return 0
	}
	
	// Check if E is a pointer type and the first element is nil
	typeE := reflect.TypeOf(di.Encoders[0])
	if typeE != nil && typeE.Kind() == reflect.Ptr && reflect.ValueOf(di.Encoders[0]).IsNil() {
		return 0
	}
	
	return di.Encoders[0].Size() * uint64(len(di.Encoders)) // Assumes all sub-encoders have same Size
}

func (di *DenseInterleaved[E]) NumBits() uint64 {
	bits := uint64(0)
	for _, enc := range di.Encoders {
		// Check if E is a pointer type and this element is nil
		typeE := reflect.TypeOf(enc)
		if typeE != nil && typeE.Kind() == reflect.Ptr && reflect.ValueOf(enc).IsNil() {
			continue // Skip nil pointers
		}
		bits += enc.NumBits()
	}
	return bits
}
func (di *DenseInterleaved[E]) Encode(pilots []uint64) error {
	return fmt.Errorf("DenseInterleaved.Encode not implemented; use EncodeDense")
}
func (di *DenseInterleaved[E]) Access(i uint64) uint64 {
	panic("DenseInterleaved.Access not applicable; use AccessDense")
}

func (di *DenseInterleaved[E]) EncodeDense(iterator PilotIterator, numPartitions uint64, numBucketsPerPartition uint64, numThreads int) error {
	di.Encoders = make([]E, numBucketsPerPartition) // Slice of E (value or pointer type)
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
			
			// Initialize the encoder based on whether E is a pointer type
			var zeroE E
			typeE := reflect.TypeOf(zeroE)
			if typeE != nil && typeE.Kind() == reflect.Ptr {
				// E is a pointer type, allocate a new instance
				elemType := typeE.Elem()
				newInstance := reflect.New(elemType).Interface().(E)
				di.Encoders[b] = newInstance
			}
			
			// Call Encode on the encoder (value or pointer)
			err := di.Encoders[b].Encode(pilotsForBucket)
			if err != nil {
				return fmt.Errorf("encoding bucket %d failed: %w", b, err)
			}
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
	if bucket >= uint64(len(di.Encoders)) {
		panic(fmt.Sprintf("DenseInterleaved.AccessDense: bucket index %d out of bounds (%d)", bucket, len(di.Encoders)))
	}
	
	// Check if E is a pointer type and the element is nil
	typeE := reflect.TypeOf(di.Encoders[bucket])
	if typeE != nil && typeE.Kind() == reflect.Ptr && reflect.ValueOf(di.Encoders[bucket]).IsNil() {
		panic(fmt.Sprintf("DenseInterleaved.AccessDense: encoder for bucket %d is nil", bucket))
	}
	
	return di.Encoders[bucket].Access(partition)
}
func (di *DenseInterleaved[E]) MarshalBinary() ([]byte, error) {
	numEncoders := uint64(len(di.Encoders))
	encodedData := make([][]byte, numEncoders)
	totalDataLen := 0
	for i, enc := range di.Encoders {
		// Marshal the encoder directly, not its address
		// This avoids double pointer issues when E is already a pointer type
		data, err := serial.TryMarshal(enc)
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
	di.Encoders = make([]E, numEncoders) // Slice of E (value or pointer type)
	for i := uint64(0); i < numEncoders; i++ {
		if offset+8 > len(data) {
			return io.ErrUnexpectedEOF
		}
		encLen := binary.LittleEndian.Uint64(data[offset:])
		offset += 8
		if uint64(offset)+encLen > uint64(len(data)) {
			return io.ErrUnexpectedEOF
		}
		
		// Handle pointer allocation if E is a pointer type
		var zeroE E
		typeE := reflect.TypeOf(zeroE)
		if typeE != nil && typeE.Kind() == reflect.Ptr {
			elemType := typeE.Elem()
			newInstance := reflect.New(elemType)
			di.Encoders[i] = newInstance.Interface().(E)
		}
		
		// Unmarshal into the address of the (potentially newly allocated) element
		err := serial.TryUnmarshal(&di.Encoders[i], data[offset:offset+int(encLen)])
		if err != nil {
			return fmt.Errorf("failed to unmarshal encoder %d: %w", i, err)
		}
		offset += int(encLen)
	}
	if offset != len(data) {
		return fmt.Errorf("extra data after DenseInterleaved unmarshal")
	}
	return nil
}

// --- DiffEncoder ---

// DiffEncoder encodes differences between consecutive elements.
type DiffEncoder[E Encoder] struct {
	Increment uint64
	Encoder   E // Store E directly (could be value or pointer type)
}

func (de *DiffEncoder[E]) Name() string {
	// Need an instance to call Name(). Get zero value's name.
	var zeroE E
	// If E is an interface type, zeroE is nil. Calling Name() on nil might panic.
	// We assume Encoder interface requires Name() to be callable on zero value.
	// If E is *MyStruct, zeroE is nil. We need an instance.
	if reflect.TypeOf(zeroE) != nil && reflect.TypeOf(zeroE).Kind() == reflect.Ptr {
		// Create a temporary instance to call Name()
		elemType := reflect.TypeOf(zeroE).Elem()
		tempInstance := reflect.New(elemType).Interface().(E)
		return "Diff" + tempInstance.Name()
	}
	// For value types or interfaces, call Name on the zero value.
	return "Diff" + zeroE.Name()
}

func (de *DiffEncoder[E]) Encode(values []uint64, increment uint64) error {
	de.Increment = increment
	n := uint64(len(values))

	// --- Ensure Encoder is initialized if it's a pointer type ---
	var zeroE E
	typeE := reflect.TypeOf(zeroE)
	if typeE != nil && typeE.Kind() == reflect.Ptr && reflect.ValueOf(de.Encoder).IsNil() {
		// E is a pointer type and it's nil - allocate it
		elemType := typeE.Elem()
		newInstance := reflect.New(elemType).Interface().(E)
		de.Encoder = newInstance
	}

	// Proceed with encoding
	if n == 0 {
		// Ensure Encode is called even for empty input, allowing initialization
		return de.Encoder.Encode([]uint64{})
	}

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
	// Call Encode directly on the Encoder field
	return de.Encoder.Encode(diffValues)
}

func (de *DiffEncoder[E]) Access(i uint64) uint64 {
	// Check if Encoder is nil (only possible if E is a pointer type)
	typeE := reflect.TypeOf(de.Encoder)
	if typeE != nil && typeE.Kind() == reflect.Ptr && reflect.ValueOf(de.Encoder).IsNil() {
		panic("DiffEncoder.Access called on nil encoder")
	}
	// Call Access directly on the Encoder field
	encodedDiff := de.Encoder.Access(i)

	// Decode the difference
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

func (de *DiffEncoder[E]) Size() uint64 {
	typeE := reflect.TypeOf(de.Encoder)
	if typeE != nil && typeE.Kind() == reflect.Ptr && reflect.ValueOf(de.Encoder).IsNil() {
		return 0
	}
	return de.Encoder.Size()
}

func (de *DiffEncoder[E]) NumBits() uint64 {
	bits := uint64(8 * 8) // Increment size
	typeE := reflect.TypeOf(de.Encoder)
	if !(typeE != nil && typeE.Kind() == reflect.Ptr && reflect.ValueOf(de.Encoder).IsNil()) {
		bits += de.Encoder.NumBits()
	}
	return bits
}

func (de *DiffEncoder[E]) MarshalBinary() ([]byte, error) {
	// Marshal the Encoder field directly, not its address
	// This avoids double pointer issues when E is already a pointer type
	encData, err := serial.TryMarshal(de.Encoder) // Pass the value directly
	if err != nil {
		return nil, fmt.Errorf("failed to marshal underlying encoder: %w", err)
	}
	
	// Prepare buffer: Increment + Encoder Data Length + Encoder Data
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
	expectedEndOffset := offset + int(encLen)
	if uint64(expectedEndOffset) > uint64(len(data)) {
		return io.ErrUnexpectedEOF
	}

	// Handle pointer allocation if E is a pointer type
	var zeroE E
	typeE := reflect.TypeOf(zeroE)
	if typeE != nil && typeE.Kind() == reflect.Ptr {
		if reflect.ValueOf(de.Encoder).IsNil() { // Check if pointer field is nil
			elemType := typeE.Elem()
			newInstance := reflect.New(elemType)
			de.Encoder = newInstance.Interface().(E)
		}
	}
	
	// Now unmarshal into the address of Encoder
	err := serial.TryUnmarshal(&de.Encoder, data[offset:expectedEndOffset])
	if err != nil {
		return fmt.Errorf("failed to unmarshal underlying encoder: %w", err)
	}
	
	offset = expectedEndOffset
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
