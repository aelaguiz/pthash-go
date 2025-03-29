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

// DiffEncoder encodes differences between consecutive elements.
// Stores a pointer to the underlying encoder instance E.
type DiffEncoder[E Encoder] struct {
	Increment uint64
	// Encoder   E // OLD: Stores the value/pointer directly
	encoderPtr *E // CHANGE: Always store a pointer to the encoder instance E
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

	// --- Ensure encoderPtr points to a valid instance ---
	if de.encoderPtr == nil {
		// We need to create a new zero value of type E and make encoderPtr point to it.
		// This works whether E is MyStruct or *MyStruct. If E is *MyStruct,
		// we create a pointer to a nil pointer, which is not what we want.
		// We need a pointer to an *allocated* E.

		// Let's use reflection again, but applied correctly to initialize de.encoderPtr.
		var zeroE E
		typeE := reflect.TypeOf(zeroE) // Get type E

		// Handle nil interface type explicitly
		if typeE == nil {
			return fmt.Errorf("cannot encode with nil encoder type E")
		}

		// Create a new instance of type E (value receiver friendly) or *E (pointer receiver friendly)
		// reflect.New(typeE) always returns a *T where T is the type passed.
		// If E is MyStruct, typeE is MyStruct, New returns *MyStruct.
		// If E is *MyStruct, typeE is *MyStruct, New returns **MyStruct. Not useful.

		// Let's allocate based on whether E itself is a pointer.
		var newInstance reflect.Value
		if typeE.Kind() == reflect.Ptr {
			// E is a pointer type (*MyStruct). We need to create a MyStruct and store *MyStruct.
			elemType := typeE.Elem()          // Get MyStruct type
			newInstance = reflect.New(elemType) // Creates *MyStruct (as reflect.Value)
		} else {
			// E is a value type (MyStruct). We need a pointer to it.
			newInstance = reflect.New(typeE) // Creates *MyStruct (as reflect.Value)
		}

		// Assign the created pointer (type *E) to de.encoderPtr
		// The type assertion ensures we assign the correct pointer type *E.
		de.encoderPtr = newInstance.Interface().(*E)
	}
	// Now de.encoderPtr is guaranteed to be non-nil and point to a valid E zero value.

	// Proceed with encoding, calling methods on the pointed-to instance
	if n == 0 {
		// Ensure Encode is called even for empty input, allowing initialization
		return (*de.encoderPtr).Encode([]uint64{})
	}

	diffValues := make([]uint64, n)
	expected := int64(0)
	for i, val := range values {
		toEncode := int64(val) - expected
		absToEncode := uint64(toEncode)
		if toEncode < 0 { absToEncode = uint64(-toEncode) }
		signBit := uint64(1); if toEncode < 0 { signBit = 0 }
		diffValues[i] = (absToEncode << 1) | signBit
		expected += int64(increment)
	}
	// Call Encode on the instance pointed to by encoderPtr
	return (*de.encoderPtr).Encode(diffValues)
}

func (de *DiffEncoder[E]) Access(i uint64) uint64 {
	if de.encoderPtr == nil {
		panic("DiffEncoder.Access called before Encode or on nil encoder")
	}
	// Call Access on the instance pointed to by encoderPtr
	encodedDiff := (*de.encoderPtr).Access(i)

	// Decode the difference
	expected := int64(i * de.Increment)
	absDiff := encodedDiff >> 1
	signBit := encodedDiff & 1
	diff := int64(absDiff); if signBit == 0 { diff = -diff }
	result := expected + diff
	return uint64(result)
}

func (de *DiffEncoder[E]) Size() uint64 {
	if de.encoderPtr == nil { return 0 }
	// Call Size on the instance pointed to by encoderPtr
	return (*de.encoderPtr).Size()
}

func (de *DiffEncoder[E]) NumBits() uint64 {
	bits := uint64(8*8) // Increment size in bits
	if de.encoderPtr != nil {
		// Call NumBits on the instance pointed to by encoderPtr
		bits += (*de.encoderPtr).NumBits()
	}
	return bits
}

func (de *DiffEncoder[E]) MarshalBinary() ([]byte, error) {
	var encData []byte
	var err error
	if de.encoderPtr == nil {
		encData = []byte{} // Marshal nil encoder as empty data slice
	} else {
		// Marshal the pointer (*E) which should point to the actual encoder instance
		encData, err = serial.TryMarshal(de.encoderPtr)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal underlying encoder pointed to by encoderPtr: %w", err)
		}
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
		// This should not happen if logic is correct
		return nil, fmt.Errorf("internal error: DiffEncoder marshal size mismatch, expected %d, wrote %d", totalSize, offset)
	}
	return buf, nil
}

func (de *DiffEncoder[E]) UnmarshalBinary(data []byte) error {
	if len(data) < 16 {
		return fmt.Errorf("cannot unmarshal DiffEncoder, data too short (%d bytes, need at least 16)", len(data))
	}
	offset := 0
	de.Increment = binary.LittleEndian.Uint64(data[offset:])
	offset += 8
	encLen := binary.LittleEndian.Uint64(data[offset:])
	offset += 8

	expectedEndOffset := offset + int(encLen)
	if uint64(expectedEndOffset) > uint64(len(data)) {
		return fmt.Errorf("cannot unmarshal DiffEncoder, underlying encoder data length (%d) exceeds available data (%d)", encLen, len(data)-offset)
	}

	encoderData := data[offset:expectedEndOffset]

	if encLen == 0 {
		// Data represents a nil/zero encoder. Set de.encoderPtr to nil.
		de.encoderPtr = nil
	} else {
		// We have data to unmarshal. Allocate a new E instance and make encoderPtr point to it.
		var zeroE E
		typeE := reflect.TypeOf(zeroE)
		if typeE == nil {
			return fmt.Errorf("cannot unmarshal DiffEncoder into nil interface type E with non-empty data")
		}

		var newInstance reflect.Value
		if typeE.Kind() == reflect.Ptr {
			elemType := typeE.Elem()
			newInstance = reflect.New(elemType) // Allocates *elemType
		} else {
			newInstance = reflect.New(typeE) // Allocates *valueType
		}
		// Assign the allocated pointer (*E) to de.encoderPtr
		de.encoderPtr = newInstance.Interface().(*E)

		// Unmarshal into the object pointed to by de.encoderPtr
		err := serial.TryUnmarshal(de.encoderPtr, encoderData)
		if err != nil {
			return fmt.Errorf("failed to unmarshal underlying encoder pointed to by encoderPtr: %w", err)
		}
	}

	// Check if we consumed exactly the expected data length overall
	if expectedEndOffset != len(data) {
		return fmt.Errorf("warning: extra data (%d bytes) after unmarshaling DiffEncoder", len(data)-expectedEndOffset)
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
