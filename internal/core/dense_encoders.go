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
// E is the underlying Encoder type (can be MyStruct or *MyStruct).
type DiffEncoder[E Encoder] struct {
	Increment uint64
	Encoder   E // CHANGE: Store E directly (value or pointer)
}

func (de *DiffEncoder[E]) Name() string {
	// Use the stored Encoder if available, otherwise create a zero value to get the name.
	var nameEncoder E // Start with zero value

	eType := reflect.TypeOf(de.Encoder) // Get type of stored Encoder E
	isPtr := eType != nil && eType.Kind() == reflect.Ptr
	isNilOrZero := false
	if eType == nil { // Handle case where E is an interface type and de.Encoder is the nil interface value
		isNilOrZero = true
	} else if isPtr {
		isNilOrZero = reflect.ValueOf(de.Encoder).IsNil()
	} else {
		isNilOrZero = reflect.ValueOf(de.Encoder).IsZero() // Check if struct is zero value
	}

	if !isNilOrZero {
		// If de.Encoder is already initialized (non-nil pointer or non-zero struct), use it
		nameEncoder = de.Encoder
	} else if isPtr && eType != nil {
		// If E is a pointer type and de.Encoder is nil, create a temporary *instance* to call Name()
		elemType := eType.Elem()
		tempInstance := reflect.New(elemType).Interface().(E) // Create *MyStruct instance
		nameEncoder = tempInstance                            // Use the temporary instance for Name()
	}
	// If E is value type and zero, nameEncoder is already the correct zero value.
	// If E is interface type and nil, nameEncoder is nil interface value, calling Name() might panic if not handled by concrete type.
	// Assuming Encoder interface requires Name() to be callable even on zero/nil value if possible.

	// Check if nameEncoder is still effectively nil/zero before calling Name
	if reflect.ValueOf(nameEncoder).IsZero() {
		// Attempt to get name from a default zero value if possible
		var defaultE E
		// Check if defaultE itself is valid before calling Name
		if reflect.TypeOf(defaultE) != nil {
			return "Diff" + defaultE.Name()
		}
		return "Diff<Unknown>" // Fallback if type info isn't available
	}

	return "Diff" + nameEncoder.Name()
}

func (de *DiffEncoder[E]) Encode(values []uint64, increment uint64) error {
	de.Increment = increment
	n := uint64(len(values))

	// --- Initialization Check ---
	// If E is a pointer type (*MyStruct), ensure de.Encoder is not nil.
	encoderType := reflect.TypeOf(de.Encoder) // Type of E
	if encoderType != nil && encoderType.Kind() == reflect.Ptr {
		if reflect.ValueOf(de.Encoder).IsNil() {
			// Allocate the underlying struct type that E points to
			elemType := encoderType.Elem()       // Get MyStruct type from *MyStruct
			newInstance := reflect.New(elemType) // Creates *MyStruct
			// Assign the newly created pointer (*MyStruct) to de.Encoder (which is type E = *MyStruct)
			de.Encoder = newInstance.Interface().(E)
		}
	} else if encoderType == nil {
		return fmt.Errorf("cannot encode with nil encoder type E")
	}
	// If E is a value type (MyStruct), de.Encoder is already a valid zero value struct.

	// --- Prepare diffValues ---
	diffValues := make([]uint64, n)
	if n > 0 {
		expected := int64(0)
		for i, val := range values {
			toEncode := int64(val) - expected
			// Use absolute value and sign bit for encoding
			absToEncode := uint64(toEncode)
			if toEncode < 0 {
				absToEncode = uint64(-toEncode)
			}
			signBit := uint64(1) // 1 for positive or zero
			if toEncode < 0 {
				signBit = 0 // 0 for negative
			}
			diffValues[i] = (absToEncode << 1) | signBit
			expected += int64(increment)
		}
	}

	// --- Call Encode on the underlying encoder ---
	// Go handles method calls correctly whether E is T or *T, if de.Encoder is addressable.
	// Since de.Encoder is a field, it is addressable.
	return de.Encoder.Encode(diffValues)
}

func (de *DiffEncoder[E]) Access(i uint64) uint64 {
	// --- Initialization Check ---
	// Ensure encoder is initialized, especially if E is a pointer type.
	encoderType := reflect.TypeOf(de.Encoder)
	if encoderType != nil && encoderType.Kind() == reflect.Ptr {
		if reflect.ValueOf(de.Encoder).IsNil() {
			panic(fmt.Sprintf("DiffEncoder[%T].Access called before Encode or on nil encoder", de.Encoder))
		}
	} else if encoderType == nil {
		panic(fmt.Sprintf("DiffEncoder[<nil>].Access called on nil encoder type"))
	}
	// Assume E value types are usable in their zero state if Access is called before Encode.

	// Call Access on the underlying encoder
	encodedDiff := de.Encoder.Access(i)

	// Decode the difference
	expected := int64(i * de.Increment)
	absDiff := encodedDiff >> 1
	signBit := encodedDiff & 1
	diff := int64(absDiff)
	if signBit == 0 { // 0 means negative in our encoding
		diff = -diff
	}
	result := expected + diff
	return uint64(result)
}

func (de *DiffEncoder[E]) Size() uint64 {
	encoderType := reflect.TypeOf(de.Encoder)
	if encoderType != nil && encoderType.Kind() == reflect.Ptr {
		if reflect.ValueOf(de.Encoder).IsNil() {
			return 0 // Size is 0 if underlying pointer encoder is nil
		}
	} else if encoderType == nil {
		return 0 // Size is 0 if type E is nil interface
	}
	// If value type, call Size() on it (might be zero).
	return de.Encoder.Size()
}

func (de *DiffEncoder[E]) NumBits() uint64 {
	bits := uint64(8 * 8) // Increment size in bits

	encoderType := reflect.TypeOf(de.Encoder)
	if encoderType != nil && encoderType.Kind() == reflect.Ptr {
		if reflect.ValueOf(de.Encoder).IsNil() {
			return bits // Only increment bits if underlying pointer encoder is nil
		}
	} else if encoderType == nil {
		return bits // Only increment bits if type E is nil interface
	}

	// Add bits from the underlying encoder
	bits += de.Encoder.NumBits()
	return bits
}

func (de *DiffEncoder[E]) MarshalBinary() ([]byte, error) {
	encoderType := reflect.TypeOf(de.Encoder)
	isPtr := encoderType != nil && encoderType.Kind() == reflect.Ptr
	isNil := false
	if isPtr {
		isNil = reflect.ValueOf(de.Encoder).IsNil()
	} else if encoderType == nil {
		isNil = true // Consider nil interface type as nil
	}

	var encData []byte
	var err error
	if isNil {
		encData = []byte{} // Marshal nil/zero encoder as empty data slice
	} else {
		// Marshal the underlying encoder (E or *E)
		encData, err = serial.TryMarshal(de.Encoder)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal underlying encoder of type %T: %w", de.Encoder, err)
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
		// return io.ErrUnexpectedEOF (less informative)
	}
	offset := 0
	de.Increment = binary.LittleEndian.Uint64(data[offset:])
	offset += 8
	encLen := binary.LittleEndian.Uint64(data[offset:])
	offset += 8

	expectedEndOffset := offset + int(encLen)
	if uint64(expectedEndOffset) > uint64(len(data)) {
		return fmt.Errorf("cannot unmarshal DiffEncoder, underlying encoder data length (%d) exceeds available data (%d)", encLen, len(data)-offset)
		// return io.ErrUnexpectedEOF
	}

	encoderData := data[offset:expectedEndOffset]

	// Determine if E is a pointer type. Use zero value's type info.
	var zeroE E
	eType := reflect.TypeOf(zeroE)
	if eType == nil {
		// Handle case where E is an interface type. We cannot instantiate it directly.
		// If encLen > 0, we cannot proceed without knowing the concrete type.
		if encLen > 0 {
			return fmt.Errorf("cannot unmarshal DiffEncoder into nil interface type E with non-empty data")
		}
		// If encLen is 0, we can set de.Encoder to nil (its zero value).
		de.Encoder = zeroE // Assign zero value (nil interface)
		return nil
	}
	isPtr := eType.Kind() == reflect.Ptr

	if encLen == 0 {
		// Data represents a nil/zero encoder. Set de.Encoder to its zero value.
		if isPtr {
			// Zero value for pointer type E (*MyStruct) is nil
			de.Encoder = reflect.Zero(eType).Interface().(E)
		} else {
			// Zero value for value type E (MyStruct) is the zero struct
			de.Encoder = reflect.Zero(eType).Interface().(E)
		}
		// We might have extra data if the original buffer was larger than necessary
		// Check if we consumed exactly the expected data length overall
		if expectedEndOffset != len(data) {
			return fmt.Errorf("warning: extra data (%d bytes) after unmarshaling zero DiffEncoder", len(data)-expectedEndOffset)
		}
		return nil
	}

	// We have data to unmarshal for the underlying encoder.
	if isPtr {
		// E is *MyStruct. Need to ensure de.Encoder points to a valid struct instance.
		if reflect.ValueOf(de.Encoder).IsNil() {
			elemType := eType.Elem()                 // Get MyStruct type
			newInstance := reflect.New(elemType)     // Creates *MyStruct
			de.Encoder = newInstance.Interface().(E) // Assign newly created *MyStruct to de.Encoder
		}
		// Now de.Encoder is a non-nil pointer (*MyStruct). Unmarshal into the pointed-to struct.
		// serial.TryUnmarshal typically needs the actual object or a pointer to it.
		// If TryUnmarshal uses reflection internally, passing de.Encoder (*MyStruct) should work.
		err := serial.TryUnmarshal(de.Encoder, encoderData)
		if err != nil {
			return fmt.Errorf("failed to unmarshal underlying pointer encoder of type %T: %w", de.Encoder, err)
		}
	} else {
		// E is MyStruct (value type).
		// serial.TryUnmarshal needs a pointer to the value to modify it.
		// Pass the address of de.Encoder (&MyStruct).
		err := serial.TryUnmarshal(&de.Encoder, encoderData)
		if err != nil {
			// Providing type info in error message
			var target E
			return fmt.Errorf("failed to unmarshal underlying value encoder of type %T: %w", target, err)
		}
	}

	// Check if we consumed exactly the expected data length overall
	if expectedEndOffset != len(data) {
		return fmt.Errorf("warning: extra data (%d bytes) after unmarshaling non-zero DiffEncoder", len(data)-expectedEndOffset)
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
