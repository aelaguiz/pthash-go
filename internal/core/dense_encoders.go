// pthash-go/internal/core/dense_encoders.go
package core

import (
	"fmt"
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
	// Need a way to get name from E. Assume E has Name().
	var zeroE E
	return "mono-" + zeroE.Name()
}

func (dm *DenseMono[E]) Size() uint64    { return dm.Encoder.Size() }
func (dm *DenseMono[E]) NumBits() uint64 { return 8*8 + dm.Encoder.NumBits() } // Add NumPartitions size

// Encode is the standard encoder interface (not typically used directly for Dense).
func (dm *DenseMono[E]) Encode(pilots []uint64) error {
	// This could encode flattened pilots if needed, but EncodeDense is primary.
	return dm.Encoder.Encode(pilots)
}

// Access is the standard encoder interface.
func (dm *DenseMono[E]) Access(i uint64) uint64 {
	return dm.Encoder.Access(i)
}

// EncodeDense implements the DenseEncoder interface.
func (dm *DenseMono[E]) EncodeDense(iterator PilotIterator, numPartitions uint64, numBucketsPerPartition uint64, numThreads int) error {
	dm.NumPartitions = numPartitions
	totalPilots := numPartitions * numBucketsPerPartition
	allPilots := make([]uint64, 0, totalPilots) // Collect all pilots first

	for iterator.HasNext() {
		allPilots = append(allPilots, iterator.Next())
	}
	if uint64(len(allPilots)) != totalPilots {
		return fmt.Errorf("DenseMono.EncodeDense: iterator provided %d pilots, expected %d", len(allPilots), totalPilots)
	}

	// Encode the flattened slice using the underlying encoder
	return dm.Encoder.Encode(allPilots)
}

// AccessDense implements the DenseEncoder interface.
// C++ Access: m_encoder.access(m_num_partitions * bucket + partition);
func (dm *DenseMono[E]) AccessDense(partition uint64, bucket uint64) uint64 {
	if dm.NumPartitions == 0 {
		return 0
	} // Avoid division by zero
	idx := dm.NumPartitions*bucket + partition
	return dm.Encoder.Access(idx)
}

// MarshalBinary / UnmarshalBinary for DenseMono
func (dm *DenseMono[E]) MarshalBinary() ([]byte, error) {
	// TODO: Marshal NumPartitions + dm.Encoder
	return nil, fmt.Errorf("DenseMono.MarshalBinary not implemented")
}
func (dm *DenseMono[E]) UnmarshalBinary(data []byte) error {
	// TODO: Unmarshal NumPartitions + dm.Encoder
	return fmt.Errorf("DenseMono.UnmarshalBinary not implemented")
}

// --- DenseInterleaved Wrapper ---

// DenseInterleaved uses a separate encoder E for each bucket index across all partitions.
type DenseInterleaved[E Encoder] struct {
	Encoders []E // One encoder per bucket index
}

func (di *DenseInterleaved[E]) Name() string {
	var zeroE E
	return "inter-" + zeroE.Name()
}

func (di *DenseInterleaved[E]) Size() uint64 {
	// Total size is sum of sizes? Or just total elements? Let's use total elements.
	if len(di.Encoders) == 0 {
		return 0
	}
	return di.Encoders[0].Size() * uint64(len(di.Encoders)) // Assumes all sub-encoders have same Size (NumPartitions)
}

func (di *DenseInterleaved[E]) NumBits() uint64 {
	bits := uint64(0)
	for _, enc := range di.Encoders {
		bits += enc.NumBits()
	}
	return bits // Add slice overhead? Minimal compared to data.
}

// Encode is the standard encoder interface (not typically used directly).
func (di *DenseInterleaved[E]) Encode(pilots []uint64) error {
	return fmt.Errorf("DenseInterleaved.Encode not implemented; use EncodeDense")
}

// Access is the standard encoder interface (not directly applicable).
func (di *DenseInterleaved[E]) Access(i uint64) uint64 {
	panic("DenseInterleaved.Access not applicable; use AccessDense")
}

// EncodeDense implements the DenseEncoder interface.
func (di *DenseInterleaved[E]) EncodeDense(iterator PilotIterator, numPartitions uint64, numBucketsPerPartition uint64, numThreads int) error {
	di.Encoders = make([]E, numBucketsPerPartition)
	totalPilots := numPartitions * numBucketsPerPartition

	// Collect all pilots first to allow parallel processing
	allPilots := make([]uint64, 0, totalPilots)
	for iterator.HasNext() {
		allPilots = append(allPilots, iterator.Next())
	}
	if uint64(len(allPilots)) != totalPilots {
		return fmt.Errorf("DenseInterleaved.EncodeDense: iterator provided %d pilots, expected %d", len(allPilots), totalPilots)
	}

	// Function to encode a range of buckets
	encodeBucketRange := func(bucketStart, bucketEnd uint64) error {
		// Extract data for this bucket range
		pilotsForBucket := make([]uint64, numPartitions)
		for b := bucketStart; b < bucketEnd; b++ {
			// Collect pilots for bucket 'b' across all partitions
			for p := uint64(0); p < numPartitions; p++ {
				idx := b*numPartitions + p // Index in the flattened allPilots slice
				pilotsForBucket[p] = allPilots[idx]
			}
			// Create and encode for this bucket
			var encoder E // New instance for this bucket
			err := encoder.Encode(pilotsForBucket)
			if err != nil {
				return fmt.Errorf("encoding bucket %d failed: %w", b, err)
			}
			di.Encoders[b] = encoder // Store the encoded result
		}
		return nil
	}

	// Parallel execution
	if numThreads > 1 && numBucketsPerPartition > uint64(numThreads) {
		var wg sync.WaitGroup
		errChan := make(chan error, numThreads)
		bucketsPerThread := (numBucketsPerPartition + uint64(numThreads) - 1) / uint64(numThreads)

		for t := 0; t < numThreads; t++ {
			start := uint64(t) * bucketsPerThread
			end := start + bucketsPerThread
			if end > numBucketsPerPartition {
				end = numBucketsPerPartition
			}
			if start >= end {
				continue
			} // Skip if no work

			wg.Add(1)
			go func(bStart, bEnd uint64) {
				defer wg.Done()
				err := encodeBucketRange(bStart, bEnd)
				errChan <- err // Send nil on success
			}(start, end)
		}
		wg.Wait()
		close(errChan)
		// Check for errors
		for err := range errChan {
			if err != nil {
				return err
			} // Return first error
		}
	} else { // Sequential execution
		err := encodeBucketRange(0, numBucketsPerPartition)
		if err != nil {
			return err
		}
	}

	return nil
}

// AccessDense implements the DenseEncoder interface.
// C++: m_encoders[bucket].access(partition);
func (di *DenseInterleaved[E]) AccessDense(partition uint64, bucket uint64) uint64 {
	if bucket >= uint64(len(di.Encoders)) {
		panic(fmt.Sprintf("DenseInterleaved.AccessDense: bucket index %d out of bounds (%d)", bucket, len(di.Encoders)))
	}
	return di.Encoders[bucket].Access(partition)
}

// MarshalBinary / UnmarshalBinary for DenseInterleaved
func (di *DenseInterleaved[E]) MarshalBinary() ([]byte, error) {
	// TODO: Marshal slice length + each encoder
	return nil, fmt.Errorf("DenseInterleaved.MarshalBinary not implemented")
}
func (di *DenseInterleaved[E]) UnmarshalBinary(data []byte) error {
	// TODO: Unmarshal slice length + each encoder
	return fmt.Errorf("DenseInterleaved.UnmarshalBinary not implemented")
}

// --- DiffEncoder (for offsets) ---
// Requires CompactEncoder to be implemented or stubbed

// DiffEncoder encodes differences between consecutive elements assuming a fixed increment.
type DiffEncoder[E Encoder] struct {
	Increment uint64
	Encoder   E // Stores the encoded differences (zigzag encoded absolute values)
}

func (de *DiffEncoder[E]) Name() string {
	var zeroE E
	return "Diff" + zeroE.Name() // e.g., DiffC
}

func (de *DiffEncoder[E]) Encode(values []uint64, increment uint64) error {
	de.Increment = increment
	n := uint64(len(values))

	// --- FIX: Allocate encoder instance ---
	// Check if E is a pointer type. This is tricky without reflection deep dive.
	// Assume for now we need to allocate if E can be nil.
	// A better approach might be a NewEncoder factory function constraint.
	// Simple fix: always allocate a new zero value of the underlying type.
	var encoder E
	// If E is a pointer type (*Something), 'encoder' is nil here. We need new().
	if reflect.TypeOf(encoder).Kind() == reflect.Ptr {
		// Create a new zero value of the element type E points to,
		// then take its address and cast to E.
		elemType := reflect.TypeOf(encoder).Elem()
		newElem := reflect.New(elemType).Interface() // Returns pointer to new zero value
		encoder = newElem.(E)                        // Assign the pointer of the correct type E
	}
	// If E is not a pointer, 'encoder' already holds the zero value.

	de.Encoder = encoder // Assign the non-nil encoder

	if n == 0 {
		return de.Encoder.Encode([]uint64{}) // Encode empty slice using the valid encoder instance
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

	// Encode the processed diffs using the underlying encoder E
	return de.Encoder.Encode(diffValues)
}

// Access decodes the difference and adds the expected value.
func (de *DiffEncoder[E]) Access(i uint64) uint64 {
	encodedDiff := de.Encoder.Access(i)
	expected := int64(i * de.Increment)

	// Decode zigzag
	absDiff := encodedDiff >> 1
	signBit := encodedDiff & 1

	diff := int64(absDiff)
	if signBit == 0 { // Was negative
		diff = -diff
	}

	// Add diff to expected value
	// Need to handle potential overflow if result exceeds uint64 max, though unlikely for offsets.
	result := expected + diff
	return uint64(result) // Cast back to uint64
}

// Size returns the number of encoded values.
func (de *DiffEncoder[E]) Size() uint64 {
	return de.Encoder.Size()
}

// NumBits returns the total number of bits used for storage.
func (de *DiffEncoder[E]) NumBits() uint64 {
	// Size of Increment + size of underlying encoder
	return 8*8 + de.Encoder.NumBits()
}

// MarshalBinary / UnmarshalBinary for DiffEncoder
func (de *DiffEncoder[E]) MarshalBinary() ([]byte, error) {
	// TODO: Marshal Increment + de.Encoder
	return nil, fmt.Errorf("DiffEncoder.MarshalBinary not implemented")
}
func (de *DiffEncoder[E]) UnmarshalBinary(data []byte) error {
	// TODO: Unmarshal Increment + de.Encoder
	return fmt.Errorf("DiffEncoder.UnmarshalBinary not implemented")
}

// --- Convenience Type Aliases (assuming underlying encoders exist) ---
type MonoR = DenseMono[*RiceEncoder]            // Requires RiceEncoder
type InterR = DenseInterleaved[*RiceEncoder]    // Requires RiceEncoder
type MonoC = DenseMono[*CompactEncoder]         // Requires CompactEncoder
type InterC = DenseInterleaved[*CompactEncoder] // Requires CompactEncoder
// type InterCInterR = DenseDual[InterC, InterR, 1, 3] // Requires DenseDual and underlying

// Add DenseDual structure later if needed
