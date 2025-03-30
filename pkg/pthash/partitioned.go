package pthash

import (
	"encoding/binary"
	"fmt"
	"io"
	"pthashgo/internal/builder"
	"pthashgo/internal/core"
	"pthashgo/internal/serial" // Use centralized helpers
	"pthashgo/internal/util"
	"sync"
	"time"
	"unsafe"
)

// PartitionedPHF implements the partitioned PTHash function (PTHash-HEM).
// Generic parameters:
// K: Key type
// H: Hasher type implementing core.Hasher[K]
// B: Bucketer type implementing core.Bucketer (for sub-PHFs)
// E: Encoder type implementing core.Encoder (for sub-PHFs)
type PartitionedPHF[K any, H core.Hasher[K], B core.Bucketer, E core.Encoder] struct {
	seed        uint64
	numKeys     uint64
	tableSize   uint64                  // Total size estimate across partitions
	partitioner *core.RangeBucketer     // Used to map key hash to partition
	partitions  []partition[K, H, B, E] // Slice of sub-PHFs
	hasher      H                       // Store hasher instance
	isMinimal   bool
	searchType  core.SearchType
}

// partition stores the offset and the sub-PHF for a single partition.
type partition[K any, H core.Hasher[K], B core.Bucketer, E core.Encoder] struct {
	offset uint64                 // Starting offset for this partition's output range
	phf    *SinglePHF[K, H, B, E] // The actual sub-PHF for this partition
}

// NewPartitionedPHF creates an empty PartitionedPHF instance.
func NewPartitionedPHF[K any, H core.Hasher[K], B core.Bucketer, E core.Encoder](minimal bool, search core.SearchType) *PartitionedPHF[K, H, B, E] {
	var hasher H // Zero value hasher
	return &PartitionedPHF[K, H, B, E]{
		isMinimal:   minimal,
		searchType:  search,
		hasher:      hasher,
		partitioner: &core.RangeBucketer{}, // Initialize as pointer
		// Other fields initialized by Build
	}
}

// Build constructs the PartitionedPHF using a completed internal memory partitioned builder.
// The builder should have already run its BuildFromKeys (which includes BuildSubPHFs).
func (f *PartitionedPHF[K, H, B, E]) Build(
	pb *builder.InternalMemoryBuilderPartitionedPHF[K, H, B],
	config *core.BuildConfig,
) (time.Duration, error) {

	start := time.Now()
	util.Log(config.Verbose, "Starting PartitionedPHF.Build final stage...")

	// Check config consistency
	if f.isMinimal != config.Minimal || f.searchType != config.Search {
		return 0, fmt.Errorf("PartitionedPHF type parameters mismatch build config")
	}
	if pb == nil {
		return 0, fmt.Errorf("builder cannot be nil")
	}
	// Basic check if sub-builders seem initialized
	if len(pb.Builders()) != int(pb.NumPartitions()) {
		return 0, fmt.Errorf("builder state inconsistent: %d sub-builders for %d partitions", len(pb.Builders()), pb.NumPartitions())
	}

	// --- Copy final state from builder ---
	f.seed = pb.Seed()
	f.numKeys = pb.NumKeys()
	f.tableSize = pb.TableSize()
	f.partitioner = pb.Partitioner() // This now assigns a pointer to a pointer
	f.hasher = pb.Hasher()           // Get hasher instance from builder
	numPartitions := pb.NumPartitions()
	f.partitions = make([]partition[K, H, B, E], numPartitions)
	subBuilders := pb.Builders()
	offsets := pb.Offsets()

	// --- Create final PHF structures for each partition ---
	// This involves encoding the data from each sub-builder into the final sub-PHF structure.
	// encodeStart := time.Now()
	var totalEncodingTime time.Duration

	// Use concurrency similar to C++ if beneficial
	numThreads := config.NumThreads
	if numThreads <= 0 {
		numThreads = 1
	}
	if numThreads > int(numPartitions) {
		numThreads = int(numPartitions)
	}
	if numPartitions == 0 {
		numThreads = 0
	} // Avoid spawning goroutines if no partitions

	if numThreads > 1 {
		var wg sync.WaitGroup
		errChan := make(chan error, numPartitions)
		timingChan := make(chan time.Duration, numPartitions)
		sem := make(chan struct{}, numThreads)

		wg.Add(int(numPartitions))
		for i := uint64(0); i < numPartitions; i++ {
			go func(idx uint64) {
				sem <- struct{}{}
				defer wg.Done()
				defer func() { <-sem }()

				subBuilder := subBuilders[idx]
				subPHF := NewSinglePHF[K, H, B, E](f.isMinimal, f.searchType) // Create empty sub-PHF

				// Sub-config primarily needed for consistency checks within subPHF.Build
				subConfig := *config
				subConfig.Minimal = f.isMinimal
				subConfig.Search = f.searchType
				// Alpha/Lambda/Buckets not strictly needed here if builder holds results

				encTime, err := subPHF.Build(subBuilder, &subConfig)
				if err != nil {
					errChan <- fmt.Errorf("partition %d final build failed: %w", idx, err)
					timingChan <- 0
					return
				}

				f.partitions[idx] = partition[K, H, B, E]{
					offset: offsets[idx],
					phf:    subPHF,
				}
				errChan <- nil
				timingChan <- encTime
			}(i)
		}

		wg.Wait()
		close(errChan)
		close(timingChan)

		var firstError error
		for err := range errChan {
			if err != nil && firstError == nil {
				firstError = err
			}
		}
		if firstError != nil {
			return 0, firstError
		}

		for encTime := range timingChan {
			totalEncodingTime += encTime
		}

	} else { // Sequential encoding
		for i := uint64(0); i < numPartitions; i++ {
			subBuilder := subBuilders[i]
			subPHF := NewSinglePHF[K, H, B, E](f.isMinimal, f.searchType)
			subConfig := *config
			subConfig.Minimal = f.isMinimal
			subConfig.Search = f.searchType

			encTime, err := subPHF.Build(subBuilder, &subConfig)
			if err != nil {
				return 0, fmt.Errorf("partition %d final build failed: %w", i, err)
			}
			f.partitions[i] = partition[K, H, B, E]{
				offset: offsets[i],
				phf:    subPHF,
			}
			totalEncodingTime += encTime
		}
	}

	totalBuildTime := time.Since(start) // Includes sub-build time stored in builder + final encoding
	util.Log(config.Verbose, "PartitionedPHF final build stage took: %v (Encoding sum: %v)", totalBuildTime, totalEncodingTime)

	// Return total encoding time (sum across partitions)
	return totalEncodingTime, nil
}

// Lookup evaluates the hash function for a key.
func (f *PartitionedPHF[K, H, B, E]) Lookup(key K) uint64 {
	// 1. Hash the key
	hash := f.hasher.Hash(key, f.seed)

	// 2. Determine the partition
	partitionIdx := f.partitioner.Bucket(hash.Mix())
	partitionIdxUint64 := uint64(partitionIdx)
	if partitionIdxUint64 >= uint64(len(f.partitions)) {
		panic(fmt.Sprintf("partition index %d out of bounds (%d)", partitionIdxUint64, len(f.partitions)))
	}

	// 3. Lookup within the sub-PHF of that partition
	part := f.partitions[partitionIdxUint64]
	if part.phf == nil {
		panic(fmt.Sprintf("lookup error: partition %d has nil sub-PHF", partitionIdxUint64))
	}
	// We need to pass the *original hash* to the sub-PHF, not the key again.
	// Modify SinglePHF to have a LookupFromHash method? Or pass hash here.
	// Let's assume SinglePHF.Lookup can handle the Hash128 directly if needed,
	// or we modify SinglePHF. For now, call the existing Lookup, assuming
	// it re-hashes internally (less efficient but works structurally).
	// *** Optimization Note: SinglePHF.Lookup should ideally take Hash128 ***
	subPosition := part.phf.Lookup(key) // Inefficient: re-hashes key

	// 4. Add the partition offset
	return part.offset + subPosition
}

// --- Accessors ---
func (f *PartitionedPHF[K, H, B, E]) NumKeys() uint64   { return f.numKeys }
func (f *PartitionedPHF[K, H, B, E]) TableSize() uint64 { return f.tableSize } // Note: Estimated total
func (f *PartitionedPHF[K, H, B, E]) Seed() uint64      { return f.seed }
func (f *PartitionedPHF[K, H, B, E]) IsMinimal() bool   { return f.isMinimal }

// --- Space Calculation ---
func (f *PartitionedPHF[K, H, B, E]) NumBits() uint64 {
	totalBits := uint64(8 * (8 + 8 + 8)) // seed, numKeys, tableSize
	totalBits += f.partitioner.NumBits()
	totalBits += uint64(unsafe.Sizeof(f.partitions)) // Size of slice header

	for _, p := range f.partitions {
		totalBits += 8 * 8 // Offset size
		if p.phf != nil {
			totalBits += p.phf.NumBits()
		}
	}
	return totalBits
}

// --- Serialization ---

const partitionedPHFMagic = "PPHF"

// MarshalBinary implements encoding.BinaryMarshaler.
func (f *PartitionedPHF[K, H, B, E]) MarshalBinary() ([]byte, error) {
	// 1. Marshal partitioner
	partitionerData, err := serial.TryMarshal(f.partitioner) // Use helper
	if err != nil {
		return nil, fmt.Errorf("failed to marshal partitioner: %w", err)
	}

	// 2. Marshal each sub-PHF
	partitionData := make([][]byte, len(f.partitions))
	totalPartitionDataLen := 0
	for i, p := range f.partitions {
		if p.phf == nil {
			partitionData[i] = []byte{}
		} else {
			data, err := serial.TryMarshal(p.phf) // Use helper
			if err != nil {
				return nil, fmt.Errorf("failed to marshal sub-PHF %d: %w", i, err)
			}
			partitionData[i] = data
			totalPartitionDataLen += len(data)
		}
	}

	// 3. Calculate total size
	headerSize := 4 + 1 + 1 + 1 + 1 + 8 + 8 + 8 // magic, version, flags, reserved, core params
	totalSize := headerSize
	totalSize += 8 + len(partitionerData)                        // partitioner len+data
	totalSize += 8                                               // numPartitions
	totalSize += len(f.partitions)*(8+8) + totalPartitionDataLen // offsets + lengths + data

	buf := make([]byte, totalSize)
	offset := 0

	// 4. Write Header
	copy(buf[offset:offset+4], []byte(partitionedPHFMagic))
	offset += 4
	buf[offset] = 1 // Version
	offset += 1
	flags := byte(0)
	if f.isMinimal {
		flags |= 1
	}
	flags |= (byte(f.searchType) << 1)
	buf[offset] = flags
	offset += 1
	buf[offset] = 0 // Reserved byte 1
	offset += 1
	buf[offset] = 0 // Reserved byte 2
	offset += 1

	// 5. Write Core Params
	binary.LittleEndian.PutUint64(buf[offset:offset+8], f.seed)
	offset += 8
	binary.LittleEndian.PutUint64(buf[offset:offset+8], f.numKeys)
	offset += 8
	binary.LittleEndian.PutUint64(buf[offset:offset+8], f.tableSize)
	offset += 8

	// 6. Write Partitioner
	binary.LittleEndian.PutUint64(buf[offset:offset+8], uint64(len(partitionerData)))
	offset += 8
	copy(buf[offset:], partitionerData)
	offset += len(partitionerData)

	// 7. Write Partitions
	numPartitions := uint64(len(f.partitions))
	binary.LittleEndian.PutUint64(buf[offset:offset+8], numPartitions)
	offset += 8
	for i := uint64(0); i < numPartitions; i++ {
		pData := partitionData[i]
		pLen := uint64(len(pData))
		binary.LittleEndian.PutUint64(buf[offset:offset+8], f.partitions[i].offset)
		offset += 8
		binary.LittleEndian.PutUint64(buf[offset:offset+8], pLen)
		offset += 8
		copy(buf[offset:], pData)
		offset += int(pLen)
	}

	if offset != totalSize {
		return nil, fmt.Errorf("internal marshal error: offset %d != totalSize %d", offset, totalSize)
	}

	return buf, nil
}

// UnmarshalBinary implements encoding.BinaryUnmarshaler.
func (f *PartitionedPHF[K, H, B, E]) UnmarshalBinary(data []byte) error {
	headerSize := 4 + 1 + 1 + 1 + 1 + 8 + 8 + 8
	if len(data) < headerSize {
		return io.ErrUnexpectedEOF
	}
	offset := 0

	// 1. Read Header
	if string(data[offset:offset+4]) != partitionedPHFMagic {
		return fmt.Errorf("invalid magic identifier")
	}
	offset += 4
	version := data[offset]
	offset += 1
	if version != 1 {
		return fmt.Errorf("unsupported version: %d", version)
	}
	flags := data[offset]
	f.isMinimal = (flags & 1) != 0
	f.searchType = core.SearchType((flags >> 1) & 3)
	offset += 1 + 1 + 1 // flags + reserved

	// 2. Read Core Params
	f.seed = binary.LittleEndian.Uint64(data[offset : offset+8])
	offset += 8
	f.numKeys = binary.LittleEndian.Uint64(data[offset : offset+8])
	offset += 8
	f.tableSize = binary.LittleEndian.Uint64(data[offset : offset+8])
	offset += 8

	// 3. Read Partitioner
	if offset+8 > len(data) {
		return io.ErrUnexpectedEOF
	}
	partitionerLen := binary.LittleEndian.Uint64(data[offset : offset+8])
	offset += 8
	if uint64(offset)+partitionerLen > uint64(len(data)) {
		return io.ErrUnexpectedEOF
	}
	f.partitioner = &core.RangeBucketer{} // Create instance
	err := serial.TryUnmarshal(f.partitioner, data[offset:offset+int(partitionerLen)])
	if err != nil {
		return fmt.Errorf("failed to unmarshal partitioner: %w", err)
	}
	offset += int(partitionerLen)

	// 4. Read Partitions
	if offset+8 > len(data) {
		return io.ErrUnexpectedEOF
	}
	numPartitions := binary.LittleEndian.Uint64(data[offset : offset+8])
	offset += 8
	f.partitions = make([]partition[K, H, B, E], numPartitions)

	for i := uint64(0); i < numPartitions; i++ {
		if offset+8+8 > len(data) {
			return io.ErrUnexpectedEOF
		} // offset + phfLen
		f.partitions[i].offset = binary.LittleEndian.Uint64(data[offset : offset+8])
		offset += 8
		phfLen := binary.LittleEndian.Uint64(data[offset : offset+8])
		offset += 8
		if uint64(offset)+phfLen > uint64(len(data)) {
			return io.ErrUnexpectedEOF
		}

		// Create a new sub-PHF instance and unmarshal into it
		subPHF := NewSinglePHF[K, H, B, E](f.isMinimal, f.searchType) // Create with matching params
		err = serial.TryUnmarshal(subPHF, data[offset:offset+int(phfLen)])
		if err != nil {
			return fmt.Errorf("failed to unmarshal sub-PHF %d: %w", i, err)
		}
		f.partitions[i].phf = subPHF
		offset += int(phfLen)
	}

	if offset != len(data) {
		return fmt.Errorf("extra data detected after unmarshaling (%d bytes remain)", len(data)-offset)
	}
	return nil
}
