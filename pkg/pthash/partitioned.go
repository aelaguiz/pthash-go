package pthash

import (
	"encoding"
	"encoding/binary"
	"fmt"
	"io"
	"pthashgo/internal/builder"
	"pthashgo/internal/core"
	"pthashgo/internal/serial"
	"pthashgo/internal/util"
	"sync"
	"time"
	"unsafe" // Needed for size calculation
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
	tableSize   uint64               // Total size estimate across partitions
	partitioner core.RangeBucketer   // Used to map key hash to partition
	partitions  []partition[K, H, B, E] // Slice of sub-PHFs
	hasher      H                    // Store hasher instance
	isMinimal   bool
	searchType  core.SearchType
}

// partition stores the offset and the sub-PHF for a single partition.
type partition[K any, H core.Hasher[K], B core.Bucketer, E core.Encoder] struct {
	offset uint64                  // Starting offset for this partition's output range
	phf    *SinglePHF[K, H, B, E] // The actual sub-PHF for this partition
}

// NewPartitionedPHF creates an empty PartitionedPHF instance.
func NewPartitionedPHF[K any, H core.Hasher[K], B core.Bucketer, E core.Encoder](minimal bool, search core.SearchType) *PartitionedPHF[K, H, B, E] {
	var hasher H // Zero value hasher
	return &PartitionedPHF[K, H, B, E]{
		isMinimal:  minimal,
		searchType: search,
		hasher:     hasher,
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
	f.partitioner = pb.Partitioner()
	f.hasher = pb.Hasher() // Get hasher instance from builder
	numPartitions := pb.NumPartitions()
	f.partitions = make([]partition[K, H, B, E], numPartitions)
	subBuilders := pb.Builders()
	offsets := pb.Offsets()

	// --- Create final PHF structures for each partition ---
	// This involves encoding the data from each sub-builder into the final sub-PHF structure.
	encodeStart := time.Now()
	var totalEncodingTime time.Duration

	// Use concurrency similar to C++ if beneficial
	numThreads := config.NumThreads
	if numThreads <= 0 { numThreads = 1}
	if numThreads > int(numPartitions) { numThreads = int(numPartitions)}
	if numPartitions == 0 { numThreads = 0 } // Avoid spawning goroutines if no partitions

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
			if err != nil && firstError == nil { firstError = err }
		}
		if firstError != nil { return 0, firstError }

		for encTime := range timingChan { totalEncodingTime += encTime }

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
	if partitionIdx >= uint64(len(f.partitions)) {
		panic(fmt.Sprintf("partition index %d out of bounds (%d)", partitionIdx, len(f.partitions)))
	}

	// 3. Lookup within the sub-PHF of that partition
	part := f.partitions[partitionIdx]
	if part.phf == nil {
		panic(fmt.Sprintf("lookup error: partition %d has nil sub-PHF", partitionIdx))
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
func (f *PartitionedPHF[K, H, B, E]) NumKeys() uint64 { return f.numKeys }
func (f *PartitionedPHF[K, H, B, E]) TableSize() uint64 { return f.tableSize } // Note: Estimated total
func (f *PartitionedPHF[K, H, B, E]) Seed() uint64    { return f.seed }
func (f *PartitionedPHF[K, H, B, E]) IsMinimal() bool { return f.isMinimal }

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
	// Need to serialize: magic, version, flags, seed, numKeys, tableSize,
	// partitioner, numPartitions, and then each partition (offset + marshaled sub-PHF).

	// 1. Marshal partitioner
	partitionerData, err := tryMarshal(&f.partitioner) // Pass pointer
	if err != nil { return nil, fmt.Errorf("failed to marshal partitioner: %w", err) }

	// 2. Marshal each sub-PHF
	partitionData := make([][]byte, len(f.partitions))
	totalPartitionDataLen := 0
	for i, p := range f.partitions {
		if p.phf == nil {
			// Handle nil sub-PHF (e.g., empty partition) - serialize as zero length?
			partitionData[i] = []byte{}
		} else {
			data, err := tryMarshal(p.phf)
			if err != nil { return nil, fmt.Errorf("failed to marshal sub-PHF %d: %w", i, err) }
			partitionData[i] = data
			totalPartitionDataLen += len(data)
		}
	}

	// 3. Calculate total size
	// header: magic(4) + version(1) + flags(1) + reserved(2) = 8
	// core: seed(8) + numKeys(8) + tableSize(8) = 24
	// partitioner: len(8) + data
	// partitions: numPartitions(8) + loop( offset(8) + phfLen(8) + phfData )
	headerSize := 8 + 24
	totalSize := headerSize
	totalSize += 8 + len(partitionerData)
	totalSize += 8 // numPartitions
	totalSize += len(f.partitions) * (8 + 8) + totalPartitionDataLen // offsets + lengths + data

	buf := make([]byte, totalSize)
	offset := 0

	// Header
	copy(buf[offset:offset+4], []byte(partitionedPHFMagic)); offset += 4
	buf[offset] = 1; offset += 1 // Version
	flags := byte(0)
	if f.isMinimal { flags |= 1 }
	flags |= (byte(f.searchType) << 1)
	buf[offset] = flags; offset += 1
	binary.LittleEndian.PutUint16(buf[offset:offset+2], 0); offset += 2 // Reserved

	// Core
	binary.LittleEndian.PutUint64(buf[offset:offset+8], f.seed); offset += 8
	binary.LittleEndian.PutUint64(buf[offset:offset+8], f.numKeys); offset += 8
	binary.LittleEndian.PutUint64(buf[offset:offset+8], f.tableSize); offset += 8

	// Partitioner
	binary.LittleEndian.PutUint64(buf[offset:offset+8], uint64(len(partitionerData))); offset += 8
	copy(buf[offset:], partitionerData); offset += len(partitionerData)

	// Partitions
	numPartitions := uint64(len(f.partitions))
	binary.LittleEndian.PutUint64(buf[offset:offset+8], numPartitions); offset += 8
	for i := uint64(0); i < numPartitions; i++ {
		pData := partitionData[i]
		pLen := uint64(len(pData))
		binary.LittleEndian.PutUint64(buf[offset:offset+8], f.partitions[i].offset); offset += 8
		binary.LittleEndian.PutUint64(buf[offset:offset+8], pLen); offset += 8
		copy(buf[offset:], pData); offset += int(pLen)
	}

	if offset != totalSize {
		return nil, fmt.Errorf("internal marshal error: offset %d != totalSize %d", offset, totalSize)
	}

	return buf, nil
}


// UnmarshalBinary implements encoding.BinaryUnmarshaler.
// Requires the types K, H, B, E to be known at compile time when calling.
func (f *PartitionedPHF[K, H, B, E]) UnmarshalBinary(data []byte) error {
	headerSize := 8 + 24
	if len(data) < headerSize { return io.ErrUnexpectedEOF }
	offset := 0

	// Header
	if string(data[offset:offset+4]) != partitionedPHFMagic { return fmt.Errorf("invalid magic identifier") }
	offset += 4
	version := data[offset]; offset += 1
	if version != 1 { return fmt.Errorf("unsupported version: %d", version) }
	flags := data[offset]; offset += 1
	f.isMinimal = (flags & 1) != 0
	f.searchType = core.SearchType((flags >> 1) & 3)
	offset += 2 // Reserved

	// Core
	f.seed = binary.LittleEndian.Uint64(data[offset:offset+8]); offset += 8
	f.numKeys = binary.LittleEndian.Uint64(data[offset:offset+8]); offset += 8
	f.tableSize = binary.LittleEndian.Uint64(data[offset:offset+8]); offset += 8

	// Partitioner
	if offset+8 > len(data) { return io.ErrUnexpectedEOF }
	partitionerLen := binary.LittleEndian.Uint64(data[offset:offset+8]); offset += 8
	if offset+int(partitionerLen) > len(data) { return io.ErrUnexpectedEOF }
	// RangeBucketer is simple, can unmarshal directly if it implements the interface
	err := tryUnmarshal(&f.partitioner, data[offset:offset+int(partitionerLen)])
	if err != nil { return fmt.Errorf("failed to unmarshal partitioner: %w", err) }
	offset += int(partitionerLen)

	// Partitions
	if offset+8 > len(data) { return io.ErrUnexpectedEOF }
	numPartitions := binary.LittleEndian.Uint64(data[offset:offset+8]); offset += 8
	f.partitions = make([]partition[K, H, B, E], numPartitions)

	for i := uint64(0); i < numPartitions; i++ {
		if offset+8+8 > len(data) { return io.ErrUnexpectedEOF } // offset + phfLen
		f.partitions[i].offset = binary.LittleEndian.Uint64(data[offset:offset+8]); offset += 8
		phfLen := binary.LittleEndian.Uint64(data[offset:offset+8]); offset += 8
		if offset+int(phfLen) > len(data) { return io.ErrUnexpectedEOF }

		// Create a new sub-PHF instance and unmarshal into it
		subPHF := NewSinglePHF[K, H, B, E](f.isMinimal, f.searchType) // Create with matching params
		err = tryUnmarshal(subPHF, data[offset:offset+int(phfLen)])
		if err != nil { return fmt.Errorf("failed to unmarshal sub-PHF %d: %w", i, err) }
		f.partitions[i].phf = subPHF
		offset += int(phfLen)
	}


	if offset != len(data) {
		return fmt.Errorf("extra data detected after unmarshaling")
	}
	return nil
}
