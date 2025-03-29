package pthash

import (
	"fmt"
	"pthashgo/internal/builder"
	"pthashgo/internal/core"
	"pthashgo/internal/util"
	"sync"
	"time"
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
	isMinimal   bool
	searchType  core.SearchType
}

// partition stores the offset and the sub-PHF for a single partition.
// Note: K, H, B, E must match the outer struct's types.
type partition[K any, H core.Hasher[K], B core.Bucketer, E core.Encoder] struct {
	offset uint64                  // Starting offset for this partition's output range
	phf    *SinglePHF[K, H, B, E] // The actual sub-PHF for this partition
}

// NewPartitionedPHF creates an empty PartitionedPHF instance.
func NewPartitionedPHF[K any, H core.Hasher[K], B core.Bucketer, E core.Encoder](minimal bool, search core.SearchType) *PartitionedPHF[K, H, B, E] {
	return &PartitionedPHF[K, H, B, E]{
		isMinimal:  minimal,
		searchType: search,
		// Other fields initialized by Build
	}
}

// Build constructs the PartitionedPHF from a completed partitioned builder.
// This method orchestrates the building of sub-PHFs using the partitioned data.
func (f *PartitionedPHF[K, H, B, E]) Build(
	pb *builder.InternalMemoryBuilderPartitionedPHF[K, H, B],
	partitionBuffers [][]core.Hash128, // Pass the partitioned hashes
	config *core.BuildConfig,
) (time.Duration, error) {

	start := time.Now()
	util.Log(config.Verbose, "Starting PartitionedPHF.Build final stage...")

	// Check config consistency
	if f.isMinimal != config.Minimal || f.searchType != config.Search {
		return 0, fmt.Errorf("PartitionedPHF type parameters mismatch build config")
	}

	// Build the sub-PHFs (this was added to the builder in this phase)
	buildTimings, err := pb.BuildSubPHFs(partitionBuffers, *config) // Pass the main config
	if err != nil {
		return 0, fmt.Errorf("failed to build sub-PHFs: %w", err)
	}
	util.Log(config.Verbose, "Finished building sub-PHFs.")

	// --- Copy final state from builder ---
	f.seed = pb.Seed()
	f.numKeys = pb.NumKeys()
	f.tableSize = pb.TableSize()
	f.partitioner = pb.Partitioner()
	numPartitions := pb.NumPartitions()
	f.partitions = make([]partition[K, H, B, E], numPartitions)
	subBuilders := pb.Builders()
	offsets := pb.Offsets()

	// --- Create final PHF structures for each partition ---
	encodeStart := time.Now()
	var totalEncodingTime time.Duration

	var wg sync.WaitGroup
	errChan := make(chan error, numPartitions)
	timingChan := make(chan time.Duration, numPartitions)
	numThreads := config.NumThreads
	if numThreads <= 0 { numThreads = 1}
	sem := make(chan struct{}, numThreads)

	wg.Add(int(numPartitions))
	for i := uint64(0); i < numPartitions; i++ {
		go func(idx uint64) {
			sem <- struct{}{}
			defer wg.Done()
			defer func() { <-sem }()

			subBuilder := subBuilders[idx]
			subPHF := NewSinglePHF[K, H, B, E](f.isMinimal, f.searchType) // Create empty sub-PHF

			// Pass a sub-config for the build step
			subConfig := *config // Copy
			subConfig.NumBuckets = subBuilder.NumBuckets() // Use actual buckets from sub-builder
			subConfig.Seed = subBuilder.Seed() // Use sub-builder seed (should match main)
			// Alpha/Lambda less critical here as structure is determined

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
	    // For parallel encoding, time is max? Or sum? C++ sums it. Let's sum.
		totalEncodingTime += encTime
	}

	totalBuildTime := time.Since(start)
	util.Log(config.Verbose, "PartitionedPHF final build stage took: %v (Encoding sum: %v)", totalBuildTime, totalEncodingTime)

	// Return total encoding time (sum across partitions)
	return totalEncodingTime, nil
}

// Lookup evaluates the hash function for a key.
func (f *PartitionedPHF[K, H, B, E]) Lookup(key K) uint64 {
	// 1. Hash the key
	// Need a hasher instance. Assume stateless or stored.
	var hasher H
	hash := hasher.Hash(key, f.seed)

	// 2. Determine the partition
	partitionIdx := f.partitioner.Bucket(hash.Mix())
	if partitionIdx >= uint64(len(f.partitions)) {
		// This could happen if the key wasn't in the original set
		// or if hash distribution is extremely skewed. Handle gracefully.
		// C++ version doesn't explicitly handle this, might rely on partitioner bounds.
		// Let's return 0 or panic, depending on desired behavior for unknown keys.
		// Or return an error? For now, maybe panic indicates issue.
		panic(fmt.Sprintf("partition index %d out of bounds (%d)", partitionIdx, len(f.partitions)))
	}

	// 3. Lookup within the sub-PHF of that partition
	part := f.partitions[partitionIdx]
	if part.phf == nil {
		// Indicates an empty partition or build error
		panic(fmt.Sprintf("lookup error: partition %d has nil sub-PHF", partitionIdx))
	}
	subPosition := part.phf.Lookup(key) // Pass original key to sub-lookup

	// 4. Add the partition offset
	return part.offset + subPosition
}

// --- Accessors ---
func (f *PartitionedPHF[K, H, B, E]) NumKeys() uint64 { return f.numKeys }
func (f *PartitionedPHF[K, H, B, E]) TableSize() uint64 { return f.tableSize } // Note: This is estimated total
func (f *PartitionedPHF[K, H, B, E]) Seed() uint64    { return f.seed }
func (f *PartitionedPHF[K, H, B, E]) IsMinimal() bool { return f.isMinimal }

// --- Space Calculation ---
func (f *PartitionedPHF[K, H, B, E]) NumBits() uint64 {
	totalBits := uint64(8 * (8 + 8 + 8)) // seed, numKeys, tableSize
	totalBits += f.partitioner.NumBits()
	totalBits += 8 * 8 // Size of partitions slice header

	for _, p := range f.partitions {
		totalBits += 8 * 8 // Offset size
		if p.phf != nil {
			totalBits += p.phf.NumBits()
		}
	}
	return totalBits
}

// TODO: Implement NumBitsForPilots / NumBitsForMapper if needed, requires summing from sub-PHFs.
// TODO: Implement serialization for PartitionedPHF.
