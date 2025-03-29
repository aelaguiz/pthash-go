package builder

import (
	"fmt"
	"pthashgo/internal/core"
	"pthashgo/internal/util"
	"reflect"
	"sort"
	"sync"
	"time"
)

// InternalMemoryBuilderPartitionedPHF holds state for building partitioned PHFs.
type InternalMemoryBuilderPartitionedPHF[K any, H core.Hasher[K], B core.Bucketer] struct {
	config        core.BuildConfig
	seed          uint64
	numKeys       uint64
	numPartitions uint64
	tableSize     uint64              // Total estimated table size across partitions
	partitioner   *core.RangeBucketer // Bucketer used to partition keys
	hasher        H                   // Hasher instance

	// Intermediate results stored in the builder
	partitionBuffers [][]core.Hash128 // Hashes belonging to each partition

	// Results of the build process (passed to PartitionedPHF.Build)
	offsets                []uint64                                   // Start offset for each partition's output range
	rawPartitionOffsets    []uint64                                   // Raw offsets (needed by DiffEncoder)
	subBuilders            []*InternalMemoryBuilderSinglePHF[K, H, B] // Builders for each partition's sub-PHF
	avgPartSize            uint64                                     // Calculated avg partition size
	numBucketsPerPartition uint64                                     // Calculated for sub-builders
	freeSlots              []uint64                                   // Free slots for minimal+dense mode

	// Timings accumulated during the build process
	timings core.BuildTimings
}

// InterleavingPilotsIterator provides pilots in interleaved order.
// This implements the PilotIterator interface for Dense Encoders.
type InterleavingPilotsIterator[K any, H core.Hasher[K], B core.Bucketer] struct {
	builders      []*InternalMemoryBuilderSinglePHF[K, H, B]
	numPartitions uint64
	numBuckets    uint64
	currBucket    uint64
	currPart      uint64
}

// NewInterleavingPilotsIterator creates an iterator that returns pilots in interleaved order.
func NewInterleavingPilotsIterator[K any, H core.Hasher[K], B core.Bucketer](pb *InternalMemoryBuilderPartitionedPHF[K, H, B]) *InterleavingPilotsIterator[K,H,B] {
	numBuckets := uint64(0)
	if len(pb.Builders()) > 0 && pb.Builders()[0] != nil {
		// Assume all sub-builders were configured with the same number of buckets
		numBuckets = pb.Builders()[0].NumBuckets()
	}
	return &InterleavingPilotsIterator[K,H,B] {
		builders:      pb.Builders(),
		numPartitions: pb.NumPartitions(),
		numBuckets:    numBuckets,
		currBucket:    0,
		currPart:      0,
	}
}

// HasNext returns true if there are more pilots to iterate.
func (it *InterleavingPilotsIterator[K,H,B]) HasNext() bool {
	return it.currBucket < it.numBuckets
}

// Next returns the next pilot in interleaved order.
func (it *InterleavingPilotsIterator[K,H,B]) Next() uint64 {
	if !it.HasNext() {
		panic("InterleavingPilotsIterator.Next called after end")
	}
	// Get pilot from current partition's builder for current bucket
	pilot := uint64(0)
	if it.builders[it.currPart] != nil && it.currBucket < uint64(len(it.builders[it.currPart].Pilots())) {
		pilot = it.builders[it.currPart].Pilots()[it.currBucket]
	} else {
		// Handle case where sub-builder might be nil or pilots slice smaller? Should ideally not happen.
		// Return 0 for safety.
	}

	// Advance partition, wrap around to next bucket if needed
	it.currPart++
	if it.currPart == it.numPartitions {
		it.currPart = 0
		it.currBucket++
	}
	return pilot
}

// Helper struct to view 'taken' status across partitions
type partitionedTakenView[K any, H core.Hasher[K], B core.Bucketer] struct {
	builders []*InternalMemoryBuilderSinglePHF[K, H, B]
	// Precompute offsets and sizes? Or look up dynamically? Dynamic for simplicity.
}

// newPartitionedTakenView creates a view of taken bits across all partitions.
func newPartitionedTakenView[K any, H core.Hasher[K], B core.Bucketer](builders []*InternalMemoryBuilderSinglePHF[K, H, B]) *partitionedTakenView[K,H,B] {
	return &partitionedTakenView[K,H,B]{builders: builders}
}

// Get returns true if the bit at position pos is set in any partition.
func (ptv *partitionedTakenView[K,H,B]) Get(pos uint64) bool {
	// Find which sub-builder 'pos' falls into. Requires knowing sub-table sizes.
	currentOffset := uint64(0)
	for _, subBuilder := range ptv.builders {
		subTableSize := subBuilder.TableSize()
		if pos < currentOffset + subTableSize {
			// Found the partition
			subPos := pos - currentOffset
			if subBuilder.Taken() == nil { // Handle nil taken vector (e.g., empty partition)
				return false
			}
			return subBuilder.Taken().Get(subPos)
		}
		currentOffset += subTableSize
	}
	// Position is out of bounds of all partitions
	// This might happen if fillFreeSlots calls Get(>= tableSize) - should return false.
	return false
}

// Size returns the total size of the taken view.
func (ptv *partitionedTakenView[K,H,B]) Size() uint64 {
	totalSize := uint64(0)
	for _, subBuilder := range ptv.builders {
		totalSize += subBuilder.TableSize()
	}
	return totalSize
}

// NewInternalMemoryBuilderPartitionedPHF creates a new partitioned builder.
func NewInternalMemoryBuilderPartitionedPHF[K any, H core.Hasher[K], B core.Bucketer](hasher H) *InternalMemoryBuilderPartitionedPHF[K, H, B] {
	// Note: The sub-builder's Bucketer type 'B' is passed in,
	// but the partitioner is always RangeBucketer.
	return &InternalMemoryBuilderPartitionedPHF[K, H, B]{
		hasher:      hasher,
		partitioner: &core.RangeBucketer{},
	}
}

// BuildFromKeys orchestrates the partitioned build process, including partitioning
// and building sub-PHFs. It stores the final state needed by PartitionedPHF.Build.
func (pb *InternalMemoryBuilderPartitionedPHF[K, H, B]) BuildFromKeys(keys []K, config core.BuildConfig) (core.BuildTimings, error) {
	numKeys := uint64(len(keys))
	if numKeys == 0 {
		return core.BuildTimings{}, fmt.Errorf("cannot build partitioned PHF for zero keys")
	}
	pb.config = config
	pb.numKeys = numKeys
	pb.timings = core.BuildTimings{} // Reset timings

	// Check hash collision probability
	err := core.CheckHashCollisionProbability[K, H](numKeys)
	if err != nil {
		return pb.timings, err
	}

	// Determine number of partitions
	avgPartitionSize := core.ComputeAvgPartitionSize(numKeys, &config)
	if avgPartitionSize == 0 || avgPartitionSize == numKeys {
		return pb.timings, fmt.Errorf("partitioned builder requires AvgPartitionSize > 0 and < numKeys (effective size was %d)", avgPartitionSize)
	}
	pb.avgPartSize = avgPartitionSize
	pb.numPartitions = core.ComputeNumPartitions(numKeys, avgPartitionSize)
	if pb.numPartitions == 0 {
		pb.numPartitions = 1 // Ensure at least one partition
	}

	util.Log(config.Verbose, "Num Partitions = %d", pb.numPartitions)
	util.Log(config.Verbose, "Avg Partition Size = %d", avgPartitionSize)

	partitionStart := time.Now()

	// Initialize seed
	if config.Seed == core.InvalidSeed {
		pb.seed = util.RandomSeed()
		util.Log(config.Verbose, "Using random seed: %d", pb.seed)
	} else {
		pb.seed = config.Seed
	}

	// Initialize partitioner
	err = pb.partitioner.Init(pb.numPartitions, 0, 0, 0) // RangeBucketer ignores other params
	if err != nil {
		return pb.timings, fmt.Errorf("failed to init partitioner: %w", err)
	}

	// --- Partition Keys ---
	pb.partitionBuffers = make([][]core.Hash128, pb.numPartitions)
	for i := range pb.partitionBuffers {
		allocHint := uint64(float64(avgPartitionSize) * 1.1)
		if allocHint == 0 {
			allocHint = 10
		}
		pb.partitionBuffers[i] = make([]core.Hash128, 0, allocHint)
	}

	if config.NumThreads > 1 && numKeys >= uint64(config.NumThreads)*100 {
		util.Log(config.Verbose, "Partitioning keys in parallel (%d threads)...", config.NumThreads)
		pb.parallelHashAndPartition(keys, pb.partitionBuffers)
	} else {
		util.Log(config.Verbose, "Partitioning keys sequentially...")
		for _, key := range keys {
			hash := pb.hasher.Hash(key, pb.seed)
			partitionIdx := pb.partitioner.Bucket(hash.Mix())
			if partitionIdx >= pb.numPartitions {
				return pb.timings, fmt.Errorf("internal error: partition index %d out of bounds (%d)", partitionIdx, pb.numPartitions)
			}
			pb.partitionBuffers[partitionIdx] = append(pb.partitionBuffers[partitionIdx], hash)
		}
	}
	util.Log(config.Verbose, "Partitioning complete.")
	pb.timings.PartitioningMicroseconds = time.Since(partitionStart)
	util.Log(config.Verbose, "Partitioning took: %v", pb.timings.PartitioningMicroseconds)

	// --- Calculate Offsets and Initialize Sub-Builders ---
	offsetStart := time.Now()
	pb.offsets = make([]uint64, pb.numPartitions+1)
	pb.rawPartitionOffsets = make([]uint64, pb.numPartitions+1) // Store raw offsets for dense mode
	pb.subBuilders = make([]*InternalMemoryBuilderSinglePHF[K, H, B], pb.numPartitions)
	pb.tableSize = 0 // Total size across all partitions
	pb.numBucketsPerPartition = core.ComputeNumBuckets(avgPartitionSize, config.Lambda)
	util.Log(config.Verbose, "Num Buckets per Partition = %d", pb.numBucketsPerPartition)

	cumulativeOffset := uint64(0)
	for i := uint64(0); i < pb.numPartitions; i++ {
		partitionSize := uint64(len(pb.partitionBuffers[i]))

		subTableSize := uint64(0)
		if partitionSize > 0 {
			subTableSize = core.MinimalTableSize(partitionSize, config.Alpha, config.Search)
		}
		pb.tableSize += subTableSize
		pb.rawPartitionOffsets[i] = cumulativeOffset // Store raw cumulative offset

		offsetIncrement := uint64(0)
		if config.DensePartitioning {
			offsetIncrement = subTableSize
		} else {
			if config.Minimal {
				offsetIncrement = partitionSize
			} else {
				offsetIncrement = subTableSize
			}
		}
		cumulativeOffset += offsetIncrement

		// Create a proper bucketer instance based on whether B is a pointer type
		// For pointer types (like *core.SkewBucketer), we need to allocate memory
		var subBucketer B

		// The test is using B = *core.SkewBucketer, so we need to create a non-nil instance
		// We can detect if B is a pointer type and create an appropriate instance
		if isPointerType := reflect.TypeOf(subBucketer).Kind() == reflect.Ptr; isPointerType {
			// For pointer types, create a new instance of the concrete type
			concreteSubBucketer := new(core.SkewBucketer)
			// Type assertion to assign to B
			var ifaceB any = concreteSubBucketer
			subBucketer = ifaceB.(B)
		}

		pb.subBuilders[i] = NewInternalMemoryBuilderSinglePHF[K, H, B](pb.hasher, subBucketer)
	}
	pb.rawPartitionOffsets[pb.numPartitions] = cumulativeOffset // Final raw offset
	pb.offsets = pb.rawPartitionOffsets // For non-dense, offsets are the raw ones. Dense will overwrite later.
	// Offset calculation is part of partitioning timing? Or separate setup time? Let's include in partitioning.
	pb.timings.PartitioningMicroseconds += time.Since(offsetStart)

	// --- Build Sub-PHFs ---
	subBuildTimings, err := pb.BuildSubPHFs(config) // Build using internal buffers
	if err != nil {
		// Check if it's a seed error, allow caller to potentially retry
		if _, ok := err.(core.SeedRuntimeError); ok {
			return pb.timings, err // Propagate seed error
		}
		return pb.timings, fmt.Errorf("failed during sub-PHF build stage: %w", err)
	}

	// Accumulate timings from sub-builds
	pb.timings.MappingOrderingMicroseconds = subBuildTimings.MappingOrderingMicroseconds
	pb.timings.SearchingMicroseconds = subBuildTimings.SearchingMicroseconds
	// Encoding time happens when the final PHF calls Build, not here.

	// --- Fill Free Slots (Only for Dense + Minimal) ---
	// C++ stores this in the partitioned builder for dense mode.
	if config.Minimal && config.DensePartitioning {
		freeSlotStart := time.Now()
		pb.freeSlots = make([]uint64, 0, pb.tableSize-pb.numKeys) // Estimate capacity
		// Need a way to logically view all 'taken' bits across sub-builders
		// Create a 'taken' view/iterator on the fly?
		takenView := newPartitionedTakenView[K, H, B](pb.subBuilders)
		fillFreeSlots(takenView, pb.numKeys, &pb.freeSlots, pb.tableSize)
		pb.timings.SearchingMicroseconds += time.Since(freeSlotStart) // Add to search time
	} else {
		pb.freeSlots = nil
	}

	// Release partition buffers memory after sub-builds are done
	pb.partitionBuffers = nil

	util.Log(config.Verbose, "Partitioned builder finished initial stages.")
	return pb.timings, nil
}

// parallelHashAndPartition distributes keys into partitions concurrently.
func (pb *InternalMemoryBuilderPartitionedPHF[K, H, B]) parallelHashAndPartition(keys []K, partitionBuffers [][]core.Hash128) {
	numKeys := uint64(len(keys))
	numThreads := pb.config.NumThreads
	keysPerThread := numKeys / uint64(numThreads)
	bufferMutexes := make([]sync.Mutex, pb.numPartitions)
	var wg sync.WaitGroup
	wg.Add(numThreads)

	for tid := 0; tid < numThreads; tid++ {
		go func(threadID int) {
			defer wg.Done()
			start := uint64(threadID) * keysPerThread
			end := start + keysPerThread
			if threadID == numThreads-1 {
				end = numKeys
			}

			localBuffers := make(map[uint64][]core.Hash128) // Thread-local temporary storage

			for i := start; i < end; i++ {
				key := keys[i]
				hash := pb.hasher.Hash(key, pb.seed)
				partitionIdx := pb.partitioner.Bucket(hash.Mix())
				localBuffers[partitionIdx] = append(localBuffers[partitionIdx], hash)
			}

			// Append thread-local results to global buffers under lock
			for partitionIdx, hashes := range localBuffers {
				bufferMutexes[partitionIdx].Lock()
				partitionBuffers[partitionIdx] = append(partitionBuffers[partitionIdx], hashes...)
				bufferMutexes[partitionIdx].Unlock()
			}
		}(tid)
	}
	wg.Wait()
}

// --- Accessors needed by PartitionedPHF.Build ---
// Already defined in previous phase: Seed, NumKeys, TableSize, NumPartitions, Partitioner, Offsets, Builders, NumBucketsPerPartition

// Hasher returns the hasher instance.
func (pb *InternalMemoryBuilderPartitionedPHF[K, H, B]) Hasher() H {
	return pb.hasher
}

// buildFromHashes is needed for partitioned builds. Add to InternalMemoryBuilderSinglePHF.
// This is a simplified version of buildFromKeysInternal, starting after hashing.
func (b *InternalMemoryBuilderSinglePHF[K, H, B]) buildFromHashes(
	hashes []core.Hash128, // Input is now hashes
	config core.BuildConfig) (core.BuildTimings, error) {

	numKeys := uint64(len(hashes))
	if numKeys == 0 {
		// Initialize defaults for an empty builder state
		b.config = config
		b.seed = config.Seed
		b.numKeys = 0
		b.tableSize = 0
		b.numBuckets = 0
		b.pilots = nil
		b.taken = core.NewBitVector(0)
		b.freeSlots = nil
		var zeroB B
		b.bucketer = zeroB              // Initialize bucketer to zero value
		_ = b.bucketer.Init(0, 0, 0, 0) // Call init, might be no-op
		return core.BuildTimings{}, nil
	}
	b.config = config // Store config

	// Alpha check already done by caller (partitioned builder)

	var timings core.BuildTimings
	start := time.Now()

	// Calculate table size and num buckets
	tableSize := core.MinimalTableSize(numKeys, config.Alpha, config.Search)
	numBuckets := config.NumBuckets                              // Use pre-calculated NumBuckets from partitioned builder
	if numBuckets == core.InvalidNumBuckets || numBuckets == 0 { // Basic check
		return core.BuildTimings{}, fmt.Errorf("invalid number of buckets (%d) provided for sub-build", numBuckets)
	}

	// Basic validation for BucketIDType size
	if numBuckets > uint64(core.MaxBucketID) {
		return core.BuildTimings{}, fmt.Errorf("number of buckets (%d) exceeds limit for BucketIDType (%d)", numBuckets, core.MaxBucketID)
	}

	b.seed = config.Seed
	b.numKeys = numKeys
	b.tableSize = tableSize
	b.numBuckets = numBuckets

	// Initialize bucketer (already created, just Init)
	effectiveTableSize := float64(numBuckets) * config.Lambda / config.Alpha
	err := b.bucketer.Init(numBuckets, config.Lambda, uint64(effectiveTableSize), config.Alpha)
	if err != nil {
		return core.BuildTimings{}, fmt.Errorf("failed to initialize bucketer: %w", err)
	}

	// --- Map + Sort (on hashes) ---
	mapStart := time.Now()
	pairs := make(pairsT, numKeys)
	for i, hash := range hashes {
		bucketID := b.bucketer.Bucket(hash.First())
		pairs[i] = core.BucketPayloadPair{BucketID: bucketID, Payload: hash.Second()}
	}
	sortFunc := func(i, j int) bool {
		if config.SecondarySort {
			if pairs[i].BucketID != pairs[j].BucketID {
				return pairs[i].BucketID > pairs[j].BucketID
			}
			return pairs[i].Payload < pairs[j].Payload
		}
		return pairs[i].Less(pairs[j])
	}
	sort.SliceStable(pairs, sortFunc)
	pairsBlocks := []pairsT{pairs} // Treat as single block
	util.Log(config.Verbose, "(Sub) Map+Sort took: %v", time.Since(mapStart))

	// --- Merge ---
	mergeStart := time.Now()
	bucketsCollector := newBucketsT()                                       // Our merger
	err = merge(pairsBlocks, bucketsCollector, false, config.SecondarySort) // Sub-merge usually not verbose
	if err != nil {
		return core.BuildTimings{}, fmt.Errorf("sub-merge failed: %w", err)
	}
	util.Log(config.Verbose, "(Sub) Merge took: %v", time.Since(mergeStart))

	timings.MappingOrderingMicroseconds = time.Since(mapStart)
	if config.Verbose {
		bucketsCollector.printBucketSizeDistribution()
	}

	// --- Search ---
	searchStart := time.Now()
	b.pilots = make([]uint64, numBuckets) // Initialize pilots slice
	pilotsWrapper := newPilotsWrapper(b.pilots)
	takenBuilder := core.NewBitVectorBuilder(tableSize) // Build taken bits

	bucketsIter := bucketsCollector.iterator()
	numNonEmpty := bucketsCollector.numBuckets()

	// Run the search function (from search.go)
	err = Search[B]( // Pass Bucketer type explicitly
		b.numKeys, b.numBuckets, numNonEmpty, b.seed, &config, // Pass sub-config
		bucketsIter, takenBuilder, pilotsWrapper, b.bucketer, // Pass this builder's bucketer
	)
	if err != nil {
		return core.BuildTimings{}, fmt.Errorf("sub-search failed: %w", err)
	}

	b.taken = takenBuilder.Build() // Finalize taken bit vector

	// --- Fill Free Slots (if minimal) ---
	if config.Minimal && numKeys < tableSize {
		b.freeSlots = make([]uint64, 0, tableSize-numKeys)
		fillFreeSlots(b.taken, numKeys, &b.freeSlots, tableSize)
	} else {
		b.freeSlots = nil // Ensure it's nil if not minimal or tableSize <= numKeys
	}
	timings.SearchingMicroseconds = time.Since(searchStart)

	util.Log(config.Verbose, "(Sub) Search took: %v", timings.SearchingMicroseconds)
	util.Log(config.Verbose, "(Sub) Build steps finished in: %v", time.Since(start))
	return timings, nil
}

func (pb *InternalMemoryBuilderPartitionedPHF[K, H, B]) NumBucketsPerPartition() uint64 {
	return pb.numBucketsPerPartition
}

// BuildSubPHFs executes the build process for each partition's sub-builder.
// Assumes partitionBuffers is populated internally.
func (pb *InternalMemoryBuilderPartitionedPHF[K, H, B]) BuildSubPHFs(
	config core.BuildConfig,
) (core.BuildTimings, error) {

	util.Log(config.Verbose, "Building Sub-PHFs...")
	if pb.partitionBuffers == nil {
		return core.BuildTimings{}, fmt.Errorf("internal error: partition buffers are nil during BuildSubPHFs")
	}

	subPHFConfig := config      // Copy config
	subPHFConfig.Seed = pb.seed // Ensure sub-builders use the same main seed
	subPHFConfig.NumBuckets = pb.numBucketsPerPartition
	subPHFConfig.Verbose = false // Quieten sub-builders usually
	subPHFConfig.NumThreads = 1  // Each sub-build runs sequentially within its thread/goroutine

	var accumulatedTimings core.BuildTimings // Used for sequential accumulation
	maxMappingOrdering := time.Duration(0)   // Used for parallel max time
	maxSearching := time.Duration(0)         // Used for parallel max time

	numThreads := config.NumThreads
	if numThreads <= 0 {
		numThreads = 1
	}

	if numThreads > 1 && pb.numPartitions >= uint64(numThreads) {
		// Parallel build
		var wg sync.WaitGroup
		wg.Add(int(pb.numPartitions))
		// Use channels for results, including potential seed errors
		errChan := make(chan error, pb.numPartitions)
		timingChan := make(chan core.BuildTimings, pb.numPartitions)
		sem := make(chan struct{}, numThreads) // Limit concurrency

		for i := uint64(0); i < pb.numPartitions; i++ {
			go func(partitionIdx uint64) {
				sem <- struct{}{} // Acquire
				defer wg.Done()
				defer func() { <-sem }() // Release

				subBuilder := pb.subBuilders[partitionIdx]
				hashes := pb.partitionBuffers[partitionIdx] // Use internal buffer

				if len(hashes) == 0 {
					// Initialize empty sub-builder state correctly
					subBuilder.numKeys = 0
					subBuilder.tableSize = 0
					subBuilder.numBuckets = 0
					subBuilder.seed = subPHFConfig.Seed
					// Handle empty partitions correctly by ensuring bucketer is non-nil
					if isPointerType := reflect.TypeOf(subBuilder.bucketer).Kind() == reflect.Ptr; isPointerType {
						// For pointer types, create a new instance
						concreteSubBucketer := new(core.SkewBucketer)
						var ifaceB any = concreteSubBucketer
						subBuilder.bucketer = ifaceB.(B)
					} else {
						var zeroBucketer B
						subBuilder.bucketer = zeroBucketer
					}
					_ = subBuilder.bucketer.Init(0, 0, 0, 0) // Call init on the non-nil bucketer
					timingChan <- core.BuildTimings{}
					errChan <- nil // No error for empty partition
					return
				}

				subTimings, err := subBuilder.buildFromHashes(hashes, subPHFConfig)
				// Send results regardless of error
				timingChan <- subTimings
				errChan <- err // Send nil on success, error object on failure

			}(i)
		}

		wg.Wait()
		close(errChan)
		close(timingChan)

		// Collect results and check for errors
		var firstError error
		for err := range errChan {
			if err != nil && firstError == nil {
				// Capture the first error encountered (could be SeedRuntimeError)
				firstError = err
			}
		}
		if firstError != nil {
			return core.BuildTimings{}, firstError // Propagate the error
		}

		for t := range timingChan {
			if t.MappingOrderingMicroseconds > maxMappingOrdering {
				maxMappingOrdering = t.MappingOrderingMicroseconds
			}
			if t.SearchingMicroseconds > maxSearching {
				maxSearching = t.SearchingMicroseconds
			}
		}
		// Assign max timings for parallel execution
		accumulatedTimings.MappingOrderingMicroseconds = maxMappingOrdering
		accumulatedTimings.SearchingMicroseconds = maxSearching

	} else {
		// Sequential build
		for i := uint64(0); i < pb.numPartitions; i++ {
			subBuilder := pb.subBuilders[i]
			hashes := pb.partitionBuffers[i]

			if len(hashes) == 0 {
				subBuilder.numKeys = 0
				subBuilder.tableSize = 0
				subBuilder.numBuckets = 0
				subBuilder.seed = subPHFConfig.Seed
				var zeroBucketer B
				subBuilder.bucketer = zeroBucketer
				_ = subBuilder.bucketer.Init(0, 0, 0, 0)
				continue
			}

			subTimings, err := subBuilder.buildFromHashes(hashes, subPHFConfig)
			if err != nil {
				// If sequential, return error immediately
				return accumulatedTimings, fmt.Errorf("partition %d build failed: %w", i, err)
			}
			// Accumulate timings sequentially
			accumulatedTimings.MappingOrderingMicroseconds += subTimings.MappingOrderingMicroseconds
			accumulatedTimings.SearchingMicroseconds += subTimings.SearchingMicroseconds
		}
	}

	util.Log(config.Verbose, "Sub-PHF Mapping+Ordering Max/Sum: %v", accumulatedTimings.MappingOrderingMicroseconds)
	util.Log(config.Verbose, "Sub-PHF Searching Max/Sum: %v", accumulatedTimings.SearchingMicroseconds)

	return accumulatedTimings, nil
}

func (pb *InternalMemoryBuilderPartitionedPHF[K, H, B]) AvgPartitionSize() uint64 {
	return pb.avgPartSize
}

// NumPartitions returns the number of partitions.
func (pb *InternalMemoryBuilderPartitionedPHF[K, H, B]) NumPartitions() uint64 {
	return pb.numPartitions
}

// Seed returns the seed used for hashing.
func (pb *InternalMemoryBuilderPartitionedPHF[K, H, B]) Seed() uint64 {
	return pb.seed
}

// NumKeys returns the number of keys.
func (pb *InternalMemoryBuilderPartitionedPHF[K, H, B]) NumKeys() uint64 {
	return pb.numKeys
}

// TableSize returns the target total table size.
func (pb *InternalMemoryBuilderPartitionedPHF[K, H, B]) TableSize() uint64 {
	return pb.tableSize
}

// Partitioner returns the partitioner instance.
func (pb *InternalMemoryBuilderPartitionedPHF[K, H, B]) Partitioner() *core.RangeBucketer {
	return pb.partitioner
}

// Builders returns the slice of sub-builders.
func (pb *InternalMemoryBuilderPartitionedPHF[K, H, B]) Builders() []*InternalMemoryBuilderSinglePHF[K, H, B] {
	return pb.subBuilders
}

// Offsets returns the array of offsets.
func (pb *InternalMemoryBuilderPartitionedPHF[K, H, B]) Offsets() []uint64 {
	return pb.offsets
}

// RawPartitionOffsets returns the raw offsets before diff encoding.
func (pb *InternalMemoryBuilderPartitionedPHF[K, H, B]) RawPartitionOffsets() []uint64 {
	return pb.rawPartitionOffsets
}

// FreeSlots returns the free slots array (for dense minimal).
func (pb *InternalMemoryBuilderPartitionedPHF[K, H, B]) FreeSlots() []uint64 {
	return pb.freeSlots
}
