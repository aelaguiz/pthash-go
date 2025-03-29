package builder

import (
	"fmt"
	"pthashgo/internal/core"
	"pthashgo/internal/util"
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
	tableSize     uint64             // Total estimated table size across partitions
	partitioner   core.RangeBucketer // Bucketer used to partition keys
	hasher        H                  // Hasher instance

	// Results of the build process (passed to PartitionedPHF.Build)
	offsets                []uint64                                   // Start offset for each partition's output range
	subBuilders            []*InternalMemoryBuilderSinglePHF[K, H, B] // Builders for each partition's sub-PHF
	avgPartSize            uint64                                     // Calculated avg partition size
	numBucketsPerPartition uint64                                     // Calculated for sub-builders
}

// NewInternalMemoryBuilderPartitionedPHF creates a new partitioned builder.
func NewInternalMemoryBuilderPartitionedPHF[K any, H core.Hasher[K], B core.Bucketer](hasher H) *InternalMemoryBuilderPartitionedPHF[K, H, B] {
	// Note: The sub-builder's Bucketer type 'B' is passed in,
	// but the partitioner is always RangeBucketer.
	return &InternalMemoryBuilderPartitionedPHF[K, H, B]{
		hasher: hasher,
	}
}

// BuildFromKeys orchestrates the partitioned build process.
// For Phase 5, it focuses on partitioning and setup. Phase 6 will build sub-PHFs.
func (pb *InternalMemoryBuilderPartitionedPHF[K, H, B]) BuildFromKeys(keys []K, config core.BuildConfig) (core.BuildTimings, error) {
	numKeys := uint64(len(keys))
	if numKeys == 0 {
		return core.BuildTimings{}, fmt.Errorf("cannot build partitioned PHF for zero keys")
	}
	pb.config = config
	pb.numKeys = numKeys

	// Check hash collision probability
	err := core.CheckHashCollisionProbability[K, H](numKeys)
	if err != nil {
		return core.BuildTimings{}, err
	}

	// Determine number of partitions
	avgPartitionSize := core.ComputeAvgPartitionSize(numKeys, &config)
	if avgPartitionSize == 0 || avgPartitionSize == numKeys {
		return core.BuildTimings{}, fmt.Errorf("partitioned builder requires AvgPartitionSize > 0 and < numKeys (effective size was %d)", avgPartitionSize)
	}
	pb.avgPartSize = avgPartitionSize
	pb.numPartitions = core.ComputeNumPartitions(numKeys, avgPartitionSize)
	if pb.numPartitions == 0 {
		pb.numPartitions = 1 // Ensure at least one partition
	}

	util.Log(config.Verbose, "Num Partitions = %d", pb.numPartitions)
	util.Log(config.Verbose, "Avg Partition Size = %d", avgPartitionSize)

	var timings core.BuildTimings
	start := time.Now()

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
		return timings, fmt.Errorf("failed to init partitioner: %w", err)
	}

	// --- Partition Keys ---
	// Create buffers to hold hashes for each partition
	partitionBuffers := make([][]core.Hash128, pb.numPartitions)
	for i := range partitionBuffers {
		// Preallocate based on average size, add some slack
		allocHint := uint64(float64(avgPartitionSize) * 1.1)
		if allocHint == 0 {
			allocHint = 10 // Minimum allocation
		}
		partitionBuffers[i] = make([]core.Hash128, 0, allocHint)
	}

	// Hash keys and distribute into partition buffers
	if config.NumThreads > 1 && numKeys >= uint64(config.NumThreads)*100 { // Heuristic for parallelism
		util.Log(config.Verbose, "Partitioning keys in parallel (%d threads)...", config.NumThreads)
		pb.parallelHashAndPartition(keys, partitionBuffers)
	} else {
		util.Log(config.Verbose, "Partitioning keys sequentially...")
		for _, key := range keys {
			hash := pb.hasher.Hash(key, pb.seed)
			partitionIdx := pb.partitioner.Bucket(hash.Mix()) // Use Mix() for partitioning
			if partitionIdx >= pb.numPartitions {
				// This indicates an issue with RangeBucketer logic or hash distribution
				return timings, fmt.Errorf("internal error: partition index %d out of bounds (%d)", partitionIdx, pb.numPartitions)
			}
			partitionBuffers[partitionIdx] = append(partitionBuffers[partitionIdx], hash)
		}
	}
	util.Log(config.Verbose, "Partitioning complete.")

	// --- Calculate Offsets and Initialize Sub-Builders ---
	pb.offsets = make([]uint64, pb.numPartitions+1)
	pb.subBuilders = make([]*InternalMemoryBuilderSinglePHF[K, H, B], pb.numPartitions)
	pb.tableSize = 0 // Total size across all partitions
	pb.numBucketsPerPartition = core.ComputeNumBuckets(avgPartitionSize, config.Lambda)
	util.Log(config.Verbose, "Num Buckets per Partition = %d", pb.numBucketsPerPartition)

	cumulativeOffset := uint64(0)
	for i := uint64(0); i < pb.numPartitions; i++ {
		partitionSize := uint64(len(partitionBuffers[i]))
		if partitionSize == 0 && config.Verbose {
			util.Log(true, "Warning: Partition %d is empty.", i)
		}

		// Calculate sub-table size for this partition
		subTableSize := uint64(0)
		if partitionSize > 0 {
			subTableSize = core.MinimalTableSize(partitionSize, config.Alpha, config.Search)
		}

		pb.tableSize += subTableSize // Accumulate total table size estimate

		pb.offsets[i] = cumulativeOffset

		// Calculate offset increment based on minimal/dense config
		offsetIncrement := uint64(0)
		if config.DensePartitioning {
			// In dense mode, offset is based on sub-table size (like C++)
			offsetIncrement = subTableSize
		} else {
			// In standard minimal mode, offset is based on actual keys in partition
			if config.Minimal {
				offsetIncrement = partitionSize
			} else {
				offsetIncrement = subTableSize
			}
		}
		cumulativeOffset += offsetIncrement

		// Initialize the sub-builder (it will be populated in Phase 6)
		// Create zero value bucketer instance for the sub-builder
		var subBucketer B
		// If B is a pointer type (*core.SkewBucketer), create it like this:
		// subBucketer = new(core.SkewBucketer) // Assuming B is *core.SkewBucketer
		// This needs careful handling based on whether B is value or pointer type.
		// Assuming B is a value type for now based on previous phases.
		pb.subBuilders[i] = NewInternalMemoryBuilderSinglePHF[K, H, B](pb.hasher, subBucketer) // Pass hasher and zero bucketer

		// *** Phase 6 Action: ***
		// *** Call pb.subBuilders[i].BuildFromHashes(partitionBuffers[i], subConfig) here ***
		// For Phase 5, we only set up the structures.
		// We need to store the hashes temporarily or pass them to Phase 6.
		// Let's store them *in* the sub-builders for now (requires adding a field).
		// Or, pass partitionBuffers to the next stage. Let's pass partitionBuffers.
	}
	pb.offsets[pb.numPartitions] = cumulativeOffset // Final offset

	timings.PartitioningMicroseconds = time.Since(start)
	util.Log(config.Verbose, "Partitioning and setup took: %v", timings.PartitioningMicroseconds)

	// --- Phase 6: Build Sub-PHFs ---
	// In this phase, we stop here. Phase 6 will take partitionBuffers and pb.subBuilders
	// and execute the build for each sub-builder.

	// We need a way to pass the partition data to the next stage/caller.
	// Let's return the partition buffers along with the builder state for now.
	// The final PHF build method will call this, then call buildSubPHFs.

	// Store partition data within the builder itself temporarily for phase 5/6 transition?
	// Or require the caller (the final PHF Build method) to manage it?
	// Let's assume the final PHF Build method manages it. This builder focuses on partitioning.

	return timings, nil
}

// parallelHashAndPartition distributes keys into partitions concurrently.
func (pb *InternalMemoryBuilderPartitionedPHF[K, H, B]) parallelHashAndPartition(keys []K, partitionBuffers [][]core.Hash128) {
	numKeys := uint64(len(keys))
	numThreads := pb.config.NumThreads
	keysPerThread := numKeys / uint64(numThreads)

	// Use mutexes for each partition buffer slice append, as append can reallocate.
	// Consider pre-allocating large enough, or using channels, or thread-local buffers + merge later for better performance.
	bufferMutexes := make([]sync.Mutex, pb.numPartitions)

	var wg sync.WaitGroup
	wg.Add(numThreads)

	for tid := 0; tid < numThreads; tid++ {
		go func(threadID int) {
			defer wg.Done()
			start := uint64(threadID) * keysPerThread
			end := start + keysPerThread
			if threadID == numThreads-1 {
				end = numKeys // Last thread takes the rest
			}

			for i := start; i < end; i++ {
				key := keys[i]
				hash := pb.hasher.Hash(key, pb.seed)
				partitionIdx := pb.partitioner.Bucket(hash.Mix()) // Use Mix() for partitioning

				// Lock the specific buffer, append, unlock
				bufferMutexes[partitionIdx].Lock()
				partitionBuffers[partitionIdx] = append(partitionBuffers[partitionIdx], hash)
				bufferMutexes[partitionIdx].Unlock()
			}
		}(tid)
	}
	wg.Wait()
}

// --- Accessors for Partitioned Builder ---

func (pb *InternalMemoryBuilderPartitionedPHF[K, H, B]) Seed() uint64 {
	return pb.seed
}

func (pb *InternalMemoryBuilderPartitionedPHF[K, H, B]) NumKeys() uint64 {
	return pb.numKeys
}

func (pb *InternalMemoryBuilderPartitionedPHF[K, H, B]) TableSize() uint64 {
	// Return the total estimated table size
	return pb.tableSize
}

func (pb *InternalMemoryBuilderPartitionedPHF[K, H, B]) NumPartitions() uint64 {
	return pb.numPartitions
}

func (pb *InternalMemoryBuilderPartitionedPHF[K, H, B]) Partitioner() core.RangeBucketer {
	return pb.partitioner
}

func (pb *InternalMemoryBuilderPartitionedPHF[K, H, B]) Offsets() []uint64 {
	return pb.offsets
}

// Builders returns the slice of sub-builders (pointers).
func (pb *InternalMemoryBuilderPartitionedPHF[K, H, B]) Builders() []*InternalMemoryBuilderSinglePHF[K, H, B] {
	return pb.subBuilders
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
// This belongs logically in Phase 6 but is defined here for structure.
func (pb *InternalMemoryBuilderPartitionedPHF[K, H, B]) BuildSubPHFs(
	partitionBuffers [][]core.Hash128,
	config core.BuildConfig, // Pass original config for thread count etc.
) (core.BuildTimings, error) {

	util.Log(config.Verbose, "Building Sub-PHFs...")
	subPHFConfig := config      // Copy config
	subPHFConfig.Seed = pb.seed // Ensure sub-builders use the same main seed
	subPHFConfig.NumBuckets = pb.numBucketsPerPartition
	subPHFConfig.Verbose = false // Quieten sub-builders usually
	subPHFConfig.NumThreads = 1  // Each sub-build runs sequentially within its thread/goroutine

	var timings core.BuildTimings
	// var totalMappingOrdering, totalSearching time.Duration
	numThreads := config.NumThreads
	if numThreads <= 0 {
		numThreads = 1
	}

	if numThreads > 1 && pb.numPartitions >= uint64(numThreads) {
		// Parallel build
		var wg sync.WaitGroup
		wg.Add(int(pb.numPartitions))
		errChan := make(chan error, pb.numPartitions)                // Buffered channel for errors
		timingChan := make(chan core.BuildTimings, pb.numPartitions) // Channel for timings

		// Use a semaphore to limit concurrency if needed, or just spawn all
		sem := make(chan struct{}, numThreads) // Limit to numThreads concurrent builds

		for i := uint64(0); i < pb.numPartitions; i++ {
			go func(partitionIdx uint64) {
				sem <- struct{}{} // Acquire semaphore slot
				defer wg.Done()
				defer func() { <-sem }() // Release semaphore slot

				subBuilder := pb.subBuilders[partitionIdx]
				hashes := partitionBuffers[partitionIdx]

				if len(hashes) == 0 {
					// Handle empty partition - sub-builder remains empty? Or init defaults?
					// Need to ensure sub-builder state is valid even if empty.
					// Let's initialize essential fields.
					subBuilder.numKeys = 0
					subBuilder.tableSize = 0
					subBuilder.numBuckets = 0 // Or subPHFConfig.NumBuckets? Let's use 0.
					subBuilder.seed = subPHFConfig.Seed
					// Ensure bucketer is initialized (even if not used)
					var zeroBucketer B
					subBuilder.bucketer = zeroBucketer
					_ = subBuilder.bucketer.Init(0, 0, 0, 0) // Call init on zero value

					timingChan <- core.BuildTimings{} // Send zero timings
					return
				}

				// Pass the actual hashes for this partition
				// The sub-builder's BuildFromKeys expects keys, but we have hashes.
				// Need a BuildFromHashes method in the single builder.
				// Let's add BuildFromHashes to InternalMemoryBuilderSinglePHF.

				// --- Assuming BuildFromHashes exists ---
				subTimings, err := subBuilder.buildFromHashes(hashes, subPHFConfig)
				if err != nil {
					errChan <- fmt.Errorf("partition %d build failed: %w", partitionIdx, err)
					timingChan <- core.BuildTimings{} // Send zero timings on error
					return
				}
				timingChan <- subTimings
				errChan <- nil // Indicate success
			}(i)
		}

		wg.Wait()
		close(errChan)
		close(timingChan)

		// Collect results and check for errors
		var firstError error
		maxMappingOrdering := time.Duration(0)
		maxSearching := time.Duration(0)

		for err := range errChan {
			if err != nil && firstError == nil {
				firstError = err // Capture the first error encountered
			}
		}
		if firstError != nil {
			return timings, firstError // Return the first error
		}

		for t := range timingChan {
			// In parallel, we take the max time for each phase
			if t.MappingOrderingMicroseconds > maxMappingOrdering {
				maxMappingOrdering = t.MappingOrderingMicroseconds
			}
			if t.SearchingMicroseconds > maxSearching {
				maxSearching = t.SearchingMicroseconds
			}
		}
		timings.MappingOrderingMicroseconds = maxMappingOrdering
		timings.SearchingMicroseconds = maxSearching

	} else {
		// Sequential build
		for i := uint64(0); i < pb.numPartitions; i++ {
			subBuilder := pb.subBuilders[i]
			hashes := partitionBuffers[i]

			if len(hashes) == 0 {
				// Handle empty partition
				subBuilder.numKeys = 0
				subBuilder.tableSize = 0
				subBuilder.numBuckets = 0
				subBuilder.seed = subPHFConfig.Seed
				var zeroBucketer B
				subBuilder.bucketer = zeroBucketer
				_ = subBuilder.bucketer.Init(0, 0, 0, 0)
				continue
			}

			// --- Assuming BuildFromHashes exists ---
			subTimings, err := subBuilder.buildFromHashes(hashes, subPHFConfig)
			if err != nil {
				return timings, fmt.Errorf("partition %d build failed: %w", i, err)
			}
			// Accumulate timings sequentially
			timings.MappingOrderingMicroseconds += subTimings.MappingOrderingMicroseconds
			timings.SearchingMicroseconds += subTimings.SearchingMicroseconds
		}
	}

	util.Log(config.Verbose, "Sub-PHF Mapping+Ordering Max/Sum: %v", timings.MappingOrderingMicroseconds)
	util.Log(config.Verbose, "Sub-PHF Searching Max/Sum: %v", timings.SearchingMicroseconds)

	return timings, nil
}

func (pb *InternalMemoryBuilderPartitionedPHF[K, H, B]) AvgPartitionSize() uint64 {
	return pb.avgPartSize
}
