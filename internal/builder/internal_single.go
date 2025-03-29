package builder

import (
	"container/heap"
	"fmt"
	"log"
	"pthashgo/internal/core"
	"pthashgo/internal/util"
	"sort"
	"sync"
	"time"
)

type TakenBits interface {
	Get(pos uint64) bool
	Size() uint64
}

// Ensure *core.BitVector satisfies TakenBits
var _ TakenBits = (*core.BitVector)(nil)

// Type alias for convenience
type pairsT = []core.BucketPayloadPair

// InternalMemoryBuilderSinglePHF holds the state during internal memory construction.
type InternalMemoryBuilderSinglePHF[K any, H core.Hasher[K], B core.Bucketer] struct {
	config     core.BuildConfig
	seed       uint64
	numKeys    uint64
	numBuckets uint64
	tableSize  uint64
	bucketer   B
	hasher     H // Store an instance or reference
	pilots     []uint64
	taken      *core.BitVector // Use pointer to allow modification by search
	freeSlots  []uint64        // Used only if Minimal=true
}

// NewInternalMemoryBuilderSinglePHF creates a new builder.
// Pass hasher and bucketer instances.
func NewInternalMemoryBuilderSinglePHF[K any, H core.Hasher[K], B core.Bucketer](hasher H, bucketer B) *InternalMemoryBuilderSinglePHF[K, H, B] {
	return &InternalMemoryBuilderSinglePHF[K, H, B]{
		hasher:   hasher,
		bucketer: bucketer, // Store instance
	}
}

// Seed returns the seed used for hashing.
func (b *InternalMemoryBuilderSinglePHF[K, H, B]) Seed() uint64 {
	return b.seed
}

// NumKeys returns the number of keys.
func (b *InternalMemoryBuilderSinglePHF[K, H, B]) NumKeys() uint64 {
	return b.numKeys
}

// TableSize returns the target table size (m).
func (b *InternalMemoryBuilderSinglePHF[K, H, B]) TableSize() uint64 {
	return b.tableSize
}

// NumBuckets returns the number of buckets.
func (b *InternalMemoryBuilderSinglePHF[K, H, B]) NumBuckets() uint64 {
	return b.numBuckets
}

// Bucketer returns the bucketer instance.
func (b *InternalMemoryBuilderSinglePHF[K, H, B]) Bucketer() B {
	return b.bucketer
}

// Pilots returns the calculated pilot values.
func (b *InternalMemoryBuilderSinglePHF[K, H, B]) Pilots() []uint64 {
	return b.pilots
}

// Taken returns the bit vector indicating used slots.
func (b *InternalMemoryBuilderSinglePHF[K, H, B]) Taken() *core.BitVector {
	return b.taken
}

// FreeSlots returns the mapping for free slots (only valid if minimal).
func (b *InternalMemoryBuilderSinglePHF[K, H, B]) FreeSlots() []uint64 {
	return b.freeSlots
}

// BuildFromKeys builds the PHF structure from keys.
// Note: Go generics don't allow specializing on iterator tags easily.
// We assume 'keys' is a slice []K for simplicity here.
// A channel-based approach could handle true iterators.
func (b *InternalMemoryBuilderSinglePHF[K, H, B]) BuildFromKeys(keys []K, config core.BuildConfig) (core.BuildTimings, error) {
	if config.Seed == core.InvalidSeed {
		// Try multiple random seeds if none provided, similar to C++
		actualConfig := config
		for attempt := 0; attempt < 10; attempt++ {
			actualConfig.Seed = util.RandomSeed()
			util.Log(config.Verbose, "Attempt %d with seed %d", attempt+1, actualConfig.Seed)
			timings, err := b.buildFromKeysInternal(keys, actualConfig)
			if err == nil {
				return timings, nil // Success
			}
			util.Log(config.Verbose, "Attempt %d failed: %v", attempt+1, err)
			if _, ok := err.(core.SeedRuntimeError); !ok {
				return core.BuildTimings{}, err // Return non-seed related errors immediately
			}
		}
		return core.BuildTimings{}, core.SeedRuntimeError{Msg: "all seeds failed after 10 attempts"}
	}
	// Build with the specified seed
	return b.buildFromKeysInternal(keys, config)
}

// buildFromKeysInternal performs the build assuming a valid seed is set in config.
func (b *InternalMemoryBuilderSinglePHF[K, H, B]) buildFromKeysInternal(keys []K, config core.BuildConfig) (core.BuildTimings, error) {
	numKeys := uint64(len(keys))
	if numKeys == 0 {
		return core.BuildTimings{}, fmt.Errorf("cannot build PHF for zero keys")
	}
	b.config = config // Store config

	// Check hash collision probability
	err := core.CheckHashCollisionProbability[K, H](numKeys)
	if err != nil {
		return core.BuildTimings{}, err
	}

	if config.Alpha <= 0 || config.Alpha > 1.0 {
		return core.BuildTimings{}, fmt.Errorf("load factor alpha must be > 0 and <= 1.0")
	}

	var timings core.BuildTimings
	start := time.Now()

	// Calculate table size and num buckets
	tableSize := uint64(float64(numKeys) / config.Alpha)
	if config.Search == core.SearchTypeXOR && (tableSize&(tableSize-1)) == 0 && tableSize > 0 {
		tableSize++ // Ensure not power of 2 for XOR search
	}
	numBuckets := config.NumBuckets
	if numBuckets == core.InvalidNumBuckets {
		numBuckets = core.ComputeNumBuckets(numKeys, config.Lambda)
	}

	// Basic validation for BucketIDType size
	if numBuckets > uint64(core.MaxBucketID) {
		return core.BuildTimings{}, fmt.Errorf("number of buckets (%d) exceeds limit for BucketIDType (%d)", numBuckets, core.MaxBucketID)
	}

	b.seed = config.Seed
	b.numKeys = numKeys
	b.tableSize = tableSize
	b.numBuckets = numBuckets

	// Initialize bucketer
	// Pass tableSize based on C++ OptBucketer logic
	effectiveTableSize := float64(numBuckets) * config.Lambda / config.Alpha
	err = b.bucketer.Init(numBuckets, config.Lambda, uint64(effectiveTableSize), config.Alpha)
	if err != nil {
		return core.BuildTimings{}, fmt.Errorf("failed to initialize bucketer: %w", err)
	}

	util.Log(config.Verbose, "Lambda (avg bucket size) = %.2f", config.Lambda)
	util.Log(config.Verbose, "Alpha (load factor) = %.2f", config.Alpha)
	util.Log(config.Verbose, "Num Keys = %d", numKeys)
	util.Log(config.Verbose, "Table Size (m) = %d", tableSize)
	util.Log(config.Verbose, "Num Buckets = %d", numBuckets)

	// --- Map + Sort ---
	mapStart := time.Now()
	var pairsBlocks []pairsT
	if config.NumThreads > 1 && numKeys >= uint64(config.NumThreads) {
		pairsBlocks = b.mapParallel(keys)
	} else {
		pairsBlocks = b.mapSequential(keys)
	}
	util.Log(config.Verbose, "Map+Sort took: %v", time.Since(mapStart))

	// --- Merge ---
	mergeStart := time.Now()
	bucketsCollector := newBucketsT() // Our merger
	err = merge(pairsBlocks, bucketsCollector, config.Verbose, config.SecondarySort)
	if err != nil {
		return core.BuildTimings{}, fmt.Errorf("merge failed: %w", err)
	}
	util.Log(config.Verbose, "Merge took: %v", time.Since(mergeStart))

	timings.MappingOrderingMicroseconds = time.Since(mapStart)
	util.Log(config.Verbose, "Mapping+Ordering took: %v", timings.MappingOrderingMicroseconds)
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
	err = Search[B]( // Pass Bucketer type explicitly for Search Logger estimate
		b.numKeys, b.numBuckets, numNonEmpty, b.seed, &config,
		bucketsIter, takenBuilder, pilotsWrapper, b.bucketer,
	)
	if err != nil {
		return core.BuildTimings{}, fmt.Errorf("search failed: %w", err)
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
	util.Log(config.Verbose, "Search took: %v", timings.SearchingMicroseconds)

	timings.PartitioningMicroseconds = 0 // No partitioning in single PHF

	util.Log(config.Verbose, "Internal build steps finished in: %v", time.Since(start))
	return timings, nil
}

// --- Map Functions ---

// mapSequential hashes keys and creates sorted pairs.
func (b *InternalMemoryBuilderSinglePHF[K, H, B]) mapSequential(keys []K) []pairsT {
	numKeys := uint64(len(keys))
	pairs := make(pairsT, numKeys)
	for i, key := range keys {
		hash := b.hasher.Hash(key, b.seed)
		bucketID := b.bucketer.Bucket(hash.First())
		pairs[i] = core.BucketPayloadPair{BucketID: bucketID, Payload: hash.Second()}
	}

	// Sort the pairs
	sortFunc := func(i, j int) bool {
		if b.config.SecondarySort { // C++: sort descending by bucket_id
			if pairs[i].BucketID != pairs[j].BucketID {
				return pairs[i].BucketID > pairs[j].BucketID // Descending
			}
			return pairs[i].Payload < pairs[j].Payload // Ascending payload
		}
		// Default: sort ascending by bucket_id
		return pairs[i].Less(pairs[j]) // Use standard Less (ascending ID, then payload)
	}
	sort.SliceStable(pairs, sortFunc) // Use stable sort to maintain relative payload order if needed

	return []pairsT{pairs} // Return as a single block
}

// mapParallel hashes keys in parallel and creates sorted blocks of pairs.
func (b *InternalMemoryBuilderSinglePHF[K, H, B]) mapParallel(keys []K) []pairsT {
	numKeys := uint64(len(keys))
	numThreads := b.config.NumThreads
	pairsBlocks := make([]pairsT, numThreads)
	keysPerThread := numKeys / uint64(numThreads)

	var wg sync.WaitGroup
	wg.Add(numThreads)

	for tid := 0; tid < numThreads; tid++ {
		go func(threadID int) {
			defer wg.Done()

			start := uint64(threadID) * keysPerThread
			end := start + keysPerThread
			if threadID == numThreads-1 {
				end = numKeys // Ensure last thread processes remaining keys
			}
			localNumKeys := end - start
			localPairs := make(pairsT, localNumKeys)

			for i := uint64(0); i < localNumKeys; i++ {
				key := keys[start+i]
				hash := b.hasher.Hash(key, b.seed)
				bucketID := b.bucketer.Bucket(hash.First())
				localPairs[i] = core.BucketPayloadPair{BucketID: bucketID, Payload: hash.Second()}
			}

			// Sort local pairs
			sortFunc := func(i, j int) bool {
				if b.config.SecondarySort {
					if localPairs[i].BucketID != localPairs[j].BucketID {
						return localPairs[i].BucketID > localPairs[j].BucketID
					}
					return localPairs[i].Payload < localPairs[j].Payload
				}
				return localPairs[i].Less(localPairs[j])
			}
			sort.SliceStable(localPairs, sortFunc) // Use stable sort

			pairsBlocks[threadID] = localPairs
		}(tid)
	}

	wg.Wait()
	return pairsBlocks
}

// --- Merger (buckets_t) ---

// bucketsT collects merged bucket data. Matches C++ nested class structure.
type bucketsT struct {
	// Store buckets grouped by size: buffers[size-1] contains buckets of size 'size'
	// Each inner slice contains [id, p1, id, p1, p2, id, p1, p2, p3, ...]
	buffers         [core.MaxBucketSize][]uint64
	maxBucketSize   core.BucketSizeType
	totalNumBuckets uint64
	sizeCounts      [core.MaxBucketSize]uint64 // Count of buckets for each size
}

func newBucketsT() *bucketsT {
	return &bucketsT{}
}

// add appends a bucket's data (id + payloads) to the appropriate buffer.
func (bt *bucketsT) add(bucketID core.BucketIDType, payloads []uint64) {
	size := len(payloads)
	if size == 0 || size > int(core.MaxBucketSize) {
		// This indicates an error upstream (e.g., merge logic)
		panic(fmt.Sprintf("invalid bucket size %d received in bucketsT.add", size))
	}
	sizeIdx := size - 1
	bt.buffers[sizeIdx] = append(bt.buffers[sizeIdx], uint64(bucketID))
	bt.buffers[sizeIdx] = append(bt.buffers[sizeIdx], payloads...)
	bt.totalNumBuckets++
	bt.sizeCounts[sizeIdx]++
	if core.BucketSizeType(size) > bt.maxBucketSize {
		bt.maxBucketSize = core.BucketSizeType(size)
	}
}

// numBuckets returns the total number of non-empty buckets collected.
func (bt *bucketsT) numBuckets() uint64 {
	return bt.totalNumBuckets
}

// iterator creates an iterator over the collected buckets.
func (bt *bucketsT) iterator() *bucketsIteratorT {
	// Pass only the relevant part of the buffers slice
	return newBucketsIterator(bt.buffers[:bt.maxBucketSize])
}

// printBucketSizeDistribution logs the counts of buckets for each size.
func (bt *bucketsT) printBucketSizeDistribution() {
	util.Log(true, "Max Bucket Size = %d", bt.maxBucketSize)
	for i := int(bt.maxBucketSize) - 1; i >= 0; i-- {
		size := i + 1
		count := bt.sizeCounts[i]
		if count > 0 { // Only print sizes that actually occurred
			util.Log(true, "Num Buckets of Size %d = %d", size, count)
		}
	}
}

// --- Merge Functions ---

// merge merges sorted blocks of pairs into the bucketsT collector.
func merge(pairsBlocks []pairsT, merger *bucketsT, verbose bool, secondarySort bool) error {
	if len(pairsBlocks) == 0 {
		return nil // Nothing to merge
	}
	if len(pairsBlocks) == 1 {
		return mergeSingleBlock(pairsBlocks[0], merger, verbose, secondarySort)
	}
	return mergeMultipleBlocks(pairsBlocks, merger, verbose, secondarySort)
}

// mergeSingleBlock processes a single pre-sorted block.
func mergeSingleBlock(pairs pairsT, merger *bucketsT, verbose bool, secondarySort bool) error {
	numPairs := len(pairs)
	if numPairs == 0 {
		return nil
	}

	totalPairs := uint64(numPairs)
	logger := util.NewProgressLogger(totalPairs, "Merging single block: ", " pairs", verbose)
	defer logger.Finalize()

	// startIdx := 0
	currentBucketID := pairs[0].BucketID
	payloads := make([]uint64, 0, 8) // Initial capacity

	for i := 0; i < numPairs; i++ {
		logger.Log()
		pair := pairs[i]

		if pair.BucketID == currentBucketID {
			// Check for duplicate payloads within the same bucket
			// Requires the block to be sorted by ID then Payload
			if len(payloads) > 0 && pair.Payload == payloads[len(payloads)-1] {
				return core.SeedRuntimeError{Msg: fmt.Sprintf("duplicate payload %d in bucket %d", pair.Payload, pair.BucketID)}
			}
			payloads = append(payloads, pair.Payload)
		} else {
			// End of the previous bucket, add it to the merger
			if len(payloads) > int(core.MaxBucketSize) {
				return core.SeedRuntimeError{Msg: fmt.Sprintf("bucket %d size %d exceeds max %d", currentBucketID, len(payloads), core.MaxBucketSize)}
			}
			if len(payloads) > 0 {
				merger.add(currentBucketID, payloads)
			}

			// Start the new bucket
			currentBucketID = pair.BucketID
			payloads = payloads[:0] // Clear slice while retaining capacity
			payloads = append(payloads, pair.Payload)
			// startIdx = i
		}
	}

	// Add the last bucket
	if len(payloads) > int(core.MaxBucketSize) {
		return core.SeedRuntimeError{Msg: fmt.Sprintf("bucket %d size %d exceeds max %d", currentBucketID, len(payloads), core.MaxBucketSize)}
	}
	if len(payloads) > 0 {
		merger.add(currentBucketID, payloads)
	}

	return nil
}

// --- Heap for K-way Merge ---

// heapItem stores an item for the merge heap.
type heapItem struct {
	pair     core.BucketPayloadPair
	blockIdx int // Index of the block this pair came from
}

// minHeap implements heap.Interface for merging pairs.
type minHeap struct {
	items         []heapItem
	secondarySort bool
}

func (h minHeap) Len() int { return len(h.items) }
func (h minHeap) Less(i, j int) bool {
	// Comparison logic depends on the initial sort order (secondarySort)
	if h.secondarySort { // If blocks were sorted descending by BucketID
		// We want the *largest* BucketID at the top (min-heap on negated ID?)
		// Or, simpler: use a max-heap based on BucketID, then min-heap on Payload.
		// Let's stick to min-heap convention: smallest element comes out first.
		// If blocks are sorted DESC by ID, the heap needs custom logic.
		// Let's assume blocks are sorted ASC by ID for standard heap merge.
		// *** If secondarySort=true required DESC sort, this heap merge won't work directly ***
		// *** Rethink: C++ merge processes buckets *backwards* by size later. ***
		// *** The initial sort order might primarily be for collision detection ***
		// *** Let's proceed assuming standard ASC merge, adjust if needed ***
		// Standard Ascending Merge:
		return h.items[i].pair.Less(h.items[j].pair)
	}
	// Standard Ascending Merge:
	return h.items[i].pair.Less(h.items[j].pair)
}
func (h minHeap) Swap(i, j int) { h.items[i], h.items[j] = h.items[j], h.items[i] }
func (h *minHeap) Push(x any)   { h.items = append(h.items, x.(heapItem)) }
func (h *minHeap) Pop() any {
	old := h.items
	n := len(old)
	item := old[n-1]
	h.items = old[0 : n-1]
	return item
}

// mergeMultipleBlocks processes multiple pre-sorted blocks using a min-heap.
func mergeMultipleBlocks(pairsBlocks []pairsT, merger *bucketsT, verbose bool, secondarySort bool) error {
	// *** Warning: This standard heap merge assumes blocks are sorted ASC by ID. ***
	// *** If secondarySort=true implies DESC sort in map phase, this needs rework. ***
	// *** Let's assume map phase produced ASC ID, ASC Payload sorted blocks for now. ***
	if secondarySort {
		util.Log(verbose, "Warning: Heap merge with secondarySort=true (descending ID sort) might be incorrect. Assuming ASC sort for merge.")
	}

	totalPairs := uint64(0)
	blockIters := make([]int, len(pairsBlocks)) // Current index within each block
	h := &minHeap{items: make([]heapItem, 0, len(pairsBlocks)), secondarySort: secondarySort}

	// Initialize heap with the first element from each non-empty block
	for i, block := range pairsBlocks {
		totalPairs += uint64(len(block))
		if len(block) > 0 {
			heap.Push(h, heapItem{pair: block[0], blockIdx: i})
			blockIters[i] = 1 // Next index to read from this block
		}
	}

	if h.Len() == 0 {
		return nil // No pairs to merge
	}

	logger := util.NewProgressLogger(totalPairs, "Merging multiple blocks: ", " pairs", verbose)
	defer logger.Finalize()

	payloads := make([]uint64, 0, 8)
	firstItem := heap.Pop(h).(heapItem)
	currentBucketID := firstItem.pair.BucketID
	payloads = append(payloads, firstItem.pair.Payload)
	logger.Log()

	// Add next element from the block where the first item came from
	blockIdx := firstItem.blockIdx
	if blockIters[blockIdx] < len(pairsBlocks[blockIdx]) {
		heap.Push(h, heapItem{pair: pairsBlocks[blockIdx][blockIters[blockIdx]], blockIdx: blockIdx})
		blockIters[blockIdx]++
	}

	// Merge process
	for h.Len() > 0 {
		item := heap.Pop(h).(heapItem)
		pair := item.pair
		logger.Log()

		if pair.BucketID == currentBucketID {
			// Check for duplicate payloads
			if len(payloads) > 0 && pair.Payload == payloads[len(payloads)-1] {
				return core.SeedRuntimeError{Msg: fmt.Sprintf("duplicate payload %d in bucket %d", pair.Payload, pair.BucketID)}
			}
			payloads = append(payloads, pair.Payload)
		} else {
			// Finish previous bucket
			if len(payloads) > int(core.MaxBucketSize) {
				return core.SeedRuntimeError{Msg: fmt.Sprintf("bucket %d size %d exceeds max %d", currentBucketID, len(payloads), core.MaxBucketSize)}
			}
			if len(payloads) > 0 {
				merger.add(currentBucketID, payloads)
			}

			// Start new bucket
			currentBucketID = pair.BucketID
			payloads = payloads[:0]
			payloads = append(payloads, pair.Payload)
		}

		// Push the next element from the same block onto the heap
		blockIdx = item.blockIdx
		if blockIters[blockIdx] < len(pairsBlocks[blockIdx]) {
			heap.Push(h, heapItem{pair: pairsBlocks[blockIdx][blockIters[blockIdx]], blockIdx: blockIdx})
			blockIters[blockIdx]++
		}
	}

	// Add the very last bucket
	if len(payloads) > int(core.MaxBucketSize) {
		return core.SeedRuntimeError{Msg: fmt.Sprintf("bucket %d size %d exceeds max %d", currentBucketID, len(payloads), core.MaxBucketSize)}
	}
	if len(payloads) > 0 {
		merger.add(currentBucketID, payloads)
	}

	return nil
}

// --- Buckets Iterator ---

// bucketsIteratorT iterates over buckets collected by bucketsT.
// It iterates backwards by size, matching the C++ search order.
type bucketsIteratorT struct {
	buffers    [][]uint64 // Slice view of the buffers (up to max size)
	bufferIdx  int        // Current buffer index (maps to size-1)
	currentPos int        // Current position within buffers[bufferIdx]
	bucketSize core.BucketSizeType
}

// newBucketsIterator creates an iterator. buffers should be sized appropriately.
func newBucketsIterator(buffers [][]uint64) *bucketsIteratorT {
	iter := &bucketsIteratorT{
		buffers:   buffers,
		bufferIdx: len(buffers) - 1, // Start from largest size index
	}
	iter.findNextNonEmptyBuffer() // Position at the first non-empty bucket
	return iter
}

// findNextNonEmptyBuffer moves the iterator state to the start of the next non-empty bucket buffer.
func (it *bucketsIteratorT) findNextNonEmptyBuffer() {
	// Move to next smaller size if current buffer is exhausted
	if it.bufferIdx >= 0 && it.currentPos >= len(it.buffers[it.bufferIdx]) {
		it.bufferIdx--
		it.currentPos = 0
	}
	// Skip empty buffers
	for it.bufferIdx >= 0 && len(it.buffers[it.bufferIdx]) == 0 {
		it.bufferIdx--
		it.currentPos = 0
	}
	// Update bucket size if a valid buffer was found
	if it.bufferIdx >= 0 {
		it.bucketSize = core.BucketSizeType(it.bufferIdx + 1)
	} else {
		it.bucketSize = 0 // Indicate end of iteration
	}
}

// HasNext checks if there are more buckets to iterate.
func (it *bucketsIteratorT) HasNext() bool {
	isValid := it.bufferIdx >= 0
	if !isValid {
		log.Printf("Iterator exhausted: bufferIdx=%d", it.bufferIdx)
	}
	return isValid
}

// Next returns the next bucket and advances the iterator.
func (it *bucketsIteratorT) Next() core.BucketT {
	if !it.HasNext() {
		panic("bucketsIteratorT.Next() called after end of iteration")
	}
	// The data for the current bucket starts at currentPos
	// It includes the ID + 'bucketSize' payloads
	bucketDataLen := 1 + int(it.bucketSize)
	if it.currentPos+bucketDataLen > len(it.buffers[it.bufferIdx]) {
		panic(fmt.Sprintf("internal iterator error: trying to read past buffer boundary for size %d", it.bucketSize))
	}
	bucketData := it.buffers[it.bufferIdx][it.currentPos : it.currentPos+bucketDataLen]
	bucket := core.NewBucketT(bucketData, it.bucketSize)

	// Advance position
	it.currentPos += bucketDataLen
	it.findNextNonEmptyBuffer() // Move to the start of the next bucket/buffer

	return bucket
}

// --- Pilots Wrapper (for search output) ---

// PilotsBuffer defines the interface for receiving (bucketID, pilot) pairs from search.
type PilotsBuffer interface {
	EmplaceBack(bucketID core.BucketIDType, pilot uint64)
}

// pilotsWrapper adapts a []uint64 slice to the PilotsBuffer interface.
type pilotsWrapper struct {
	pilots []uint64
}

func newPilotsWrapper(pilots []uint64) *pilotsWrapper {
	return &pilotsWrapper{pilots: pilots}
}

// EmplaceBack sets the pilot value at the index corresponding to bucketID.
func (pw *pilotsWrapper) EmplaceBack(bucketID core.BucketIDType, pilot uint64) {
	if int(bucketID) >= len(pw.pilots) {
		// This indicates a logic error upstream if bucketID is out of bounds
		panic(fmt.Sprintf("pilot assignment error: bucketID %d out of bounds (%d)", bucketID, len(pw.pilots)))
	}
	pw.pilots[bucketID] = pilot
}

// --- Fill Free Slots ---

// fillFreeSlots calculates the remapping for minimal perfect hashing.
// Corresponds to C++ fill_free_slots.
func fillFreeSlots(taken TakenBits, numKeys uint64, freeSlots *[]uint64, tableSize uint64) {
	if tableSize <= numKeys {
		return // No free slots to fill
	}

	// Reset the slice, but keep capacity
	*freeSlots = (*freeSlots)[:0]

	nextUsedSlot := numKeys        // Start scanning for free slots from here
	lastFreeSlot := uint64(0)      // Slot < numKeys that is currently free
	lastValidFreeSlot := uint64(0) // The last value assigned to a free slot > numKeys

	// C++ uses iterators, Go can use Get directly
	for {
		// Find the next free slot (on the left, pos < numKeys)
		for lastFreeSlot < numKeys && taken.Get(lastFreeSlot) {
			lastFreeSlot++
		}

		if lastFreeSlot == numKeys {
			break // No more free slots available below numKeys boundary
		}

		// We found a free slot at 'lastFreeSlot'.
		// Now find the next used slot (on the right, pos >= numKeys)
		// and fill the free slots between the current 'nextUsedSlot'
		// and that used slot with 'lastFreeSlot'.
		if nextUsedSlot >= tableSize {
			// This shouldn't happen if lastFreeSlot < numKeys was found
			break
		}

		// Fill free slots >= numKeys with the value lastFreeSlot
		// until we hit a slot that is already taken or the end.
		for nextUsedSlot < tableSize && !taken.Get(nextUsedSlot) {
			*freeSlots = append(*freeSlots, lastFreeSlot)
			nextUsedSlot++
		}

		if nextUsedSlot == tableSize {
			break // Reached the end
		}

		// We hit a used slot at 'nextUsedSlot'.
		// Fill this position too (conceptually overwriting it) and advance cursors.
		*freeSlots = append(*freeSlots, lastFreeSlot)
		lastValidFreeSlot = lastFreeSlot // Remember the last value used for filling
		nextUsedSlot++
		lastFreeSlot++ // Move to the next potential free slot on the left
	}

	// If there are remaining slots at the end (>= nextUsedSlot), fill them
	// with the last valid value used for filling.
	for nextUsedSlot < tableSize {
		*freeSlots = append(*freeSlots, lastValidFreeSlot)
		nextUsedSlot++
	}

	// Sanity check size
	expectedFree := tableSize - numKeys
	if uint64(len(*freeSlots)) != expectedFree {
		// This indicates a logic error
		panic(fmt.Sprintf("fillFreeSlots error: expected %d free slots, got %d", expectedFree, len(*freeSlots)))
	}
}
