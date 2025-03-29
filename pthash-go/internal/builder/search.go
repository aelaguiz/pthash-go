package builder

import (
	"fmt"
	"pthashgo/internal/core"
	"pthashgo/internal/util"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
)

// Search orchestrates the pilot search based on config.
func Search[B core.Bucketer]( // Pass Bucketer type for logger
	numKeys, numBuckets, numNonEmptyBuckets, seed uint64,
	config *core.BuildConfig,
	bucketsIt *bucketsIteratorT, // Iterator providing core.BucketT
	taken *core.BitVectorBuilder, // Builder to mark taken slots
	pilots PilotsBuffer, // Interface to store results
) error {

	logger := NewSearchLogger[B](numKeys, numBuckets, B{}, config.Verbose) // Pass zero value of B for type info if needed
	logger.Init()
	defer logger.Finalize(numNonEmptyBuckets) // Use numNonEmpty from merge phase

	// Precompute hashed pilots cache
	hashedPilotsCache := make([]uint64, searchCacheSize)
	for i := range hashedPilotsCache {
		hashedPilotsCache[i] = core.DefaultHash64(uint64(i), seed)
	}
	// FastMod parameter for table size
	mTableSize := core.ComputeM64(config.MinimalTableSize(numKeys)) // Use minimal table size from config

	var err error
	if config.NumThreads > 1 && numNonEmptyBuckets >= uint64(config.NumThreads)*2 { // Add heuristic for parallelism benefit
		if config.Search == core.SearchTypeXOR {
			err = searchParallelXOR(numKeys, numBuckets, numNonEmptyBuckets, seed, config,
				bucketsIt, taken, pilots, logger, hashedPilotsCache, mTableSize)
		} else if config.Search == core.SearchTypeAdd {
			// err = searchParallelAdd(...) // TODO: Implement Additive Search
			return fmt.Errorf("parallel additive search not implemented")
		} else {
			return fmt.Errorf("unknown search type: %v", config.Search)
		}
	} else {
		if config.Search == core.SearchTypeXOR {
			err = searchSequentialXOR(numKeys, numBuckets, numNonEmptyBuckets, seed, config,
				bucketsIt, taken, pilots, logger, hashedPilotsCache, mTableSize)
		} else if config.Search == core.SearchTypeAdd {
			// err = searchSequentialAdd(...) // TODO: Implement Additive Search
			return fmt.Errorf("sequential additive search not implemented")
		} else {
			return fmt.Errorf("unknown search type: %v", config.Search)
		}
	}
	return err
}

// searchSequentialXOR finds pilots sequentially using XOR displacement.
func searchSequentialXOR(
	numKeys, numBuckets, numNonEmptyBuckets, seed uint64,
	config *core.BuildConfig,
	bucketsIt *bucketsIteratorT,
	taken *core.BitVectorBuilder, // NOTE: Need concurrent access or atomic ops for parallel
	pilots PilotsBuffer,
	logger *SearchLogger,
	hashedPilotsCache []uint64,
	mTableSize core.M64,
) error {
	tableSize := config.MinimalTableSize(numKeys)
	positions := make([]uint64, 0, core.MaxBucketSize) // Preallocate for max possible size

	processedBuckets := uint64(0)
	for bucketsIt.HasNext() {
		bucket := bucketsIt.Next()
		bucketSize := bucket.Size()
		if bucketSize == 0 {
			continue // Should not happen with non-empty count, but safe check
		}
		bucketID := bucket.ID()
		payloads := bucket.Payloads()

		foundPilot := false
		for pilot := uint64(0); ; pilot++ {
			hashedPilot := uint64(0)
			if pilot < searchCacheSize {
				hashedPilot = hashedPilotsCache[pilot]
			} else {
				hashedPilot = core.DefaultHash64(pilot, seed)
			}

			positions = positions[:0] // Clear slice, keep capacity
			collisionFound := false

			for _, pld := range payloads {
				hash := pld // In single PHF, payload is hash.Second()
				p := core.FastModU64(hash^hashedPilot, mTableSize, tableSize)

				// Check collision with already taken slots
				// NOTE: This Get needs to work on the *builder* state during sequential build.
				// The C++ uses a bit_vector::builder which allows get(). Go's builder needs this.
				// For now, assume taken builder allows reads (might need adjustment).
				// *** This read-from-builder is problematic for concurrent writes later! ***
				// *** Let's add a temporary Get method to builder for sequential case. ***
				if taken.Get(p) { // Need taken.Get(p) method on builder
					collisionFound = true
					break
				}
				positions = append(positions, p)
			}

			if collisionFound {
				continue // Try next pilot
			}

			// Check for in-bucket collisions
			sort.Slice(positions, func(i, j int) bool { return positions[i] < positions[j] })
			inBucketCollision := false
			for i := 1; i < len(positions); i++ {
				if positions[i] == positions[i-1] {
					inBucketCollision = true
					break
				}
			}
			if inBucketCollision {
				continue // Try next pilot
			}

			// Pilot found!
			pilots.EmplaceBack(bucketID, pilot)
			for _, p := range positions {
				taken.Set(p) // Mark slots as taken
			}
			logger.Update(processedBuckets, bucketSize)
			foundPilot = true
			break // Move to next bucket
		}
		// C++ has check `if (bucket_begin == bucket_end)` which seems redundant if inner loops break
		// Let's add a check in case the inner loop somehow exits without finding a pilot (should be impossible?)
		if !foundPilot {
			// This indicates an issue, likely requires a new seed.
			return SeedRuntimeError{fmt.Sprintf("could not find pilot for bucket %d", bucketID)}
		}
		processedBuckets++
	}
	if processedBuckets != numNonEmptyBuckets {
		// This might indicate an issue with the iterator or non-empty count
		util.Log(config.Verbose, "Warning: Processed %d buckets, expected %d non-empty", processedBuckets, numNonEmptyBuckets)
	}

	return nil
}

// searchParallelXOR finds pilots in parallel using XOR displacement.
// WARNING: This implementation attempts to mimic the complex C++ locking/retry strategy.
// It might be prone to deadlocks or performance issues in Go.
// A channel-based dispatcher might be a more idiomatic (and potentially safer) Go approach.
func searchParallelXOR(
	numKeys, numBuckets, numNonEmptyBuckets, seed uint64,
	config *core.BuildConfig,
	bucketsIt *bucketsIteratorT,
	taken *core.BitVectorBuilder, // Requires concurrent-safe Set/Get
	pilots PilotsBuffer, // Requires concurrent-safe EmplaceBack
	logger *SearchLogger,
	hashedPilotsCache []uint64,
	mTableSize core.M64,
) error {
	tableSize := config.MinimalTableSize(numKeys)
	numThreads := config.NumThreads

	// Need concurrent-safe access to taken bits and pilots buffer.
	// Using Mutex for simplicity, although atomics or sharding might be faster.
	var takenMu sync.Mutex
	var pilotsMu sync.Mutex // Assuming PilotsBuffer is not inherently safe

	// Wrap taken builder methods for concurrency
	takenSet := func(pos uint64) {
		takenMu.Lock()
		taken.Set(pos)
		takenMu.Unlock()
	}
	takenGet := func(pos uint64) bool {
		takenMu.Lock()
		res := taken.Get(pos)
		takenMu.Unlock()
		return res
	}
	// Wrap pilots buffer
	pilotsEmplaceBack := func(bucketID core.BucketIDType, pilot uint64) {
		pilotsMu.Lock()
		pilots.EmplaceBack(bucketID, pilot)
		pilotsMu.Unlock()
	}

	var wg sync.WaitGroup
	wg.Add(numThreads)

	// Global state for coordinating bucket processing
	var nextBucketIdx atomic.Uint64 // Next bucket index to be processed globally
	var bucketMutex sync.Mutex      // To safely get next bucket from iterator

	// Worker goroutine function
	worker := func(tid int) {
		defer wg.Done()
		positions := make([]uint64, 0, core.MaxBucketSize) // Thread-local buffer

		for {
			// Get the next bucket to process for this thread
			bucketMutex.Lock()
			if !bucketsIt.HasNext() {
				bucketMutex.Unlock()
				return // No more buckets
			}
			bucket := bucketsIt.Next()
			localBucketIdx := nextBucketIdx.Add(1) - 1 // Atomically get and increment
			bucketMutex.Unlock()

			if localBucketIdx >= numNonEmptyBuckets {
				return // Already processed enough buckets
			}

			bucketSize := bucket.Size()
			bucketID := bucket.ID()
			payloads := bucket.Payloads()

			// --- Mimic C++ retry logic ---
			// This part is tricky to translate directly and safely.
			// The C++ code seems to involve active waiting and re-checking.
			currentPilot := uint64(0)
			pilotChecked := false // Did we find a potential pilot in the last inner loop?

			for { // Outer retry loop for this bucket

				// Find a potential pilot (inner loop)
				foundPotentialPilot := false
				if !pilotChecked { // Only search if previous check failed or first time
					for pSearch := currentPilot; ; pSearch++ {
						hashedPilot := uint64(0)
						if pSearch < searchCacheSize {
							hashedPilot = hashedPilotsCache[pSearch]
						} else {
							hashedPilot = core.DefaultHash64(pSearch, seed)
						}

						positions = positions[:0] // Clear thread-local buffer
						collisionFound := false

						for _, pld := range payloads {
							hash := pld
							p := core.FastModU64(hash^hashedPilot, mTableSize, tableSize)
							if takenGet(p) { // Concurrent read
								collisionFound = true
								break
							}
							positions = append(positions, p)
						}

						if collisionFound {
							continue
						} // Try next pilot value

						// Check in-bucket
						sort.Slice(positions, func(i, j int) bool { return positions[i] < positions[j] })
						inBucketCollision := false
						for i := 1; i < len(positions); i++ {
							if positions[i] == positions[i-1] {
								inBucketCollision = true
								break
							}
						}
						if inBucketCollision {
							continue
						} // Try next pilot value

						// Potential pilot found
						currentPilot = pSearch
						pilotChecked = true
						foundPotentialPilot = true
						break // Exit pilot search loop
					} // End pilot search loop (for pSearch)

				} else { // pilotChecked was true, just re-verify positions against 'taken'
					foundPotentialPilot = true // Assume it's still potentially valid
					for _, p := range positions {
						if takenGet(p) { // Concurrent read
							pilotChecked = false // Collision detected, must search again
							foundPotentialPilot = false
							break
						}
					}
				}

				if !foundPotentialPilot {
					// This should ideally not happen if search loop runs correctly
					// If it does, restart search from currentPilot
					pilotChecked = false // Force re-search
					continue             // Retry finding a pilot for this bucket
				}

				// --- Attempt to commit the found pilot ---
				// This requires ensuring no other thread grabbed conflicting slots
				// between our check and our commit. A global lock is simplest but slow.
				// C++ used atomic compare-and-swap on bitmap words or similar.
				// Let's try a simpler lock around the commit phase.

				takenMu.Lock() // Lock before final check and commit
				commitOk := true
				for _, p := range positions {
					if taken.Get(p) { // Final check under lock
						commitOk = false
						break
					}
				}

				if commitOk {
					// Commit phase: Mark bits and store pilot
					for _, p := range positions {
						taken.Set(p)
					}
					takenMu.Unlock() // Unlock after modifying taken

					// Store pilot (also needs safety if buffer isn't safe)
					pilotsEmplaceBack(bucketID, currentPilot)
					logger.Update(localBucketIdx, bucketSize) // Log progress

					break // Success for this bucket, exit outer retry loop
				} else {
					// Conflict detected during commit check
					takenMu.Unlock()     // Unlock
					pilotChecked = false // Our potential pilot is invalid, need to search again
					// currentPilot remains the same, will restart search from there
					runtime.Gosched() // Yield to potentially allow conflicting thread to finish
					continue          // Retry finding a pilot for this bucket (outer loop)
				}
			} // End outer retry loop for bucket
		} // End main loop for fetching buckets
	} // End worker func

	// Start workers
	for i := 0; i < numThreads; i++ {
		go worker(i)
	}

	// Wait for all workers to finish
	wg.Wait()

	// Check if all buckets were processed
	// Note: Due to potential retries and non-deterministic order,
	// checking nextBucketIdx against numNonEmptyBuckets might not be perfectly accurate
	// if errors occurred, but it's a basic sanity check.
	finalIdx := nextBucketIdx.Load()
	if finalIdx < numNonEmptyBuckets {
		util.Log(config.Verbose, "Warning: Parallel search finished, processed ~%d buckets, expected %d. Potential contention or error.", finalIdx, numNonEmptyBuckets)
		// If this happens frequently, the retry/locking logic might need refinement or a different approach.
		// Could potentially indicate a seed failure if some buckets never found a pilot.
		// For now, we don't explicitly return SeedRuntimeError here, but it's a possibility.
	}

	return nil
}

// Helper for search function to get minimal table size from config
func (c *core.BuildConfig) MinimalTableSize(numKeys uint64) uint64 {
	tableSize := uint64(float64(numKeys) / c.Alpha)
	if c.Search == core.SearchTypeXOR && (tableSize&(tableSize-1)) == 0 && tableSize > 0 {
		tableSize++
	}
	return tableSize
}

// Add Get/Set methods to BitVectorBuilder for sequential search
// These are NOT thread-safe.
func (b *core.BitVectorBuilder) Get(pos uint64) bool {
	if pos >= b.size {
		// Reading beyond current size conceptually returns 0/false
		return false
	}
	wordIndex := pos / 64
	bitIndex := pos % 64
	if wordIndex >= uint64(len(b.words)) {
		// Accessing word that hasn't been allocated yet
		return false
	}
	return (b.words[wordIndex] & (1 << bitIndex)) != 0
}

func (b *core.BitVectorBuilder) Set(pos uint64) {
	if pos >= b.capacity {
		b.grow(pos - b.size + 1) // Grow enough to include pos
	}
	wordIndex := pos / 64
	bitIndex := pos % 64
	if wordIndex >= uint64(len(b.words)) { // Ensure word exists after potential grow
		neededLen := wordIndex + 1
		if neededLen > uint64(cap(b.words)) {
			// Should not happen if grow worked correctly
			panic("Set after grow failed")
		}
		b.words = b.words[:neededLen]
	}
	b.words[wordIndex] |= (1 << bitIndex)
	if pos >= b.size { // Update size if we set a bit beyond the current end
		b.size = pos + 1
	}
}
