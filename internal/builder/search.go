package builder

import (
	"fmt"
	"log"
	"pthashgo/internal/core"
	"pthashgo/internal/util"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

const maxPilotAttempts = 50_000_000 // TEMPORARY DEBUG LIMIT

// Search orchestrates the pilot search based on config.
func Search[B core.Bucketer]( // Pass Bucketer type for logger
	numKeys, numBuckets, numNonEmptyBuckets, seed uint64,
	config *core.BuildConfig,
	bucketsIt *bucketsIteratorT, // Iterator providing core.BucketT
	taken *core.BitVectorBuilder, // Builder to mark taken slots
	pilots PilotsBuffer, // Interface to store results
	bucketer B, // Pass the actual bucketer instance
) error {

	logger := NewSearchLogger[B](numKeys, numBuckets, bucketer, config.Verbose)
	logger.Init()
	defer logger.Finalize(numNonEmptyBuckets) // Use numNonEmpty from merge phase

	// Precompute hashed pilots cache (only needed for XOR)
	var hashedPilotsCache []uint64
	if config.Search == core.SearchTypeXOR {
		hashedPilotsCache = make([]uint64, searchCacheSize)
		for i := range hashedPilotsCache {
			hashedPilotsCache[i] = core.DefaultHash64(uint64(i), seed)
		}
	}

	// FastMod parameter(s) for table size
	tableSize := core.MinimalTableSize(numKeys, config.Alpha, config.Search)
	mTableSize64 := core.ComputeM64(tableSize)         // For XOR
	mTableSize32 := core.ComputeM32(uint32(tableSize)) // For ADD

	var err error
	log.Printf("Search decision: NumThreads=%d, NumNonEmptyBuckets=%d, Required=%d",
		config.NumThreads, numNonEmptyBuckets, uint64(config.NumThreads)*2)

	// --- Use appropriate search function based on config ---
	isParallel := config.NumThreads > 1 && numNonEmptyBuckets >= uint64(config.NumThreads)*2

	switch config.Search {
	case core.SearchTypeXOR:
		if isParallel {
			log.Printf("Using PARALLEL search (XOR) with %d threads", config.NumThreads)
			err = searchParallelXOR(numKeys, numBuckets, numNonEmptyBuckets, seed, config,
				bucketsIt, taken, pilots, logger, hashedPilotsCache, mTableSize64, tableSize)
		} else {
			log.Printf("Using SEQUENTIAL search (XOR)")
			err = searchSequentialXOR(numKeys, numBuckets, numNonEmptyBuckets, seed, config,
				bucketsIt, taken, pilots, logger, hashedPilotsCache, mTableSize64, tableSize)
		}
	case core.SearchTypeAdd:
		if isParallel {
			log.Printf("Using PARALLEL search (ADD) with %d threads", config.NumThreads)
			// err = searchParallelAdd(...) // TODO: Implement Additive Search Parallel
			return fmt.Errorf("parallel additive search not implemented")
		} else {
			log.Printf("Using SEQUENTIAL search (ADD)")
			err = searchSequentialAdd(numKeys, numBuckets, numNonEmptyBuckets, seed, config,
				bucketsIt, taken, pilots, logger, mTableSize32, tableSize) // Pass m32
		}
	default:
		return fmt.Errorf("unknown search type: %v", config.Search)
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
	tableSize uint64,
) error {
	positions := make([]uint64, 0, core.MaxBucketSize) // Preallocate for max possible size

	processedBuckets := uint64(0)
	searchStartTime := time.Now()
	log.Printf("Starting searchSequentialXOR: numKeys=%d, numBuckets=%d, tableSize=%d",
		numKeys, numBuckets, tableSize)

	for bucketsIt.HasNext() {
		bucketStartTime := time.Now()
		log.Printf("Processing bucket %d of %d (%v elapsed since search start)",
			processedBuckets+1, numNonEmptyBuckets, time.Since(searchStartTime))

		bucket := bucketsIt.Next()
		bucketSize := bucket.Size()
		if bucketSize == 0 {
			log.Printf("WARNING: Empty bucket encountered (should not happen)")
			continue // Should not happen with non-empty count, but safe check
		}
		bucketID := bucket.ID()
		payloads := bucket.Payloads()
		log.Printf("Bucket %d: size=%d, payloads=%v", bucketID, bucketSize, payloads)

		foundPilot := false
		pilotSearchStartTime := time.Now()
		for pilot := uint64(0); ; pilot++ {
			// Log progress every 100,000 iterations
			if pilot > 0 && pilot%100000 == 0 {
				log.Printf("WARNING: Still searching bucket %d (size %d) - tried %d pilots over %v",
					bucketID, bucketSize, pilot, time.Since(pilotSearchStartTime))
				log.Printf("Payloads: %v", payloads)
			}

			hashedPilot := uint64(0)
			if pilot < searchCacheSize {
				hashedPilot = hashedPilotsCache[pilot]
			} else {
				hashedPilot = core.DefaultHash64(pilot, seed)
			}

			positions = positions[:0] // Clear slice, keep capacity
			collisionFound := false

			// For extensive debugging when stuck
			if pilot > 0 && pilot%1000000 == 0 { // Log details once per million attempts
				log.Printf("Detail for bucket %d (pilot=%d): tableSize=%d", bucketID, pilot, tableSize)
			}

			for i, pld := range payloads {
				hash := pld // In single PHF, payload is hash.Second()
				p := core.FastModU64(hash^hashedPilot, mTableSize, tableSize)

				// Log detailed collision info occasionally
				if pilot > 0 && pilot%1000000 == 0 {
					log.Printf("  Payload[%d]=%d, XOR=%d, Position=%d, Taken=%v",
						i, pld, hash^hashedPilot, p, taken.Get(p))
				}

				// Check collision with already taken slots
				if taken.Get(p) {
					if pilot > 0 && pilot%1000000 == 0 {
						log.Printf("  Collision at position %d for payload %d", p, pld)
					}
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
					if pilot > 0 && pilot%1000000 == 0 {
						log.Printf("  In-bucket collision: position %d appears multiple times", positions[i])
					}
					inBucketCollision = true
					break
				}
			}
			if inBucketCollision {
				continue // Try next pilot
			}

			// Pilot found!
			pilots.EmplaceBack(bucketID, pilot)
			log.Printf("Found pilot %d for bucket %d after %v",
				pilot, bucketID, time.Since(bucketStartTime))

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
			return core.SeedRuntimeError{Msg: fmt.Sprintf("could not find pilot for bucket %d", bucketID)}
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
	tableSize uint64,
) error {
	numThreads := config.NumThreads

	// Need concurrent-safe access to taken bits and pilots buffer.
	// Using Mutex for simplicity, although atomics or sharding might be faster.
	var takenMu sync.Mutex
	var pilotsMu sync.Mutex // Assuming PilotsBuffer is not inherently safe

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
		log.Printf("Worker thread %d started", tid)
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
							// Optimistic check without lock - might be stale but that's OK
							// Final verification will happen under lock
							if taken.Get(p) {
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
						// Optimistic check without lock - might be stale but that's OK
						if taken.Get(p) {
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
						taken.Set(p) // Direct call under lock
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
	log.Printf("Starting %d worker goroutines for parallel search", numThreads)
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
func searchSequentialAdd(
	numKeys, numBuckets, numNonEmptyBuckets, seed uint64,
	config *core.BuildConfig,
	bucketsIt *bucketsIteratorT,
	taken *core.BitVectorBuilder, // NOTE: Not concurrent safe
	pilots PilotsBuffer,
	logger *SearchLogger,
	// No hashedPilotsCache needed for additive
	// Needs m64 and tableSize for fastmod32
	m64 core.M32, // Precomputed M for fastmod32
	tableSize uint64, // Needed for d parameter
) error {
	positions := make([]uint64, 0, core.MaxBucketSize)
	d32 := uint32(tableSize) // Precompute for modulo

	processedBuckets := uint64(0)

	for bucketsIt.HasNext() {
		bucket := bucketsIt.Next()
		bucketSize := bucket.Size()
		if bucketSize == 0 {
			continue
		} // Should not happen
		bucketID := bucket.ID()
		payloads := bucket.Payloads() // These are hash.Second() values

		foundPilot := false
		for pilot := uint64(0); ; pilot++ { // Infinite loop until pilot found (or seed error)

			s := core.FastDivU32(uint32(pilot), m64) // Calculate s = pilot / tableSize (approx)

			positions = positions[:0] // Clear slice
			collisionFound := false

			for _, pld := range payloads {
				hashSecond := pld
				// Calculate position: fastmod_u32(((mix(h2+s)) >> 33) + pilot, M, d)
				valToMix := hashSecond + uint64(s)
				mixedHash := core.Mix64(valToMix)
				term1 := mixedHash >> 33
				sum := term1 + pilot // Add full pilot
				p := uint64(core.FastModU32(uint32(sum), m64, d32))

				// Check collision with taken bits
				if taken.Get(p) {
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
		} // End pilot search loop

		if !foundPilot {
			return core.SeedRuntimeError{Msg: fmt.Sprintf("could not find pilot for bucket %d (ADD)", bucketID)}
		}
		processedBuckets++
	} // End bucket iteration

	if processedBuckets != numNonEmptyBuckets {
		util.Log(config.Verbose, "Warning (ADD): Processed %d buckets, expected %d non-empty", processedBuckets, numNonEmptyBuckets)
	}
	return nil
}
