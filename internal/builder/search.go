package builder

import (
	"fmt"
	"log"
	"pthashgo/internal/core"
	"pthashgo/internal/util"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const DefaultMaxPilotAttempts uint64 = 50_000_000 // Default limit for pilot search attempts

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
				bucketsIt, taken, pilots, logger, hashedPilotsCache, mTableSize64, tableSize, DefaultMaxPilotAttempts)
		}
	case core.SearchTypeAdd:
		if isParallel {
			log.Printf("Using PARALLEL search (ADD) with %d threads", config.NumThreads)
			// err = searchParallelAdd(...) // TODO: Implement Additive Search Parallel
			return fmt.Errorf("parallel additive search not implemented")
		} else {
			log.Printf("Using SEQUENTIAL search (ADD)")
			err = searchSequentialAdd(numKeys, numBuckets, numNonEmptyBuckets, seed, config,
				bucketsIt, taken, pilots, logger, mTableSize32, tableSize, DefaultMaxPilotAttempts) // Pass m32
		}
	default:
		return fmt.Errorf("unknown search type: %v", config.Search)
	}

	return err
}

func searchSequentialXOR(
	numKeys, numBuckets, numNonEmptyBuckets, seed uint64,
	config *core.BuildConfig,
	bucketsIt *bucketsIteratorT,
	taken *core.BitVectorBuilder, // For sequential, direct access is safe
	pilots PilotsBuffer,
	logger *SearchLogger,
	hashedPilotsCache []uint64,
	mTableSize core.M64,
	tableSize uint64,
	maxAttempts ...uint64, // Optional parameter to override default max attempts
) error {
	maxPilotAttempts := DefaultMaxPilotAttempts
	if len(maxAttempts) > 0 {
		maxPilotAttempts = maxAttempts[0]
	}

	positions := make([]uint64, 0, core.MaxBucketSize) // Preallocate for max possible size
	processedBuckets := uint64(0)
	searchStartTime := time.Now()
	util.Log(config.Verbose, "Starting searchSequentialXOR: numKeys=%d, numBuckets=%d, tableSize=%d",
		numKeys, numBuckets, tableSize)

	for bucketsIt.HasNext() {
		bucket := bucketsIt.Next()
		bucketSize := bucket.Size()
		if bucketSize == 0 {
			util.Log(config.Verbose, "WARNING: Empty bucket encountered (should not happen)")
			continue
		}
		bucketID := bucket.ID()
		payloads := bucket.Payloads()

		// Timing and initial logging for the bucket
		pilotSearchStartTime := time.Now()
		if config.Verbose {
			log.Printf("Processing bucket %d (ID: %d) of %d, size: %d (%v elapsed since search start)",
				processedBuckets+1, bucketID, numNonEmptyBuckets, bucketSize, time.Since(searchStartTime))
			// Optionally log payloads only if size is small or debugging intensely
			// if bucketSize < 10 { log.Printf("  Payloads: %v", payloads) }
		}

		// Deep Debugging Control
		enableDeepDebug := false
		const deepDebugPilotThreshold = 2_000_000 // Log details if search exceeds this many pilots

		foundPilot := false
		for pilot := uint64(0); ; pilot++ { // Continue until pilot found or limit hit

			if pilot >= maxPilotAttempts {
				util.Log(config.Verbose, "ERROR: Pilot search limit (%d) reached for bucket %d (size %d)", maxPilotAttempts, bucketID, bucketSize)
				// Log the problematic payloads when the limit is hit
				log.Printf("FAILING BUCKET %d PAYLOADS: %v", bucketID, payloads)
				return core.SeedRuntimeError{Msg: fmt.Sprintf("pilot search limit reached for bucket %d", bucketID)}
			}

			// Activate deep debug if search is taking long
			if config.Verbose && !enableDeepDebug && pilot == deepDebugPilotThreshold {
				log.Printf("DEEP DEBUG [XOR]: Starting detailed logging for slow bucket %d (size %d)", bucketID, bucketSize)
				enableDeepDebug = true
			}

			// Occasional progress update for long searches (less frequent than deep debug)
			if config.Verbose && !enableDeepDebug && pilot > 0 && pilot%10_000_000 == 0 {
				log.Printf("INFO [XOR]: Still searching bucket %d (size %d) - tried %dM pilots over %v",
					bucketID, bucketSize, pilot/1000000, time.Since(pilotSearchStartTime))
			}

			// --- Calculate Pilot Hash ---
			hashedPilot := uint64(0)
			if pilot < searchCacheSize {
				hashedPilot = hashedPilotsCache[pilot]
			} else {
				hashedPilot = core.DefaultHash64(pilot, seed) // Use the main config seed
			}

			// --- Check Collisions for this Pilot ---
			positions = positions[:0] // Reset positions slice for this attempt
			externalCollision := false
			internalCollision := false
			var collisionDetails strings.Builder // Buffer for deep debug output

			if enableDeepDebug {
				fmt.Fprintf(&collisionDetails, "  Bucket %d / Pilot %d (hashed=%d):\n", bucketID, pilot, hashedPilot)
			}

			// 1. Check for external collisions (vs 'taken') and collect positions
			for idx, pld := range payloads {
				hash := pld // Payload is hash.Second()
				p := core.FastModU64(hash^hashedPilot, mTableSize, tableSize)
				positions = append(positions, p) // Always collect for internal check

				isTaken := taken.Get(p)
				if enableDeepDebug {
					fmt.Fprintf(&collisionDetails, "    Payload[%2d]=%d -> XOR=%d -> Pos=%-5d -> Taken=%t\n",
						idx, pld, hash^hashedPilot, p, isTaken)
				}
				if isTaken {
					externalCollision = true
					if enableDeepDebug {
						fmt.Fprintf(&collisionDetails, "    -> External collision at pos %d\n", p)
						// Don't break in deep debug, collect all info
					} else {
						break // Optimization: Stop checking payloads if external collision found
					}
				}
			}

			// 2. Check for internal collisions (within the bucket for this pilot)
			//    Only necessary if no external collisions were found (unless deep debugging)
			if !externalCollision || enableDeepDebug {
				if len(positions) > 1 { // Only need to sort if more than one element
					sort.Slice(positions, func(i, j int) bool { return positions[i] < positions[j] })
					for i := 1; i < len(positions); i++ {
						if positions[i] == positions[i-1] {
							internalCollision = true
							if enableDeepDebug {
								fmt.Fprintf(&collisionDetails, "    -> Internal collision at pos %d\n", positions[i])
								// Don't break in deep debug
							} else {
								break // Optimization: Stop checking if internal collision found
							}
						}
					}
				}
			}

			// --- Log deep debug info if enabled ---
			if enableDeepDebug {
				if !externalCollision && !internalCollision {
					fmt.Fprint(&collisionDetails, "    -> OK: No collisions found\n")
				}
				log.Print(collisionDetails.String()) // Print collected details for this pilot
			}

			// --- Decide next step ---
			if externalCollision || internalCollision {
				continue // Collision found, try the next pilot
			}

			// --- Pilot Found! ---
			foundPilot = true
			pilots.EmplaceBack(bucketID, pilot) // Store the successful pilot
			for _, p := range positions {
				taken.Set(p) // Mark slots as taken *after* success confirmation
			}
			logger.Update(processedBuckets, bucketSize) // Update progress

			if config.Verbose { // Log success details, especially if it was slow
				searchDuration := time.Since(pilotSearchStartTime)
				if searchDuration > 50*time.Millisecond || pilot >= deepDebugPilotThreshold {
					log.Printf("INFO [XOR]: Found pilot %d for bucket %d (size %d) after %v",
						pilot, bucketID, bucketSize, searchDuration)
				}
			}
			break // Exit the pilot search loop for this bucket
		} // End pilot search loop

		if !foundPilot {
			// This path should now only be reachable if maxPilotAttempts is hit
			// The error is returned inside the loop upon hitting the limit
			// This panic is a safeguard against unexpected loop termination.
			panic(fmt.Sprintf("Internal Error: Pilot search loop for bucket %d exited without finding pilot or hitting limit", bucketID))
		}
		processedBuckets++
	} // End bucket iteration

	if processedBuckets != numNonEmptyBuckets {
		util.Log(config.Verbose, "Warning (XOR): Processed %d buckets, expected %d non-empty", processedBuckets, numNonEmptyBuckets)
	}

	return nil // Success
}

// searchParallelXOR finds pilots in parallel using XOR displacement.
// WARNING: This implementation attempts to mimic the complex C++ locking/retry strategy.
// It might be prone to deadlocks or performance issues in Go.
// A channel-based dispatcher might be a more idiomatic (and potentially safer) Go approach.
func searchParallelXOR(
	numKeys, numBuckets, numNonEmptyBuckets, seed uint64,
	config *core.BuildConfig,
	bucketsIt *bucketsIteratorT,
	taken *core.BitVectorBuilder, // Now internally thread-safe
	pilots PilotsBuffer, // Requires concurrent-safe EmplaceBack
	logger *SearchLogger,
	hashedPilotsCache []uint64,
	mTableSize core.M64,
	tableSize uint64,
) error {
	numThreads := config.NumThreads

	log.Printf("RACE-DEBUG: Starting searchParallelXOR with %d threads", numThreads)

	// Keep mutexes for other shared resources
	var pilotsMu sync.Mutex // Assuming PilotsBuffer is not inherently safe
	var loggerMu sync.Mutex // Mutex for logger to fix race

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

			// Simplified pilot search logic - no need for outer retry loop
			foundPilotForBucket := false
			
			for currentPilot := uint64(0); ; currentPilot++ {
				hashedPilot := uint64(0)
				if currentPilot < searchCacheSize {
					hashedPilot = hashedPilotsCache[currentPilot]
				} else {
					hashedPilot = core.DefaultHash64(currentPilot, seed)
				}

				positions = positions[:0] // Clear thread-local buffer
				optimisticCheckFailed := false

				for _, pld := range payloads {
					hash := pld
					p := core.FastModU64(hash^hashedPilot, mTableSize, tableSize)
					
					// Optimistic check - thread-safe now
					if taken.Get(p) {
						optimisticCheckFailed = true
						break
					}
					positions = append(positions, p)
				}

				if optimisticCheckFailed {
					continue // Try next pilot value
				}

				// Check in-bucket collisions
				sort.Slice(positions, func(i, j int) bool { return positions[i] < positions[j] })
				inBucketCollision := false
				for i := 1; i < len(positions); i++ {
					if positions[i] == positions[i-1] {
						inBucketCollision = true
						break
					}
				}
				if inBucketCollision {
					continue // Try next pilot value
				}

				// Pilot found - directly set bits (thread-safe now)
				for _, p := range positions {
					taken.Set(p)
				}

				// Store the pilot
				pilotsEmplaceBack(bucketID, currentPilot)

				// Update progress
				loggerMu.Lock()
				logger.Update(localBucketIdx, bucketSize)
				loggerMu.Unlock()

				foundPilotForBucket = true
				break // Success for this bucket
			}

			if !foundPilotForBucket {
				log.Printf("Worker: Thread %d couldn't find pilot for bucket %d - this shouldn't happen", tid, bucketID)
				// This would only happen if we hit limitations not caught in the search loop
			}
		}
	}

	// Start workers
	log.Printf("Starting %d worker goroutines for parallel search", numThreads)
	for i := 0; i < numThreads; i++ {
		go worker(i)
	}

	// Wait for all workers to finish
	wg.Wait()

	// Check if all buckets were processed
	finalIdx := nextBucketIdx.Load()
	if finalIdx < numNonEmptyBuckets {
		util.Log(config.Verbose, "Warning: Parallel search finished, processed ~%d buckets, expected %d. Potential contention or error.", finalIdx, numNonEmptyBuckets)
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
	maxAttempts ...uint64, // Optional parameter to override default max attempts
) error {
	maxPilotAttempts := DefaultMaxPilotAttempts
	if len(maxAttempts) > 0 {
		maxPilotAttempts = maxAttempts[0]
	}

	positions := make([]uint64, 0, core.MaxBucketSize)
	d32 := uint32(tableSize) // Precompute for modulo

	processedBuckets := uint64(0)
	searchStartTime := time.Now()
	log.Printf("Starting searchSequentialAdd: numKeys=%d, numBuckets=%d, tableSize=%d",
		numKeys, numBuckets, tableSize)

	for bucketsIt.HasNext() {
		bucket := bucketsIt.Next()
		bucketSize := bucket.Size()
		if bucketSize == 0 {
			continue
		} // Should not happen
		bucketID := bucket.ID()
		payloads := bucket.Payloads() // These are hash.Second() values

		bucketStartTime := time.Now()
		log.Printf("Processing bucket %d of %d (%v elapsed since search start)",
			processedBuckets+1, numNonEmptyBuckets, time.Since(searchStartTime))

		foundPilot := false
		pilotSearchStartTime := time.Now()

		for pilot := uint64(0); pilot < maxPilotAttempts; pilot++ {
			// Log progress every 100,000 iterations
			if pilot > 0 && pilot%100000 == 0 {
				log.Printf("WARNING: Still searching bucket %d (size %d) - tried %d pilots over %v",
					bucketID, bucketSize, pilot, time.Since(pilotSearchStartTime))
				log.Printf("Payloads: %v", payloads)
			}

			s := core.FastDivU32(uint32(pilot), m64) // Calculate s = pilot / tableSize (approx)

			positions = positions[:0] // Clear slice
			collisionFound := false

			// For extensive debugging when stuck
			if pilot > 0 && pilot%1000000 == 0 { // Log details once per million attempts
				log.Printf("Detail for bucket %d (pilot=%d): tableSize=%d", bucketID, pilot, tableSize)
			}

			for i, pld := range payloads {
				hashSecond := pld
				// Calculate position: fastmod_u32(((mix(h2+s)) >> 33) + pilot, M, d)
				valToMix := hashSecond + uint64(s)
				mixedHash := core.Mix64(valToMix)
				term1 := mixedHash >> 33
				sum := term1 + pilot // Add full pilot
				p := uint64(core.FastModU32(uint32(sum), m64, d32))

				// Log detailed collision info occasionally
				if pilot > 0 && pilot%1000000 == 0 {
					log.Printf("  Payload[%d]=%d, MixShift=%d, Sum=%d, Position=%d, Taken=%v",
						i, pld, term1, sum, p, taken.Get(p))
				}

				// Check collision with taken bits
				// RACE-DEBUG: This Get() call is not protected by a mutex!
				isTaken := taken.Get(p)
				if isTaken {
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
		} // End pilot search loop

		if !foundPilot {
			// Check if pilot limit was reached (should be unreachable with above condition)
			return core.SeedRuntimeError{Msg: fmt.Sprintf("could not find pilot for bucket %d (ADD) after %d attempts", bucketID, maxPilotAttempts)}
		}
		processedBuckets++
	} // End bucket iteration

	if processedBuckets != numNonEmptyBuckets {
		util.Log(config.Verbose, "Warning (ADD): Processed %d buckets, expected %d non-empty", processedBuckets, numNonEmptyBuckets)
	}
	return nil
}
