// File: internal/builder/internal_single_test.go
// (Create this file if it doesn't exist)
package builder

import (
	"pthashgo/internal/core"
	"pthashgo/internal/util"
	"testing"
)

// Test building a single sub-PHF builder directly from hashes.
func TestBuildFromHashes(t *testing.T) {
	// Simulate data for one partition
	numKeysInPartition := uint64(10000)
	partitionSeed := uint64(9876) // Seed used for partitioning originally (doesn't matter here)
	buildSeed := uint64(112233)   // The seed for THIS sub-build

	// Generate random hashes (as if partitioned) - Use Hash128
	hashes := make([]core.Hash128, numKeysInPartition)
	hasher := core.NewXXHash128Hasher[uint64]() // Use a hasher just to generate hash-like data
	tempKeys := util.DistinctUints64(numKeysInPartition, partitionSeed)
	for i := range hashes {
		hashes[i] = hasher.Hash(tempKeys[i], partitionSeed) // Generate Hash128 values
	}

	// --- Setup Config for Sub-Build ---
	config := core.DefaultBuildConfig()
	config.Alpha = 0.94
	config.Lambda = 4.0 // Typical values
	config.Minimal = true
	config.Search = core.SearchTypeXOR
	config.Verbose = true // Enable logs for this specific test
	config.NumThreads = 1 // Sub-builds are sequential
	config.Seed = buildSeed
	config.NumBuckets = core.ComputeNumBuckets(numKeysInPartition, config.Lambda) // Calculate expected buckets

	// --- Setup Builder ---
	type K = uint64 // Key type isn't used directly by buildFromHashes
	type H = core.XXHash128Hasher[K]
	type B = core.SkewBucketer
	subHasher := core.NewXXHash128Hasher[K]()                                      // Need an instance
	subBucketer := new(B)                                                          // Need an instance, pointer type
	builder := NewInternalMemoryBuilderSinglePHF[K, H, *B](subHasher, subBucketer) // Pass pointer type

	// --- Run the build ---
	timings, err := builder.buildFromHashes(hashes, config)

	// --- Verification ---
	if err != nil {
		if seedErr, ok := err.(core.SeedRuntimeError); ok {
			t.Logf("Sub-build failed with SeedRuntimeError (expected for some inputs): %v", seedErr)
			// Optionally: t.Skipf(...) if we don't want the log noise
		} else {
			t.Fatalf("buildFromHashes failed with unexpected error: %v", err)
		}
	} else {
		// Build succeeded, perform basic checks on the builder state
		t.Logf("Sub-build succeeded. Timings: MapOrd=%v, Search=%v", timings.MappingOrderingMicroseconds, timings.SearchingMicroseconds)
		if builder.NumKeys() != numKeysInPartition {
			t.Errorf("NumKeys mismatch: got %d, want %d", builder.NumKeys(), numKeysInPartition)
		}
		if builder.Seed() != config.Seed {
			t.Errorf("Seed mismatch: got %d, want %d", builder.Seed(), config.Seed)
		}
		if builder.NumBuckets() == 0 || uint64(len(builder.Pilots())) != builder.NumBuckets() {
			t.Errorf("Pilot slice size (%d) doesn't match num buckets (%d)", len(builder.Pilots()), builder.NumBuckets())
		}
		if builder.Taken() == nil || builder.Taken().Size() != builder.TableSize() {
			t.Errorf("Taken bitvector size (%d) doesn't match table size (%d)", builder.Taken().Size(), builder.TableSize())
		}
		// Check free slots only if minimal and successful
		if config.Minimal {
			expectedFree := uint64(0)
			if builder.TableSize() > builder.NumKeys() {
				expectedFree = builder.TableSize() - builder.NumKeys()
			}
			if uint64(len(builder.FreeSlots())) != expectedFree {
				t.Errorf("FreeSlots size mismatch: got %d, want %d", len(builder.FreeSlots()), expectedFree)
			}
		}
	}
}
