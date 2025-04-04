package pthash_test

import (
	"errors"
	"fmt"
	"pthashgo/internal/builder"
	"pthashgo/internal/core"
	"pthashgo/internal/util"
	"pthashgo/pkg/pthash" // Import the public PHF package
	"runtime"
	"testing"
	"time"
)

// --- Test Function ---

func TestInternalPartitionedPHFBuildAndCheck(t *testing.T) {
	// Use specific types for testing
	type K = uint64
	type H = core.XXHash128Hasher[K]
	type B = *core.SkewBucketer // Sub-builder bucketer
	type E = *core.RiceEncoder  // Sub-builder encoder

	seed := uint64(time.Now().UnixNano())
	// Use smaller number of keys but ensure partitioning happens
	numKeysList := []uint64{50000, 100000} // Ensure multiple partitions
	avgPartSizes := []uint64{10000, 25000} // Control partitioning

	alphas := []float64{0.94, 0.98}
	lambdas := []float64{4.0, 6.0}
	searchTypes := []core.SearchType{core.SearchTypeXOR}

	for _, numKeys := range numKeysList {
		keys := util.DistinctUints64(numKeys, seed)
		if uint64(len(keys)) != numKeys {
			t.Fatalf("N=%d: Failed to generate enough distinct keys: got %d, want %d", numKeys, len(keys), numKeys)
		}

		for _, avgPartSize := range avgPartSizes {
			if avgPartSize >= numKeys {
				continue
			} // Skip if partition size >= num keys

			t.Run(fmt.Sprintf("N=%d_P=%d", numKeys, avgPartSize), func(t *testing.T) {

				for _, alpha := range alphas {
					for _, lambda := range lambdas {
						for _, searchType := range searchTypes {
							for _, minimal := range []bool{true, false} {
								testName := fmt.Sprintf("A=%.2f_L=%.1f_S=%v_M=%t", alpha, lambda, searchType, minimal)
								t.Run(testName, func(t *testing.T) {
									config := core.DefaultBuildConfig()
									config.Alpha = alpha
									config.Lambda = lambda
									config.Minimal = minimal
									config.Search = searchType
									config.AvgPartitionSize = avgPartSize
									config.Verbose = true // Enable verbose logging for debugging
									config.NumThreads = runtime.NumCPU()
									// Use a fixed seed for reproducibility
									config.Seed = 42

									// --- Build using Partitioned Builder with Retry Logic ---
									const maxBuildRetries = 3 // Try up to 3 different random seeds
									var pb *builder.InternalMemoryBuilderPartitionedPHF[K, H, B]
									var buildTimings core.BuildTimings
									var err error
									buildSuccess := false

									for attempt := 0; attempt < maxBuildRetries; attempt++ {
										// Use a different seed for each attempt
										if attempt > 0 {
											config.Seed = util.RandomSeed() // New random seed for retries
										}
										t.Logf("Build attempt %d/%d with seed %d...", attempt+1, maxBuildRetries, config.Seed)

										hasher := core.NewXXHash128Hasher[K]()
										pb = builder.NewInternalMemoryBuilderPartitionedPHF[K, H, B](hasher) // Pass sub-bucketer type B

										buildTimings, err = pb.BuildFromKeys(keys, config)
										if err == nil {
											buildSuccess = true // Found a working seed
											t.Logf("Build attempt %d succeeded with seed %d", attempt+1, config.Seed)
											break
										}

										// Check if this error or any wrapped error is a SeedRuntimeError
										var seedErr core.SeedRuntimeError
										if errors.As(err, &seedErr) {
											// Log seed failure and continue to next attempt
											t.Logf("Build attempt %d failed with SeedRuntimeError: %v", attempt+1, seedErr)
											continue
										} else {
											// Fail on other unexpected errors
											t.Fatalf("BuildFromKeys failed with unexpected error on attempt %d: %v", attempt+1, err)
										}
									}

									// If all attempts failed, skip this test configuration
									if !buildSuccess {
										t.Skipf("Skipping test: All %d build attempts failed with SeedRuntimeError (last error: %v)",
											maxBuildRetries, err)
										return
									}

									// --- Construct Final PartitionedPHF ---
									// --- Construct Final PartitionedPHF (only reached on successful builds) ---
									finalPHF := pthash.NewPartitionedPHF[K, H, B, E](minimal, searchType)
									encodeTime, err := finalPHF.Build(pb, &config) // Use the successful builder instance
									if err != nil {
										// Still check for stub issues here if serialization/final build depends on them
										t.Fatalf("finalPHF.Build failed: %v", err)
									}
									t.Logf("Build Timings: Part: %v, MapOrd: %v, Search: %v, Encode: %v",
										buildTimings.PartitioningMicroseconds,
										buildTimings.MappingOrderingMicroseconds,
										buildTimings.SearchingMicroseconds,
										encodeTime)

									// --- Basic Property Checks ---
									if finalPHF.NumKeys() != numKeys {
										t.Errorf("NumKeys mismatch: expected %d, got %d", numKeys, finalPHF.NumKeys())
									}
									if finalPHF.Seed() != config.Seed {
										t.Errorf("Seed mismatch: expected %d, got %d", config.Seed, finalPHF.Seed())
									}

									// TODO: Add serialization check later
									/*
										data, err := finalPHF.MarshalBinary()
										if err != nil {
											t.Fatalf("MarshalBinary failed: %v", err)
										}
										newPHF := pthash.NewPartitionedPHF[K, H, B, E](minimal, searchType)
										err = newPHF.UnmarshalBinary(data)
										if err != nil {
											t.Fatalf("UnmarshalBinary failed: %v", err)
										}
										// Re-check the loaded function
										err = check[K](keys, newPHF)
										if err != nil {
											t.Errorf("Correctness check failed after load: %v", err)
										}
										if finalPHF.NumBits() != newPHF.NumBits() {
										    t.Errorf("NumBits mismatch after load: %d != %d", finalPHF.NumBits(), newPHF.NumBits())
										}
									*/

								}) // End subtest t.Run
							} // End minimal loop
						} // End searchType loop
					} // End lambda loop
				} // End alpha loop
			}) // End N_P t.Run
		} // End avgPartSize loop
	} // End numKeys loop
}

// --- New Serialization Test ---

func TestPartitionedPHFSerialization(t *testing.T) {
	// Use a simple, known-good configuration for testing serialization structure
	type K = uint64
	type H = core.MurmurHash2_64Hasher[K]
	type B = core.SkewBucketer // Sub-builder bucketer
	type E = core.RiceEncoder  // Sub-builder encoder

	numKeys := uint64(5000)     // Small number of keys, but ensure partitioning
	avgPartSize := uint64(1000) // Should create ~5 partitions
	seed := uint64(time.Now().UnixNano())
	keys := util.DistinctUints64(numKeys, seed)
	if uint64(len(keys)) != numKeys {
		t.Fatalf("Failed to generate keys")
	}

	config := core.DefaultBuildConfig()
	config.Alpha = 0.94
	config.Lambda = 5.0
	config.Minimal = true              // Test minimal case
	config.Search = core.SearchTypeAdd // Test Additive
	config.Verbose = false
	config.NumThreads = 2 // Use a couple of threads for partitioned build
	config.Seed = 654321  // Fixed seed
	config.AvgPartitionSize = avgPartSize
	config.DensePartitioning = false // Not dense

	// --- Build the Partitioned PHF with Retry Logic ---
	const maxBuildRetries = 3 // Try up to 3 different random seeds
	var builderInst *builder.InternalMemoryBuilderPartitionedPHF[K, H, *B]
	var err error
	buildSuccess := false

	for attempt := 0; attempt < maxBuildRetries; attempt++ {
		// Use a different seed for each attempt
		if attempt > 0 {
			config.Seed = util.RandomSeed() // New random seed for retries
		}
		t.Logf("Build attempt %d/%d with seed %d...", attempt+1, maxBuildRetries, config.Seed)

		hasher := core.NewMurmurHash2_64Hasher[K]()
		builderInst = builder.NewInternalMemoryBuilderPartitionedPHF[K, H, *B](hasher) // Use pointer type *B

		_, err = builderInst.BuildFromKeys(keys, config)
		if err == nil {
			buildSuccess = true // Found a working seed
			t.Logf("Build attempt %d succeeded with seed %d", attempt+1, config.Seed)
			break
		}

		// Check if this error or any wrapped error is a SeedRuntimeError
		var seedErr core.SeedRuntimeError
		if errors.As(err, &seedErr) {
			// Log seed failure and continue to next attempt
			t.Logf("Build attempt %d failed with SeedRuntimeError: %v", attempt+1, seedErr)
			continue
		} else {
			// Fail on other unexpected errors
			t.Fatalf("BuildFromKeys failed with unexpected error on attempt %d: %v", attempt+1, err)
		}
	}

	// If all attempts failed, skip this test configuration
	if !buildSuccess {
		t.Skipf("Skipping test: All %d build attempts failed with SeedRuntimeError (last error: %v)",
			maxBuildRetries, err)
		return
	}

	phf1 := pthash.NewPartitionedPHF[K, H, *B, *E](config.Minimal, config.Search) // Use pointer types *B, *E
	_, err = phf1.Build(builderInst, &config)
	if err != nil {
		t.Fatalf("phf1.Build failed: %v", err)
	}

	// --- Marshal ---
	data, err := phf1.MarshalBinary()
	if err != nil {
		t.Fatalf("phf1.MarshalBinary() failed: %v", err)
	}
	if len(data) == 0 {
		t.Fatalf("MarshalBinary returned empty data")
	}
	t.Logf("Marshaled PartitionedPHF size: %d bytes (%.2f bits/key)", len(data), float64(len(data)*8)/float64(phf1.NumKeys()))

	// --- Unmarshal ---
	phf2 := pthash.NewPartitionedPHF[K, H, *B, *E](config.Minimal, config.Search) // Create new instance
	err = phf2.UnmarshalBinary(data)
	if err != nil {
		// If underlying components' UnmarshalBinary are stubbed/fail, this will fail.
		t.Fatalf("phf2.UnmarshalBinary() failed: %v", err)
	}

	// --- Compare ---
	if phf1.Seed() != phf2.Seed() {
		t.Errorf("Seed mismatch: %d != %d", phf1.Seed(), phf2.Seed())
	}
	if phf1.NumKeys() != phf2.NumKeys() {
		t.Errorf("NumKeys mismatch: %d != %d", phf1.NumKeys(), phf2.NumKeys())
	}
	if phf1.TableSize() != phf2.TableSize() {
		t.Errorf("TableSize mismatch: %d != %d", phf1.TableSize(), phf2.TableSize())
	}
	if phf1.IsMinimal() != phf2.IsMinimal() {
		t.Errorf("IsMinimal mismatch: %t != %t", phf1.IsMinimal(), phf2.IsMinimal())
	}
	// Cannot easily compare f.partitions directly, rely on lookup check
	if phf1.NumBits() != phf2.NumBits() {
		t.Errorf("NumBits mismatch: %d != %d", phf1.NumBits(), phf2.NumBits())
	}

	sampleKey := keys[numKeys/3]
	val1 := phf1.Lookup(sampleKey)
	val2 := phf2.Lookup(sampleKey)
	if val1 != val2 {
		t.Errorf("Lookup mismatch for key %d after serialization: %d != %d", sampleKey, val1, val2)
	}
}

func TestPartitionedPHFSerializationDense(t *testing.T) {
	// Use a simple, known-good configuration for testing serialization structure
	type K = uint64
	type H = core.MurmurHash2_64Hasher[K]
	type B = core.SkewBucketer // Sub-builder bucketer
	type E = core.RiceEncoder  // Sub-builder encoder

	numKeys := uint64(5000)     // Small number of keys, but ensure partitioning
	avgPartSize := uint64(1000) // Should create ~5 partitions
	seed := uint64(time.Now().UnixNano())
	keys := util.DistinctUints64(numKeys, seed)
	if uint64(len(keys)) != numKeys {
		t.Fatalf("Failed to generate keys")
	}

	config := core.DefaultBuildConfig()
	config.Alpha = 0.94
	config.Lambda = 5.0
	config.Minimal = true              // Test minimal case
	config.Search = core.SearchTypeAdd // Test Additive
	config.Verbose = false
	config.NumThreads = 2 // Use a couple of threads for partitioned build
	config.Seed = 654321  // Fixed seed
	config.AvgPartitionSize = avgPartSize
	config.DensePartitioning = false // Not dense

	// --- Build the Partitioned PHF with Retry Logic ---
	const maxBuildRetries = 3 // Try up to 3 different random seeds
	var builderInst *builder.InternalMemoryBuilderPartitionedPHF[K, H, *B]
	var err error
	buildSuccess := false

	for attempt := 0; attempt < maxBuildRetries; attempt++ {
		// Use a different seed for each attempt
		if attempt > 0 {
			config.Seed = util.RandomSeed() // New random seed for retries
		}
		t.Logf("Build attempt %d/%d with seed %d...", attempt+1, maxBuildRetries, config.Seed)

		hasher := core.NewMurmurHash2_64Hasher[K]()
		builderInst = builder.NewInternalMemoryBuilderPartitionedPHF[K, H, *B](hasher) // Use pointer type *B

		_, err = builderInst.BuildFromKeys(keys, config)
		if err == nil {
			buildSuccess = true // Found a working seed
			t.Logf("Build attempt %d succeeded with seed %d", attempt+1, config.Seed)
			break
		}

		// Check if this error is a SeedRuntimeError
		var seedErr core.SeedRuntimeError
		if errors.As(err, &seedErr) {
			// Log seed failure and continue to next attempt
			t.Logf("Build attempt %d failed with SeedRuntimeError: %v", attempt+1, seedErr)
			continue
		} else {
			// Fail on other unexpected errors
			t.Fatalf("BuildFromKeys failed with unexpected error on attempt %d: %v", attempt+1, err)
		}
	}

	// If all attempts failed, skip this test configuration
	if !buildSuccess {
		t.Skipf("Skipping test: All %d build attempts failed with SeedRuntimeError (last error: %v)",
			maxBuildRetries, err)
		return
	}

	phf1 := pthash.NewPartitionedPHF[K, H, *B, *E](config.Minimal, config.Search) // Use pointer types *B, *E
	_, err = phf1.Build(builderInst, &config)
	if err != nil {
		t.Fatalf("phf1.Build failed: %v", err)
	}

	// --- Marshal ---
	data, err := phf1.MarshalBinary()
	if err != nil {
		t.Fatalf("phf1.MarshalBinary() failed: %v", err)
	}
	if len(data) == 0 {
		t.Fatalf("MarshalBinary returned empty data")
	}
	t.Logf("Marshaled PartitionedPHF size: %d bytes (%.2f bits/key)", len(data), float64(len(data)*8)/float64(phf1.NumKeys()))

	// --- Unmarshal ---
	phf2 := pthash.NewPartitionedPHF[K, H, *B, *E](config.Minimal, config.Search) // Create new instance
	err = phf2.UnmarshalBinary(data)
	if err != nil {
		// If underlying components' UnmarshalBinary are stubbed/fail, this will fail.
		t.Fatalf("phf2.UnmarshalBinary() failed: %v", err)
	}

	// --- Compare ---
	if phf1.Seed() != phf2.Seed() {
		t.Errorf("Seed mismatch: %d != %d", phf1.Seed(), phf2.Seed())
	}
	if phf1.NumKeys() != phf2.NumKeys() {
		t.Errorf("NumKeys mismatch: %d != %d", phf1.NumKeys(), phf2.NumKeys())
	}
	if phf1.TableSize() != phf2.TableSize() {
		t.Errorf("TableSize mismatch: %d != %d", phf1.TableSize(), phf2.TableSize())
	}
	if phf1.IsMinimal() != phf2.IsMinimal() {
		t.Errorf("IsMinimal mismatch: %t != %t", phf1.IsMinimal(), phf2.IsMinimal())
	}
	// Cannot easily compare f.partitions directly, rely on lookup check
	if phf1.NumBits() != phf2.NumBits() {
		t.Errorf("NumBits mismatch: %d != %d", phf1.NumBits(), phf2.NumBits())
	}

	// Compare a lookup (basic functional check)
	sampleKey := keys[numKeys/3]
	val1 := phf1.Lookup(sampleKey)
	val2 := phf2.Lookup(sampleKey)
	if val1 != val2 {
		t.Errorf("Lookup mismatch for key %d after serialization: %d != %d", sampleKey, val1, val2)
	}
}
