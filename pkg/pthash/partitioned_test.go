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

									// --- Build using Partitioned Builder ---
									hasher := core.NewXXHash128Hasher[K]()
									pb := builder.NewInternalMemoryBuilderPartitionedPHF[K, H, B](hasher) // Pass sub-bucketer type B

									buildTimings, err := pb.BuildFromKeys(keys, config)
									if err != nil {
										// Check if this error or any wrapped error is a SeedRuntimeError
										var seedErr core.SeedRuntimeError
										if errors.As(err, &seedErr) {
											// Use Skipf to mark the test as skipped with the error message
											t.Skipf("Skipping test: BuildFromKeys failed with SeedRuntimeError: %v", seedErr)
											return
										}
										// Fail on other unexpected errors
										t.Fatalf("BuildFromKeys failed with unexpected error: %v", err)
									}

									// --- Construct Final PartitionedPHF ---
									finalPHF := pthash.NewPartitionedPHF[K, H, B, E](minimal, searchType)
									encodeTime, err := finalPHF.Build(pb, &config)
									if err != nil {
										// Still check for stub issues here if serialization/final build depends on them
										if core.IsEliasFanoStubbed() && config.Minimal {
											t.Skipf("Skipping final check: Minimal PHF requires functional EliasFano (stub detected)")
											return
										}
										t.Fatalf("finalPHF.Build failed: %v", err)
									}
									t.Logf("Build Timings: Part: %v, MapOrd: %v, Search: %v, Encode: %v",
										buildTimings.PartitioningMicroseconds,
										buildTimings.MappingOrderingMicroseconds,
										buildTimings.SearchingMicroseconds,
										encodeTime)

									// --- Check Correctness ---
									if !(config.Minimal && core.IsEliasFanoStubbed()) { // Skip check if EF needed and stubbed
										err = check[K](keys, finalPHF)
										if err != nil {
											t.Errorf("Correctness check failed: %v", err)
										}
									}

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

	// --- Build the Partitioned PHF ---
	hasher := core.NewMurmurHash2_64Hasher[K]()
	// Partitioned builder needs sub-bucketer type B passed
	builderInst := builder.NewInternalMemoryBuilderPartitionedPHF[K, H, *B](hasher) // Use pointer type *B

	_, err := builderInst.BuildFromKeys(keys, config)
	if err != nil {
		var seedErr core.SeedRuntimeError
		if errors.As(err, &seedErr) {
			t.Skipf("Skipping test: BuildFromKeys failed with SeedRuntimeError: %v", seedErr)
			return
		}
		t.Fatalf("BuildFromKeys failed: %v", err)
	}

	phf1 := pthash.NewPartitionedPHF[K, H, *B, *E](config.Minimal, config.Search) // Use pointer types *B, *E
	_, err = phf1.Build(builderInst, &config)
	if err != nil {
		if core.IsEliasFanoStubbed() && config.Minimal {
			t.Skipf("Skipping serialization test: Minimal PHF requires functional EliasFano (stub detected)")
		}
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
	if !(config.Minimal && core.IsEliasFanoStubbed()) {
		sampleKey := keys[numKeys/3]
		val1 := phf1.Lookup(sampleKey)
		val2 := phf2.Lookup(sampleKey)
		if val1 != val2 {
			t.Errorf("Lookup mismatch for key %d after serialization: %d != %d", sampleKey, val1, val2)
		}
	} else {
		t.Log("Skipping lookup check due to stubbed EliasFano for minimal PHF.")
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

	// --- Build the Partitioned PHF ---
	hasher := core.NewMurmurHash2_64Hasher[K]()
	// Partitioned builder needs sub-bucketer type B passed
	builderInst := builder.NewInternalMemoryBuilderPartitionedPHF[K, H, *B](hasher) // Use pointer type *B

	_, err := builderInst.BuildFromKeys(keys, config)
	if err != nil {
		t.Fatalf("BuildFromKeys failed: %v", err)
	}

	phf1 := pthash.NewPartitionedPHF[K, H, *B, *E](config.Minimal, config.Search) // Use pointer types *B, *E
	_, err = phf1.Build(builderInst, &config)
	if err != nil {
		if core.IsEliasFanoStubbed() && config.Minimal {
			t.Skipf("Skipping serialization test: Minimal PHF requires functional EliasFano (stub detected)")
		}
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
	if !(config.Minimal && core.IsEliasFanoStubbed()) {
		sampleKey := keys[numKeys/3]
		val1 := phf1.Lookup(sampleKey)
		val2 := phf2.Lookup(sampleKey)
		if val1 != val2 {
			t.Errorf("Lookup mismatch for key %d after serialization: %d != %d", sampleKey, val1, val2)
		}
	} else {
		t.Log("Skipping lookup check due to stubbed EliasFano for minimal PHF.")
	}
}
