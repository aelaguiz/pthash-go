package pthash_test

import (
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
									config.Verbose = false // Keep tests quiet unless debugging
									config.NumThreads = runtime.NumCPU()
									// Use a fixed seed for reproducibility
									config.Seed = 42

									// --- Build using Partitioned Builder ---
									hasher := core.NewXXHash128Hasher[K]()
									pb := builder.NewInternalMemoryBuilderPartitionedPHF[K, H, B](hasher) // Pass sub-bucketer type B

									buildTimings, err := pb.BuildFromKeys(keys, config)
									if err != nil {
										if _, ok := err.(core.SeedRuntimeError); ok {
											t.Logf("Build failed with SeedRuntimeError (seed %d): %v - Skipping check", config.Seed, err)
											return // Skip check for this seed
										}
										t.Fatalf("Builder.BuildFromKeys failed with non-seed error: %v", err)
									}

									// --- Construct Final PartitionedPHF ---
									finalPHF := pthash.NewPartitionedPHF[K, H, B, E](minimal, searchType)
									encodeTime, err := finalPHF.Build(pb, &config)
									if err != nil {
										t.Fatalf("finalPHF.Build failed: %v", err)
									}
									t.Logf("Build Timings: Part: %v, MapOrd: %v, Search: %v, Encode: %v",
										buildTimings.PartitioningMicroseconds,
										buildTimings.MappingOrderingMicroseconds,
										buildTimings.SearchingMicroseconds,
										encodeTime)

									// --- Check Correctness ---
									err = check[K](keys, finalPHF) // Use the same check function
									if err != nil {
										t.Errorf("Correctness check failed: %v", err)
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
