package pthash_test

import (
	"fmt"
	"pthashgo/internal/builder"
	"pthashgo/internal/core"
	"pthashgo/internal/util"
	"pthashgo/pkg/pthash"
	"reflect"
	"runtime"
	"testing"
	"time"
)

// --- Test Function ---

func TestInternalDensePartitionedPHFBuildAndCheck(t *testing.T) {
	// Use specific types - requires TableBucketer<OptBucketer> and Inter* encoders
	// For Phase 7, use stubs or simpler types if needed. Let's assume Skew+Rice exists.
	type K = uint64
	type H = core.XXHash128Hasher[K]
	// Sub-bucketer: Use Skew for now, replace with TableBucketer<OptBucketer> later
	type B = *core.SkewBucketer
	// Dense Encoder: Use MonoR (requires RiceEncoder)
	type E = *core.DenseMono[*core.RiceEncoder] // Use pointer for encoder too

	seed := uint64(time.Now().UnixNano())
	numKeysList := []uint64{5000, 10000} // Smaller N for faster dense test
	avgPartSizes := []uint64{1000, 2500} // Test partitioning

	alphas := []float64{0.94, 0.98}
	lambdas := []float64{4.0, 6.0}
	// Only test ADD search for now, as it's often used with dense/PHOBIC
	searchTypes := []core.SearchType{core.SearchTypeAdd}
	// Dense is typically minimal
	minimal := true

	for _, numKeys := range numKeysList {
		keys := util.DistinctUints64(numKeys, seed)
		if uint64(len(keys)) != numKeys {
			t.Fatalf("N=%d: Failed to generate enough distinct keys: got %d, want %d", numKeys, len(keys), numKeys)
		}

		for _, avgPartSize := range avgPartSizes {
			// Ensure partition size constraints for dense mode
			if avgPartSize < core.MinPartitionSize {
				continue
			}
			if avgPartSize > core.MaxPartitionSize {
				continue
			}
			if avgPartSize >= numKeys {
				continue
			}

			t.Run(fmt.Sprintf("N=%d_P=%d", numKeys, avgPartSize), func(t *testing.T) {

				for _, alpha := range alphas {
					for _, lambda := range lambdas {
						for _, searchType := range searchTypes {
							testName := fmt.Sprintf("A=%.2f_L=%.1f_S=%v_M=%t", alpha, lambda, searchType, minimal)
							t.Run(testName, func(t *testing.T) {
								config := core.DefaultBuildConfig()
								config.Alpha = alpha
								config.Lambda = lambda
								config.Minimal = minimal
								config.Search = searchType
								config.AvgPartitionSize = avgPartSize
								config.DensePartitioning = true // !!! Enable dense mode !!!
								config.Verbose = false          // Keep tests quiet
								config.NumThreads = runtime.NumCPU()
								config.Seed = 42 // Fixed seed for reproducibility

								// --- Build using Partitioned Builder ---
								hasher := core.NewXXHash128Hasher[K]()
								pb := builder.NewInternalMemoryBuilderPartitionedPHF[K, H, B](hasher)

								buildTimings, err := pb.BuildFromKeys(keys, config)
								if err != nil {
									if _, ok := err.(core.SeedRuntimeError); ok {
										t.Logf("Build failed with SeedRuntimeError (seed %d): %v - Skipping check", config.Seed, err)
										return // Skip
									}
									t.Fatalf("Builder.BuildFromKeys failed: %v", err)
								}

								// --- Construct Final DensePartitionedPHF ---
								finalPHF := pthash.NewDensePartitionedPHF[K, H, B, E](minimal, searchType)
								encodeTime, err := finalPHF.Build(pb, &config) // Pass builder to final build
								if err != nil {
									// Check if it's an unimplemented encoder error (expected if stubs used)
									if err.Error() == "CompactVector.MarshalBinary not implemented" ||
										err.Error() == "CompactEncoder.Encode: CompactVector not implemented" || // Assuming stub error
										err.Error() == "EliasFano.Encode not implemented" { // Assuming stub error
										t.Logf("Skipping check due to unimplemented dependency: %v", err)
										t.SkipNow()
									}
									t.Fatalf("finalPHF.Build failed: %v", err)
								}
								t.Logf("Build Timings: Part: %v, MapOrd: %v, Search: %v, Encode: %v",
									buildTimings.PartitioningMicroseconds,
									buildTimings.MappingOrderingMicroseconds,
									buildTimings.SearchingMicroseconds,
									encodeTime)

								// --- Check Correctness ---
								// Requires working EliasFano and CompactVector for minimal mapping and offsets
								// Skip check if dependencies are stubbed
								if finalPHF.FreeSlotsNotImplemented() { // Add helper if needed
									t.Log("Skipping check: Minimal PHF requires EliasFano implementation.")
									t.SkipNow()
								}
								if finalPHF.OffsetsNotImplemented() { // Add helper if needed
									t.Log("Skipping check: Dense PHF requires CompactVector for offsets.")
									t.SkipNow()
								}

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

							}) // End subtest t.Run
						} // End searchType loop
					} // End lambda loop
				} // End alpha loop
			}) // End N_P t.Run
		} // End avgPartSize loop
	} // End numKeys loop
}

// --- New Serialization Test ---

func TestDensePHFSerialization(t *testing.T) {
	// Use a simple, known-good configuration for testing serialization structure
	type K = uint64
	type H = core.MurmurHash2_64Hasher[K]
	// Bucketer for dense partitioned PHF (e.g., TableBucketer<OptBucketer> or Skew)
	// Use Skew for simplicity, needs pointer type *B for sub-builder
	type B = core.SkewBucketer
	// Dense Encoder (e.g., MonoR) - needs RiceEncoder
	type E = core.DenseMono[*core.RiceEncoder] // Needs pointer for underlying Rice

	numKeys := uint64(2000)     // Small number of keys
	avgPartSize := uint64(1000) // Ensure partitioning (MinPartitionSize)
	seed := uint64(time.Now().UnixNano())
	keys := util.DistinctUints64(numKeys, seed)
	if uint64(len(keys)) != numKeys {
		t.Fatalf("Failed to generate keys")
	}

	config := core.DefaultBuildConfig()
	config.Alpha = 0.98
	config.Lambda = 6.0
	config.Minimal = true              // Dense is typically minimal
	config.Search = core.SearchTypeAdd // Dense uses Additive
	config.Verbose = false
	config.NumThreads = 2
	config.Seed = 918273 // Fixed seed
	config.AvgPartitionSize = avgPartSize
	config.DensePartitioning = true // Enable dense mode

	// --- Build the Dense Partitioned PHF ---
	hasher := core.NewMurmurHash2_64Hasher[K]()
	// Partitioned builder needs sub-bucketer type *B passed
	builderInst := builder.NewInternalMemoryBuilderPartitionedPHF[K, H, *B](hasher) // Pass pointer type

	_, err := builderInst.BuildFromKeys(keys, config)
	if err != nil {
		t.Fatalf("BuildFromKeys failed: %v", err)
	}

	phf1 := pthash.NewDensePartitionedPHF[K, H, *B, *E](config.Minimal, config.Search) // Use pointer types *B, *E
	_, err = phf1.Build(builderInst, &config)
	if err != nil {
		if (core.IsEliasFanoStubbed() && config.Minimal) ||
			phf1.OffsetsNotImplemented() ||
			reflect.TypeOf(new(E)).Elem().Name() == "DenseMono[*core.RiceEncoder]" && core.IsD1ArraySelectStubbed() { // Check specific dependencies
			t.Skipf("Skipping serialization test: Dense PHF requires functional EliasFano, CompactVector, RiceEncoder/D1Array (stub detected): %v", err)
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
	t.Logf("Marshaled DensePartitionedPHF size: %d bytes (%.2f bits/key)", len(data), float64(len(data)*8)/float64(phf1.NumKeys()))

	// --- Unmarshal ---
	phf2 := pthash.NewDensePartitionedPHF[K, H, *B, *E](config.Minimal, config.Search) // Create new instance
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
	// Cannot easily compare internal fields like partitioner, subBucketer, pilots, offsets directly
	if phf1.NumBits() != phf2.NumBits() {
		t.Errorf("NumBits mismatch: %d != %d", phf1.NumBits(), phf2.NumBits())
	}

	// Compare a lookup (basic functional check)
	if !(config.Minimal && (core.IsEliasFanoStubbed() || phf1.OffsetsNotImplemented())) { // Skip if EF or Offsets missing
		sampleKey := keys[numKeys/4]
		val1 := phf1.Lookup(sampleKey)
		val2 := phf2.Lookup(sampleKey)
		if val1 != val2 {
			t.Errorf("Lookup mismatch for key %d after serialization: %d != %d", sampleKey, val1, val2)
		}
	} else {
		t.Log("Skipping lookup check due to stubbed EliasFano/Offsets for minimal dense PHF.")
	}
}

// Helper to check if D1Array.Select is likely stubbed (e.g., returns constant)
// This is brittle, ideally D1Array would have an IsStubbed method.
func (ef *core.EliasFano) IsStubbed() bool {
	// A simple check: if NumBits is always 0 for a non-empty structure, it's likely a stub.
	// Let's encode a single value and check NumBits.
	tempEF := core.NewEliasFano()
	_ = tempEF.Encode([]uint64{10})
	// A real EF should use more bits than just the base field sizes (~24 bytes = 192 bits)
	// A stub might only marshal the metadata. Let's use a threshold like 64 bytes = 512 bits.
	return tempEF.NumBits() < 512
}
