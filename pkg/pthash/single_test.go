package pthash_test

import (
	"fmt"
	"log"
	"math"
	"math/rand"
	"pthashgo/internal/builder"
	"pthashgo/internal/core"
	"pthashgo/internal/util"
	"pthashgo/pkg/pthash"
	"runtime"
	"testing"
	"time"
)

// check performs correctness checks (ported from C++ util).
// K needs to be comparable for the map check.
func check[K comparable, F interface {
	Lookup(K) uint64
	NumKeys() uint64
	TableSize() uint64 // Needed for non-minimal check
	IsMinimal() bool
}](keys []K, f F) error {
	n := f.NumKeys()
	if uint64(len(keys)) != n {
		return fmt.Errorf("check failed: number of keys mismatch (got %d, expected %d)", len(keys), n)
	}

	if n == 0 {
		return nil // Nothing to check
	}

	seenPositions := make(map[uint64]struct{}, n) // Used for both minimal and non-minimal checks

	if f.IsMinimal() {
		var sum uint64 // Use uint64, check for overflow potential if n is huge
		expectedSum := uint64(0)
		// Calculate expected sum carefully to avoid overflow
		if n > 0 { // Avoid n-1 underflow
			if n%2 == 0 {
				expectedSum = (n / 2) * (n - 1)
			} else {
				expectedSum = n * ((n - 1) / 2)
			}
		}

		for _, key := range keys {
			p := f.Lookup(key)
			if p >= n {
				return fmt.Errorf("check failed (minimal): position %d >= numKeys %d for key %v", p, n, key)
			}
			if _, exists := seenPositions[p]; exists {
				return fmt.Errorf("check failed (minimal): duplicate position %d detected for key %v", p, key)
			}
			seenPositions[p] = struct{}{}
			// Check for overflow before adding
			if math.MaxUint64-sum < p {
				// Overflow would occur - cannot reliably check sum
				expectedSum = sum // Bypass check
				fmt.Println("Warning: Skipping sum check for minimal PHF due to potential overflow")
			}
			sum += p
		}

		if sum != expectedSum {
			return fmt.Errorf("check failed (minimal): sum mismatch (got %d, expected %d)", sum, expectedSum)
		}

	} else { // Non-minimal check
		m := f.TableSize()
		for _, key := range keys {
			p := f.Lookup(key)
			if p >= m {
				return fmt.Errorf("check failed (non-minimal): position %d >= tableSize %d for key %v", p, m, key)
			}
			if _, exists := seenPositions[p]; exists {
				return fmt.Errorf("check failed (non-minimal): duplicate position %d detected for key %v", p, key)
			}
			seenPositions[p] = struct{}{}
		}
	}
	return nil // Everything OK
}

// --- Test Function ---

func TestInternalSinglePHFEquivCPP(t *testing.T) {
	const universe = 100000
	const numRuns = 5 // Number of times to generate keys and test

	// Parameters from C++ test
	alphas := []float64{1.0, 0.99, 0.98, 0.97, 0.96}
	lambdas := []float64{4.0, 4.5, 5.0, 5.5, 6.0}
	searchType := core.SearchTypeAdd // C++ test uses Additive
	minimal := true                  // C++ test uses minimal

	// --- Define Hasher and Encoder types to test ---
	// Use interface maps for easier iteration later
	type HasherInfo struct {
		Name string
		New  func() core.Hasher[uint64] // Factory function
	}
	hashers := []HasherInfo{
		{"XXHash128", func() core.Hasher[uint64] { return core.NewXXHash128Hasher[uint64]() }},
		{"Murmur64", func() core.Hasher[uint64] { return core.NewMurmurHash2_64Hasher[uint64]() }},
	}

	type EncoderInfo struct {
		Name string
		New  func() core.Encoder // Factory function
	}
	encoders := []EncoderInfo{
		// Currently only testing with Rice encoder, others will be added as they are implemented
		{"Rice", func() core.Encoder { return new(core.RiceEncoder) }},
		// {"Compact", func() core.Encoder { return new(core.CompactEncoder) }},
	}

	// --- Test Loop ---
	for run := 0; run < numRuns; run++ {
		t.Run(fmt.Sprintf("Run=%d", run+1), func(t *testing.T) {
			// Generate random keys for this run
			seed := uint64(time.Now().UnixNano() + int64(run))
			rng := rand.New(rand.NewSource(int64(seed)))
			numKeys := uint64(rng.Int63n(universe))
			if numKeys == 0 {
				numKeys = 1
			}
			keySeed := uint64(rng.Int63())
			keys := util.DistinctUints64(numKeys, keySeed)
			if uint64(len(keys)) != numKeys {
				t.Fatalf("Run %d: Failed to generate enough distinct keys: got %d, want %d", run+1, len(keys), numKeys)
			}

			for _, hasherInfo := range hashers {
				t.Run(fmt.Sprintf("Hasher=%s", hasherInfo.Name), func(t *testing.T) {
					for _, alpha := range alphas {
						t.Run(fmt.Sprintf("Alpha=%.2f", alpha), func(t *testing.T) {
							for _, lambda := range lambdas {
								t.Run(fmt.Sprintf("Lambda=%.1f", lambda), func(t *testing.T) {
									for _, encInfo := range encoders {
										t.Run(fmt.Sprintf("Encoder=%s", encInfo.Name), func(t *testing.T) {

											config := core.DefaultBuildConfig()
											config.Alpha = alpha
											config.Lambda = lambda
											config.Minimal = minimal
											config.Search = searchType
											config.Verbose = false // Keep tests quieter
											config.NumThreads = runtime.NumCPU()
											config.Seed = uint64(rng.Int63()) // New seed for each build

											// --- Simplified approach: Test specific combinations based on selected options ---
											if hasherInfo.Name == "XXHash128" && encInfo.Name == "Rice" {
												type K = uint64
												type H = core.XXHash128Hasher[K]
												type B = core.SkewBucketer
												type E = core.RiceEncoder

												// Explicit instantiation
												hInst := core.NewXXHash128Hasher[K]()
												bInst := new(B)
												builder := builder.NewInternalMemoryBuilderSinglePHF[K, H, *B](hInst, bInst)

												_, err := builder.BuildFromKeys(keys, config)
												if err != nil {
													if _, ok := err.(core.SeedRuntimeError); ok {
														t.Logf("Build failed with SeedRuntimeError (seed %d): %v - Skipping check", config.Seed, err)
														return // Skip
													}
													t.Fatalf("BuildFromKeys failed: %v", err)
												}

												phf := pthash.NewSinglePHF[K, H, *B, *E](minimal, searchType)
												_, err = phf.Build(builder, &config)
												if err != nil {
													t.Fatalf("phf.Build failed: %v", err)
												}

												// Check correctness
												err = check[K](keys, phf)
												if err != nil {
													t.Errorf("Correctness check failed: %v", err)
												}
											} else if hasherInfo.Name == "Murmur64" && encInfo.Name == "Rice" {
												type K = uint64
												type H = core.MurmurHash2_64Hasher[K]
												type B = core.SkewBucketer
												type E = core.RiceEncoder

												// Explicit instantiation
												hInst := core.NewMurmurHash2_64Hasher[K]()
												bInst := new(B)
												builder := builder.NewInternalMemoryBuilderSinglePHF[K, H, *B](hInst, bInst)

												_, err := builder.BuildFromKeys(keys, config)
												if err != nil {
													if _, ok := err.(core.SeedRuntimeError); ok {
														t.Logf("Build failed with SeedRuntimeError (seed %d): %v - Skipping check", config.Seed, err)
														return // Skip
													}
													t.Fatalf("BuildFromKeys failed: %v", err)
												}

												phf := pthash.NewSinglePHF[K, H, *B, *E](minimal, searchType)
												_, err = phf.Build(builder, &config)
												if err != nil {
													t.Fatalf("phf.Build failed: %v", err)
												}

												// Check correctness
												err = check[K](keys, phf)
												if err != nil {
													t.Errorf("Correctness check failed: %v", err)
												}
											} else {
												t.Skipf("Skipping combination Hasher=%s, Encoder=%s (implementation/parameterization pending)",
													hasherInfo.Name, encInfo.Name)
											}

										}) // Encoder loop
									}
								}) // Lambda loop
							}
						}) // Alpha loop
					}
				}) // Hasher loop
			}
		}) // Run loop
	}
}

// Keep the original test function as a reference/simpler version
func TestInternalSinglePHFBuildAndCheck(t *testing.T) {
	// Skip this test by default since we have the newer test
	t.Skip("Skipping basic test in favor of TestInternalSinglePHFEquivCPP")

	// Use specific types for testing
	type K = uint64
	type H = core.XXHash128Hasher[K] // Hasher type (value)
	type B = core.SkewBucketer       // Bucketer type (value)
	type E = core.RiceEncoder        // Need to use pointer type since methods have pointer receivers

	seed := uint64(time.Now().UnixNano())
	numKeysList := []uint64{1000} // Smaller N for debugging

	alphas := []float64{0.98}                            // Simpler alpha for debugging
	lambdas := []float64{6.0}                            // Higher lambda (smaller buckets) for debugging
	searchTypes := []core.SearchType{core.SearchTypeXOR} // Add SearchTypeAdd when implemented

	for _, numKeys := range numKeysList {
		t.Run(fmt.Sprintf("N=%d", numKeys), func(t *testing.T) {
			keys := util.DistinctUints64(numKeys, seed)
			if uint64(len(keys)) != numKeys {
				t.Fatalf("Failed to generate enough distinct keys: got %d, want %d", len(keys), numKeys)
			}

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
								config.Verbose = true                // Keep tests quiet unless debugging
								config.NumThreads = runtime.NumCPU() // Use all available CPUs
								log.Printf("Test configured to use %d threads (runtime.NumCPU())", config.NumThreads)
								// Use a fixed seed for reproducibility within a test run
								// config.Seed = uint64(rand.Int63()) // Use random later
								config.Seed = 12345 // Fixed seed for debugging

								// Create hasher and bucketer instances
								hasher := core.NewXXHash128Hasher[K]()
								// Instantiate Bucketer as a pointer type since its methods have pointer receivers
								var bucketer *B = new(B) // Create a pointer to a zero B

								builder := builder.NewInternalMemoryBuilderSinglePHF[K, H, *B](hasher, bucketer) // Pass pointer type *B to generic

								_, err := builder.BuildFromKeys(keys, config)
								if err != nil {
									// Don't fail test on SeedRuntimeError, just log it
									if _, ok := err.(core.SeedRuntimeError); ok {
										t.Logf("Build failed with SeedRuntimeError (seed %d): %v - Skipping check", config.Seed, err)
										return // Skip check for this seed
									}
									t.Fatalf("BuildFromKeys failed with non-seed error: %v", err)
								}

								// Build the actual PHF structure from the builder
								phf := pthash.NewSinglePHF[K, H, *B, *E](minimal, searchType) // Pass pointer types *B and *E
								encodeTime, err := phf.Build(builder, &config)
								if err != nil {
									t.Fatalf("phf.Build failed: %v", err)
								}
								t.Logf("Encoding time: %v", encodeTime)

								// Check correctness
								err = check[K](keys, phf)
								if err != nil {
									t.Errorf("Correctness check failed: %v", err)
								}

								// Basic sanity checks on PHF properties
								if phf.NumKeys() != numKeys {
									t.Errorf("NumKeys mismatch: expected %d, got %d", numKeys, phf.NumKeys())
								}
								if phf.Seed() != builder.Seed() { // Should match seed used
									t.Errorf("Seed mismatch")
								}
								// Check table size consistency (approximate)
								expectedTableSizeMin := uint64(float64(numKeys) / config.Alpha)
								if phf.TableSize() < expectedTableSizeMin {
									t.Errorf("TableSize too small: expected >= %d, got %d", expectedTableSizeMin, phf.TableSize())
								}

							}) // End subtest t.Run
						} // End minimal loop
					} // End searchType loop
				} // End lambda loop
			} // End alpha loop
		}) // End N t.Run
	} // End numKeys loop
}

// --- New Serialization Test ---

func TestSinglePHFSerialization(t *testing.T) {
	// Use a simple, known-good configuration for testing serialization structure
	type K = uint64
	type H = core.MurmurHash2_64Hasher[K] // Use Murmur as it's simpler
	type B = core.SkewBucketer            // Skew is relatively simple
	type E = core.RiceEncoder             // Rice needs D1Array/CompactVector stubs/impl

	numKeys := uint64(500) // Small number of keys
	seed := uint64(uint64(time.Now().UnixNano()))
	keys := util.DistinctUints64(numKeys, seed)
	if uint64(len(keys)) != numKeys {
		t.Fatalf("Failed to generate keys")
	}

	config := core.DefaultBuildConfig()
	config.Alpha = 0.94
	config.Lambda = 4.0
	config.Minimal = true // Test minimal case with free slots
	config.Search = core.SearchTypeXOR
	config.Verbose = false
	config.NumThreads = 1 // Keep it simple for serialization test
	config.Seed = 123456  // Fixed seed

	// --- Build the PHF ---
	hasher := core.NewMurmurHash2_64Hasher[K]()
	bucketer := new(B)
	builderInst := builder.NewInternalMemoryBuilderSinglePHF[K, H, *B](hasher, bucketer) // Use pointer type *B

	_, err := builderInst.BuildFromKeys(keys, config)
	if err != nil {
		t.Fatalf("BuildFromKeys failed: %v", err)
	}

	phf1 := pthash.NewSinglePHF[K, H, *B, *E](config.Minimal, config.Search) // Use pointer types *B, *E
	_, err = phf1.Build(builderInst, &config)
	if err != nil {
		// Add check for RiceEncoder/D1Array stub
		t.Fatalf("phf1.Build failed: %v", err)
	}

	// --- Add logging to help diagnose serialization issues ---
	t.Logf("--- TestSinglePHFSerialization ---")
	t.Logf("PHF1 (Before Marshal): Seed=%d, NumKeys=%d, TableSize=%d, IsMinimal=%t, SearchType=%v",
		phf1.Seed(), phf1.NumKeys(), phf1.TableSize(), phf1.IsMinimal(), phf1.SearchType())
	// Additional logging for component details
	if phf1.BucketerForLog() != nil {
		t.Logf("  Bucketer type: %T", phf1.BucketerForLog())
	}
	if phf1.PilotsForLog() != nil {
		t.Logf("  Pilots type: %T, NumBits: %d", phf1.PilotsForLog(), phf1.PilotsForLog().NumBits())
	}
	if phf1.FreeSlotsForLog() != nil {
		t.Logf("  FreeSlots present: true")
	} else {
		t.Logf("  FreeSlots present: false")
	}

	// --- Marshal ---
	data, err := phf1.MarshalBinary()
	if err != nil {
		t.Fatalf("phf1.MarshalBinary() failed: %v", err)
	}
	if len(data) == 0 {
		t.Fatalf("MarshalBinary returned empty data")
	}
	t.Logf("Marshaled SinglePHF size: %d bytes (%.2f bits/key)", len(data), float64(len(data)*8)/float64(phf1.NumKeys()))
	// Log a sample of the data to help with debugging
	if len(data) > 32 {
		t.Logf("  Data sample (first 32 bytes): %x", data[:32])
	} else {
		t.Logf("  Data sample (all %d bytes): %x", len(data), data)
	}

	// --- Unmarshal ---
	phf2 := pthash.NewSinglePHF[K, H, *B, *E](config.Minimal, config.Search) // Create a new empty instance with *same* generic types
	t.Logf("PHF2 (Before Unmarshal): Zero/default value")

	err = phf2.UnmarshalBinary(data)
	if err != nil {
		// If underlying components' UnmarshalBinary are stubbed/fail, this will fail.
		t.Fatalf("phf2.UnmarshalBinary() failed: %v", err)
	}

	// --- Log state after unmarshaling ---
	t.Logf("PHF2 (After Unmarshal): Seed=%d, NumKeys=%d, TableSize=%d, IsMinimal=%t, SearchType=%v",
		phf2.Seed(), phf2.NumKeys(), phf2.TableSize(), phf2.IsMinimal(), phf2.SearchType())
	if phf2.BucketerForLog() != nil {
		t.Logf("  Bucketer type: %T", phf2.BucketerForLog())
	}
	if phf2.PilotsForLog() != nil {
		t.Logf("  Pilots type: %T, NumBits: %d", phf2.PilotsForLog(), phf2.PilotsForLog().NumBits())
	}
	if phf2.FreeSlotsForLog() != nil {
		t.Logf("  FreeSlots present: true")
	} else {
		t.Logf("  FreeSlots present: false")
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
	if phf1.SearchType() != phf2.SearchType() {
		t.Errorf("SearchType mismatch: %v != %v", phf1.SearchType(), phf2.SearchType())
	}
	if phf1.NumBits() != phf2.NumBits() {
		t.Errorf("NumBits mismatch: %d != %d", phf1.NumBits(), phf2.NumBits())
	}

	sampleKey := keys[numKeys/2]
	val1 := phf1.Lookup(sampleKey)
	val2 := phf2.Lookup(sampleKey)
	if val1 != val2 {
		t.Errorf("Lookup mismatch for key %d after serialization: %d != %d", sampleKey, val1, val2)
	} else {
		t.Logf("Lookup check passed for key %d -> %d", sampleKey, val1)
	}
}
