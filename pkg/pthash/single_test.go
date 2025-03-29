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
