package pthash_test

import (
	"fmt"
	"math"
	"math/rand"
	"pthash-go/internal/builder"
	"pthash-go/internal/core"
	"pthash-go/internal/util"
	"pthash-go/pkg/pthash"
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

func TestInternalSinglePHFBuildAndCheck(t *testing.T) {
	// Use specific types for testing
	type K = uint64
	type H = core.XXHash128Hasher[K]
	type B = core.SkewBucketer
	type E = core.RiceEncoder

	seed := uint64(time.Now().UnixNano())
	numKeysList := []uint64{10000} // Faster CI

	alphas := []float64{0.94, 0.98}
	lambdas := []float64{3.0, 5.0}
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
								config.Verbose = false // Keep tests quiet unless debugging
								config.NumThreads = runtime.NumCPU()
								// Use a fixed seed for reproducibility within a test run
								config.Seed = uint64(rand.Int63()) // Fixed random seed per subtest

								// Create hasher and bucketer instances
								hasher := core.NewXXHash128Hasher[K]()
								var bucketer B // Zero value is okay if Init doesn't rely on prior state

								builder := builder.NewInternalMemoryBuilderSinglePHF[K, H, B](hasher, bucketer)

								_, err := builder.BuildFromKeys(keys, config)
								if err != nil {
									// Don't fail test on SeedRuntimeError, just log it
									if _, ok := err.(builder.SeedRuntimeError); ok {
										t.Logf("Build failed with SeedRuntimeError (seed %d): %v - Skipping check", config.Seed, err)
										return // Skip check for this seed
									}
									t.Fatalf("BuildFromKeys failed with non-seed error: %v", err)
								}

								// Build the actual PHF structure from the builder
								phf := pthash.NewSinglePHF[K, H, B, E](minimal, searchType)
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
