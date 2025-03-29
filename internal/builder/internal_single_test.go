package builder

import (
	"fmt"
	"math/rand"
	"pthashgo/internal/core"
	"pthashgo/internal/util"
	"pthashgo/pkg/pthash"
	"runtime"
	"testing"
	"time"
	// Import specific encoders when implemented
	// "pthashgo/internal/core/encoders"
)

// check function remains the same as provided in Phase 4 review response

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
		// Add encoders as they are implemented
		{"Rice", func() core.Encoder { return new(core.RiceEncoder) }},
		// {"RiceRice", func() core.Encoder { return core.NewDualEncoder[core.RiceEncoder, core.RiceEncoder]() }}, // Requires DualEncoder
		{"Compact", func() core.Encoder { return new(core.CompactEncoder) }}, // Requires CompactVector
		// {"PartitionedCompact", func() core.Encoder { /* ... */ }}, // Requires PartitionedCompact impl
		// {"CompactCompact", func() core.Encoder { return core.NewDualEncoder[core.CompactEncoder, core.CompactEncoder]() }},
		// {"Dictionary", func() core.Encoder { /* ... */ }}, // Requires Dictionary impl
		// {"DictionaryDictionary", func() core.Encoder { /* ... */ }},
		// {"EliasFano", func() core.Encoder { return core.NewEliasFano() }}, // Placeholder
		// {"DictionaryEliasFano", func() core.Encoder { /* ... */ }},
		// {"SDC", func() core.Encoder { /* ... */ }}, // Requires SDC impl
	}

	// --- Test Loop ---
	for run := 0; run < numRuns; run++ {
		t.Run(fmt.Sprintf("Run=%d", run+1), func(t *testing.T) {
			// Generate random keys for this run
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(run)))
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

											// --- Dynamic Instantiation based on loop variables ---
											// Need to use reflection or pass types if using Go generics directly.
											// This structure assumes we can select implementations based on name/factory.

											config := core.DefaultBuildConfig()
											config.Alpha = alpha
											config.Lambda = lambda
											config.Minimal = minimal
											config.Search = searchType
											config.Verbose = false // Keep tests quieter
											config.NumThreads = runtime.NumCPU()
											config.Seed = uint64(rng.Int63()) // New seed for each build

											// Instantiate components
											hasher := hasherInfo.New()
											bucketer := new(core.SkewBucketer) // C++ test uses Skew
											encoder := encInfo.New()           // Instantiate specific encoder

											// --- Generic types for Builder and PHF ---
											// We need to call the generic functions with the *correct types*.
											// This is tricky without fully generic functions/methods based on runtime types.
											// A possible approach is to have helper functions for each combination
											// or use reflection (which can be complex and less type-safe).

											// --- Simplified approach: Test one combination explicitly first ---
											// Let's assume Hasher=XXH128, Bucketer=Skew, Encoder=Rice for now
											// Replace these explicit types later if parameterizing fully
											if hasherInfo.Name == "XXHash128" && encInfo.Name == "Rice" {
												type K = uint64
												type H = core.XXHash128Hasher[K]
												type B = core.SkewBucketer
												type E = core.RiceEncoder

												// Explicit instantiation
												hInst := core.NewXXHash128Hasher[K]()
												bInst := new(B)
												builder := NewInternalMemoryBuilderSinglePHF[K, H, *B](hInst, bInst)

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
											// --- End Simplified Approach ---

										}) // Encoder loop
									} // Lambda loop
								}) // Alpha loop
							} // Hasher loop
						}) // Run loop
					} // Key generation loop
				}) // N loop
			}
		})
	} // Test func
}
