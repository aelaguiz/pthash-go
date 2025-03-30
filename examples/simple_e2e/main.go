package main

import (
	"fmt"
	"log"
	"math"
	"os"
	"pthashgo/internal/builder"
	"pthashgo/internal/core"
	"pthashgo/internal/util"
	"pthashgo/pkg/pthash"
	"runtime"
)

// check performs correctness checks (copied from single_test.go for self-containment)
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

func main() {
	log.Println("--- PTHash-Go Simple End-to-End Example ---")

	// --- 1. Define Parameters & Generate Keys ---
	numKeys := uint64(1000)
	inputSeed := uint64(42)   // Seed for generating input keys
	buildSeed := uint64(1234) // Seed for building the PHF

	log.Printf("Generating %d distinct uint64 keys (seed: %d)...", numKeys, inputSeed)
	keys := util.DistinctUints64(numKeys, inputSeed)
	if uint64(len(keys)) != numKeys {
		log.Fatalf("Failed to generate enough distinct keys.")
	}
	log.Printf("Generated %d keys.", len(keys))

	// --- 2. Choose PHF Configuration ---
	// We'll use SinglePHF with common defaults.
	// This example focuses on correctness, not performance.
	type K = uint64
	type H = core.MurmurHash2_64Hasher[K]
	type B = core.SkewBucketer
	type E = core.RiceEncoder // Or use CompactEncoder if Rice/D1Array is problematic

	config := core.DefaultBuildConfig()
	config.Minimal = true // Build a Minimal Perfect Hash Function
	config.Verbose = true // Show build steps
	config.Seed = buildSeed
	config.NumThreads = runtime.NumCPU() // Use multiple cores for build steps if available

	// Temporarily to get past race
	// config.NumThreads = 1
	// Use default Alpha and Lambda for simplicity
	log.Printf("Build Config: N=%d, Alpha=%.2f, Lambda=%.1f, Minimal=%t, Seed=%d, Threads=%d",
		numKeys, config.Alpha, config.Lambda, config.Minimal, config.Seed, config.NumThreads)

	// --- 3. Instantiate Builder ---
	log.Println("Instantiating builder...")
	hasher := core.NewMurmurHash2_64Hasher[K]()
	bucketer := new(B) // Use pointer type because SkewBucketer methods use pointer receiver
	builderInst := builder.NewInternalMemoryBuilderSinglePHF[K, H, *B](hasher, bucketer)

	// --- 4. Run Builder ---
	log.Println("Running builder (BuildFromKeys)...")
	_, err := builderInst.BuildFromKeys(keys, config)
	if err != nil {
		// Handle potential seed error
		if seedErr, ok := err.(core.SeedRuntimeError); ok {
			log.Fatalf("BUILD FAILED: %v. Try a different seed (%d) or different parameters.", seedErr, config.Seed)
		}
		log.Fatalf("BUILD FAILED: Unexpected error during BuildFromKeys: %v", err)
	}
	log.Println("Builder finished successfully.")

	// --- 5. Build Final PHF ---
	log.Println("Building final PHF structure...")
	// Match generic types with builder (*B) and choose Encoder type (*E)
	phf := pthash.NewSinglePHF[K, H, *B, *E](config.Minimal, config.Search)
	encodeDuration, err := phf.Build(builderInst, &config)
	if err != nil {
		log.Fatalf("BUILD FAILED: Error during final PHF build stage: %v", err)
	}
	log.Printf("Final PHF built successfully (Encoding took: %v).", encodeDuration)
	log.Printf("Stats: NumKeys=%d, TableSize=%d, Bits/Key=%.2f",
		phf.NumKeys(), phf.TableSize(), float64(phf.NumBits())/float64(phf.NumKeys()))

	// --- 6. Perform Lookups ---
	log.Println("Performing lookups for all keys...")
	results := make([]uint64, numKeys)
	for i, key := range keys {
		results[i] = phf.Lookup(key)
		// Optional: Print some lookups
		if i < 5 || uint64(i) >= numKeys-5 {
			fmt.Printf("  Lookup(%d) -> %d\n", key, results[i])
		}
		if i == 5 {
			fmt.Println("  ...")
		}
	}
	log.Println("Lookups complete.")

	// --- 7. Check Correctness ---
	log.Println("Checking correctness (Minimal Perfect Hash Function properties)...")
	err = check[K](keys, phf)
	if err != nil {
		log.Printf("--- CORRECTNESS CHECK FAILED ---")
		log.Fatalf("Error: %v", err)
		os.Exit(1)
	}

	log.Println("--- CORRECTNESS CHECK PASSED ---")
	log.Println("The function correctly maps all input keys to unique values in [0, N-1].")
}
