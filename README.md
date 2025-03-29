# PTHash-Go

[![Go Reference](https://pkg.go.dev/badge/github.com/your-username/pthash-go.svg)](https://pkg.go.dev/github.com/your-username/pthash-go)
[![Go Report Card](https://goreportcard.com/badge/github.com/your-username/pthash-go)](https://goreportcard.com/report/github.com/your-username/pthash-go)
<!-- Add build status badge once CI is set up -->

A Go implementation of [PTHash](https://github.com/jermp/pthash) - a library for fast and compact minimal perfect hash functions. This is a port of the C++ library described in the research papers below.

## What is a Minimal Perfect Hash Function?

Given a set *S* of *n* distinct keys, a function *f* that bijectively maps the keys of *S* into the first *n* natural numbers `[0, n-1]` is called a *minimal perfect hash function* (MPHF) for *S*.

These functions are valuable for:

*   Search engines and databases to quickly assign identifiers to static sets of variable-length keys.
*   Efficient lookups in hash tables without collisions.
*   Memory-efficient data structures for large key sets.
*   Applications where space efficiency and fast lookups are critical.

## Features

*   **Minimal Perfect Hash Functions**: Maps keys to the minimal perfect range `[0, n-1]`. (Non-minimal variant also possible via config).
*   **Efficiency**: Aims for fast lookups with compact space usage.
*   **Configurable**: Offers different trade-offs (construction time, lookup time, space) via Hasher, Bucketer, Encoder, and build parameters (`lambda`, `alpha`).
*   **Partitioning**: Supports partitioning for potentially faster construction on multi-core machines (internal memory).
*   **Dense Partitioning (PHOBIC)**: Implements the densely partitioned structure for optimized space/lookup trade-offs.
*   **Go-focused**: Native Go implementation using interfaces, generics, and goroutines for concurrency.
*   **Test Coverage**: Includes unit and integration tests.

## Current Development Status

**Status:** Significant progress: Core internal memory features ported, but key components are missing, incomplete, or require performance optimization and correctness verification.

This Go port has successfully translated the core structure and logic for building **internal memory** single, partitioned, and dense partitioned (PHOBIC) minimal perfect hash functions. Foundational elements like hashing, bucketing, configuration, basic bit manipulation (`BitVector`, `CompactVector`), the build flow (`internal/builder`), and PHF types (`pkg/pthash`) are implemented. Unit and integration tests cover many of these components.

However, several critical areas require attention:

*   **Missing Features:**
    *   **External Memory:** Construction in external memory is **not implemented**.
    *   **Advanced Encoders:** Key encoders from C++ are missing: `Dictionary`, `SDC`, `PartitionedCompact`, `Dual` wrappers (e.g., `RiceRice`, `CompactCompact`), and Dense Dual variants.
    *   **Parallel Additive Search:** `searchParallelAdd` is missing.

*   **Performance Bottlenecks:**
    *   **Rank/Select (`D1Array`):** The `Select` implementation uses slow linear scans, making dependent encoders (`RiceEncoder`, `EliasFano`) **functionally correct but extremely slow**. This is a major bottleneck requiring an optimized implementation (e.g., broadword techniques).
    *   **Fastmod (`FastModU64`):** The 64-bit implementation, while aiming for correctness, may be slower than C++ intrinsics and needs performance/precision verification against C++.

*   **Concurrency Concerns:**
    *   **Parallel XOR Search (`searchParallelXOR`):** The current mutex-based implementation attempts to mirror complex C++ logic but is **highly questionable under contention** and may have race conditions or deadlocks. It needs rigorous testing (`go test -race ./...`) and likely a rewrite using more idiomatic Go concurrency patterns (e.g., channels, sharded locks).

*   **Incompleteness & Verification:**
    *   **Serialization:** While top-level types have marshal methods, many underlying components (`D1Array`, advanced encoders, possibly complex bucketers) have incomplete or untested serialization, preventing robust save/load.
    *   **Elias-Fano Encoder:** While structurally present, its dependency on the slow `D1Array.Select` limits its usability, particularly for the `freeSlots` mechanism needed for truly minimal PHFs.
    *   **Additive Search Precision:** Potential precision issues for large table sizes need verification.
    *   **Hasher Differences:** Minor differences in seeding (`XXHash128Hasher`) compared to C++ might exist.
    *   **Generics/Reflection:** Use of reflection for generic instantiation (`TableBucketer`) adds some overhead.

*   **Tooling & Benchmarking:**
    *   The `cmd/build` and `cmd/example` tools are basic placeholders.
    *   Benchmarking infrastructure and comparison scripts (`make_markdown_table.py`) are not ported.

**In summary:** The internal memory core is functional but requires significant work on performance (esp. `D1Array.Select`), concurrency correctness (`searchParallelXOR`), feature completeness (external memory, encoders), and tooling to be a fully equivalent and robust port of the C++ PTHash library.

## Building

```bash
# Ensure Go 1.18+ is installed

# Get dependencies (currently just xxhash)
go get

# Build commands and run tests
make all

# Or just build the executables (currently placeholders)
make build

# Run tests (recommended with race detector)
make test-race
Use code with caution.
Markdown
Usage (Illustrative Example)
(Note: This example uses components that might be placeholders or have performance limitations, e.g., RiceEncoder depends on the slow D1Array).

package main

import (
	"fmt"
	"log"
	"runtime"

	"pthashgo/internal/builder" // Use internal components directly for example
	"pthashgo/internal/core"
	"pthashgo/internal/util"
	"pthashgo/pkg/pthash"
)

func main() {
	// 1. Define keys
	numKeys := uint64(50000)
	keys := util.DistinctUints64(numKeys, 12345)

	// 2. Define generic types for a specific PHF configuration
	type K = uint64
	type H = core.XXHash128Hasher[K]  // Hasher
	type B = core.SkewBucketer        // Bucketer (must match builder's B)
	type E = core.RiceEncoder         // Encoder (currently slow due to D1Array)

	// 3. Configure build
	config := core.DefaultBuildConfig()
	config.Verbose = true
	config.NumThreads = runtime.NumCPU()
	config.Seed = 9876
	config.Minimal = true        // Target minimal PHF
	config.Search = core.SearchTypeXOR // Use XOR search

	// Example: Build a Single (non-partitioned) PHF
	log.Println("Building Single PHF...")

	// 4. Create builder instance (needs concrete types)
	hasher := core.NewXXHash128Hasher[K]()
	bucketer := new(B) // Use pointer if methods have pointer receiver
	builderInst := builder.NewInternalMemoryBuilderSinglePHF[K, H, *B](hasher, bucketer) // Pass pointer type *B

	// 5. Run the builder steps
	_, err := builderInst.BuildFromKeys(keys, config)
	if err != nil {
		// Handle potential SeedRuntimeError by trying a different seed if needed
		log.Fatalf("BuildFromKeys failed: %v", err)
	}

	// 6. Create the final PHF from the builder
	log.Println("Creating final PHF structure...")
	phf := pthash.NewSinglePHF[K, H, *B, *E](config.Minimal, config.Search) // Pass pointer types *B, *E
	_, err = phf.Build(builderInst, &config)
	if err != nil {
		log.Fatalf("phf.Build failed: %v", err)
	}

	log.Printf("Build complete. NumKeys: %d, TableSize: %d, Bits/Key: %.2f\n",
		phf.NumKeys(), phf.TableSize(), float64(phf.NumBits())/float64(phf.NumKeys()))

	// 7. Use the function (example lookup)
	log.Println("Performing lookups...")
	errorCount := 0
	correctCount := 0
	seen := make(map[uint64]bool, phf.NumKeys())

	// WARNING: Minimal check might fail/panic currently due to placeholder EliasFano
	if phf.IsMinimal() && core.IsEliasFanoStubbed() { // Need IsEliasFanoStubbed() helper
		log.Println("WARNING: Skipping full minimal check due to incomplete EliasFano.")
	}

	for i, key := range keys {
		hashValue := phf.Lookup(key)
		// fmt.Printf("Key: %d -> Hash: %d\n", key, hashValue) // Verbose

		// Check for minimal property errors (if EF was implemented)
		if phf.IsMinimal() && !core.IsEliasFanoStubbed() {
			if hashValue >= phf.NumKeys() {
				fmt.Printf("  ERROR: Minimal check failed! Hash %d >= NumKeys %d for Key %d\n", hashValue, phf.NumKeys(), key)
				errorCount++
			} else if seen[hashValue] {
				fmt.Printf("  ERROR: Minimal check failed! Duplicate position %d for Key %d\n", hashValue, key)
				errorCount++
			} else {
				seen[hashValue] = true
				correctCount++
			}
		} else if !phf.IsMinimal() { // Non-minimal check
			if hashValue >= phf.TableSize() {
				fmt.Printf("  ERROR: Non-minimal check failed! Hash %d >= TableSize %d for Key %d\n", hashValue, phf.TableSize(), key)
				errorCount++
			} else if seen[hashValue] {
				fmt.Printf("  ERROR: Non-minimal check failed! Duplicate position %d for Key %d\n", hashValue, key)
				errorCount++
			} else {
				seen[hashValue] = true
				correctCount++
			}
		}

		if i > 100 && errorCount == 0 && phf.IsMinimal() && core.IsEliasFanoStubbed() { // Stop early if EF is stubbed
			break
		}
	}
	if phf.IsMinimal() && !core.IsEliasFanoStubbed() {
		expectedSum := uint64(0)
		if n := phf.NumKeys(); n > 0 {
			if n%2 == 0 { expectedSum = (n / 2) * (n - 1) } else { expectedSum = n * ((n - 1) / 2) }
		}
		actualSum := uint64(0)
		for p := range seen { actualSum += p} // Check sum if possible
		if len(seen) == int(phf.NumKeys()) && actualSum != expectedSum {
			log.Printf("WARNING: Minimal check sum mismatch (Got: %d, Want: %d)", actualSum, expectedSum)
			// Don't necessarily fail the test, could be overflow, but log it.
		}
	}

	if errorCount > 0 {
		log.Printf("ERRORS DETECTED during check! Correct count: %d/%d", correctCount, phf.NumKeys())
	} else if !(phf.IsMinimal() && core.IsEliasFanoStubbed()) {
		log.Printf("Basic correctness check passed for %d keys.", correctCount)
	} else {
        log.Printf("Lookups performed (minimal check skipped due to EliasFano stub).")
    }
}
Use code with caution.
Go
Based on Research
This is a Go port of the PTHash C++ library, which is based on the following research papers:

PTHash: Revisiting FCH Minimal Perfect Hashing (SIGIR 2021)

Parallel and External-Memory Construction of Minimal Perfect Hash Functions with PTHash (TKDE 2023)

PHOBIC: Perfect Hashing with Optimized Bucket Sizes and Interleaved Coding (ESA 2024)

Please, cite these papers if you use this library.

License
MIT License - see the LICENSE file for details (assuming MIT based on original repo, license file should be added).

Contributing
Contributions are welcome! Please feel free to submit a Pull Request or open an Issue, particularly focusing on the incomplete or potentially problematic areas outlined in the "Current Development Status" section. Rigorous testing (especially with -race) and performance benchmarking are highly valuable.

