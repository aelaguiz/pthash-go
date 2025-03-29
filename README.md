```markdown
# PTHash-Go

[![Go Reference](https://pkg.go.dev/badge/github.com/aelaguiz/pthash-go.svg)](https://pkg.go.dev/github.com/aelaguiz/pthash-go)

A Go implementation of [PTHash](https://github.com/jermp/pthash) - a library for fast and compact minimal perfect hash functions.

## What is a Minimal Perfect Hash Function?

Given a set *S* of *n* distinct keys, a function *f* that bijectively maps the keys of *S* into the first *n* natural numbers is called a *minimal perfect hash function* (MPHF) for *S*.

These functions are valuable for:

*   Search engines and databases to quickly assign identifiers to static sets of variable-length keys
*   Efficient lookups in hash tables without collisions
*   Memory-efficient data structures for large key sets
*   Applications where space efficiency and fast lookups are critical

## Features (Targeted)

*(Note: Some features like external memory and all encoders/search types are still under development).*

*   **Minimal Perfect Hash Functions**: Map keys to a minimal perfect range `[0, n-1]`. (Non-minimal variant possible but not primary focus).
*   **Efficiency**: Aims for fast lookups with compact space usage, comparable to the C++ implementation.
*   **Configurable**: Offers different trade-offs between construction time, lookup time, and space via Hasher, Bucketer, Encoder, and build parameters (`lambda`, `alpha`).
*   **Go-focused**: Native Go implementation using interfaces, generics, and goroutines for concurrency.
*   **Test Coverage**: Includes unit and integration tests to ensure correctness against C++ logic where possible.

## Current Development Status

**Status:** Under Development (Internal Memory Partitioned PHF Core Complete)

The port is progressing through phases. The core logic for building **internal memory** single and partitioned minimal perfect hash functions is largely implemented.

### Development Phases

1.  ✅ Foundation & Core Utilities (`fastmod`, `hasher`, `bitvector`, `types`, `config`)
2.  ✅ Bucketers (`Skew`, `Uniform`, `Range`, `Opt`, `Table`) & Basic Encoders (`Rice`, `Compact`)
3.  ✅ Internal Memory Single Builder - Core Logic (`map`, `merge`, `search` (XOR), `fillFreeSlots`)
4.  ✅ Internal Memory Single PHF Type (`SinglePHF`, basic tests, serialization structure)
5.  ✅ Partitioning Foundation (`RangeBucketer`, partitioned builder structure)
6.  ✅ Internal Memory Partitioned PHF (`BuildFromKeys`, `BuildSubPHFs`, `PartitionedPHF` type, basic tests, serialization structure)
7.  ◻️ Dense Partitioning / PHOBIC (`DensePartitionedPHF`, `DenseEncoder` wrappers, `DiffEncoder`) - *Partially Done (wrappers exist)*
8.  ◻️ Advanced Encoders & Optimization (`D1Array`, `EliasFano`, `Dictionary`, `SDC`, `Dual`, performance tuning) - *Partially Done (`D1Array`/`EF` placeholders exist)*
9.  ◻️ External Memory Construction
10. ◻️ Tooling, Benchmarking & Finalization (`cmd/build` completion, benchmarks, Python script port)

## Known Issues / Areas of Concern

*   **Parallel Search Implementation:** The current parallel XOR search (`searchParallelXOR`) attempts to mirror the complex C++ retry/locking strategy. This Go implementation might be prone to race conditions, deadlocks, or suboptimal performance. It requires rigorous testing with `-race` and potentially a redesign using more idiomatic Go concurrency patterns (e.g., channel-based work dispatch).
*   **Placeholder `D1Array` (Rank/Select):** The `internal/core/d1array.go` implementation uses slow linear scans. This makes encoders relying on it (like `RiceEncoder`) functionally correct for basic tests but **prohibitively slow** for real use. A proper, efficient Rank/Select data structure is needed.
*   **Placeholder/Missing `EliasFano`:** The `internal/core/encoder.go` contains only a stub for `EliasFano`. This blocks the minimal mapping functionality (`freeSlots`) in the final PHF types.
*   **Missing Additive Search:** The Additive Displacement search algorithm (`SearchTypeAdd`) logic is missing from `internal/builder/search.go` and PHF `Lookup` methods.
*   **(Minor) Additive Search Math (in `SinglePHF.Lookup`):** The placeholder logic for Additive Search uses `uint32` casts on the `pilot` variable, which might deviate slightly from the C++ intermediate calculations using `uint64_t`. This needs verification once Additive Search is fully implemented.
*   **Missing Encoders:** Advanced encoders like `Dictionary`, `SDC`, `PartitionedCompact`, and `Dual` wrappers (`R-R`, `D-D`, etc.) are not yet ported. Dense encoders depend on base encoders like `CompactEncoder`.
*   **Missing External Memory:** External memory construction logic is not yet ported.
*   **Incomplete Tooling:** `cmd/build` can parse flags but lacks the core dispatch logic to build the specified PHF configuration. The example (`cmd/example`) is a placeholder. The Python script for result table generation is not ported.
*   **Incomplete Serialization:** While the structure for `MarshalBinary`/`UnmarshalBinary` exists for PHF types, several underlying components (encoders, bucketers) have TODOs, making full serialization non-functional currently.

## Building

```bash
# Build commands and run tests
make all

# Or just build the commands
make build
```

## Usage

```go
// Example using SinglePHF with implemented components (XXHash, SkewBucketer, RiceEncoder).
// Note: Performance of RiceEncoder is currently limited by placeholder D1Array.
package main

import (
	"fmt"
	"log"

	"pthashgo/internal/core"
	"pthashgo/internal/builder"
	"pthashgo/internal/util"
	"pthashgo/pkg/pthash"
)

func main() {
	// 1. Define keys
	keys := util.DistinctUints64(10000, 12345) // Use uint64 keys for this example

	// 2. Define generic types
	type K = uint64
	type H = core.XXHash128Hasher[K]
	type B = core.SkewBucketer
	type E = core.RiceEncoder // Depends on D1Array placeholder currently

	// 3. Configure build
	config := core.DefaultBuildConfig()
	config.Verbose = true
	config.NumThreads = 1 // Use sequential for simplicity
	config.Seed = 9876

	// 4. Create builder instance (needs concrete types)
	hasher := core.NewXXHash128Hasher[K]()
	bucketer := new(B) // Pointer type for SkewBucketer methods
	builder := builder.NewInternalMemoryBuilderSinglePHF[K, H, *B](hasher, bucketer)

	// 5. Run the builder steps
	log.Println("Building internal structure...")
	_, err := builder.BuildFromKeys(keys, config)
	if err != nil {
		log.Fatalf("BuildFromKeys failed: %v", err)
	}

	// 6. Create the final PHF from the builder
	log.Println("Creating final PHF...")
	phf := pthash.NewSinglePHF[K, H, *B, *E](config.Minimal, config.Search) // Pass pointer types *B, *E
	_, err = phf.Build(builder, &config)
	if err != nil {
		log.Fatalf("phf.Build failed: %v", err)
	}

	log.Printf("Build complete. NumKeys: %d, Bits/Key: %.2f\n", phf.NumKeys(), float64(phf.NumBits())/float64(phf.NumKeys()))

	// 7. Use the function (example lookup)
	log.Println("Performing lookups...")
	errorCount := 0
	for i, key := range keys {
		hashValue := phf.Lookup(key)
		fmt.Printf("Key: %d -> Hash: %d\n", key, hashValue)
		// Basic check for minimal property (position should be < numKeys)
		if config.Minimal && hashValue >= phf.NumKeys() {
			fmt.Printf("  ERROR: Minimal check failed! Hash >= NumKeys\n")
			errorCount++
		}
		if i > 10 && errorCount == 0 { // Stop printing after a few successful lookups if no errors
			fmt.Println("... (lookups continue)")
			break
		}
	}
	if errorCount > 0 {
		log.Printf("ERRORS DETECTED during minimal check!")
	} else {
		log.Println("Basic minimal check passed (for first few keys).")
	}
}
```

## Based on Research

This is a Go port of the PTHash C++ library, which is based on the following research papers:

*   [*PTHash: Revisiting FCH Minimal Perfect Hashing*](https://dl.acm.org/doi/10.1145/3404835.3462849) (SIGIR 2021)
*   [*Parallel and External-Memory Construction of Minimal Perfect Hash Functions with PTHash*](https://ieeexplore.ieee.org/document/10210677) (TKDE 2023)
*   [*PHOBIC: Perfect Hashing with Optimized Bucket Sizes and Interleaved Coding*](https://drops.dagstuhl.de/entities/document/10.4230/LIPIcs.ESA.2024.69) (ESA 2024)

**Please, cite these papers if you use this library.**

## License

MIT License - see the LICENSE file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request or open an Issue, especially regarding the known issues listed above.
```