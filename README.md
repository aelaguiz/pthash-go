# PTHash-Go

[![Go Reference](https://pkg.go.dev/badge/github.com/aelaguiz/pthash-go.svg)](https://pkg.go.dev/github.com/aelaguiz/pthash-go)

A Go implementation of [PTHash](https://github.com/jermp/pthash) - a library for fast and compact minimal perfect hash functions.

## What is a Minimal Perfect Hash Function?

Given a set *S* of *n* distinct keys, a function *f* that bijectively maps the keys of *S* into the first *n* natural numbers is called a *minimal perfect hash function* (MPHF) for *S*. 

These functions are valuable for:
- Search engines and databases to quickly assign identifiers to static sets of variable-length keys
- Efficient lookups in hash tables without collisions
- Memory-efficient data structures for large key sets
- Applications where space efficiency and fast lookups are critical

## Features

- **Minimal Perfect Hash Functions**: Map keys to a minimal perfect range
- **Efficiency**: Fast lookups with compact space usage
- **Configurable**: Different trade-offs between construction time, lookup time, and space
- **Go-focused**: Native Go implementation with idiomatic code
- **Test Coverage**: Comprehensive test suite to ensure correctness

## Current Development Status

**Status:** Under development (Phase 6 Complete)

### Development Phases

1. ✅ Foundation & Core Utilities
2. ✅ Bucketers & Simple Encoder
3. ✅ Internal Memory Single Builder - Core Logic
4. ✅ Internal Memory Single PHF & Basic Testing
5. ✅ Partitioning Foundation
6. ✅ Internal Memory Partitioned PHF
7. ◻️ Dense Partitioning / PHOBIC
8. ◻️ Advanced Encoders & Optimization
9. ◻️ External Memory
10. ◻️ Tooling, Benchmarking & Finalization

## Building

```bash
make build
```

## Usage

```go
// Basic usage (future API)
package main

import (
    "fmt"
    "github.com/aelaguiz/pthash-go/pkg/pthash"
)

func main() {
    // This is a simplified example of planned usage
    builder := pthash.NewSingleBuilder()
    
    // Add keys
    keys := []string{"apple", "banana", "cherry", "date", "elderberry"}
    for _, key := range keys {
        builder.AddKey([]byte(key))
    }
    
    // Build the function
    phf, err := builder.Build()
    if err != nil {
        panic(err)
    }
    
    // Use the function
    for _, key := range keys {
        hash := phf.GetHash([]byte(key))
        fmt.Printf("%s -> %d\n", key, hash)
    }
}
```

## Based on Research

This is a Go port of the PTHash C++ library, which is based on the following research papers:

- [*PTHash: Revisiting FCH Minimal Perfect Hashing*](https://dl.acm.org/doi/10.1145/3404835.3462849) (SIGIR 2021)
- [*Parallel and External-Memory Construction of Minimal Perfect Hash Functions with PTHash*](https://ieeexplore.ieee.org/document/10210677) (TKDE 2023)
- [*PHOBIC: Perfect Hashing with Optimized Bucket Sizes and Interleaved Coding*](https://drops.dagstuhl.de/entities/document/10.4230/LIPIcs.ESA.2024.69) (ESA 2024)

## License

See the LICENSE file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.