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
