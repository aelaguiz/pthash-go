package core

import (
	"fmt"
	"math"
	"os"
	"runtime"
	"time"
)

// Constants matching C++ version
const (
	InvalidSeed       = uint64(math.MaxUint64)
	InvalidNumBuckets = uint64(math.MaxUint64)
	MinPartitionSize  = 1000
	MaxPartitionSize  = 5000 // For dense partitioning constraint
	DefaultTmpDirname = "."
	ConstA            = 0.6 // skew_bucketer related
	ConstB            = 0.3 // dual encoder related
)

// AvailableRAM attempts to estimate available RAM (simple version).
// Go doesn't have a direct equivalent to sysconf(_SC_PHYS_PAGES).
// This is a placeholder; real applications might need Cgo or platform-specific code,
// or rely on user input/container limits.
func AvailableRAM() uint64 {
	// Very rough estimate - use 75% of total system memory reported by runtime
	// This is NOT the same as free/available RAM.
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	totalRAM := mem.Sys // Total bytes obtained from system
	if totalRAM == 0 {
		return 4 * 1024 * 1024 * 1024 // Default to 4GB if stats are unavailable
	}
	return uint64(float64(totalRAM) * 0.75)
}

// SearchType defines the pilot search algorithm.
type SearchType int

const (
	SearchTypeXOR SearchType = iota
	SearchTypeAdd
)

// BuildTimings stores timings for different build stages.
type BuildTimings struct {
	PartitioningMicroseconds    time.Duration
	MappingOrderingMicroseconds time.Duration
	SearchingMicroseconds       time.Duration
	EncodingMicroseconds        time.Duration
}

// BuildConfig holds parameters for building the PHF.
type BuildConfig struct {
	Lambda            float64 // Avg. bucket size
	Alpha             float64 // Load factor
	Search            SearchType
	AvgPartitionSize  uint64 // 0 for single PHF
	NumBuckets        uint64 // Overrides lambda if set != InvalidNumBuckets
	NumThreads        int
	Seed              uint64 // Use InvalidSeed for random
	RAM               uint64 // Max RAM to use (bytes)
	TmpDir            string
	SecondarySort     bool // Used in C++ merge, maybe relevant in Go sort
	DensePartitioning bool
	Minimal           bool
	Verbose           bool
}

// DefaultBuildConfig creates a configuration with default values.
func DefaultBuildConfig() BuildConfig {
	return BuildConfig{
		Lambda:            4.5,
		Alpha:             0.98,
		Search:            SearchTypeXOR,
		AvgPartitionSize:  0,
		NumBuckets:        InvalidNumBuckets,
		NumThreads:        runtime.NumCPU(),
		Seed:              InvalidSeed,
		RAM:               uint64(float64(AvailableRAM()) * 0.75),
		TmpDir:            os.TempDir(), // Use system temp dir by default
		SecondarySort:     false,        // Default from C++ internal builder
		DensePartitioning: false,
		Minimal:           true,
		Verbose:           true,
	}
}

// ComputeAvgPartitionSize adjusts and validates the average partition size.
func ComputeAvgPartitionSize(numKeys uint64, config *BuildConfig) uint64 {
	avgPartitionSize := config.AvgPartitionSize
	fmt.Printf("[DEBUG] ComputeAvgPartitionSize: Input numKeys=%d, config.Avg=%d, config.Dense=%t\n", numKeys, config.AvgPartitionSize, config.DensePartitioning)
	if avgPartitionSize == 0 { // Not partitioned explicitly
		return 0
	}

	updated := false
	if avgPartitionSize < MinPartitionSize {
		avgPartitionSize = MinPartitionSize
		updated = true
	}
	if config.DensePartitioning && avgPartitionSize > MaxPartitionSize {
		avgPartitionSize = MaxPartitionSize
		updated = true
	}
	if numKeys < avgPartitionSize {
		avgPartitionSize = numKeys // Cannot be larger than total keys
		updated = true
	}

	if updated && config.Verbose {
		// Consider using a logger here
		// fmt.Printf("Warning: Adjusted avg_partition_size to %d\n", avgPartitionSize)
	}
	fmt.Printf("[DEBUG] ComputeAvgPartitionSize: Returning %d\n", avgPartitionSize)
	return avgPartitionSize
}

// ComputeNumBuckets calculates the number of buckets based on keys and lambda.
func ComputeNumBuckets(numKeys uint64, avgBucketSize float64) uint64 {
	if avgBucketSize == 0.0 {
		// Handle error or panic, division by zero
		panic("average bucket size cannot be zero")
	}
	return uint64(math.Ceil(float64(numKeys) / avgBucketSize))
}

// ComputeNumPartitions calculates the number of partitions.
func ComputeNumPartitions(numKeys uint64, avgPartitionSize uint64) uint64 {
	fmt.Printf("[DEBUG] ComputeNumPartitions: Input numKeys=%d, avgPartitionSize=%d\n", numKeys, avgPartitionSize)
	if avgPartitionSize == 0 {
		return 0 // Or 1? C++ returns >= 1. Let's return 0 if not partitioned.
	}
	result := uint64(math.Ceil(float64(numKeys) / float64(avgPartitionSize)))
	fmt.Printf("[DEBUG] ComputeNumPartitions: Calculated partitions=%d\n", result)
	return result
}

// MinimalTableSize calculates the target table size based on config.
func MinimalTableSize(numKeys uint64, alpha float64, search SearchType) uint64 {
	if alpha == 0 { // Avoid division by zero
		return numKeys // Or some other sensible default/error
	}
	tableSize := uint64(float64(numKeys) / alpha)
	if search == SearchTypeXOR && (tableSize&(tableSize-1)) == 0 && tableSize > 0 {
		tableSize++
	}
	// Ensure table size is at least numKeys if minimal
	if tableSize < numKeys {
		// This can happen if alpha > 1, which should be caught earlier,
		// but also due to float precision for alpha very close to 1.
		tableSize = numKeys
	}
	return tableSize
}
