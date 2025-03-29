// pthash-go/internal/builder/internal_partitioned_test.go
package builder

import (
	"fmt"
	"pthashgo/internal/core"
	"pthashgo/internal/util"
	"runtime"
	"testing"
)

// Helper function to create a partitioned builder for testing.
func createPartitionedBuilder[K any, H core.Hasher[K], B core.Bucketer]() *InternalMemoryBuilderPartitionedPHF[K, H, B] {
	var hasher H
	// This builder doesn't actually USE the sub-bucketer B during partitioning phase,
	// only the sub-builders created will hold it. So its zero value is okay here.
	return NewInternalMemoryBuilderPartitionedPHF[K, H, B](hasher)
}

// Helper to run build and perform common checks.
// Returns the calculated partition sizes for offset verification.
func runPartitionBuild(t *testing.T, config core.BuildConfig, keys []uint64) (*InternalMemoryBuilderPartitionedPHF[uint64, core.XXHash128Hasher[uint64], *core.SkewBucketer], [][]core.Hash128, error) {
	t.Helper()
	t.Logf("[TEST_DEBUG] runPartitionBuild called with numKeys=%d, config.AvgPartitionSize=%d", len(keys), config.AvgPartitionSize)
	if config.Seed == core.InvalidSeed {
		config.Seed = 1 // Use a fixed default seed for deterministic tests
	}

	// Need concrete types for the test builder instance
	type K = uint64
	type H = core.XXHash128Hasher[K]
	type B = *core.SkewBucketer // Sub-builder bucketer type, now using pointer

	hasher := core.NewXXHash128Hasher[K]()
	builder := NewInternalMemoryBuilderPartitionedPHF[K, H, B](hasher)

	// --- Call the partitioning part of BuildFromKeys ---
	// We need to slightly modify BuildFromKeys or extract the partitioning logic
	// to test it without triggering sub-builds. Let's simulate the partitioning part.

	numKeys := uint64(len(keys))
	builder.config = config
	builder.numKeys = numKeys
	builder.seed = config.Seed

	avgPartitionSize := core.ComputeAvgPartitionSize(numKeys, &config)
	builder.avgPartSize = avgPartitionSize
	builder.numPartitions = core.ComputeNumPartitions(numKeys, avgPartitionSize)
	if builder.numPartitions == 0 {
		builder.numPartitions = 1
	}
	
	// The only truly invalid case is if no partitions would be created after adjustments
	if numKeys > 0 && builder.numPartitions == 0 {
		return builder, nil, fmt.Errorf("test setup error: calculated numPartitions is 0 for numKeys %d, avgPartitionSize %d", numKeys, avgPartitionSize)
	}

	// Initialize partitioner
	err := builder.partitioner.Init(builder.numPartitions, 0, 0, 0)
	if err != nil {
		return builder, nil, fmt.Errorf("failed to init partitioner: %w", err)
	}

	// Partition keys
	partitionBuffers := make([][]core.Hash128, builder.numPartitions)
	for i := range partitionBuffers {
		allocHint := uint64(float64(avgPartitionSize) * 1.1)
		if allocHint == 0 {
			allocHint = 10
		}
		partitionBuffers[i] = make([]core.Hash128, 0, allocHint)
	}
	// Use parallel partitioning if requested and feasible
	if config.NumThreads > 1 && numKeys >= uint64(config.NumThreads)*100 {
		builder.parallelHashAndPartition(keys, partitionBuffers)
	} else {
		for _, key := range keys {
			hash := builder.hasher.Hash(key, builder.seed)
			partitionIdx := builder.partitioner.Bucket(hash.Mix())
			if partitionIdx >= builder.numPartitions {
				return builder, nil, fmt.Errorf("partition index %d out of bounds (%d)", partitionIdx, builder.numPartitions)
			}
			partitionBuffers[partitionIdx] = append(partitionBuffers[partitionIdx], hash)
		}
	}

	// Calculate offsets and initialize sub-builders (stubs for this test)
	builder.offsets = make([]uint64, builder.numPartitions+1)
	builder.subBuilders = make([]*InternalMemoryBuilderSinglePHF[K, H, B], builder.numPartitions)
	builder.tableSize = 0 // Total size across all partitions
	builder.numBucketsPerPartition = core.ComputeNumBuckets(avgPartitionSize, config.Lambda)

	cumulativeOffset := uint64(0)
	for i := uint64(0); i < builder.numPartitions; i++ {
		partitionSize := uint64(len(partitionBuffers[i]))
		subTableSize := uint64(0)
		if partitionSize > 0 {
			subTableSize = core.MinimalTableSize(partitionSize, config.Alpha, config.Search)
		}
		builder.tableSize += subTableSize
		builder.offsets[i] = cumulativeOffset

		offsetIncrement := uint64(0)
		if config.DensePartitioning {
			offsetIncrement = subTableSize
		} else {
			if config.Minimal {
				offsetIncrement = partitionSize
			} else {
				offsetIncrement = subTableSize
			}
		}
		cumulativeOffset += offsetIncrement

		// Create placeholder sub-builder
		builder.subBuilders[i] = NewInternalMemoryBuilderSinglePHF[K, H, B](builder.hasher, new(core.SkewBucketer))
		// We don't build it here
	}
	builder.offsets[builder.numPartitions] = cumulativeOffset

	return builder, partitionBuffers, nil
}

// TestPartitioningBasic tests basic partitioning logic and counts.
func TestPartitioningBasic(t *testing.T) {
	numKeys := uint64(10000)
	avgPartitionSize := uint64(2000)
	expectedPartitions := uint64(5) // 10000 / 2000
	keys := util.DistinctUints64(numKeys, 123)

	config := core.DefaultBuildConfig()
	config.AvgPartitionSize = avgPartitionSize
	config.NumThreads = 1 // Test sequential first
	config.Verbose = false

	builder, buffers, err := runPartitionBuild(t, config, keys)
	if err != nil {
		t.Fatalf("runPartitionBuild failed: %v", err)
	}

	// --- Assertions ---
	if builder.NumPartitions() != expectedPartitions {
		t.Errorf("Expected %d partitions, got %d", expectedPartitions, builder.NumPartitions())
	}
	if len(builder.Builders()) != int(expectedPartitions) {
		t.Errorf("Expected %d sub-builders, got %d", expectedPartitions, len(builder.Builders()))
	}
	if len(builder.Offsets()) != int(expectedPartitions)+1 {
		t.Errorf("Expected %d offsets, got %d", expectedPartitions+1, len(builder.Offsets()))
	}

	// Check total keys partitioned
	totalKeysInBuffers := uint64(0)
	for i, buf := range buffers {
		totalKeysInBuffers += uint64(len(buf))
		if len(buf) == 0 {
			t.Logf("Partition %d is empty", i)
		}
	}
	if totalKeysInBuffers != numKeys {
		t.Errorf("Total keys in partitions (%d) does not match input numKeys (%d)", totalKeysInBuffers, numKeys)
	}

	// Basic check on first and last offset
	if builder.Offsets()[0] != 0 {
		t.Errorf("First offset should be 0, got %d", builder.Offsets()[0])
	}
	// Last offset depends on config, check later
}

// TestPartitioningParallel checks if parallel partitioning yields the same total keys.
func TestPartitioningParallel(t *testing.T) {
	numKeys := uint64(50000) // Larger set for parallelism
	avgPartitionSize := uint64(5000)
	expectedPartitions := uint64(10)
	keys := util.DistinctUints64(numKeys, 456)

	config := core.DefaultBuildConfig()
	config.AvgPartitionSize = avgPartitionSize
	config.NumThreads = runtime.NumCPU() // Use multiple threads
	if config.NumThreads < 2 {
		t.Skip("Skipping parallel test: only 1 CPU available")
	}
	config.Verbose = false

	builder, buffers, err := runPartitionBuild(t, config, keys)
	if err != nil {
		t.Fatalf("runPartitionBuild failed: %v", err)
	}

	// --- Assertions ---
	if builder.NumPartitions() != expectedPartitions {
		t.Errorf("Expected %d partitions, got %d", expectedPartitions, builder.NumPartitions())
	}
	// Check total keys partitioned
	totalKeysInBuffers := uint64(0)
	for _, buf := range buffers {
		totalKeysInBuffers += uint64(len(buf))
	}
	if totalKeysInBuffers != numKeys {
		t.Errorf("Total keys in partitions (%d) does not match input numKeys (%d)", totalKeysInBuffers, numKeys)
	}
	t.Logf("Parallel partitioning completed with %d threads.", config.NumThreads)
}

// TestPartitioningOffsetCalculation tests offset calculation under different modes.
func TestPartitioningOffsetCalculation(t *testing.T) {
	numKeys := uint64(2000)
	avgPartitionSize := uint64(500)
	numPartitions := uint64(4)
	keys := util.DistinctUints64(numKeys, 789)

	scenarios := []struct {
		name             string
		minimal          bool
		dense            bool
		expectedOffsetFn func(partSizes []uint64, alpha float64) []uint64 // Function to calculate expected offsets
	}{
		{
			name:    "Minimal=T_Dense=F",
			minimal: true,
			dense:   false,
			expectedOffsetFn: func(partSizes []uint64, alpha float64) []uint64 {
				offsets := make([]uint64, len(partSizes)+1)
				cumulative := uint64(0)
				for i, size := range partSizes {
					offsets[i] = cumulative
					cumulative += size // Offset increments by actual partition size
				}
				offsets[len(partSizes)] = cumulative
				return offsets
			},
		},
		{
			name:    "Minimal=F_Dense=F",
			minimal: false,
			dense:   false,
			expectedOffsetFn: func(partSizes []uint64, alpha float64) []uint64 {
				offsets := make([]uint64, len(partSizes)+1)
				cumulative := uint64(0)
				for i, size := range partSizes {
					offsets[i] = cumulative
					subTableSize := uint64(0)
					if size > 0 {
						// Use MinimalTableSize as it handles XOR power-of-2 adjustment
						subTableSize = core.MinimalTableSize(size, alpha, core.SearchTypeXOR) // Search type doesn't matter much for size calc here
					}
					cumulative += subTableSize // Offset increments by sub-table size
				}
				offsets[len(partSizes)] = cumulative
				return offsets
			},
		},
		{
			name:    "Minimal=T_Dense=T", // Dense overrides minimal for offset calc
			minimal: true,
			dense:   true,
			expectedOffsetFn: func(partSizes []uint64, alpha float64) []uint64 {
				// Same as Minimal=F_Dense=T or Minimal=F_Dense=F
				offsets := make([]uint64, len(partSizes)+1)
				cumulative := uint64(0)
				for i, size := range partSizes {
					offsets[i] = cumulative
					subTableSize := uint64(0)
					if size > 0 {
						subTableSize = core.MinimalTableSize(size, alpha, core.SearchTypeXOR)
					}
					cumulative += subTableSize // Offset increments by sub-table size
				}
				offsets[len(partSizes)] = cumulative
				return offsets
			},
		},
	}

	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			config := core.DefaultBuildConfig()
			config.AvgPartitionSize = avgPartitionSize
			config.NumThreads = 1
			config.Minimal = scenario.minimal
			config.DensePartitioning = scenario.dense
			config.Alpha = 0.9 // Use a fixed alpha for predictability
			config.Verbose = false

			builder, buffers, err := runPartitionBuild(t, config, keys)
			if err != nil {
				t.Fatalf("runPartitionBuild failed: %v", err)
			}

			if builder.NumPartitions() != numPartitions {
				t.Fatalf("Expected %d partitions, got %d", numPartitions, builder.NumPartitions())
			}

			// Get actual partition sizes
			actualPartSizes := make([]uint64, numPartitions)
			for i := range buffers {
				actualPartSizes[i] = uint64(len(buffers[i]))
			}

			// Calculate expected offsets
			expectedOffsets := scenario.expectedOffsetFn(actualPartSizes, config.Alpha)
			actualOffsets := builder.Offsets()

			if len(actualOffsets) != len(expectedOffsets) {
				t.Fatalf("Offset slice length mismatch: got %d, want %d", len(actualOffsets), len(expectedOffsets))
			}

			for i := 0; i < len(expectedOffsets); i++ {
				if actualOffsets[i] != expectedOffsets[i] {
					t.Errorf("Offset mismatch at index %d: got %d, want %d", i, actualOffsets[i], expectedOffsets[i])
				}
			}
			t.Logf("Actual Offsets: %v", actualOffsets)
		})
	}
}

// TestPartitioningAccessors checks basic accessor methods.
func TestPartitioningAccessors(t *testing.T) {
	numKeys := uint64(100)
	avgPartitionSize := uint64(20)
	numPartitions := uint64(5)
	seed := uint64(999)
	keys := util.DistinctUints64(numKeys, seed)

	config := core.DefaultBuildConfig()
	config.AvgPartitionSize = avgPartitionSize
	config.NumThreads = 1
	config.Seed = seed
	config.Verbose = false

	builder, _, err := runPartitionBuild(t, config, keys)
	if err != nil {
		t.Fatalf("runPartitionBuild failed: %v", err)
	}

	if builder.Seed() != seed {
		t.Errorf("Seed() mismatch: got %d, want %d", builder.Seed(), seed)
	}
	if builder.NumKeys() != numKeys {
		t.Errorf("NumKeys() mismatch: got %d, want %d", builder.NumKeys(), numKeys)
	}
	if builder.NumPartitions() != numPartitions {
		t.Errorf("NumPartitions() mismatch: got %d, want %d", builder.NumPartitions(), numPartitions)
	}
	// Use a pointer method call for partitioner
	p := builder.Partitioner()
	if p.NumBuckets() != numPartitions {
		t.Errorf("Partitioner().NumBuckets() mismatch: got %d, want %d", p.NumBuckets(), numPartitions)
	}
	// Cannot easily check TableSize without running sub-builds fully
}
