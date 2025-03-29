// pthash-go/internal/core/config_test.go
package core

import (
	"testing"
)

func TestComputeAvgPartitionSize(t *testing.T) {
	cfg := DefaultBuildConfig()
	cfg.Verbose = false // Silence warnings during test

	tests := []struct {
		name             string
		numKeys          uint64
		avgPartitionSize uint64
		dense            bool
		expected         uint64
	}{
		{"ZeroAvg", 10000, 0, false, 0},
		{"Normal", 100000, 20000, false, 20000},
		{"SmallAvg", 100000, 500, false, MinPartitionSize},
		{"LargeAvg", 10000, 20000, false, 10000}, // Capped by numKeys
		{"DenseLargeAvg", 100000, 6000, true, MaxPartitionSize},
		{"DenseSmallAvg", 100000, 500, true, MinPartitionSize},
		{"DenseOkAvg", 100000, 3000, true, 3000},
		{"DenseLargeAvgCapped", 1000, 6000, true, 1000}, // Capped by numKeys first
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			localCfg := cfg
			localCfg.AvgPartitionSize = tt.avgPartitionSize
			localCfg.DensePartitioning = tt.dense
			if got := ComputeAvgPartitionSize(tt.numKeys, &localCfg); got != tt.expected {
				t.Errorf("ComputeAvgPartitionSize(%d, cfg{Avg:%d, Dense:%t}) = %d, want %d", tt.numKeys, tt.avgPartitionSize, tt.dense, got, tt.expected)
			}
		})
	}
}

func TestComputeNumBuckets(t *testing.T) {
	tests := []struct {
		numKeys uint64
		lambda  float64
		want    uint64
	}{
		{100, 5.0, 20},
		{101, 5.0, 21},
		{99, 5.0, 20},
		{10000, 4.3, 2326}, // ceil(10000 / 4.3)
		{0, 5.0, 0},
	}
	for _, tt := range tests {
		if got := ComputeNumBuckets(tt.numKeys, tt.lambda); got != tt.want {
			t.Errorf("ComputeNumBuckets(%d, %f) = %d, want %d", tt.numKeys, tt.lambda, got, tt.want)
		}
	}
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("ComputeNumBuckets with lambda=0 should panic")
		}
	}()
	_ = ComputeNumBuckets(100, 0.0) // Should panic
}

func TestComputeNumPartitions(t *testing.T) {
	tests := []struct {
		numKeys          uint64
		avgPartitionSize uint64
		want             uint64
	}{
		{100000, 20000, 5},
		{100001, 20000, 6},
		{99999, 20000, 5},
		{10000, 1000, 10},
		{10000, 0, 0}, // avgSize=0 means not partitioned
		{0, 1000, 0},
	}
	for _, tt := range tests {
		if got := ComputeNumPartitions(tt.numKeys, tt.avgPartitionSize); got != tt.want {
			t.Errorf("ComputeNumPartitions(%d, %d) = %d, want %d", tt.numKeys, tt.avgPartitionSize, got, tt.want)
		}
	}
}
func TestMinimalTableSize(t *testing.T) {
	tests := []struct {
		numKeys uint64
		alpha   float64
		search  SearchType
		want    uint64 // CORRECTED VALUES
	}{
		{100, 0.98, SearchTypeXOR, 102}, // uint64(100/0.98) = 102. Not pow2. Want 102.
		{100, 0.98, SearchTypeAdd, 102}, // uint64(100/0.98) = 102.
		{125, 0.98, SearchTypeXOR, 127}, // uint64(125/0.98) = 127. Not pow2. Want 127.
		{128, 1.0, SearchTypeXOR, 129},  // uint64(128/1.0) = 128. Is pow2. Want 128+1=129. (This was correct)
		{128, 1.0, SearchTypeAdd, 128},  // uint64(128/1.0) = 128. (This was correct)
		{100, 1.0, SearchTypeXOR, 100},  // uint64(100/1.0) = 100. Not pow2. Want 100.
		{100, 1.0, SearchTypeAdd, 100},  // uint64(100/1.0) = 100.
		{99, 1.01, SearchTypeXOR, 99},   // uint64(99/1.01) = 98. tableSize < numKeys, capped at 99.
	}
	for _, tt := range tests {
		// Keep the test execution logic the same
		// Use the MinimalTableSize function which should implement truncation + XOR adjust
		if got := MinimalTableSize(tt.numKeys, tt.alpha, tt.search); got != tt.want {
			t.Errorf("MinimalTableSize(%d, %f, %v) = %d, want %d", tt.numKeys, tt.alpha, tt.search, got, tt.want)
		}
	}
}
