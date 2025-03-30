package builder

import (
	"pthashgo/internal/core"
	"sync"
	"testing"
)

// TestConcurrentBitVectorBuilderRace demonstrates the race condition
// that happens in the parallel search implementation
func TestConcurrentBitVectorBuilderRace(t *testing.T) {
	// Create a BitVectorBuilder
	builder := core.NewBitVectorBuilder(1000)
	
	// Set up two goroutines to simulate the race
	var wg sync.WaitGroup
	wg.Add(2)
	
	// Worker 1: Reads without lock
	go func() {
		defer wg.Done()
		for i := uint64(0); i < 100; i++ {
			// This simulates the unlocked reads in searchParallelXOR
			_ = builder.Get(i) 
			// Note: No mutex protection, like in the original code
		}
	}()
	
	// Worker 2: Writes with lock (but the lock doesn't help since Worker 1 doesn't use it)
	go func() {
		defer wg.Done()
		var mu sync.Mutex
		for i := uint64(0); i < 100; i++ {
			// This simulates the locked writes in searchParallelXOR
			mu.Lock()
			builder.Set(i)
			mu.Unlock()
		}
	}()
	
	wg.Wait()
	// Run this test with: go test -race ./internal/builder -run TestConcurrentBitVectorBuilderRace
}
