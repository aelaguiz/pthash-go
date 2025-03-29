// pthashgo/internal/builder/search_concurrency_test.go
package builder

import (
	"math/rand"
	"pthashgo/internal/core"
	"runtime"
	"sync"
	"testing"
)

// TestBitVectorBuilderConcurrency uses multiple goroutines to concurrently
// call Get and Set on the same BitVectorBuilder instance.
// Run with 'go test -race ./...' to detect data races.
// This simulates the problematic access pattern in searchParallelXOR's
// optimistic read + locked write approach IF the builder itself is not atomic.
func TestBitVectorBuilderConcurrency(t *testing.T) {
	const numBits = 100000
	const numGoroutines = 8
	const numOpsPerGoroutine = 5000

	builder := core.NewBitVectorBuilder(numBits)

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for g := 0; g < numGoroutines; g++ {
		go func(gid int) {
			defer wg.Done()
			// Use gid as a pseudo-random seed for this goroutine
			localSeed := int64(gid + 1)
			rng := rand.New(rand.NewSource(localSeed))

			for i := 0; i < numOpsPerGoroutine; i++ {
				pos := uint64(rng.Int63n(numBits)) // Random position

				// Simulate interleaved read/write access pattern
				if rng.Intn(2) == 0 {
					// Simulate read (optimistic check in parallel search)
					_ = builder.Get(pos)
				} else {
					// Simulate write (setting taken bit in parallel search)
					// Note: The real parallel search has a lock *around* Set,
					// but the race happens because Get is done *without* the lock.
					// This test simulates concurrent Get and Set without ANY lock
					// on the builder itself, which highlights the builder's non-thread-safety.
					builder.Set(pos)
				}
				// Yield occasionally to increase chance of race conditions
				if i%100 == 0 {
					runtime.Gosched()
				}
			}
		}(g)
	}

	wg.Wait()

	// The test primarily relies on the -race detector.
	// No specific output validation needed here, just checking for race reports.
	t.Logf("BitVectorBuilder concurrency test finished. Check for 'DATA RACE' reports if run with -race.")
	// Basic sanity check on final size
	if builder.Size() > numBits*2 { // Size might grow beyond numBits if Set is called near end
		t.Logf("Builder final size: %d (capacity %d)", builder.Size(), builder.Capacity())
	}
}
