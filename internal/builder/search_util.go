package builder

import (
	"pthashgo/internal/core" // Assuming core has Bucketer interface for estimate
	"pthashgo/internal/util"
	"time"
)

const searchCacheSize = 1000

// SearchLogger logs progress during the pilot search phase.
type SearchLogger struct {
	numKeys     uint64
	numBuckets  uint64
	step        uint64
	placedKeys  uint64
	timer       time.Time
	enabled     bool
	lastLogTime time.Time
	lastBucket  uint64
}

// NewSearchLogger creates a logger for the search phase.
// Pass a Bucketer instance to help estimate steps more accurately if needed, otherwise nil.
func NewSearchLogger[B core.Bucketer](numKeys, numBuckets uint64, bucketer B, enabled bool) *SearchLogger {
	sl := &SearchLogger{
		numKeys:    numKeys,
		numBuckets: numBuckets,
		enabled:    enabled,
	}
	if enabled {
		// Estimate step based on numBuckets (similar to C++)
		sl.step = 1
		if numBuckets > 20 {
			sl.step = numBuckets / 20
		}
		// TODO: Could potentially refine step based on bucketer type/distribution if needed
	}
	return sl
}

// Init starts the timer and logs the start message.
func (sl *SearchLogger) Init() {
	if !sl.enabled {
		return
	}
	util.Log(true, "Search Start")
	sl.timer = time.Now()
	sl.lastLogTime = sl.timer
}

// Update logs progress if a step boundary is crossed.
func (sl *SearchLogger) Update(bucketIdx uint64, bucketSize core.BucketSizeType) {
	if !sl.enabled {
		return
	}
	sl.placedKeys += uint64(bucketSize)
	// Throttle logging to avoid excessive output
	now := time.Now()
	if bucketIdx > 0 && (bucketIdx%sl.step == 0 || bucketIdx == sl.numBuckets-1) && now.Sub(sl.lastLogTime) > 100*time.Millisecond {
		sl.print(bucketIdx)
		sl.lastLogTime = now
	}
}

// Finalize logs the end message and summary statistics.
func (sl *SearchLogger) Finalize(processedBuckets uint64) {
	if !sl.enabled {
		return
	}
	// Ensure final stats are printed even if last update was skipped
	sl.print(processedBuckets)
	util.Log(true, "Search End")
	emptyBuckets := sl.numBuckets - processedBuckets
	emptyPerc := 0.0
	if sl.numBuckets > 0 {
		emptyPerc = float64(emptyBuckets*100) / float64(sl.numBuckets)
	}
	util.Log(true, " == %d empty buckets (%.2f%%)", emptyBuckets, emptyPerc)
}

// print logs the current progress status.
func (sl *SearchLogger) print(currentBucket uint64) {
	elapsed := time.Since(sl.timer)
	stepBuckets := currentBucket - sl.lastBucket
	if stepBuckets == 0 && currentBucket != 0 { // Avoid printing 0 buckets done if called multiple times at end
		return
	}

	keysPerc := 0.0
	if sl.numKeys > 0 {
		keysPerc = float64(sl.placedKeys*100) / float64(sl.numKeys)
	}
	bucketsPerc := 0.0
	if sl.numBuckets > 0 {
		bucketsPerc = float64((currentBucket+1)*100) / float64(sl.numBuckets) // +1 because index is 0-based
	}

	// Simple log message, not replicating the \r update precisely
	util.Log(true, "  Processed ~%d buckets in %.2fs (Total Keys: %.2f%%, Total Buckets: %.2f%%)",
		stepBuckets, elapsed.Seconds(), keysPerc, bucketsPerc)

	// Reset timer and count for next interval measurement
	sl.timer = time.Now()
	sl.lastBucket = currentBucket
}
