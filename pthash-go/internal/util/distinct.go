package util

import (
	"log"
	"math/rand"
	"sort"
	"time"

	"pthash-go/internal/core"
)

// DistinctUints64 generates numKeys distinct uint64 values using the given seed.
func DistinctUints64(numKeys uint64, seed uint64) []uint64 {
	if numKeys == 0 {
		return []uint64{}
	}

	var src rand.Source
	if seed == core.InvalidSeed {
		src = rand.NewSource(time.Now().UnixNano())
	} else {
		src = rand.NewSource(int64(seed))
	}
	rng := rand.New(src)

	// Oversample slightly to increase chance of getting enough unique keys
	oversampleFactor := 1.05
	if numKeys > (1<<64)/2 { // Avoid overflow if numKeys is huge
		oversampleFactor = 1.01
	}
	allocSize := uint64(float64(numKeys) * oversampleFactor)
	if allocSize < numKeys+10 { // Ensure at least a small buffer
		allocSize = numKeys + 10
	}
	if allocSize == 0 { // Handle case where numKeys is very small
	    allocSize = numKeys
	}


	keys := make([]uint64, allocSize)
	Log(true, "Generating %d random numbers (target %d distinct)...", allocSize, numKeys)
	for i := range keys {
		keys[i] = rng.Uint64()
	}

	Log(true, "Sorting and finding unique numbers...")
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })

	// In-place unique
	uniqueCount := 0
	if len(keys) > 0 {
		uniqueCount = 1
		for i := 1; i < len(keys); i++ {
			if keys[i] != keys[i-1] {
				keys[uniqueCount] = keys[i]
				uniqueCount++
			}
		}
	}
	keys = keys[:uniqueCount] // Keep only unique keys found so far

	Log(true, "Found %d unique keys initially.", uniqueCount)

	// Fill gaps if needed (unlikely for large random uint64 space, but matches C++)
	if uint64(uniqueCount) < numKeys {
		log.Printf("Warning: Needed to fill gaps to reach %d keys (had %d)", numKeys, uniqueCount)
		needed := numKeys - uint64(uniqueCount)
		keys = append(keys, make([]uint64, needed)...) // Allocate space for missing keys

		currentIdx := uniqueCount
		for i := 0; i < uniqueCount-1 && uint64(currentIdx) < numKeys; i++ {
			first := keys[i]
			second := keys[i+1]
			// Iterate between first+1 and second, adding values
			for val := first + 1; val < second && uint64(currentIdx) < numKeys; val++ {
				keys[currentIdx] = val
				currentIdx++
			}
		}

		// If still not enough (e.g., gaps were small or only one initial key), add from the end
		lastVal := uint64(0)
		if uniqueCount > 0 {
			lastVal = keys[uniqueCount-1]
		}
		for uint64(currentIdx) < numKeys {
		    lastVal++ // Note: Potential for overflow if lastVal was MaxUint64
		    // Very basic check to avoid immediate duplicate of the very last element if gaps failed
		    if uniqueCount > 0 && lastVal == keys[uniqueCount-1] {
		        lastVal++
		    }
			keys[currentIdx] = lastVal
			currentIdx++
		}
		// We might have duplicates if we added sequentially, re-unique and trim
		sort.Slice(keys[:currentIdx], func(i, j int) bool { return keys[i] < keys[j] })
		finalUniqueCount := 0
        if currentIdx > 0 {
            finalUniqueCount = 1
            for i := 1; i < currentIdx; i++ {
                if keys[i] != keys[i-1] {
                    keys[finalUniqueCount] = keys[i]
                    finalUniqueCount++
                }
            }
        }

		if uint64(finalUniqueCount) < numKeys {
		    // This should be extremely rare with uint64 unless numKeys is near 2^64
		    log.Printf("ERROR: Could not generate enough distinct keys after filling gaps. Required: %d, Got: %d", numKeys, finalUniqueCount)
		    // Decide how to handle: return error, panic, or return fewer keys?
		    // For now, trim to what we have.
		    keys = keys[:finalUniqueCount]
		} else {
		    keys = keys[:numKeys] // Trim precisely to numKeys
		}


	} else {
		// Had enough or more unique keys initially
		keys = keys[:numKeys] // Trim to exactly numKeys
	}

	Log(true, "Shuffling final %d keys...", len(keys))
	rng.Shuffle(len(keys), func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })

	Log(true, "Key generation complete.")
	return keys
}
