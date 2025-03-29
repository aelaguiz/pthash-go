package builder

import (
	"pthashgo/internal/core"
	"sort"
	"testing"
)

func TestPilotSearch(t *testing.T) {
	// Create minimal test data based on actual values
	tableSize := uint64(1020)
	mTableSize := core.ComputeM64(tableSize)
	seed := uint64(12345)

	// Payloads from the problematic bucket in the logs
	payloads := []uint64{
		33770903594394249, 49276032860695964, 50011429118829857,
		66645594591238275, 76147522730065663, 81469312820681203,
	}

	// Try a reasonable number of pilots
	maxPilots := uint64(10000)
	foundValidPilot := false

	for pilot := uint64(0); pilot < maxPilots; pilot++ {
		// Get hashed pilot
		hashedPilot := core.DefaultHash64(pilot, seed)

		// Calculate positions for each payload
		positions := make(map[uint64]bool)
		collision := false

		for _, payload := range payloads {
			pos := core.FastModU64(payload^hashedPilot, mTableSize, tableSize)

			// Check if this position was already seen (in-bucket collision)
			if positions[pos] {
				collision = true
				break
			}
			positions[pos] = true
		}

		if !collision {
			t.Logf("Found valid pilot: %d (after trying %d pilots)", pilot, pilot+1)
			foundValidPilot = true
			break
		}

		// Log progress occasionally
		if pilot > 0 && pilot%1000 == 0 {
			t.Logf("Tried %d pilots, still searching...", pilot)
		}
	}

	if !foundValidPilot {
		t.Errorf("Could not find a valid pilot in %d attempts - this suggests a problem with the algorithm", maxPilots)
	}
}

// pthash-go/internal/builder/search_test.go (add this test)

func TestPilotSearchIsolatedBucket226(t *testing.T) {
	// Data from the failing log
	// seed := uint64(12345)      // Seed from logs seems to be related to DefaultHash64 pilot hashing
	configSeed := uint64(42)   // Main seed from the failing test run
	tableSize := uint64(53191) // From the log details
	payloads := []uint64{
		150743299263303226, 180833034105839476, 1034953631228210476, 1365720959623264726,
		1935437411159900476, 2470577845671041476, 3134857234262600476, 3329664059196887476,
		3337135848095143726, 3605631379142296726, 3630869643680651476, 3985838339522500726,
		4578737909950872976, 4674602149595520976, 5176501392146664226, 5197226853450125476,
		5544374839435022476, 5751390635581456726, 5959052526039385726, 6152026478314959226,
		6521203221836316976, 6886849009241775976, 7087267011864212476, 7218846133201245226,
		7693663053998000476, 7858132056672397726, 8142448119047095726, 8352279777661668226,
		8855320146890157976, 9146487601552249726, 9351083512717059226, 9466314801453678226,
		9773046394645235476, 9786208415397817726, 9790234746976968226, 10078832262437277976,
		10104414886772225476, 10668830387995093726, 10781307305675926726, 11053690597411179976,
	}
	bucketID := core.BucketIDType(226) // From logs

	mTableSize := core.ComputeM64(tableSize)
	hashedPilotsCache := make([]uint64, searchCacheSize)
	for i := range hashedPilotsCache {
		hashedPilotsCache[i] = core.DefaultHash64(uint64(i), configSeed) // Use main config seed for pilot hashing
	}

	positions := make([]uint64, 0, len(payloads))
	foundPilot := int64(-1) // Store the successful pilot, -1 if none found

	// Limit attempts for the test
	const testMaxAttempts = 50_000_000 // Use the same limit as search

	for pilot := uint64(0); pilot < testMaxAttempts; pilot++ {
		hashedPilot := uint64(0)
		if pilot < searchCacheSize {
			hashedPilot = hashedPilotsCache[pilot]
		} else {
			hashedPilot = core.DefaultHash64(pilot, configSeed)
		}

		positions = positions[:0]
		inBucketCollision := false
		for _, pld := range payloads {
			p := core.FastModU64(pld^hashedPilot, mTableSize, tableSize)
			positions = append(positions, p)
		}

		sort.Slice(positions, func(i, j int) bool { return positions[i] < positions[j] })
		for i := 1; i < len(positions); i++ {
			if positions[i] == positions[i-1] {
				inBucketCollision = true
				break
			}
		}

		if !inBucketCollision {
			foundPilot = int64(pilot)
			break // Found a pilot that works *in isolation*
		}
	}

	if foundPilot != -1 {
		t.Logf("Bucket %d IS solvable in isolation: found pilot %d", bucketID, foundPilot)
		// If this passes, the problem is likely collisions with the 'taken' set,
		// potentially caused by concurrency issues (#4a) or iteration order (#5)
		// or just bad luck with this seed (#6).
	} else {
		t.Errorf("Bucket %d IS NOT solvable in isolation within %d attempts. Suggests issue with hash/mod or true seed failure.", bucketID, testMaxAttempts)
		// If this fails, the problem is more likely #1, #2, or #6.
	}
}
