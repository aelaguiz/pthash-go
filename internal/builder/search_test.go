package builder

import (
	"pthashgo/internal/core"
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
