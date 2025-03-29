package core

import (
	"testing"
)

func TestFastModU64Basic(t *testing.T) {
	tableSize := uint64(1020)
	m := ComputeM64(tableSize)
	
	// Test some basic values
	for i := uint64(0); i < 100; i++ {
		expected := i % tableSize
		actual := FastModU64(i, m, tableSize)
		if expected != actual {
			t.Errorf("FastModU64(%d, m, %d) = %d, want %d", i, tableSize, actual, expected)
		}
	}
}

// Verify that FastModU64 produces expected distribution
func TestFastModU64Distribution(t *testing.T) {
	tableSize := uint64(1020)
	m := ComputeM64(tableSize)
	seed := uint64(12345)
	pilot := uint64(1000000)
	
	// Test with real values from the logs
	payloads := []uint64{
		33770903594394249, 49276032860695964, 50011429118829857,
		66645594591238275, 76147522730065663, 81469312820681203,
	}
	
	hashedPilot := DefaultHash64(pilot, seed)
	
	// Store positions to check for duplicates
	positions := make(map[uint64]int)
	
	// For each payload, calculate position and check
	for i, payload := range payloads {
		xor := payload ^ hashedPilot
		pos := FastModU64(xor, m, tableSize)
		positions[pos]++
		
		// Also compare with standard modulo
		stdPos := xor % tableSize
		
		t.Logf("Payload[%d]=%d, XOR=%d, FastModPos=%d, StdModPos=%d", 
			i, payload, xor, pos, stdPos)
		
		// Verify FastModU64 equals standard modulo
		if pos != stdPos {
			t.Errorf("FastModU64 result differs from standard modulo: %d != %d", pos, stdPos)
		}
	}
	
	// Check for duplicates in positions
	for pos, count := range positions {
		if count > 1 {
			t.Logf("Position %d appears %d times", pos, count)
		}
	}
}
