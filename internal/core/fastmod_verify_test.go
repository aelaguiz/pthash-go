// pthash-go/internal/core/fastmod_verify_test.go
package core

import (
	"fmt"
	"testing"
)

// --- Verification Tests based on Python Calculations ---

func TestComputeM64_Verification(t *testing.T) {
	testCases := []struct {
		d        uint64
		expected M64
	}{
		// PASTE_M64_CASES_HERE_START
		// Test case for d = 1
		{
			d:        1,
			expected: M64{0x0000000000000000, 0x0000000000000000}, // M_128 = 0x100000000000000000000000000000000
		},
		// Test case for d = 1020
		{
			d:        1020,
			expected: M64{0x4040404040404041, 0x0040404040404040}, // M_128 = 0x00404040404040404040404040404041
		},
		// Test case for d = 4294967295
		{
			d:        4294967295,
			expected: M64{0x0000000100000002, 0x0000000100000001}, // M_128 = 0x00000001000000010000000100000002
		},
		// Test case for d = 9223372036854775807
		{
			d:        9223372036854775807,
			expected: M64{0x0000000000000005, 0x0000000000000002}, // M_128 = 0x00000000000000020000000000000005
		},
		// Test case for d = 18446744073709551615
		{
			d:        18446744073709551615,
			expected: M64{0x0000000000000002, 0x0000000000000001}, // M_128 = 0x00000000000000010000000000000002
		},
		// PASTE_M64_CASES_HERE_END
	}

	// --- Test execution logic remains the same ---
	if len(testCases) == 0 {
		t.Skip("No M64 verification test cases found/inserted.")
	}
	for _, tc := range testCases {
		t.Run(fmt.Sprintf("d=%d", tc.d), func(t *testing.T) {
			actual := ComputeM64(tc.d)
			if actual != tc.expected {
				t.Errorf("ComputeM64(%d)\n  got: M64{Low:0x%016x, High:0x%016x}\n want: M64{Low:0x%016x, High:0x%016x}",
					tc.d, actual[0], actual[1], tc.expected[0], tc.expected[1])
			}
		})
	}
}

func TestMul128U64_Verification(t *testing.T) {
	testCases := []struct {
		name         string // Add a name field for clarity
		InputLowHi   uint64 // Renamed for consistency
		InputLowLo   uint64 // Renamed for consistency
		InputD       uint64 // Renamed for consistency
		expectedHigh uint64
	}{
		// PASTE_MUL128_CASES_HERE_START
		// Test case: 1*5
		{
			name:         "1*5",
			InputLowHi:   0x0000000000000000,
			InputLowLo:   0x0000000000000001,
			InputD:       0x0000000000000005,
			expectedHigh: 0x0000000000000000, // Product192 = 0x000000000000000000000000000000000000000000000005
		},
		// Test case: (1<<64)*5
		{
			name:         "(1<<64)*5",
			InputLowHi:   0x0000000000000001,
			InputLowLo:   0x0000000000000000,
			InputD:       0x0000000000000005,
			expectedHigh: 0x0000000000000000, // Product192 = 0x000000000000000000000000000000050000000000000000
		},
		// Test case: (2^64-1)*2
		{
			name:         "(2^64-1)*2",
			InputLowHi:   0x0000000000000000,
			InputLowLo:   0xffffffffffffffff,
			InputD:       0x0000000000000002,
			expectedHigh: 0x0000000000000000, // Product192 = 0x00000000000000000000000000000001fffffffffffffffe
		},
		// Test case: (2^63)*2
		{
			name:         "(2^63)*2",
			InputLowHi:   0x0000000000000000,
			InputLowLo:   0x8000000000000000,
			InputD:       0x0000000000000002,
			expectedHigh: 0x0000000000000000, // Product192 = 0x000000000000000000000000000000010000000000000000
		},
		// Test case: (2^64+2^63)*2
		{
			name:         "(2^64+2^63)*2",
			InputLowHi:   0x0000000000000001,
			InputLowLo:   0x8000000000000000,
			InputD:       0x0000000000000002,
			expectedHigh: 0x0000000000000000, // Product192 = 0x000000000000000000000000000000030000000000000000
		},
		// Test case: Large values
		{
			name:         "Large values",
			InputLowHi:   0xabcdef0123456789,
			InputLowLo:   0x9876543210fedcba,
			InputD:       0x1122334455667788,
			expectedHigh: 0x0b7fa0a0b41f260c, // Product192 = 0x0b7fa0a0b41f260cb0209e42846d85431ff714bd341bb8d0
		},
		// Test case: Max values
		{
			name:         "Max values",
			InputLowHi:   0xffffffffffffffff,
			InputLowLo:   0xffffffffffffffff,
			InputD:       0xffffffffffffffff,
			expectedHigh: 0xfffffffffffffffe, // Product192 = 0xfffffffffffffffeffffffffffffffff0000000000000001
		},
		// PASTE_MUL128_CASES_HERE_END
	}

	// --- Test execution logic remains the same ---
	if len(testCases) == 0 {
		t.Skip("No mul128_u64 verification test cases found/inserted.")
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actualHigh := mul128_u64(tc.InputLowHi, tc.InputLowLo, tc.InputD) // Use renamed fields
			if actualHigh != tc.expectedHigh {
				t.Errorf("mul128_u64(0x%x, 0x%x, 0x%x)\n  got high: 0x%016x\n want high: 0x%016x",
					tc.InputLowHi, tc.InputLowLo, tc.InputD, actualHigh, tc.expectedHigh)
			}
		})
	}
}

func TestFastModU64_Verification(t *testing.T) {
	testCases := []struct {
		InputA      uint64 // Renamed for consistency
		InputD      uint64 // Renamed for consistency
		M           M64    // Use M64 struct
		ExpectedMod uint64 // Renamed for consistency
	}{
		// PASTE_FASTMOD64_CASES_HERE_START
		// Test case for a = 0, d = 1020
		{
			InputA:      0,
			InputD:      1020,
			M:           M64{0x4040404040404041, 0x0040404040404040},
			ExpectedMod: 0,
		},
		// Test case for a = 1, d = 1020
		{
			InputA:      1,
			InputD:      1020,
			M:           M64{0x4040404040404041, 0x0040404040404040},
			ExpectedMod: 1,
		},
		// Test case for a = 1019, d = 1020
		{
			InputA:      1019,
			InputD:      1020,
			M:           M64{0x4040404040404041, 0x0040404040404040},
			ExpectedMod: 1019,
		},
		// Test case for a = 1020, d = 1020
		{
			InputA:      1020,
			InputD:      1020,
			M:           M64{0x4040404040404041, 0x0040404040404040},
			ExpectedMod: 0,
		},
		// Test case for a = 1021, d = 1020
		{
			InputA:      1021,
			InputD:      1020,
			M:           M64{0x4040404040404041, 0x0040404040404040},
			ExpectedMod: 1,
		},
		// Test case for a = 33770903594394249, d = 1020
		{
			InputA:      33770903594394249,
			InputD:      1020,
			M:           M64{0x4040404040404041, 0x0040404040404040},
			ExpectedMod: 9,
		},
		// Test case for a = 49276032860695964, d = 1020
		{
			InputA:      49276032860695964,
			InputD:      1020,
			M:           M64{0x4040404040404041, 0x0040404040404040},
			ExpectedMod: 224,
		},
		// Test case for a = 0, d = 9223372036854775807
		{
			InputA:      0,
			InputD:      9223372036854775807,
			M:           M64{0x0000000000000005, 0x0000000000000002},
			ExpectedMod: 0,
		},
		// Test case for a = 1234567890123456789, d = 9223372036854775807
		{
			InputA:      1234567890123456789,
			InputD:      9223372036854775807,
			M:           M64{0x0000000000000005, 0x0000000000000002},
			ExpectedMod: 1234567890123456789,
		},
		// Test case for a = 9223372036854775806, d = 9223372036854775807
		{
			InputA:      9223372036854775806,
			InputD:      9223372036854775807,
			M:           M64{0x0000000000000005, 0x0000000000000002},
			ExpectedMod: 9223372036854775806,
		},
		// Test case for a = 9223372036854775807, d = 9223372036854775807
		{
			InputA:      9223372036854775807,
			InputD:      9223372036854775807,
			M:           M64{0x0000000000000005, 0x0000000000000002},
			ExpectedMod: 0,
		},
		// Test case for a = 9223372036854775808, d = 9223372036854775807
		{
			InputA:      9223372036854775808,
			InputD:      9223372036854775807,
			M:           M64{0x0000000000000005, 0x0000000000000002},
			ExpectedMod: 1,
		},
		// Test case for a = 18446744073709551615, d = 9223372036854775807
		{
			InputA:      18446744073709551615,
			InputD:      9223372036854775807,
			M:           M64{0x0000000000000005, 0x0000000000000002},
			ExpectedMod: 1,
		},
		// PASTE_FASTMOD64_CASES_HERE_END
	}

	// --- Test execution logic remains the same ---
	if len(testCases) == 0 {
		t.Skip("No FastModU64 verification test cases found/inserted.")
	}
	for _, tc := range testCases {
		t.Run(fmt.Sprintf("a=%d,d=%d", tc.InputA, tc.InputD), func(t *testing.T) {
			// M is now directly part of the test case struct
			actualMod := FastModU64(tc.InputA, tc.M, tc.InputD)
			if actualMod != tc.ExpectedMod {
				t.Errorf("FastModU64(%d, M, %d)\n  got mod: %d\n want mod: %d\n  M={Low:0x%016x, High:0x%016x}",
					tc.InputA, tc.InputD, actualMod, tc.ExpectedMod, tc.M[0], tc.M[1])
			}

			// Also verify against standard modulo
			// Skip std check if d is 0 (although ComputeM64 should panic)
			if tc.InputD != 0 {
				stdMod := tc.InputA % tc.InputD
				if actualMod != stdMod {
					t.Errorf("FastModU64 != standard %% (%d vs %d)", actualMod, stdMod)
				}
			} else if actualMod != 0 { // Modulo 0 is undefined, fastmod might return 0 or panic
				t.Errorf("FastModU64 with d=0 should result in 0 or panic, got %d", actualMod)
			}
		})
	}
}
