// pthashgo/internal/core/compact_vector_test.go
package core

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

func TestCompactVectorBasic(t *testing.T) {
	tests := []struct {
		n uint64
		w uint8
	}{
		{0, 0}, {0, 8}, {10, 0}, {10, 1}, {10, 7}, {10, 8},
		{100, 3}, {100, 15}, {100, 64},
		{64, 10}, {65, 10}, // Boundary cases around words
		{128, 7}, {129, 7},
	}

	for _, tc := range tests {
		t.Run(fmt.Sprintf("N=%d,W=%d", tc.n, tc.w), func(t *testing.T) {
			cv := NewCompactVector(tc.n, tc.w)
			if cv.Size() != tc.n {
				t.Errorf("Expected size %d, got %d", tc.n, cv.Size())
			}
			if cv.width != tc.w {
				t.Errorf("Expected width %d, got %d", tc.w, cv.width)
			}
			expectedBits := uint64(0)
			if tc.n > 0 && tc.w > 0 {
				expectedWords := (tc.n*uint64(tc.w) + 63) / 64
				expectedBits = expectedWords * 64
			}
			if cv.NumBitsStored() != expectedBits {
				t.Errorf("Expected stored bits %d, got %d", expectedBits, cv.NumBitsStored())
			}

			// Test setting and getting zeros (should always work)
			for i := uint64(0); i < tc.n; i++ {
				if cv.Access(i) != 0 {
					t.Errorf("Initial value at index %d not zero", i)
				}
				cv.Set(i, 0) // Should not panic
				if cv.Access(i) != 0 {
					t.Errorf("Value at index %d not zero after setting zero", i)
				}
			}
		})
	}
}

func TestCompactVectorSetGet(t *testing.T) {
	seed := time.Now().UnixNano()
	rng := rand.New(rand.NewSource(seed))
	t.Logf("Using seed: %d", seed)

	n := uint64(150) // Test across a few words

	for width := uint8(1); width <= 64; width++ {
		t.Run(fmt.Sprintf("Width=%d", width), func(t *testing.T) {
			cv := NewCompactVector(n, width)
			originalValues := make([]uint64, n)
			maxValue := uint64(1) << width // Max value is 2^width - 1

			// Set random values within the width
			for i := uint64(0); i < n; i++ {
				val := rng.Uint64()
				if width < 64 {
					val %= maxValue // Ensure value fits width
				}
				originalValues[i] = val
				// Test Set
				func() {
					defer func() {
						if r := recover(); r != nil {
							t.Fatalf("Panic during Set(idx=%d, val=%d) with width %d: %v", i, val, width, r)
						}
					}()
					cv.Set(i, val)
				}()
			}

			// Verify values with Access
			for i := uint64(0); i < n; i++ {
				accessedVal := uint64(0)
				func() {
					defer func() {
						if r := recover(); r != nil {
							t.Fatalf("Panic during Access(idx=%d) with width %d: %v", i, width, r)
						}
					}()
					accessedVal = cv.Access(i)
				}()
				if accessedVal != originalValues[i] {
					t.Errorf("Width %d: Access(%d) got %d, want %d", width, i, accessedVal, originalValues[i])
					// Try to dump relevant words for debugging multi-word issues
					if i > 0 && (i*uint64(width))/64 != ((i-1)*uint64(width))/64 { // Boundary case
						pos := i * uint64(width)
						wordIdx := pos / 64
						if wordIdx > 0 {
							t.Logf("  Word[%d-1]: %064b", wordIdx-1, cv.data.bits[wordIdx-1])
						}
						if wordIdx < uint64(len(cv.data.bits)) {
							t.Logf("  Word[%d]:   %064b", wordIdx, cv.data.bits[wordIdx])
						}
						if wordIdx+1 < uint64(len(cv.data.bits)) {
							t.Logf("  Word[%d+1]: %064b", wordIdx+1, cv.data.bits[wordIdx+1])
						}
					}
				}
			}
		})
	}
}

func TestCompactVectorPanics(t *testing.T) {
	// Test width 0 set non-zero
	cv0 := NewCompactVector(10, 0)
	assertPanic(t, "Set(0, 1) W=0", func() { cv0.Set(0, 1) })

	// Test width > 64
	assertPanic(t, "NewCompactVector W=65", func() { NewCompactVector(10, 65) })

	// Test out of bounds access/set
	cv1 := NewCompactVector(10, 8)
	assertPanic(t, "Access(10)", func() { cv1.Access(10) })
	assertPanic(t, "Set(10, 0)", func() { cv1.Set(10, 0) })

	// Test setting value too large for width
	cv2 := NewCompactVector(10, 4) // Max value is 15
	assertPanic(t, "Set(0, 16) W=4", func() { cv2.Set(0, 16) })
	cv2.Set(0, 15) // Should not panic
}

// Helper to assert panics
func assertPanic(t *testing.T, name string, f func()) {
	t.Helper()
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("Function %s did not panic as expected", name)
		}
	}()
	f()
}
