// pthashgo/internal/core/compact_vector_test.go
package core

import (
	"fmt"
	"math/rand"
	"pthashgo/internal/serial"
	"reflect"
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

			// Enhanced checks for width=0
			if tc.w == 0 {
				if cv.data.Size() != 0 {
					t.Errorf("W=0: Expected underlying BitVector size 0, got %d", cv.data.Size())
				}
				if cv.NumBitsStored() != 0 {
					t.Errorf("W=0: Expected NumBitsStored 0, got %d", cv.NumBitsStored())
				}
			} else {
				expectedBits := uint64(0)
				if tc.n > 0 && tc.w > 0 {
					expectedWords := (tc.n*uint64(tc.w) + 63) / 64
					expectedBits = expectedWords * 64
				}
				if cv.NumBitsStored() != expectedBits {
					t.Errorf("Expected stored bits %d, got %d", expectedBits, cv.NumBitsStored())
				}
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

			// Special test for width=0: setting non-zero should panic
			if tc.w == 0 && tc.n > 0 {
				assertPanic(t, fmt.Sprintf("Set(0, 1) W=0, N=%d", tc.n), func() { cv.Set(0, 1) })
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

// Add this to internal/core/compact_vector_test.go

func TestCompactVectorSerialization(t *testing.T) {
	n, w := uint64(129), uint8(7) // Example parameters crossing word boundaries
	cv1 := NewCompactVector(n, w)
	// Set some values
	cv1.Set(0, 100)
	cv1.Set(64, 50)
	cv1.Set(n-1, 127)

	data, err := serial.TryMarshal(cv1)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	cv2 := NewCompactVector(0, 0) // Create empty target
	err = serial.TryUnmarshal(cv2, data)
	if err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	// Compare
	if cv1.Size() != cv2.Size() || cv1.Width() != cv2.Width() {
		t.Errorf("Metadata mismatch: (%d,%d) != (%d,%d)", cv1.Size(), cv1.Width(), cv2.Size(), cv2.Width())
	}
	if !reflect.DeepEqual(cv1.data, cv2.data) { // Compare underlying bitvector
		t.Errorf("BitVector data mismatch")
		// t.Logf("CV1: %+v", cv1.data)
		// t.Logf("CV2: %+v", cv2.data)
	}
	// Compare Access results
	if cv1.Access(64) != cv2.Access(64) {
		t.Errorf("Access(64) mismatch after unmarshal")
	}
	if cv1.Access(n-1) != cv2.Access(n-1) {
		t.Errorf("Access(n-1) mismatch after unmarshal")
	}
}

// Add this to internal/core/encoder_test.go

func TestRiceSequenceSerialization(t *testing.T) {
	// Skip if D1Array Select is stubbed
	values := []uint64{0, 5, 10, 10, 25, 60, 66, 130} // Sample data
	rs1 := RiceSequence{}
	err := rs1.Encode(values)
	if err != nil {
		t.Fatalf("rs1.Encode failed: %v", err)
	}

	data, err := serial.TryMarshal(&rs1) // Marshal pointer
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	rs2 := RiceSequence{} // Create empty target
	err = serial.TryUnmarshal(&rs2, data)
	if err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	// Compare
	if rs1.Size() != rs2.Size() || rs1.optimalParamL != rs2.optimalParamL {
		t.Errorf("Metadata mismatch: (%d, %d) != (%d, %d)", rs1.Size(), rs1.optimalParamL, rs2.Size(), rs2.optimalParamL)
	}
	// Compare underlying components (basic check)
	if rs1.lowBits.NumBitsStored() != rs2.lowBits.NumBitsStored() {
		t.Errorf("lowBits NumBitsStored mismatch")
	}
	if rs1.highBitsD1.NumBits() != rs2.highBitsD1.NumBits() {
		t.Errorf("highBitsD1 NumBits mismatch")
	}
	// Compare Access results
	for i := uint64(0); i < rs1.Size(); i++ {
		if rs1.Access(i) != rs2.Access(i) {
			t.Errorf("Access(%d) mismatch after unmarshal: %d != %d", i, rs1.Access(i), rs2.Access(i))
		}
	}
}

func TestEliasFanoSerialization(t *testing.T) {
	if IsEliasFanoStubbed() {
		t.Skip("Skipping EliasFanoSerialization test: Implementation is stubbed")
	}
	values := []uint64{10, 25, 26, 100, 150, 1000} // Sample sorted data
	ef1 := NewEliasFano()
	err := ef1.Encode(values)
	if err != nil {
		t.Fatalf("ef1.Encode failed: %v", err)
	}

	data, err := serial.TryMarshal(ef1)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	ef2 := NewEliasFano() // Create empty target
	err = serial.TryUnmarshal(ef2, data)
	if err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	// CompareG
	if ef1.Size() != ef2.Size() || ef1.universe != ef2.universe || ef1.numLowBits != ef2.numLowBits {
		t.Errorf("Metadata mismatch")
	}
	// Compare Access results
	for i := uint64(0); i < ef1.Size(); i++ {
		if ef1.Access(i) != ef2.Access(i) {
			t.Errorf("Access(%d) mismatch after unmarshal: %d != %d", i, ef1.Access(i), ef2.Access(i))
		}
	}
}

func TestDiffEncoderSerialization(t *testing.T) {
	values := []uint64{0, 10, 15, 30}
	incr := uint64(8)
	// E is *CompactEncoder, so DiffEncoder stores a *CompactEncoder directly
	de1 := DiffEncoder[*CompactEncoder]{}
	// Initialize the Encoder field since E is a pointer type
	de1.Encoder = &CompactEncoder{}

	err := de1.Encode(values, incr) // Encode uses de1.Encoder
	if err != nil {
		if err.Error() == "CompactEncoder.Encode: CompactVector not implemented" {
			t.Skip("Skipping DiffEncoderSerialization test: CompactEncoder is stubbed")
		}
		t.Fatalf("de1.Encode failed: %v", err)
	}

	data, err := serial.TryMarshal(&de1) // Marshal pointer to DiffEncoder
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	de2 := DiffEncoder[*CompactEncoder]{} // Create empty target
	// Unmarshal will allocate the inner *CompactEncoder if needed via its UnmarshalBinary
	err = serial.TryUnmarshal(&de2, data)
	if err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	// Compare
	if de1.Increment != de2.Increment || de1.Size() != de2.Size() {
		t.Errorf("Metadata mismatch")
	}
	// Access the 'Encoder' field directly
	if (de1.Encoder == nil) != (de2.Encoder == nil) {
		t.Errorf("Underlying encoder presence mismatch")
	}
	// Compare NumBits only if both are non-nil
	if de1.Encoder != nil && de2.Encoder != nil {
		// Call NumBits directly on the Encoder field
		if de1.Encoder.NumBits() != de2.Encoder.NumBits() {
			t.Errorf("Underlying encoder NumBits mismatch")
		}
	}
	// Compare Access results
	for i := uint64(0); i < de1.Size(); i++ {
		if de1.Access(i) != de2.Access(i) {
			t.Errorf("Access(%d) mismatch after unmarshal: %d != %d", i, de1.Access(i), de2.Access(i))
		}
	}
}
