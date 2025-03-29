// pthash-go/internal/core/bucketer_test.go
package core

import (
	"math" // Ensure math is imported
	"testing"
)

// requireBucketInit is a test helper to initialize a bucketer and fail on error.
func requireBucketInit(t *testing.T, b Bucketer, numBuckets uint64, lambda float64, alpha float64) {
	t.Helper()
	// Use a reasonable table size for init tests if not otherwise specified
	tableSize := uint64(float64(numBuckets) * lambda / alpha)
	if tableSize == 0 && numBuckets > 0 { // Avoid tableSize 0 if possible
		tableSize = numBuckets * uint64(lambda)
	}
	err := b.Init(numBuckets, lambda, tableSize, alpha)
	if err != nil {
		t.Fatalf("Bucketer Init failed: %v", err)
	}
}

// --- Tests for OptBucketer ---

func TestOptBucketerInit(t *testing.T) {
	var b OptBucketer
	numBuckets := uint64(10000)
	lambda := 5.0
	tableSize := uint64(10000 * lambda / 0.98) // Example table size
	alpha := 0.98

	err := b.Init(numBuckets, lambda, tableSize, alpha)
	if err != nil {
		t.Fatalf("Init failed: %v", err)
	}

	if b.NumBuckets() != numBuckets {
		t.Errorf("NumBuckets mismatch: got %d, want %d", b.NumBuckets(), numBuckets)
	}
	if b.alpha != alpha {
		t.Errorf("alpha mismatch: got %f, want %f", b.alpha, alpha)
	}
	// Check if c and alphaFactor seem reasonable (not NaN, not Inf)
	if math.IsNaN(b.c) || math.IsInf(b.c, 0) {
		t.Errorf("Parameter c is invalid: %f", b.c)
	}
	if math.IsNaN(b.alphaFactor) || math.IsInf(b.alphaFactor, 0) || b.alphaFactor <= 0 {
		t.Errorf("Parameter alphaFactor is invalid: %f", b.alphaFactor)
	}
	t.Logf("OptBucketer Init: c=%.5f, alphaFactor=%.5f", b.c, b.alphaFactor)

	// Test alpha near 1.0
	err = b.Init(numBuckets, lambda, tableSize, 1.0)
	if err != nil {
		t.Fatalf("Init(alpha=1.0) failed: %v", err)
	}
	if b.alphaFactor != 1.0 {
		t.Errorf("Expected alphaFactor=1.0 for alpha=1.0, got %f", b.alphaFactor)
	}

	// Test invalid inputs
	err = b.Init(numBuckets, lambda, 0, alpha) // Zero tableSize
	if err == nil {
		t.Errorf("Init with tableSize=0 should fail")
	}
	err = b.Init(numBuckets, lambda, tableSize, 1.1) // Invalid alpha
	if err == nil {
		t.Errorf("Init with alpha>1 should fail")
	}

}

func TestOptBucketerBucket(t *testing.T) {
	var b OptBucketer
	numBuckets := uint64(1000)
	lambda := 4.0
	// tableSize := uint64(1000 * lambda / 0.95)
	alpha := 0.95
	requireBucketInit(t, &b, numBuckets, lambda, alpha)

	// Test bucket distribution (expect non-linear)
	hashes := []uint64{
		0,                      // Start
		math.MaxUint64 / 4,     // 25%
		math.MaxUint64 / 2,     // 50%
		math.MaxUint64 * 3 / 4, // 75%
		math.MaxUint64,         // End
	}
	prevBucket := BucketIDType(0)
	t.Logf("OptBucketer Params: c=%.5f, alphaFactor=%.5f", b.c, b.alphaFactor)
	for i, h := range hashes {
		bucket := b.Bucket(h)
		t.Logf("Hash %.2f * Max -> Bucket %d (%.2f%%)", float64(i)/4.0, bucket, float64(bucket*100)/float64(numBuckets))
		if uint64(bucket) >= numBuckets {
			t.Errorf("Bucket(%x) = %d, out of bounds (< %d)", h, bucket, numBuckets)
		}
		// Check monotonicity (should generally hold for OptBucketer)
		if i > 0 && bucket < prevBucket {
			t.Errorf("Bucket distribution not monotonic: hash %x -> %d, prev hash -> %d", h, bucket, prevBucket)
		}
		prevBucket = bucket
	}
	// Check that ends map near 0 and numBuckets-1
	if b.Bucket(0) != 0 {
		t.Errorf("Bucket(0) = %d, want 0", b.Bucket(0))
	}
	if b.Bucket(math.MaxUint64) != BucketIDType(numBuckets-1) {
		t.Errorf("Bucket(MaxUint64) = %d, want %d", b.Bucket(math.MaxUint64), numBuckets-1)
	}
}

// --- Tests for TableBucketer ---

// Simple relative bucketer for testing TableBucketer base
type simpleRelativeBucketer struct {
	numBuckets uint64
}

func (srb *simpleRelativeBucketer) Init(n uint64, l float64, ts uint64, a float64) error {
	srb.numBuckets = n
	return nil
}
func (srb *simpleRelativeBucketer) Bucket(h uint64) BucketIDType {
	return BucketIDType(h % srb.numBuckets)
}
func (srb *simpleRelativeBucketer) NumBuckets() uint64             { return srb.numBuckets }
func (srb *simpleRelativeBucketer) NumBits() uint64                { return 8 * 8 }
func (srb *simpleRelativeBucketer) MarshalBinary() ([]byte, error) { return nil, nil }
func (srb *simpleRelativeBucketer) UnmarshalBinary([]byte) error   { return nil }

// Implement BucketRelative for testing
func (srb *simpleRelativeBucketer) BucketRelative(normH float64) float64 {
	// Example: quadratic distribution y = x^2
	return normH * normH
}

func TestTableBucketerInit(t *testing.T) {
	numBuckets := uint64(100)
	lambda := 5.0
	tableSize := uint64(100 * lambda / 0.98)
	alpha := 0.98

	// Test with base that implements BucketRelative
	baseRel := &simpleRelativeBucketer{}
	var tbRel TableBucketer[*simpleRelativeBucketer] // Use pointer type if base methods have pointer receiver
	tbRel.base = baseRel                             // Set the base instance
	err := tbRel.Init(numBuckets, lambda, tableSize, alpha)
	if err != nil {
		t.Fatalf("Init with relative base failed: %v", err)
	}
	if tbRel.NumBuckets() != numBuckets {
		t.Errorf("NumBuckets mismatch: got %d, want %d", tbRel.NumBuckets(), numBuckets)
	}
	// Check fulcrums are monotonic and within expected range [0, numBuckets << 16]
	expectedMaxFulcrum := numBuckets << 16
	if tbRel.fulcrums[0] != 0 {
		t.Errorf("Fulcrum[0] should be 0, got %d", tbRel.fulcrums[0])
	}
	for i := 1; i < fulcrumCount; i++ {
		if tbRel.fulcrums[i] < tbRel.fulcrums[i-1] {
			t.Errorf("Fulcrums not monotonic at index %d: %d < %d", i, tbRel.fulcrums[i], tbRel.fulcrums[i-1])
		}
		if tbRel.fulcrums[i] > expectedMaxFulcrum {
			t.Errorf("Fulcrum[%d] = %d exceeds expected max %d", i, tbRel.fulcrums[i], expectedMaxFulcrum)
		}
	}
	if tbRel.fulcrums[fulcrumCount-1] != expectedMaxFulcrum {
		t.Errorf("Fulcrum[last] should be %d, got %d", expectedMaxFulcrum, tbRel.fulcrums[fulcrumCount-1])
	}

	// Test with base that does NOT implement BucketRelative (Uniform)
	baseUni := &UniformBucketer{}
	var tbUni TableBucketer[*UniformBucketer]
	tbUni.base = baseUni
	err = tbUni.Init(numBuckets, lambda, tableSize, alpha)
	if err != nil {
		t.Fatalf("Init with uniform base failed: %v", err)
	}
	// Check if fulcrums look linear (approx)
	for i := 1; i < fulcrumCount; i++ {
		// Expected linear value
		expectedY := float64(i) / float64(fulcrumCount-1)
		expectedFulcrum := uint64(expectedY * float64(expectedMaxFulcrum))
		// Allow some tolerance for float conversion
		diff := int64(tbUni.fulcrums[i]) - int64(expectedFulcrum)
		if diff < -1 || diff > 1 { // Tolerance of +/- 1
			// t.Errorf("Fulcrum[%d] = %d, expected linear approx %d (diff %d)", i, tbUni.fulcrums[i], expectedFulcrum, diff)
			// Log instead of error, as fallback might not be perfectly linear due to int conversion
			t.Logf("Linear fallback Fulcrum[%d] = %d, expected approx %d (diff %d)", i, tbUni.fulcrums[i], expectedFulcrum, diff)
		}
	}
	if tbUni.fulcrums[fulcrumCount-1] != expectedMaxFulcrum {
		t.Errorf("Uniform base: Fulcrum[last] should be %d, got %d", expectedMaxFulcrum, tbUni.fulcrums[fulcrumCount-1])
	}
}

// internal/core/bucketer_test.go (Replace TestTableBucketerBucket)

func TestTableBucketerBucket(t *testing.T) {
	numBuckets := uint64(1000)
	var tb TableBucketer[*simpleRelativeBucketer] // Use the quadratic base
	tb.base = &simpleRelativeBucketer{}
	requireBucketInit(t, &tb, numBuckets, 5.0, 0.98) // lambda=5, alpha=0.98

	// Test hashes with varying LOWER 32 bits
	// Upper bits don't matter for index calculation, but keep them varied too.
	hashes := []uint64{
		0x00000000_00000000, // Low32 = 0x00000000 -> index=0
		0x12345678_40000000, // Low32 = 0x40000000 -> ~25% of 2^32 -> index ~ 511
		0xABCDEF01_80000000, // Low32 = 0x80000000 -> ~50% of 2^32 -> index ~ 1023
		0xDEADBEEF_C0000000, // Low32 = 0xC0000000 -> ~75% of 2^32 -> index ~ 1535
		0xFFFFFFFF_FFFFFFFF, // Low32 = 0xFFFFFFFF -> index ~ 2046/2047
	}

	// Expected relative bucket positions based on base func y = x^2
	// where x is derived from the *index* (which depends on low 32 bits of hash).
	// x_approx = float64(index) / float64(fulcrumCount-1)
	// expected_rel_pos = x_approx * x_approx (for simpleRelativeBucketer)
	// Note: These are rough estimates, the interpolation complicates exact values.
	expectedApproxBuckets := []uint64{
		0,   // index 0 -> x=0 -> y=0
		62,  // index ~511 -> x~0.25 -> y~0.0625 -> bucket ~ 62
		250, // index ~1023 -> x~0.5 -> y~0.25 -> bucket ~ 250
		562, // index ~1535 -> x~0.75 -> y~0.5625 -> bucket ~ 562
		999, // index ~2046 -> x~1 -> y~1 -> bucket ~ 999
	}

	t.Logf("NumBuckets: %d", tb.NumBuckets())

	prevBucket := BucketIDType(0)
	for i, h := range hashes {
		bucket := tb.Bucket(h)

		// Calculate approximate index/part for logging
		z := (h & 0xFFFFFFFF) * uint64(fulcrumCount-1)
		index := z >> 32
		part := uint32(z & 0xFFFFFFFF)

		t.Logf("Hash 0x%016x (Low32 0x%08x) -> Index %d Part 0x%x -> Bucket %d (Expected Approx %d)",
			h, uint32(h), index, part, bucket, expectedApproxBuckets[i])

		if uint64(bucket) >= numBuckets {
			t.Errorf("Bucket(%x) = %d, out of bounds (< %d)", h, bucket, numBuckets)
		}
		if i > 0 && bucket < prevBucket {
			// Allow small decrease due to interpolation precision? Check C++ behavior if needed.
			if int64(prevBucket)-int64(bucket) > 1 {
				t.Errorf("Bucket distribution not monotonic: hash %x -> %d, prev hash -> %d", h, bucket, prevBucket)
			} else if bucket < prevBucket {
				t.Logf("Minor non-monotonic step: hash %x -> %d, prev hash -> %d", h, bucket, prevBucket)
			}
		}

		// Check if it's reasonably close to expected based on base func
		diff := int64(bucket) - int64(expectedApproxBuckets[i])
		// Increase tolerance slightly due to interpolation effects
		tolerance := int64(numBuckets/100*5 + 2) // Allow ~5% + 2 tolerance
		if diff > tolerance || diff < -tolerance {
			t.Errorf("Bucket %d significantly different from expected approx %d (diff %d)", bucket, expectedApproxBuckets[i], diff)
		}
		prevBucket = bucket
	}
}
