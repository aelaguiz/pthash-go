package core

import (
	"encoding"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"math"
	"math/bits"
	"pthashgo/internal/serial"
)

// Bucketer defines the interface for assigning hashes to buckets.
type Bucketer interface {
	// Init initializes the bucketer. Parameters match C++ Opt/Skew conventions.
	Init(numBuckets uint64, lambda float64, tableSize uint64, alpha float64) error
	// Bucket returns the bucket ID for a given hash (typically hash.First()).
	Bucket(hash uint64) BucketIDType
	// NumBuckets returns the total number of buckets.
	NumBuckets() uint64
	// NumBits returns the size of the bucketer's internal state in bits.
	NumBits() uint64
	// Add serialization methods
	encoding.BinaryMarshaler
	encoding.BinaryUnmarshaler
	// TODO: Add serialization methods (e.g., MarshalBinary, UnmarshalBinary) later.
}

// --- SkewBucketer Implementation ---

// SkewBucketer implements the skew bucketing strategy.
type SkewBucketer struct {
	numDenseBuckets   uint64
	numSparseBuckets  uint64
	mNumDenseBuckets  M64 // Fastmod parameter for dense buckets
	mNumSparseBuckets M64 // Fastmod parameter for sparse buckets
}

// Init initializes the SkewBucketer.
func (b *SkewBucketer) Init(numBuckets uint64, lambda float64, tableSize uint64, alpha float64) error {
	if numBuckets == 0 {
		return fmt.Errorf("SkewBucketer requires numBuckets > 0")
	}
	b.numDenseBuckets = uint64(ConstB * float64(numBuckets)) // ConstB = 0.3
	b.numSparseBuckets = numBuckets - b.numDenseBuckets
	if b.numDenseBuckets > 0 {
		b.mNumDenseBuckets = ComputeM64(b.numDenseBuckets)
	} else {
		// Ensure mNumDenseBuckets is zeroed or invalid if numDenseBuckets is 0
		b.mNumDenseBuckets = M64{0, 0}
	}
	if b.numSparseBuckets > 0 {
		b.mNumSparseBuckets = ComputeM64(b.numSparseBuckets)
	} else {
		b.mNumSparseBuckets = M64{0, 0}
	}
	return nil
}

// Bucket assigns a hash to a bucket using the skew strategy.
// C++: (hash < T) ? fastmod_u64(...) : dense + fastmod_u64(...)
// T = constants::a * static_cast<double>(UINT64_MAX); // ConstA = 0.6
func (b *SkewBucketer) Bucket(hash uint64) BucketIDType {
	// Calculate threshold T carefully to avoid float precision issues if possible
	// T = 0.6 * 2^64
	const threshold = uint64(float64(math.MaxUint64) * ConstA) // May have precision limits

	if hash < threshold {
		if b.numDenseBuckets == 0 {
			return 0 // Avoid division by zero if no dense buckets
		}
		// NOTE: Ensure FastModU64 is correctly implemented.
		mod := FastModU64(hash, b.mNumDenseBuckets, b.numDenseBuckets)
		return BucketIDType(mod) // Cast result to BucketIDType
	}

	if b.numSparseBuckets == 0 {
		// If hash >= threshold but no sparse buckets, map to the last dense bucket?
		// C++ implies it maps to numDenseBuckets index, which might be out of bounds if sparse=0.
		// Let's return the last valid dense bucket ID if sparse is 0.
		if b.numDenseBuckets > 0 {
			return BucketIDType(b.numDenseBuckets - 1)
		}
		return 0 // Only one bucket total if both are 0 (shouldn't happen with Init check)
	}
	mod := FastModU64(hash, b.mNumSparseBuckets, b.numSparseBuckets)
	return BucketIDType(b.numDenseBuckets + mod) // Cast offset sum
}

// NumBuckets returns the total number of buckets.
func (b *SkewBucketer) NumBuckets() uint64 {
	return b.numDenseBuckets + b.numSparseBuckets
}

// NumBits returns the storage size in bits.
func (b *SkewBucketer) NumBits() uint64 {
	// sizeof(uint64)*2 + sizeof(M64)*2
	return (8 + 8 + 16 + 16) * 8
}

// MarshalBinary implements encoding.BinaryMarshaler.
func (b *SkewBucketer) MarshalBinary() ([]byte, error) {
	buf := make([]byte, 8+8+16+16) // numDense, numSparse, M64_dense, M64_sparse
	offset := 0
	binary.LittleEndian.PutUint64(buf[offset:], b.numDenseBuckets)
	offset += 8
	binary.LittleEndian.PutUint64(buf[offset:], b.numSparseBuckets)
	offset += 8
	binary.LittleEndian.PutUint64(buf[offset:], b.mNumDenseBuckets[0]) // Low
	offset += 8
	binary.LittleEndian.PutUint64(buf[offset:], b.mNumDenseBuckets[1]) // High
	offset += 8
	binary.LittleEndian.PutUint64(buf[offset:], b.mNumSparseBuckets[0]) // Low
	offset += 8
	binary.LittleEndian.PutUint64(buf[offset:], b.mNumSparseBuckets[1]) // High
	return buf, nil
}

// UnmarshalBinary implements encoding.BinaryUnmarshaler.
func (b *SkewBucketer) UnmarshalBinary(data []byte) error {
	if len(data) < (8 + 8 + 16 + 16) {
		return io.ErrUnexpectedEOF
	}
	offset := 0
	b.numDenseBuckets = binary.LittleEndian.Uint64(data[offset:])
	offset += 8
	b.numSparseBuckets = binary.LittleEndian.Uint64(data[offset:])
	offset += 8
	b.mNumDenseBuckets[0] = binary.LittleEndian.Uint64(data[offset:]) // Low
	offset += 8
	b.mNumDenseBuckets[1] = binary.LittleEndian.Uint64(data[offset:]) // High
	offset += 8
	b.mNumSparseBuckets[0] = binary.LittleEndian.Uint64(data[offset:]) // Low
	offset += 8
	b.mNumSparseBuckets[1] = binary.LittleEndian.Uint64(data[offset:]) // High
	// Basic validation
	if b.numDenseBuckets+b.numSparseBuckets == 0 && (len(data) != (8 + 8 + 16 + 16)) {
		// Allow 0 buckets, but data should match size
		return fmt.Errorf("skew bucketer consistency error after unmarshal")
	}
	return nil
}

// --- UniformBucketer Implementation ---

// UniformBucketer assigns hashes uniformly across buckets using fastmod.
type UniformBucketer struct {
	numBuckets  uint64
	mNumBuckets M64 // Fastmod parameter
}

// Init initializes the UniformBucketer.
func (b *UniformBucketer) Init(numBuckets uint64, lambda float64, tableSize uint64, alpha float64) error {
	if numBuckets == 0 {
		return fmt.Errorf("UniformBucketer requires numBuckets > 0")
	}
	b.numBuckets = numBuckets
	b.mNumBuckets = ComputeM64(numBuckets)
	return nil
}

// Bucket assigns a hash to a bucket using fastmod.
func (b *UniformBucketer) Bucket(hash uint64) BucketIDType {
	mod := FastModU64(hash, b.mNumBuckets, b.numBuckets)
	return BucketIDType(mod)
}

// NumBuckets returns the total number of buckets.
func (b *UniformBucketer) NumBuckets() uint64 {
	return b.numBuckets
}

// NumBits returns the storage size in bits.
func (b *UniformBucketer) NumBits() uint64 {
	// sizeof(uint64) + sizeof(M64)
	return (8 + 16) * 8
}

// MarshalBinary implements encoding.BinaryMarshaler.
func (b *UniformBucketer) MarshalBinary() ([]byte, error) {
	buf := make([]byte, 8+16) // numBuckets, M64
	offset := 0
	binary.LittleEndian.PutUint64(buf[offset:], b.numBuckets)
	offset += 8
	binary.LittleEndian.PutUint64(buf[offset:], b.mNumBuckets[0]) // Low
	offset += 8
	binary.LittleEndian.PutUint64(buf[offset:], b.mNumBuckets[1]) // High
	return buf, nil
}

// UnmarshalBinary implements encoding.BinaryUnmarshaler.
func (b *UniformBucketer) UnmarshalBinary(data []byte) error {
	if len(data) < (8 + 16) {
		return io.ErrUnexpectedEOF
	}
	offset := 0
	b.numBuckets = binary.LittleEndian.Uint64(data[offset:])
	offset += 8
	b.mNumBuckets[0] = binary.LittleEndian.Uint64(data[offset:]) // Low
	offset += 8
	b.mNumBuckets[1] = binary.LittleEndian.Uint64(data[offset:]) // High
	return nil
}

// --- RangeBucketer Implementation (for partitioning) ---

// RangeBucketer assigns hashes to partitions based on the high bits.
type RangeBucketer struct {
	numBuckets uint64 // Number of partitions
	// No fastmod M needed based on C++ Bucket method
}

// Init initializes the RangeBucketer. Ignores lambda, tableSize, alpha.
func (b *RangeBucketer) Init(numBuckets uint64, lambda float64, tableSize uint64, alpha float64) error {
	b.numBuckets = numBuckets
	return nil
}

// internal/core/bucketer.go

// Bucket calculates partition = ((hash >> 32U) * m_num_buckets) >> 32U;
// Corrected Implementation
func (b *RangeBucketer) Bucket(hash uint64) BucketIDType {
	if b.numBuckets == 0 {
		return BucketIDType(0)
	}
	// 1. Get high 32 bits of hash
	hi32 := uint32(hash >> 32)

	// 2. Multiply hi32 (as uint64) by numBuckets (uint64), get 128-bit result
	productHi, productLo := bits.Mul64(uint64(hi32), b.numBuckets)

	// 3. Shift the 128-bit result right by 32 bits.
	// Result = (productHi << 32) | (productLo >> 32)
	result := (productHi << 32) | (productLo >> 32)

	// 4. Cast to BucketIDType (uint32)
	// Check if result exceeds MaxBucketID before casting
	if result > uint64(MaxBucketID) {
		// This implies numPartitions *might* be too large for BucketIDType,
		// although the Init check should prevent this specific RangeBucketer case.
		// It's safer to clamp or panic if this occurs unexpectedly.
		// Let's clamp to the maximum possible BucketIDType value.
		return MaxBucketID
	}
	return BucketIDType(result)
}

// NumBuckets returns the total number of partitions.
func (b *RangeBucketer) NumBuckets() uint64 {
	return b.numBuckets
}

// NumBits returns the storage size in bits.
func (b *RangeBucketer) NumBits() uint64 {
	// sizeof(uint64)
	return 8 * 8
}

// MarshalBinary implements encoding.BinaryMarshaler.
func (b *RangeBucketer) MarshalBinary() ([]byte, error) {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, b.numBuckets)
	return buf, nil
}

// UnmarshalBinary implements encoding.BinaryUnmarshaler.
func (b *RangeBucketer) UnmarshalBinary(data []byte) error {
	if len(data) < 8 {
		return io.ErrUnexpectedEOF
	}
	b.numBuckets = binary.LittleEndian.Uint64(data)
	return nil
}

// OptBucketer implements the optimized bucketing function described in the papers.
type OptBucketer struct {
	c           float64 // Derived from lambda and table size
	numBuckets  uint64
	alpha       float64 // Store alpha for calculation
	alphaFactor float64 // Precomputed 1.0 / baseFunc(alpha)
}

// baseFunc is the core non-linear mapping function.
func (b *OptBucketer) baseFunc(normalizedHash float64) float64 {
	// C++: (normalized_hash + (1 - normalized_hash) * std::log(1 - normalized_hash)) * (1.0 - m_c) + m_c * normalized_hash;
	// Use math.Log for natural logarithm (ln)
	oneMinusNH := 1.0 - normalizedHash
	logPart := 0.0
	if oneMinusNH > 0 { // Avoid log(0) or log(negative)
		logPart = oneMinusNH * math.Log(oneMinusNH)
	}
	return (normalizedHash+logPart)*(1.0-b.c) + b.c*normalizedHash
}

// Init initializes the OptBucketer.
func (b *OptBucketer) Init(numBuckets uint64, lambda float64, tableSize uint64, alpha float64) error {
	if numBuckets == 0 {
		// OptBucketer likely doesn't make sense with 0 buckets, but handle gracefully.
		b.numBuckets = 0
		b.c = 0
		b.alpha = alpha
		b.alphaFactor = 1.0
		return nil
		// return fmt.Errorf("OptBucketer requires numBuckets > 0")
	}
	if tableSize == 0 || alpha <= 0 || alpha > 1.0 {
		// Need valid tableSize and alpha for calculations
		return fmt.Errorf("OptBucketer requires tableSize > 0 and 0 < alpha <= 1.0")
	}
	b.numBuckets = numBuckets
	b.alpha = alpha
	// C++: m_c = 0.2 * lambda / std::sqrt(table_size);
	b.c = 0.2 * lambda / math.Sqrt(float64(tableSize))

	// C++: 1.0 / baseFunc(alpha); (handle alpha near 1.0)
	if alpha > 0.999999 { // Tolerance for near 1.0
		b.alphaFactor = 1.0
	} else {
		baseAlpha := b.baseFunc(alpha)
		if baseAlpha <= 0 {
			// This indicates potential numerical instability or invalid parameters
			// Maybe default to 1.0 or return an error?
			b.alphaFactor = 1.0 // Default fallback
			fmt.Printf("Warning: OptBucketer baseFunc(alpha=%.4f) resulted in %.4f, using alphaFactor=1.0\n", alpha, baseAlpha)
			// return fmt.Errorf("OptBucketer baseFunc(alpha) resulted in non-positive value: %f", baseAlpha)
		} else {
			b.alphaFactor = 1.0 / baseAlpha
		}
	}
	return nil
}

// bucketRelative applies the core transformation scaled by alphaFactor.
func (b *OptBucketer) bucketRelative(normalizedHash float64) float64 {
	// C++: m_alpha_factor * baseFunc(m_alpha * normalized_hash);
	return b.alphaFactor * b.baseFunc(b.alpha*normalizedHash)
}

// Bucket assigns a hash to a bucket using the optimized function.
func (b *OptBucketer) Bucket(hash uint64) BucketIDType {
	if b.numBuckets == 0 {
		return 0
	}
	normalizedHash := float64(hash) / float64(math.MaxUint64)
	normalizedBucket := b.bucketRelative(normalizedHash)
	// Clamp result to [0, numBuckets - 1]
	bucketID := uint64(normalizedBucket * float64(b.numBuckets))
	if bucketID >= b.numBuckets {
		bucketID = b.numBuckets - 1
	}
	return BucketIDType(bucketID)
}

// NumBuckets returns the total number of buckets.
func (b *OptBucketer) NumBuckets() uint64 {
	return b.numBuckets
}

// NumBits returns the storage size in bits.
func (b *OptBucketer) NumBits() uint64 {
	// sizeof(c float64) + sizeof(numBuckets uint64) + sizeof(alpha float64) + sizeof(alphaFactor float64)
	return (8 + 8 + 8 + 8) * 8
}

// MarshalBinary placeholder
func (b *OptBucketer) MarshalBinary() ([]byte, error) {
	buf := make([]byte, 8+8+8+8)
	binary.LittleEndian.PutUint64(buf[0:8], math.Float64bits(b.c))
	binary.LittleEndian.PutUint64(buf[8:16], b.numBuckets)
	binary.LittleEndian.PutUint64(buf[16:24], math.Float64bits(b.alpha))
	binary.LittleEndian.PutUint64(buf[24:32], math.Float64bits(b.alphaFactor))
	return buf, nil
}

// UnmarshalBinary placeholder
func (b *OptBucketer) UnmarshalBinary(data []byte) error {
	if len(data) < 32 {
		return io.ErrUnexpectedEOF
	}
	b.c = math.Float64frombits(binary.LittleEndian.Uint64(data[0:8]))
	b.numBuckets = binary.LittleEndian.Uint64(data[8:16])
	b.alpha = math.Float64frombits(binary.LittleEndian.Uint64(data[16:24]))
	b.alphaFactor = math.Float64frombits(binary.LittleEndian.Uint64(data[24:32]))
	return nil
}

// FULCS constant (must match C++)
const fulcrumCount = 2048 // C++ FULCS = 2048

// TableBucketer uses interpolation between precomputed fulcrum points
// based on an underlying BaseBucketer's relative distribution.
type TableBucketer[Base Bucketer] struct {
	base       Base                 // The underlying bucketer (e.g., OptBucketer)
	fulcrums   [fulcrumCount]uint64 // Precomputed points for interpolation
	numBuckets uint64               // Store numBuckets locally for convenience/serialization
}

// NewTableBucketer creates a new TableBucketer.
func NewTableBucketer[Base Bucketer](base Base) *TableBucketer[Base] {
	// Store the provided base instance directly
	return &TableBucketer[Base]{base: base}
}

// Init initializes the TableBucketer and computes fulcrums.
func (tb *TableBucketer[Base]) Init(numBuckets uint64, lambda float64, tableSize uint64, alpha float64) error {
	// 1. Initialize the base bucketer
	// Create a new instance of Base type if needed, or assume tb.base is pre-set
	err := tb.base.Init(numBuckets, lambda, tableSize, alpha)
	if err != nil {
		return fmt.Errorf("failed to initialize base bucketer: %w", err)
	}
	tb.numBuckets = tb.base.NumBuckets() // Store the actual number of buckets

	if tb.numBuckets == 0 {
		// Handle gracefully, maybe set all fulcrums to 0
		for i := range tb.fulcrums {
			tb.fulcrums[i] = 0
		}
		return nil
	}

	// 2. Compute fulcrums based on base.bucketRelative
	// We need a bucketRelative method on the Bucketer interface or specific types.
	// Let's assume OptBucketer (common case) has bucketRelative.
	// This requires B to potentially satisfy a richer interface than just Bucketer.
	type relativeBucketer interface {
		BucketRelative(normalizedHash float64) float64
	}

	relBucketer, ok := any(tb.base).(relativeBucketer)
	if !ok {
		// Fallback or error if base doesn't provide relative mapping
		// Fallback: Use linear distribution (like UniformBucketer)
		fmt.Println("Warning: Base bucketer does not implement BucketRelative, using linear fulcrums.")
		for xi := 0; xi < fulcrumCount; xi++ {
			x := float64(xi) / float64(fulcrumCount-1)
			y := x                                          // Linear mapping
			fulcV := uint64(y * float64(tb.numBuckets<<16)) // Scale to 16 fractional bits
			tb.fulcrums[xi] = fulcV
		}
	} else {
		// Compute using base.BucketRelative
		tb.fulcrums[0] = 0
		numBucketsShifted := tb.numBuckets << 16 // Pre-shifted value
		for xi := 0; xi < fulcrumCount-1; xi++ {
			x := float64(xi) / float64(fulcrumCount-1)
			y := relBucketer.BucketRelative(x)              // Use the base bucketer's relative function
			fulcV := uint64(y * float64(numBucketsShifted)) // Scale result
			tb.fulcrums[xi+1] = fulcV
		}
		tb.fulcrums[fulcrumCount-1] = numBucketsShifted // Ensure last point is exact
	}

	return nil
}

// Bucket performs interpolation between fulcrums.
func (tb *TableBucketer[Base]) Bucket(hash uint64) BucketIDType {
	if tb.numBuckets == 0 {
		return 0
	}
	// C++: uint64_t z = (hash & 0xFFFFFFFF) * uint64_t(FULCS - 1);
	z := (hash & 0xFFFFFFFF) * uint64(fulcrumCount-1)
	index := z >> 32
	part := uint32(z & 0xFFFFFFFF)
	part64 := uint64(part)
	partComp64 := uint64(0xFFFFFFFF) - part64 // Use 0xFFFFFFFF for 32-bit complement

	f1 := tb.fulcrums[index]
	f2 := tb.fulcrums[index+1]

	// --- CORRECTED CALCULATION ---
	// Calculate Product1 = f1 * part64 (128 bits)
	hi1, lo1 := bits.Mul64(f1, part64)
	// Calculate v1 = Product1 >> 32
	// v1 = (hi1 << (64 - 32)) | (lo1 >> 32)
	v1 := (hi1 << 32) | (lo1 >> 32)

	// Calculate Product2 = f2 * partComp64 (128 bits)
	hi2, lo2 := bits.Mul64(f2, partComp64)
	// Calculate v2 = Product2 >> 32
	// v2 = (hi2 << (64 - 32)) | (lo2 >> 32)
	v2 := (hi2 << 32) | (lo2 >> 32)
	// --- END CORRECTED CALCULATION ---

	// C++: return (v1 + v2) >> 16;
	// Add v1 and v2, then shift right by 16
	// Standard Go addition handles potential overflow correctly for uint64
	result := (v1 + v2) >> 16

	log.Printf("hash=0x%x -> z=0x%x index=%d part=0x%x f1=%d f2=%d v1=%d v2=%d result=%d", hash, z, index, part64, f1, f2, v1, v2, result)

	// Clamp result as interpolation might slightly exceed bounds
	if result >= tb.numBuckets {
		return BucketIDType(tb.numBuckets - 1)
	}
	return BucketIDType(result)
}

// NumBuckets returns the total number of buckets.
func (tb *TableBucketer[Base]) NumBuckets() uint64 {
	return tb.numBuckets // Return stored value
}

// NumBits returns the storage size in bits.
func (tb *TableBucketer[Base]) NumBits() uint64 {
	baseBits := tb.base.NumBits()
	fulcrumBits := uint64(len(tb.fulcrums) * 8 * 8) // 2048 * uint64
	numBucketsBits := uint64(8 * 8)
	return baseBits + fulcrumBits + numBucketsBits
}

// MarshalBinary implements encoding.BinaryMarshaler.
func (tb *TableBucketer[Base]) MarshalBinary() ([]byte, error) {
	// 1. Marshal base
	baseData, err := serial.TryMarshal(tb.base) // Marshals the actual base instance (value or pointer)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal base bucketer: %w", err)
	}

	// 2. Calculate size: numBuckets(8) + baseLen(8) + baseData + fulcrums (fixed size)
	fulcrumSize := fulcrumCount * 8
	totalSize := 8 + 8 + len(baseData) + fulcrumSize
	buf := make([]byte, totalSize)
	offset := 0

	// 3. Write numBuckets
	binary.LittleEndian.PutUint64(buf[offset:], tb.numBuckets)
	offset += 8

	// 4. Write base bucketer
	binary.LittleEndian.PutUint64(buf[offset:], uint64(len(baseData)))
	offset += 8
	copy(buf[offset:], baseData)
	offset += len(baseData)

	// 5. Write fulcrums
	for _, f := range tb.fulcrums {
		binary.LittleEndian.PutUint64(buf[offset:], f)
		offset += 8
	}

	if offset != totalSize {
		return nil, fmt.Errorf("TableBucketer marshal size mismatch")
	}
	return buf, nil
}

// UnmarshalBinary implements encoding.BinaryUnmarshaler.
func (tb *TableBucketer[Base]) UnmarshalBinary(data []byte) error {
	if len(data) < 16 { // Need at least numBuckets + baseLen
		return io.ErrUnexpectedEOF
	}
	offset := 0

	// 1. Read numBuckets
	tb.numBuckets = binary.LittleEndian.Uint64(data[offset:])
	offset += 8

	// 2. Read base bucketer
	baseLen := binary.LittleEndian.Uint64(data[offset:])
	offset += 8
	if uint64(offset)+baseLen > uint64(len(data)) {
		return io.ErrUnexpectedEOF
	}

	// Unmarshal into the existing tb.base
	// First check if tb.base is directly unmarshalable
	if unmarshaler, ok := any(tb.base).(encoding.BinaryUnmarshaler); ok {
		err := unmarshaler.UnmarshalBinary(data[offset : offset+int(baseLen)])
		if err != nil {
			return fmt.Errorf("failed to unmarshal base bucketer directly: %w", err)
		}
	} else {
		return fmt.Errorf("base bucketer type %T does not implement encoding.BinaryUnmarshaler", tb.base)
	}

	offset += int(baseLen)

	// 3. Read fulcrums
	fulcrumSize := fulcrumCount * 8
	if offset+fulcrumSize > len(data) {
		return io.ErrUnexpectedEOF
	}
	for i := 0; i < fulcrumCount; i++ {
		tb.fulcrums[i] = binary.LittleEndian.Uint64(data[offset:])
		offset += 8
	}

	if offset != len(data) {
		return fmt.Errorf("extra data after TableBucketer unmarshal")
	}
	return nil
}
