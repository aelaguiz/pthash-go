package core

import (
	"encoding"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"math/bits"
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
	// TODO: Serialize numDenseBuckets, numSparseBuckets, mNumDenseBuckets, mNumSparseBuckets
	return nil, fmt.Errorf("SkewBucketer.MarshalBinary not implemented")
}

// UnmarshalBinary implements encoding.BinaryUnmarshaler.
func (b *SkewBucketer) UnmarshalBinary(data []byte) error {
	// TODO: Deserialize fields
	return fmt.Errorf("SkewBucketer.UnmarshalBinary not implemented")
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
	// TODO: Serialize numBuckets and mNumBuckets
	return nil, fmt.Errorf("UniformBucketer.MarshalBinary not implemented")
}

// UnmarshalBinary implements encoding.BinaryUnmarshaler.
func (b *UniformBucketer) UnmarshalBinary(data []byte) error {
	// TODO: Deserialize fields
	return fmt.Errorf("UniformBucketer.UnmarshalBinary not implemented")
}

// --- RangeBucketer Implementation (for partitioning) ---

// RangeBucketer assigns hashes to partitions based on the high bits.
type RangeBucketer struct {
	numBuckets uint64 // Number of partitions
	// No fastmod M needed based on C++ Bucket method
}

// Init initializes the RangeBucketer. Ignores lambda, tableSize, alpha.
func (b *RangeBucketer) Init(numBuckets uint64, lambda float64, tableSize uint64, alpha float64) error {
	// This method satisfies the Bucketer interface
	b.numBuckets = numBuckets
	return nil
}

// Bucket calculates partition = ((hash >> 32U) * m_num_buckets) >> 32U;
func (b *RangeBucketer) Bucket(hash uint64) uint64 { // Note: returns uint64 partition ID
	if b.numBuckets == 0 {
		return 0
	}
	hi := hash >> 32
	// Calculate (hi * numBuckets) >> 32 using 128-bit intermediate product
	prodHi, _ := bits.Mul64(hi, b.numBuckets)
	return prodHi
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

// --- OptBucketer / TableBucketer (Placeholders for later phases) ---
type OptBucketer struct {
	// TODO in Phase 7+
}

func (b *OptBucketer) Init(numBuckets uint64, lambda float64, tableSize uint64, alpha float64) error {
	return fmt.Errorf("OptBucketer not implemented")
}

// MarshalBinary implements encoding.BinaryMarshaler.
func (b *OptBucketer) MarshalBinary() ([]byte, error) {
	return nil, fmt.Errorf("OptBucketer.MarshalBinary not implemented")
}

// UnmarshalBinary implements encoding.BinaryUnmarshaler.
func (b *OptBucketer) UnmarshalBinary(data []byte) error {
	return fmt.Errorf("OptBucketer.UnmarshalBinary not implemented")
}
func (b *OptBucketer) Bucket(hash uint64) BucketIDType { return 0 }
func (b *OptBucketer) NumBuckets() uint64              { return 0 }
func (b *OptBucketer) NumBits() uint64                 { return 0 }

type TableBucketer struct {
	// TODO in Phase 7+
}

func (b *TableBucketer) Init(numBuckets uint64, lambda float64, tableSize uint64, alpha float64) error {
	return fmt.Errorf("TableBucketer not implemented")
}

// MarshalBinary implements encoding.BinaryMarshaler.
func (b *TableBucketer) MarshalBinary() ([]byte, error) {
	return nil, fmt.Errorf("TableBucketer.MarshalBinary not implemented")
}

// UnmarshalBinary implements encoding.BinaryUnmarshaler.
func (b *TableBucketer) UnmarshalBinary(data []byte) error {
	return fmt.Errorf("TableBucketer.UnmarshalBinary not implemented")
}
func (b *TableBucketer) Bucket(hash uint64) BucketIDType { return 0 }
func (b *TableBucketer) NumBuckets() uint64              { return 0 }
func (b *TableBucketer) NumBits() uint64                 { return 0 }
