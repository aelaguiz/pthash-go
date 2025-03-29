package core

import "math/bits"

// Bucketer defines the interface for assigning hashes to buckets.
type Bucketer interface {
	// Init initializes the bucketer. Parameters match C++ Opt/Skew.
	Init(numBuckets uint64, lambda float64, tableSize uint64, alpha float64) error
	// Bucket returns the bucket ID for a given hash (typically hash.First()).
	Bucket(hash uint64) BucketIDType
	// NumBuckets returns the total number of buckets.
	NumBuckets() uint64
	// TODO: Add NumBits() and serialization methods later.
}

// --- Placeholder Implementations ---

type SkewBucketer struct {
	numDenseBuckets   uint64
	numSparseBuckets  uint64
	mNumDenseBuckets  M64 // Fastmod parameter
	mNumSparseBuckets M64 // Fastmod parameter
}

func (b *SkewBucketer) Init(numBuckets uint64, lambda float64, tableSize uint64, alpha float64) error {
	// TODO: Implement Init logic from C++
	b.numDenseBuckets = uint64(ConstB * float64(numBuckets))
	b.numSparseBuckets = numBuckets - b.numDenseBuckets
	if b.numDenseBuckets > 0 {
		b.mNumDenseBuckets = ComputeM64(b.numDenseBuckets)
	}
	if b.numSparseBuckets > 0 {
		b.mNumSparseBuckets = ComputeM64(b.numSparseBuckets)
	}
	return nil
}
func (b *SkewBucketer) Bucket(hash uint64) BucketIDType { /* TODO */ return 0 }
func (b *SkewBucketer) NumBuckets() uint64              { return b.numDenseBuckets + b.numSparseBuckets }

type UniformBucketer struct {
	numBuckets  uint64
	mNumBuckets M64 // Fastmod parameter
}

func (b *UniformBucketer) Init(numBuckets uint64, lambda float64, tableSize uint64, alpha float64) error {
	b.numBuckets = numBuckets
	if numBuckets > 0 {
		b.mNumBuckets = ComputeM64(numBuckets)
	}
	return nil
}
func (b *UniformBucketer) Bucket(hash uint64) BucketIDType { /* TODO */ return 0 }
func (b *UniformBucketer) NumBuckets() uint64              { return b.numBuckets }

// RangeBucketer used for partitioning
type RangeBucketer struct {
	numBuckets uint64
	// mNumBuckets M64 // C++ uses direct multiplication/shift, not fastmod here
}

func (b *RangeBucketer) Init(numBuckets uint64) error {
	b.numBuckets = numBuckets
	// No fastmod M needed based on C++ Bucket method
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
func (b *RangeBucketer) NumBuckets() uint64 { return b.numBuckets }

// TODO: Implement OptBucketer, TableBucketer later
