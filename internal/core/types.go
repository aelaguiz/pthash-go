package core

import "fmt"

// Hash128 represents a 128-bit hash value.
type Hash128 struct {
	High uint64
	Low  uint64
}

// First returns the high 64 bits.
func (h Hash128) First() uint64 {
	return h.High
}

// Second returns the low 64 bits.
func (h Hash128) Second() uint64 {
	return h.Low
}

// Mix combines the high and low parts using XOR.
func (h Hash128) Mix() uint64 {
	// Note: C++ murmurhash2_64 uses a different mixing function.
	// If murmur is used, its specific mix should be implemented.
	// For xxhash, XOR is a reasonable default mix.
	return h.High ^ h.Low
}

// BucketIDType defines the type for bucket identifiers.
// Default to uint32 to match C++ default.
// Use build tags or config to switch to uint64 if needed.
type BucketIDType uint32

const MaxBucketID = ^BucketIDType(0)

// BucketSizeType defines the type for bucket sizes.
type BucketSizeType uint8

const MaxBucketSize = BucketSizeType(255)

// BucketPayloadPair stores a bucket ID and a payload (second part of hash).
// Ensure proper alignment if memory mapping/direct serialization is used later.
// Go typically handles alignment well, but be mindful.
// C++ used #pragma pack(push, 4) - Go doesn't have direct equivalent.
// Check struct size and alignment if issues arise.
type BucketPayloadPair struct {
	BucketID BucketIDType
	Payload  uint64
}

// Less compares two BucketPayloadPair structs, primarily by BucketID, then Payload.
func (bpp BucketPayloadPair) Less(other BucketPayloadPair) bool {
	if bpp.BucketID != other.BucketID {
		return bpp.BucketID < other.BucketID
	}
	return bpp.Payload < other.Payload
}

// String provides a string representation.
func (bpp BucketPayloadPair) String() string {
	return fmt.Sprintf("{BucketID: %d, Payload: %d}", bpp.BucketID, bpp.Payload)
}

// --- Error Type ---

// SeedRuntimeError indicates that the chosen seed resulted in a configuration
// that PTHash cannot resolve (e.g., duplicate payloads, bucket too large).
type SeedRuntimeError struct {
	Msg string
}

func (e SeedRuntimeError) Error() string {
	return fmt.Sprintf("seed did not work: %s", e.Msg)
}

// BucketT provides a view over a slice representing a single bucket's data.
// The underlying slice typically contains [bucket_id, payload1, payload2, ...].
type BucketT struct {
	data []uint64 // Slice containing [id, p1, p2, ...]
	size BucketSizeType
}

// NewBucketT creates a BucketT view. data slice must contain id + size elements.
func NewBucketT(dataSlice []uint64, size BucketSizeType) BucketT {
	if len(dataSlice) != 1+int(size) {
		// Or return an error? Panic indicates internal logic error.
		panic(fmt.Sprintf("NewBucketT: dataSlice length %d does not match size %d", len(dataSlice), size))
	}
	return BucketT{data: dataSlice, size: size}
}

// ID returns the bucket ID.
func (b BucketT) ID() BucketIDType {
	if len(b.data) == 0 {
		panic("ID() called on empty BucketT data")
	}
	// Assume ID fits in BucketIDType
	return BucketIDType(b.data[0])
}

// Payloads returns a slice containing only the payload values.
func (b BucketT) Payloads() []uint64 {
	if len(b.data) <= 1 {
		return []uint64{} // Return empty slice if no payloads
	}
	return b.data[1:]
}

// Size returns the number of payloads in the bucket.
func (b BucketT) Size() BucketSizeType {
	return b.size
}

// pthash-go/internal/core/types.go
// Add this method to BucketT struct
func (b BucketT) Data() []uint64 {
	return b.data
}
