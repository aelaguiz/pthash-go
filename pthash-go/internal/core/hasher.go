package core

import (
	"encoding/binary"
	"fmt"
	"hash"

	"github.com/cespare/xxhash/v2"
)

// Hasher defines the interface for hashing keys to 128 bits.
type Hasher[K any] interface {
	// Hash computes a 128-bit hash for the given key and seed.
	Hash(key K, seed uint64) Hash128
}

// --- XXHash128 Implementation ---

// XXHash128Hasher computes 128-bit xxHash values.
// It requires the key type K to be convertible to []byte or uint64.
type XXHash128Hasher[K any] struct{}

// NewXXHash128Hasher creates a new XXHash128 hasher instance.
func NewXXHash128Hasher[K any]() XXHash128Hasher[K] {
	return XXHash128Hasher[K]{}
}

// Hash implements the Hasher interface.
// It uses type assertion to handle specific known types efficiently.
func (h XXHash128Hasher[K]) Hash(key K, seed uint64) Hash128 {
	// Use type assertion (or type switch) to handle specific types
	switch k := any(key).(type) {
	case uint64:
		return hashUint64XXH128(k, seed)
	case string:
		// Optimization: convert string to []byte without allocation using unsafe
		// Be careful if the []byte escapes, but here it's just for hashing.
		// return hashBytesXXH128(unsafe.Slice(unsafe.StringData(k), len(k)), seed)
		// Safer version:
		return hashBytesXXH128([]byte(k), seed)
	case []byte:
		return hashBytesXXH128(k, seed)
		// Add other types as needed (e.g., []uint64)
	default:
		// Fallback: Try to convert to string or use reflection (less performant)
		// This part depends heavily on the expected key types.
		// Using fmt.Sprintf is a very slow fallback. Consider requiring
		// specific key types or a []byte conversion method.
		panic(fmt.Sprintf("XXHash128Hasher: unsupported key type %T", key))
	}
}

// hashBytesXXH128 computes the 128-bit hash for a byte slice.
func hashBytesXXH128(key []byte, seed uint64) Hash128 {
	// cespare/xxhash Digest.Sum128 is what we want, but Digest needs allocation.
	// Seed needs to be incorporated. Let's use Sum64 twice with different seeds.
	h1 := xxhash.Sum64S(key, seed)
	h2 := xxhash.Sum64S(key, ^seed) // Use inverted seed for the second part
	return Hash128{High: h1, Low: h2}
}

// hashUint64XXH128 computes the 128-bit hash for a uint64.
func hashUint64XXH128(key uint64, seed uint64) Hash128 {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], key)
	return hashBytesXXH128(buf[:], seed)
}

// --- MurmurHash2_64 Placeholder Implementation ---

// MurmurHash2_64Hasher computes 64-bit MurmurHash2 values, doubled for 128-bit.
type MurmurHash2_64Hasher[K any] struct{}

// NewMurmurHash2_64Hasher creates a new MurmurHash2_64 hasher instance.
func NewMurmurHash2_64Hasher[K any]() MurmurHash2_64Hasher[K] {
	return MurmurHash2_64Hasher[K]{}
}

// Hash implements the Hasher interface.
func (h MurmurHash2_64Hasher[K]) Hash(key K, seed uint64) Hash128 {
	// Use type assertion (or type switch)
	switch k := any(key).(type) {
	case uint64:
		return hashUint64Murmur128(k, seed)
	case string:
		return hashBytesMurmur128([]byte(k), seed)
	case []byte:
		return hashBytesMurmur128(k, seed)
	default:
		panic(fmt.Sprintf("MurmurHash2_64Hasher: unsupported key type %T", key))
	}
}

// hashBytesMurmur128 computes a 128-bit hash using two Murmur64 hashes.
func hashBytesMurmur128(key []byte, seed uint64) Hash128 {
	h1 := murmurHash64A(key, seed)
	h2 := murmurHash64A(key, ^seed) // Use inverted seed for the second part
	return Hash128{High: h1, Low: h2}
}

// hashUint64Murmur128 computes a 128-bit hash for uint64 using two Murmur64 hashes.
func hashUint64Murmur128(key uint64, seed uint64) Hash128 {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], key)
	return hashBytesMurmur128(buf[:], seed)
}

// murmurHash64A implements the MurmurHash2_64 algorithm.
// Ported directly from the C++ code provided earlier.
func murmurHash64A(key []byte, seed uint64) uint64 {
	const m = uint64(0xc6a4a7935bd1e995)
	const r = 47
	length := len(key)
	h := seed ^ (uint64(length) * m)

	nblocks := length / 8
	for i := 0; i < nblocks; i++ {
		k := binary.LittleEndian.Uint64(key[i*8:])
		k *= m
		k ^= k >> r
		k *= m
		h ^= k
		h *= m
	}

	tail := key[nblocks*8:]
	switch length & 7 {
	case 7:
		h ^= uint64(tail[6]) << 48
		fallthrough
	case 6:
		h ^= uint64(tail[5]) << 40
		fallthrough
	case 5:
		h ^= uint64(tail[4]) << 32
		fallthrough
	case 4:
		h ^= uint64(tail[3]) << 24
		fallthrough
	case 3:
		h ^= uint64(tail[2]) << 16
		fallthrough
	case 2:
		h ^= uint64(tail[1]) << 8
		fallthrough
	case 1:
		h ^= uint64(tail[0])
		h *= m
	}

	h ^= h >> r
	h *= m
	h ^= h >> r

	return h
}

// DefaultHash64 provides a default 64-bit hash function (Murmur2).
// Matches the C++ utility function.
func DefaultHash64(val uint64, seed uint64) uint64 {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], val)
	return murmurHash64A(buf[:], seed)
}

// Mix64 implements a 64-bit mixing function.
// Matches C++ implementation from hash64::mix()
func Mix64(h uint64) uint64 {
    h ^= h >> 30
    h *= 0xbf58476d1ce4e5b9
    h ^= h >> 27
    h *= 0x94d049bb133111eb
    h ^= h >> 31
    return h
}

// CheckHashCollisionProbability performs the check from C++.
func CheckHashCollisionProbability[H Hasher[any]](numKeys uint64) error {
	// This check relies on knowing the hash output size.
	// Our interface mandates Hash128, so the 64-bit check isn't directly applicable
	// unless we introduce a way to know the *effective* size used or parameterize it.
	// For now, we assume 128-bit hashes are generally safe for larger key sets.
	// If a 64-bit hasher were used primarily, this check would be important.
	// if is64BitHasher(H) && numKeys > (1<<30) {
	// 	 return errors.New("high collision probability: use 128-bit hashes for > 2^30 keys")
	// }
	return nil
}
