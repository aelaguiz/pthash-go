package pthash

import (
	"encoding"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"time"

	"pthashgo/internal/builder"
	"pthashgo/internal/core"
	"pthashgo/internal/serial"
)

// SinglePHF implements the non-partitioned PTHash function.
// Generic parameters:
// K: Key type
// H: Hasher type implementing core.Hasher[K]
// B: Bucketer type implementing core.Bucketer
// E: Encoder type implementing core.Encoder
type SinglePHF[K any, H core.Hasher[K], B core.Bucketer, E core.Encoder] struct {
	seed       uint64
	numKeys    uint64
	tableSize  uint64
	m128       core.M64        // Fastmod parameter for 128-bit hashes (XOR search)
	m64        core.M32        // Fastmod parameter for 64-bit hashes (ADD search)
	hasher     H               // Instance of the hasher (zero value initially)
	bucketer   B               // Instance of the bucketer
	pilots     E               // Encoded pilot values
	freeSlots  *core.EliasFano // Elias-Fano for free slots if minimal (pointer for nil possibility)
	isMinimal  bool
	searchType core.SearchType
}

// NewSinglePHF creates an empty SinglePHF instance.
// The minimal and searchType parameters should match the intended build config.
func NewSinglePHF[K any, H core.Hasher[K], B core.Bucketer, E core.Encoder](minimal bool, search core.SearchType) *SinglePHF[K, H, B, E] {
	// Create zero values for generic types
	var hasher H
	var bucketer B
	var encoder E

	return &SinglePHF[K, H, B, E]{
		isMinimal:  minimal,
		searchType: search,
		hasher:     hasher, // Will be properly initialized if needed by Lookup, or assumed stateless
		bucketer:   bucketer,
		pilots:     encoder,
		// freeSlots remains nil initially
	}
}

// Build constructs the MPHF from a completed internal memory builder.
func (f *SinglePHF[K, H, B, E]) Build(
	b *builder.InternalMemoryBuilderSinglePHF[K, H, B],
	config *core.BuildConfig,
) (time.Duration, error) {

	start := time.Now()

	// Check for consistency between PHF type and build config
	if f.isMinimal != config.Minimal {
		return 0, fmt.Errorf("template parameter 'Minimal' (%t) must be equal to config.Minimal (%t)", f.isMinimal, config.Minimal)
	}
	if f.searchType != config.Search {
		return 0, fmt.Errorf("template parameter 'Search' (%v) must be equal to config.Search (%v)", f.searchType, config.Search)
	}

	// --- Copy essential parameters ---
	f.seed = b.Seed()
	f.numKeys = b.NumKeys()
	f.tableSize = b.TableSize()
	if f.tableSize > 0 { // Avoid division by zero if build somehow failed earlier
		f.m128 = core.ComputeM64(f.tableSize)
		f.m64 = core.ComputeM32(uint32(f.tableSize)) // Assumes tableSize fits uint32 for Add search's hash mixing step
		if f.tableSize > math.MaxUint32 && f.searchType == core.SearchTypeAdd {
			// The ADD search variant's mixing uses fastmod32. This might be an issue.
			// C++ shifts hash.second() >> 33 before fastmod32, maybe that's okay?
			// Needs careful check if tableSize > 2^32.
			// For now, proceed assuming C++ handles it or tableSize is usually smaller.
		}
	}
	f.bucketer = b.Bucketer() // Copy bucketer state

	// --- Encode Pilots ---
	pilotsData := b.Pilots() // Get slice []uint64
	// Need to create a new instance of the encoder E to call Encode on.
	// Assuming E has a zero value that's usable or a New() function.
	var encoder E // Create a new zero-value encoder instance
	// If E requires specific initialization, this needs adjustment.
	f.pilots = encoder

	err := f.pilots.Encode(pilotsData) // Encode into the PHF's encoder instance
	if err != nil {
		return 0, fmt.Errorf("failed to encode pilots: %w", err)
	}

	// --- Encode Free Slots (if minimal) ---
	if f.isMinimal && f.numKeys < f.tableSize {
		freeSlotsData := b.FreeSlots()
		if len(freeSlotsData) != int(f.tableSize-f.numKeys) {
			return 0, fmt.Errorf("internal error: incorrect number of free slots found (%d != %d)", len(freeSlotsData), f.tableSize-f.numKeys)
		}
		f.freeSlots = core.NewEliasFano() // Create new EliasFano instance
		err = f.freeSlots.Encode(freeSlotsData)
		if err != nil {
			return 0, fmt.Errorf("failed to encode free slots: %w", err)
		}
	} else {
		f.freeSlots = nil // Ensure it's nil otherwise
	}

	elapsed := time.Since(start)
	return elapsed, nil
}

// Lookup evaluates the hash function for a key.
func (f *SinglePHF[K, H, B, E]) Lookup(key K) uint64 {
	hash := f.hasher.Hash(key, f.seed)        // Assumes f.hasher is usable (stateless or initialized)
	bucket := f.bucketer.Bucket(hash.First()) // Assumes f.bucketer is initialized
	pilot := f.pilots.Access(uint64(bucket))  // Assumes f.pilots is initialized and Access takes uint64

	var p uint64
	switch f.searchType {
	case core.SearchTypeXOR:
		hashedPilot := core.DefaultHash64(pilot, f.seed)
		p = core.FastModU64(hash.Second()^hashedPilot, f.m128, f.tableSize)
	case core.SearchTypeAdd:
		// Assumes tableSize fits in uint32 for fastmod32 operations
		m64 := core.M32(f.m64) // Stored m64 is already uint64, this cast needed? No. M32 is uint64 alias.
		d32 := uint32(f.tableSize)

		// s = fastmod::fastdiv_u32(pilot, m_M_64);
		// C++ passes uint64_t pilot to fastdiv_u32(uint32_t, uint64_t).
		// Assuming implicit cast/truncation happens in C++ call based on signature.
		s := core.FastDivU32(uint32(pilot), m64) // Keep the uint32 cast here based on C++ sig

		valToMix := hash.Second() + uint64(s)
		mixedHash := core.Mix64(valToMix)

		// p = fastmod::fastmod_u32(((hash64(...).mix()) >> 33) + pilot, M, table_size);
		term1 := mixedHash >> 33
		// *** CORRECTION: Add the FULL uint64 pilot BEFORE casting to uint32 ***
		sum := term1 + pilot // Add full pilot here

		p = uint64(core.FastModU32(uint32(sum), m64, d32)) // Cast to uint32 ONLY for the final modulo operation
	default:
		panic(fmt.Sprintf("unknown search type: %v", f.searchType))
	}

	if f.isMinimal {
		if p < f.numKeys {
			return p
		}
		if f.freeSlots == nil {
			// This should not happen if p >= numKeys and tableSize > numKeys
			panic(fmt.Sprintf("minimal lookup error: p (%d) >= numKeys (%d) but freeSlots is nil", p, f.numKeys))
		}
		// Access Elias-Fano for rank mapping
		rank := p - f.numKeys
		return f.freeSlots.Access(rank) // Assumes Access takes rank (0-based index into free slots)
	}

	return p
}

// --- Accessors ---

func (f *SinglePHF[K, H, B, E]) NumKeys() uint64             { return f.numKeys }
func (f *SinglePHF[K, H, B, E]) TableSize() uint64           { return f.tableSize }
func (f *SinglePHF[K, H, B, E]) Seed() uint64                { return f.seed }
func (f *SinglePHF[K, H, B, E]) IsMinimal() bool             { return f.isMinimal }
func (f *SinglePHF[K, H, B, E]) SearchType() core.SearchType { return f.searchType }

// --- Space Calculation ---

func (f *SinglePHF[K, H, B, E]) NumBitsForPilots() uint64 {
	// seed, numKeys, tableSize, m64 (uint64), m128 (2*uint64) + pilots
	baseBits := uint64(8 * (8 + 8 + 8 + 8 + 16)) // Size of fixed fields
	pilotBits := f.pilots.NumBits()
	return baseBits + pilotBits
}

func (f *SinglePHF[K, H, B, E]) NumBitsForMapper() uint64 {
	bucketerBits := f.bucketer.NumBits()
	freeSlotsBits := uint64(0)
	if f.freeSlots != nil {
		freeSlotsBits = f.freeSlots.NumBits()
	}
	return bucketerBits + freeSlotsBits
}

func (f *SinglePHF[K, H, B, E]) NumBits() uint64 {
	return f.NumBitsForPilots() + f.NumBitsForMapper()
}

// --- Serialization (Basic Example using encoding.BinaryMarshaler/Unmarshaler) ---

const singlePHFMagic = "SPHF" // Magic identifier for file type

// MarshalBinary implements encoding.BinaryMarshaler.
func (f *SinglePHF[K, H, B, E]) MarshalBinary() ([]byte, error) {
	// 1. Calculate total size needed
	bucketerData, err := tryMarshal(f.bucketer)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal bucketer: %w", err)
	}
	pilotsData, err := tryMarshal(f.pilots)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal pilots: %w", err)
	}
	freeSlotsData := []byte{}
	hasFreeSlots := f.freeSlots != nil
	if hasFreeSlots {
		freeSlotsData, err = tryMarshal(f.freeSlots)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal free slots: %w", err)
		}
	}

	// Size: magic(4) + version(1) + isMinimal(1) + searchType(1) + reserved(1)
	//       + seed(8) + numKeys(8) + tableSize(8) + m64(8) + m128(16)
	//       + bucketerLen(8) + bucketerData
	//       + pilotsLen(8) + pilotsData
	//       + hasFreeSlots(1) + freeSlotsLen(8) + freeSlotsData
	headerSize := 4 + 1 + 1 + 1 + 1 + 8 + 8 + 8 + 8 + 16
	totalSize := headerSize + 8 + len(bucketerData) + 8 + len(pilotsData) + 1 + 8 + len(freeSlotsData)
	buf := make([]byte, totalSize)
	offset := 0

	// Magic & Versioning
	copy(buf[offset:offset+4], []byte(singlePHFMagic))
	offset += 4
	buf[offset] = 1 // Version
	offset += 1
	// Flags
	flags := byte(0)
	if f.isMinimal {
		flags |= 1
	}
	flags |= (byte(f.searchType) << 1) // Assume searchType fits in a few bits
	if hasFreeSlots {
		flags |= (1 << 3)
	}
	buf[offset] = flags
	offset += 1
	buf[offset] = 0 // Reserved
	offset += 1

	// Core Params
	binary.LittleEndian.PutUint64(buf[offset:offset+8], f.seed)
	offset += 8
	binary.LittleEndian.PutUint64(buf[offset:offset+8], f.numKeys)
	offset += 8
	binary.LittleEndian.PutUint64(buf[offset:offset+8], f.tableSize)
	offset += 8
	binary.LittleEndian.PutUint64(buf[offset:offset+8], uint64(f.m64))
	offset += 8
	binary.LittleEndian.PutUint64(buf[offset:offset+8], f.m128[0]) // Low
	offset += 8
	binary.LittleEndian.PutUint64(buf[offset:offset+8], f.m128[1]) // High
	offset += 8

	// Bucketer
	binary.LittleEndian.PutUint64(buf[offset:offset+8], uint64(len(bucketerData)))
	offset += 8
	copy(buf[offset:offset+len(bucketerData)], bucketerData)
	offset += len(bucketerData)

	// Pilots
	binary.LittleEndian.PutUint64(buf[offset:offset+8], uint64(len(pilotsData)))
	offset += 8
	copy(buf[offset:offset+len(pilotsData)], pilotsData)
	offset += len(pilotsData)

	// Free Slots (conditional)
	buf[offset] = 0
	if hasFreeSlots {
		buf[offset] = 1
	}
	offset += 1
	binary.LittleEndian.PutUint64(buf[offset:offset+8], uint64(len(freeSlotsData)))
	offset += 8
	if hasFreeSlots {
		copy(buf[offset:offset+len(freeSlotsData)], freeSlotsData)
		offset += len(freeSlotsData)
	}

	if offset != totalSize {
		return nil, fmt.Errorf("internal marshal error: offset %d != totalSize %d", offset, totalSize)
	}
	return buf, nil
}

// UnmarshalBinary implements encoding.BinaryUnmarshaler.
// WARNING: Generic types B and E cannot be reconstructed from bytes alone.
// The caller needs to know the types B and E when calling UnmarshalBinary
// on an empty SinglePHF instance created with NewSinglePHF<...>(...).
func (f *SinglePHF[K, H, B, E]) UnmarshalBinary(data []byte) error {
	headerSize := 4 + 1 + 1 + 1 + 1 + 8 + 8 + 8 + 8 + 16
	if len(data) < headerSize {
		return io.ErrUnexpectedEOF
	}
	offset := 0

	// Magic & Version
	if string(data[offset:offset+4]) != singlePHFMagic {
		return fmt.Errorf("invalid magic identifier")
	}
	offset += 4
	version := data[offset]
	if version != 1 {
		return fmt.Errorf("unsupported version: %d", version)
	}
	offset += 1

	// Flags
	flags := data[offset]
	offset += 1
	f.isMinimal = (flags & 1) != 0
	f.searchType = core.SearchType((flags >> 1) & 3) // Extract search type bits
	hasFreeSlots := (flags & (1 << 3)) != 0
	_ = data[offset] // Reserved byte
	offset += 1

	// Core Params
	f.seed = binary.LittleEndian.Uint64(data[offset : offset+8])
	offset += 8
	f.numKeys = binary.LittleEndian.Uint64(data[offset : offset+8])
	offset += 8
	f.tableSize = binary.LittleEndian.Uint64(data[offset : offset+8])
	offset += 8
	f.m64 = core.M32(binary.LittleEndian.Uint64(data[offset : offset+8]))
	offset += 8
	f.m128[0] = binary.LittleEndian.Uint64(data[offset : offset+8])
	offset += 8 // Low
	f.m128[1] = binary.LittleEndian.Uint64(data[offset : offset+8])
	offset += 8 // High

	// Bucketer
	if offset+8 > len(data) {
		return io.ErrUnexpectedEOF
	}
	bucketerLen := binary.LittleEndian.Uint64(data[offset : offset+8])
	offset += 8
	if offset+int(bucketerLen) > len(data) {
		return io.ErrUnexpectedEOF
	}
	err := tryUnmarshal(&f.bucketer, data[offset:offset+int(bucketerLen)]) // Unmarshal into existing instance
	if err != nil {
		return fmt.Errorf("failed to unmarshal bucketer: %w", err)
	}
	offset += int(bucketerLen)

	// Pilots
	if offset+8 > len(data) {
		return io.ErrUnexpectedEOF
	}
	pilotsLen := binary.LittleEndian.Uint64(data[offset : offset+8])
	offset += 8
	if offset+int(pilotsLen) > len(data) {
		return io.ErrUnexpectedEOF
	}
	err = tryUnmarshal(&f.pilots, data[offset:offset+int(pilotsLen)]) // Unmarshal into existing instance
	if err != nil {
		return fmt.Errorf("failed to unmarshal pilots: %w", err)
	}
	offset += int(pilotsLen)

	// Free Slots (conditional)
	if offset+1+8 > len(data) {
		return io.ErrUnexpectedEOF
	}
	hasFreeSlotsRead := data[offset] == 1
	offset += 1
	if hasFreeSlotsRead != hasFreeSlots {
		return fmt.Errorf("mismatch in hasFreeSlots flag")
	}
	freeSlotsLen := binary.LittleEndian.Uint64(data[offset : offset+8])
	offset += 8
	if offset+int(freeSlotsLen) > len(data) {
		return io.ErrUnexpectedEOF
	}
	if hasFreeSlots {
		f.freeSlots = core.NewEliasFano() // Create instance before unmarshaling
		err = tryUnmarshal(f.freeSlots, data[offset:offset+int(freeSlotsLen)])
		if err != nil {
			return fmt.Errorf("failed to unmarshal free slots: %w", err)
		}
	} else {
		f.freeSlots = nil
	}
	offset += int(freeSlotsLen)

	if offset != len(data) {
		return fmt.Errorf("extra data detected after unmarshaling")
	}
	return nil
}

// tryMarshal attempts to marshal an object if it implements BinaryMarshaler.
func tryMarshal(v interface{}) ([]byte, error) {
	if marshaler, ok := v.(encoding.BinaryMarshaler); ok {
		return marshaler.MarshalBinary()
	}
	// If not implemented, fallback to serial implementation
	return serial.TryMarshal(v)
}

// tryUnmarshal attempts to unmarshal data into a pointer if it implements BinaryUnmarshaler.
// v must be a pointer to the target object (e.g., &f.bucketer).
func tryUnmarshal(v interface{}, data []byte) error {
	if unmarshaler, ok := v.(encoding.BinaryUnmarshaler); ok {
		return unmarshaler.UnmarshalBinary(data)
	}
	return serial.TryUnmarshal(v, data)
}
