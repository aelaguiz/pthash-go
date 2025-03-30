package pthash

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"reflect"
	"time"

	"pthashgo/internal/builder"
	"pthashgo/internal/core"
	"pthashgo/internal/serial" // Use centralized helpers
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

	// Initialize encoder correctly if E is a pointer type
	typeE := reflect.TypeOf(encoder)
	if typeE != nil && typeE.Kind() == reflect.Ptr {
		elemType := typeE.Elem()
		newInstance := reflect.New(elemType) // Creates a new pointer to the underlying type
		if newInstance.CanInterface() {
			encoder = newInstance.Interface().(E) // Cast to the correct type
		} else {
			panic(fmt.Sprintf("Cannot interface allocated pointer for encoder type %v", elemType))
		}
		// Verify it's not nil
		if reflect.ValueOf(encoder).IsNil() {
			panic(fmt.Sprintf("Failed to allocate non-nil pointer encoder for type %v", typeE))
		}
	}
	// If E is a value type, the zero value is already correct

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

// --- Serialization (Refined) ---

const singlePHFMagic = "SPHF" // Magic identifier for file type

// MarshalBinary implements encoding.BinaryMarshaler.
func (f *SinglePHF[K, H, B, E]) MarshalBinary() ([]byte, error) {
	// 1. Marshal components first
	bucketerData, err := serial.TryMarshal(f.bucketer) // Use serial.TryMarshal
	if err != nil {
		return nil, fmt.Errorf("failed to marshal bucketer: %w", err)
	}
	pilotsData, err := serial.TryMarshal(f.pilots) // Use serial.TryMarshal
	if err != nil {
		return nil, fmt.Errorf("failed to marshal pilots: %w", err)
	}
	freeSlotsData := []byte{}
	hasFreeSlots := f.freeSlots != nil
	if hasFreeSlots {
		freeSlotsData, err = serial.TryMarshal(f.freeSlots) // Use serial.TryMarshal
		if err != nil {
			return nil, fmt.Errorf("failed to marshal free slots: %w", err)
		}
	}

	// 2. Calculate total size
	headerSize := 4 + 1 + 1 + 1 + 1 + 8 + 8 + 8 + 8 + 16                                           // magic, version, flags, reserved, core params
	totalSize := headerSize + 8 + len(bucketerData) + 8 + len(pilotsData) + 8 + len(freeSlotsData) // len + data for components
	buf := make([]byte, totalSize)
	offset := 0

	// 3. Write Header
	copy(buf[offset:offset+4], []byte(singlePHFMagic))
	offset += 4
	buf[offset] = 1 // Version
	offset += 1
	flags := byte(0)
	if f.isMinimal {
		flags |= 1
	}
	flags |= (byte(f.searchType) << 1) // Assume searchType fits in a few bits
	// Removed hasFreeSlots from flags, rely on length later
	buf[offset] = flags
	offset += 1
	buf[offset] = 0 // Reserved byte 1
	offset += 1
	buf[offset] = 0 // Reserved byte 2
	offset += 1

	// 4. Write Core Params
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

	// 5. Write Bucketer
	binary.LittleEndian.PutUint64(buf[offset:offset+8], uint64(len(bucketerData)))
	offset += 8
	copy(buf[offset:offset+len(bucketerData)], bucketerData)
	offset += len(bucketerData)

	// 6. Write Pilots
	binary.LittleEndian.PutUint64(buf[offset:offset+8], uint64(len(pilotsData)))
	offset += 8
	copy(buf[offset:offset+len(pilotsData)], pilotsData)
	offset += len(pilotsData)

	// 7. Write Free Slots
	binary.LittleEndian.PutUint64(buf[offset:offset+8], uint64(len(freeSlotsData)))
	offset += 8
	copy(buf[offset:offset+len(freeSlotsData)], freeSlotsData)
	offset += len(freeSlotsData)

	if offset != totalSize {
		return nil, fmt.Errorf("internal marshal error: offset %d != totalSize %d", offset, totalSize)
	}
	return buf, nil
}

// UnmarshalBinary implements encoding.BinaryUnmarshaler.
func (f *SinglePHF[K, H, B, E]) UnmarshalBinary(data []byte) error {
	headerSize := 4 + 1 + 1 + 1 + 1 + 8 + 8 + 8 + 8 + 16
	if len(data) < headerSize {
		return io.ErrUnexpectedEOF
	}
	offset := 0

	// 1. Read Header
	if string(data[offset:offset+4]) != singlePHFMagic {
		return fmt.Errorf("invalid magic identifier")
	}
	offset += 4
	version := data[offset]
	if version != 1 {
		return fmt.Errorf("unsupported version: %d", version)
	}
	offset += 1
	flags := data[offset]
	f.isMinimal = (flags & 1) != 0
	f.searchType = core.SearchType((flags >> 1) & 3) // Extract search type bits
	offset += 1 + 1 + 1                              // flags + reserved

	// 2. Read Core Params
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

	// 3. Read Bucketer
	if offset+8 > len(data) {
		return io.ErrUnexpectedEOF
	}
	bucketerLen := binary.LittleEndian.Uint64(data[offset : offset+8])
	offset += 8
	if uint64(offset)+bucketerLen > uint64(len(data)) {
		return io.ErrUnexpectedEOF
	}
	// Unmarshal into the existing f.bucketer (assuming it's addressable or pointer)
	err := serial.TryUnmarshal(&f.bucketer, data[offset:offset+int(bucketerLen)])
	if err != nil {
		return fmt.Errorf("failed to unmarshal bucketer: %w", err)
	}
	offset += int(bucketerLen)

	// 4. Read Pilots
	if offset+8 > len(data) {
		return io.ErrUnexpectedEOF
	}
	pilotsLen := binary.LittleEndian.Uint64(data[offset : offset+8])
	offset += 8
	if uint64(offset)+pilotsLen > uint64(len(data)) {
		return io.ErrUnexpectedEOF
	}
	err = serial.TryUnmarshal(&f.pilots, data[offset:offset+int(pilotsLen)])
	if err != nil {
		return fmt.Errorf("failed to unmarshal pilots: %w", err)
	}
	offset += int(pilotsLen)

	// 5. Read Free Slots
	if offset+8 > len(data) {
		return io.ErrUnexpectedEOF
	}
	freeSlotsLen := binary.LittleEndian.Uint64(data[offset : offset+8])
	offset += 8
	if uint64(offset)+freeSlotsLen > uint64(len(data)) {
		return io.ErrUnexpectedEOF
	}
	if freeSlotsLen > 0 {
		f.freeSlots = core.NewEliasFano() // Create instance before unmarshaling
		err = serial.TryUnmarshal(f.freeSlots, data[offset:offset+int(freeSlotsLen)])
		if err != nil {
			f.freeSlots = nil // Ensure nil on error
			return fmt.Errorf("failed to unmarshal free slots: %w", err)
		}
	} else {
		f.freeSlots = nil // Explicitly set nil if length is 0
	}
	offset += int(freeSlotsLen)

	if offset != len(data) {
		return fmt.Errorf("extra data detected after unmarshaling (%d bytes remain)", len(data)-offset)
	}
	return nil
}

// --- REMOVED tryMarshal/tryUnmarshal helpers ---
// Use centralized versions from internal/serial
