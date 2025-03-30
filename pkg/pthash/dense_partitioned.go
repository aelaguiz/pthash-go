package pthash

import (
	"encoding/binary"
	"fmt"
	"io"
	"pthashgo/internal/builder"
	"pthashgo/internal/core"
	"pthashgo/internal/serial" // Use centralized helpers
	"pthashgo/internal/util"
	"reflect" // Import reflect for type checking
	"time"
)

// DensePartitionedPHF implements the densely partitioned PHF (PHOBIC).
// Generic parameters:
// K: Key type
// H: Hasher type implementing core.Hasher[K]
// B: Bucketer type implementing core.Bucketer (e.g., TableBucketer[OptBucketer])
// E: DenseEncoder type implementing core.DenseEncoder (e.g., InterCInterR)
type DensePartitionedPHF[K any, H core.Hasher[K], B core.Bucketer, E core.DenseEncoder] struct {
	seed                   uint64
	numKeys                uint64
	tableSize              uint64                                  // Total size estimate (sum of sub-table sizes)
	partitioner            *core.RangeBucketer                     // Maps key hash -> partition
	subBucketer            B                                       // Maps hash -> sub-bucket WITHIN a partition
	pilots                 E                                       // Stores pilots (interleaved or mono)
	offsets                *core.DiffEncoder[*core.CompactEncoder] // Stores partition start offsets (diff encoded)
	freeSlots              *core.EliasFano                         // For minimal mapping
	hasher                 H
	isMinimal              bool
	searchType             core.SearchType
	numPartitions          uint64 // Store for convenience
	numBucketsPerPartition uint64 // Store for convenience
}

// NewDensePartitionedPHF creates an empty DensePartitionedPHF instance.
func NewDensePartitionedPHF[K any, H core.Hasher[K], B core.Bucketer, E core.DenseEncoder](minimal bool, search core.SearchType) *DensePartitionedPHF[K, H, B, E] {
	var hasher H
	var subBucketer B
	var pilots E // Zero value of type E (could be value or pointer type)

	// Initialize pilots correctly if E is a pointer type
	typeE := reflect.TypeOf(pilots)
	// Check if E is defined (not nil interface) and is a pointer kind
	if typeE != nil && typeE.Kind() == reflect.Ptr {
		// E is a pointer type (like *DenseMono[*RiceEncoder]).
		// Create the underlying struct instance.
		elemType := typeE.Elem()
		newInstance := reflect.New(elemType) // e.g., creates *DenseMono[*RiceEncoder]
		// Assign the created pointer back to pilots
		if newInstance.CanInterface() {
			pilots = newInstance.Interface().(E)
		} else {
			// This case might occur for unexported types, unlikely here
			panic(fmt.Sprintf("Cannot interface allocated pointer for type %v", elemType))
		}
		// Double-check pilots is not nil after allocation attempt
		if reflect.ValueOf(pilots).IsNil() {
			panic(fmt.Sprintf("Failed to allocate non-nil pointer for type %v", typeE))
		}
	}
	// If E is a value type, the zero value 'pilots' is already initialized

	offsetsEnc := &core.DiffEncoder[*core.CompactEncoder]{} // Initialize diff encoder
	return &DensePartitionedPHF[K, H, B, E]{
		isMinimal:   minimal,
		searchType:  search,
		hasher:      hasher,
		subBucketer: subBucketer,
		pilots:      pilots, // Assign the potentially initialized 'pilots'
		offsets:     offsetsEnc,
		// partitioner and freeSlots initialized in Build
	}
}

// Build constructs the DensePartitionedPHF from a completed partitioned builder.
func (f *DensePartitionedPHF[K, H, B, E]) Build(
	pb *builder.InternalMemoryBuilderPartitionedPHF[K, H, B], // Builder type matches sub-bucketer B
	config *core.BuildConfig,
) (time.Duration, error) {

	start := time.Now()
	util.Log(config.Verbose, "Starting DensePartitionedPHF.Build final stage...")

	// Config consistency checks
	if !config.DensePartitioning {
		return 0, fmt.Errorf("DensePartitionedPHF requires config.DensePartitioning=true")
	}
	if f.isMinimal != config.Minimal || f.searchType != config.Search {
		return 0, fmt.Errorf("DensePartitionedPHF type parameters mismatch build config")
	}
	if pb == nil {
		return 0, fmt.Errorf("builder cannot be nil")
	}
	if len(pb.Builders()) != int(pb.NumPartitions()) {
		return 0, fmt.Errorf("builder state inconsistent: %d sub-builders for %d partitions", len(pb.Builders()), pb.NumPartitions())
	}

	// --- Copy final state ---
	f.seed = pb.Seed()
	f.numKeys = pb.NumKeys()
	f.tableSize = pb.TableSize()
	f.partitioner = pb.Partitioner()
	f.hasher = pb.Hasher()
	f.numPartitions = pb.NumPartitions()
	f.numBucketsPerPartition = pb.NumBucketsPerPartition()
	// Initialize sub-bucketer using parameters from the first sub-builder (assume consistent)
	if len(pb.Builders()) > 0 && pb.Builders()[0] != nil {
		f.subBucketer = pb.Builders()[0].Bucketer() // Copy initialized sub-bucketer
	} else if f.numPartitions > 0 {
		// If all partitions were empty, initialize manually
		err := f.subBucketer.Init(f.numBucketsPerPartition, config.Lambda, 0, config.Alpha) // Table size approx 0?
		if err != nil {
			return 0, fmt.Errorf("failed to init sub-bucketer for empty partitions: %w", err)
		}
	}

	// --- Encode Offsets ---
	rawOffsets := pb.RawPartitionOffsets() // Get offsets before diff encoding
	if len(rawOffsets) != int(f.numPartitions+1) {
		return 0, fmt.Errorf("internal error: wrong number of raw offsets %d for %d partitions", len(rawOffsets), f.numPartitions)
	}
	// Calculate increment for diff encoding (average partition range size)
	increment := uint64(0)
	if f.numPartitions > 0 {
		increment = f.tableSize / f.numPartitions // Approximate avg sub-table size
	}
	// Requires CompactEncoder to be functional or stubbed
	err := f.offsets.Encode(rawOffsets, increment)
	if err != nil {
		return 0, fmt.Errorf("failed to encode offsets: %w", err)
	}

	// --- Encode Pilots ---
	// Need the interleaving iterator from the builder
	pilotsIter := builder.NewInterleavingPilotsIterator(pb)
	err = f.pilots.EncodeDense(pilotsIter, f.numPartitions, f.numBucketsPerPartition, config.NumThreads)
	if err != nil {
		return 0, fmt.Errorf("failed to encode pilots densely: %w", err)
	}

	// --- Encode Free Slots ---
	if f.isMinimal && f.numKeys < f.tableSize {
		freeSlotsData := pb.FreeSlots()
		if len(freeSlotsData) != int(f.tableSize-f.numKeys) {
			return 0, fmt.Errorf("internal error: incorrect number of free slots found for dense (%d != %d)", len(freeSlotsData), f.tableSize-f.numKeys)
		}
		f.freeSlots = core.NewEliasFano() // Create new instance
		err = f.freeSlots.Encode(freeSlotsData)
		if err != nil {
			return 0, fmt.Errorf("failed to encode free slots: %w", err)
		}
	} else {
		f.freeSlots = nil
	}

	elapsed := time.Since(start)
	util.Log(config.Verbose, "DensePartitionedPHF final build stage took: %v", elapsed)
	return elapsed, nil
}

// Lookup evaluates the hash function.
func (f *DensePartitionedPHF[K, H, B, E]) Lookup(key K) uint64 {
	// 1. Hash key
	hash := f.hasher.Hash(key, f.seed)

	// 2. Find partition
	partitionIdx := f.partitioner.Bucket(hash.Mix())
	partitionIdxUint64 := uint64(partitionIdx)
	if partitionIdxUint64 >= f.numPartitions {
		panic(fmt.Sprintf("partition index %d out of bounds (%d)", partitionIdxUint64, f.numPartitions))
	}

	// 3. Get partition range start/end using diff-encoded offsets
	partitionOffset := f.offsets.Access(partitionIdxUint64)
	nextPartitionOffset := f.offsets.Access(partitionIdxUint64 + 1)
	partitionSize := nextPartitionOffset - partitionOffset // Size of the hash range for this partition

	// 4. Find sub-bucket within partition
	subBucket := f.subBucketer.Bucket(hash.First()) // Use hash.First() for sub-bucket

	// 5. Get pilot using dense access
	pilot := f.pilots.AccessDense(partitionIdxUint64, uint64(subBucket))

	// 6. Calculate position within partition using search type
	var pSub uint64 // Position relative to partition start (in [0, partitionSize))
	if partitionSize == 0 {
		pSub = 0 // Avoid division by zero if partition is empty
	} else if f.searchType == core.SearchTypeXOR {
		// Needs partition-specific fastmod param
		mPartSize := core.ComputeM64(partitionSize)
		hashedPilot := core.DefaultHash64(pilot, f.seed)
		pSub = core.FastModU64(hash.Second()^hashedPilot, mPartSize, partitionSize)
	} else { // SearchTypeAdd
		// Needs partition-specific fastmod param (M32 version)
		mPartSize32 := core.ComputeM32(uint32(partitionSize)) // Assumes partitionSize fits uint32
		dPartSize32 := uint32(partitionSize)
		s := core.FastDivU32(uint32(pilot), mPartSize32)
		valToMix := hash.Second() + uint64(s)
		mixedHash := core.Mix64(valToMix)
		term1 := mixedHash >> 33
		term2 := uint32(pilot)
		sum := term1 + uint64(term2)
		pSub = uint64(core.FastModU32(uint32(sum), mPartSize32, dPartSize32))
	}

	// 7. Calculate final position
	p := partitionOffset + pSub

	// 8. Apply minimal mapping if needed
	if f.isMinimal {
		if p < f.numKeys {
			return p
		}
		if f.freeSlots == nil {
			panic(fmt.Sprintf("minimal dense lookup error: p (%d) >= numKeys (%d) but freeSlots is nil", p, f.numKeys))
		}
		rank := p - f.numKeys
		return f.freeSlots.Access(rank)
	}

	return p
}

// --- Accessors ---
func (f *DensePartitionedPHF[K, H, B, E]) NumKeys() uint64   { return f.numKeys }
func (f *DensePartitionedPHF[K, H, B, E]) TableSize() uint64 { return f.tableSize } // Estimated total
func (f *DensePartitionedPHF[K, H, B, E]) Seed() uint64      { return f.seed }
func (f *DensePartitionedPHF[K, H, B, E]) IsMinimal() bool   { return f.isMinimal }

// --- Space Calculation ---
func (f *DensePartitionedPHF[K, H, B, E]) NumBits() uint64 {
	totalBits := uint64(8 * (8 + 8 + 8)) // seed, numKeys, tableSize
	totalBits += f.partitioner.NumBits()
	totalBits += f.subBucketer.NumBits()
	totalBits += f.pilots.NumBits()
	totalBits += f.offsets.NumBits()
	if f.freeSlots != nil {
		totalBits += f.freeSlots.NumBits()
	}
	totalBits += 8*8 + 8*8 // numPartitions, numBucketsPerPartition
	return totalBits
}

// Helper methods for test skipping
func (f *DensePartitionedPHF[K, H, B, E]) FreeSlotsNotImplemented() bool {
	// Check if EliasFano is just a stub
	return f.freeSlots != nil && f.freeSlots.NumBits() == 0 // Example stub check
}

func (f *DensePartitionedPHF[K, H, B, E]) OffsetsNotImplemented() bool {
	// Check if CompactVector (via DiffEncoder) is just a stub
	return f.offsets != nil && f.offsets.NumBits() == 0 // Example stub check
}

// --- Serialization ---

const densePartitionedPHFMagic = "DPHF"

// MarshalBinary implements encoding.BinaryMarshaler.
func (f *DensePartitionedPHF[K, H, B, E]) MarshalBinary() ([]byte, error) {
	// 1. Marshal components
	partitionerData, err := serial.TryMarshal(f.partitioner)
	if err != nil {
		return nil, fmt.Errorf("marshal partitioner: %w", err)
	}
	subBucketerData, err := serial.TryMarshal(f.subBucketer)
	if err != nil {
		return nil, fmt.Errorf("marshal subBucketer: %w", err)
	}
	pilotsData, err := serial.TryMarshal(f.pilots)
	if err != nil {
		return nil, fmt.Errorf("marshal pilots: %w", err)
	}
	offsetsData, err := serial.TryMarshal(f.offsets)
	if err != nil {
		return nil, fmt.Errorf("marshal offsets: %w", err)
	}
	freeSlotsData := []byte{}
	if f.freeSlots != nil {
		freeSlotsData, err = serial.TryMarshal(f.freeSlots)
		if err != nil {
			return nil, fmt.Errorf("marshal freeSlots: %w", err)
		}
	}

	// 2. Calculate size
	// header: magic(4)+version(1)+flags(1)+reserved(2)=8
	// core: seed(8)+numKeys(8)+tableSize(8)+numPart(8)+numBucketsP(8)=40
	// components: len(8)+data for each
	headerSize := 8 + 40
	totalSize := headerSize
	totalSize += 8 + len(partitionerData)
	totalSize += 8 + len(subBucketerData)
	totalSize += 8 + len(pilotsData)
	totalSize += 8 + len(offsetsData)
	totalSize += 8 + len(freeSlotsData)
	buf := make([]byte, totalSize)
	offset := 0

	// 3. Write Header
	copy(buf[offset:offset+4], []byte(densePartitionedPHFMagic))
	offset += 4
	buf[offset] = 1
	offset += 1 // Version
	flags := byte(0)
	if f.isMinimal {
		flags |= 1
	}
	flags |= (byte(f.searchType) << 1)
	buf[offset] = flags
	offset += 1
	binary.LittleEndian.PutUint16(buf[offset:offset+2], 0)
	offset += 2 // Reserved

	// 4. Write Core
	binary.LittleEndian.PutUint64(buf[offset:], f.seed)
	offset += 8
	binary.LittleEndian.PutUint64(buf[offset:], f.numKeys)
	offset += 8
	binary.LittleEndian.PutUint64(buf[offset:], f.tableSize)
	offset += 8
	binary.LittleEndian.PutUint64(buf[offset:], f.numPartitions)
	offset += 8
	binary.LittleEndian.PutUint64(buf[offset:], f.numBucketsPerPartition)
	offset += 8

	// 5. Write Components
	putComponent := func(data []byte) {
		binary.LittleEndian.PutUint64(buf[offset:], uint64(len(data)))
		offset += 8
		copy(buf[offset:], data)
		offset += len(data)
	}
	putComponent(partitionerData)
	putComponent(subBucketerData)
	putComponent(pilotsData)
	putComponent(offsetsData)
	putComponent(freeSlotsData)

	if offset != totalSize {
		return nil, fmt.Errorf("internal marshal error: offset %d != totalSize %d", offset, totalSize)
	}
	return buf, nil
}

// UnmarshalBinary
// File: pkg/pthash/dense_partitioned.go

func (f *DensePartitionedPHF[K, H, B, E]) UnmarshalBinary(data []byte) error {
	headerSize := 4 + 1 + 1 + 1 + 1 + 8 + 8 + 8 + 8 + 8 // Adjusted size for core params
	if len(data) < headerSize {
		return io.ErrUnexpectedEOF
	}
	offset := 0

	// 1. Read Header (same as before)
	if string(data[offset:offset+4]) != densePartitionedPHFMagic {
		return fmt.Errorf("invalid magic")
	}
	offset += 4
	version := data[offset]
	offset += 1
	if version != 1 {
		return fmt.Errorf("unsupported version %d", version)
	}
	flags := data[offset]
	offset += 1
	f.isMinimal = (flags & 1) != 0
	f.searchType = core.SearchType((flags >> 1) & 3)
	offset += 2 // Reserved

	// 2. Read Core Params (same as before)
	f.seed = binary.LittleEndian.Uint64(data[offset:])
	offset += 8
	f.numKeys = binary.LittleEndian.Uint64(data[offset:])
	offset += 8
	f.tableSize = binary.LittleEndian.Uint64(data[offset:])
	offset += 8
	f.numPartitions = binary.LittleEndian.Uint64(data[offset:])
	offset += 8
	f.numBucketsPerPartition = binary.LittleEndian.Uint64(data[offset:])
	offset += 8

	// 3. Read Components - Allocate Pointers *before* calling TryUnmarshal

	// Helper to read length-prefixed data and unmarshal
	readComponent := func(target interface{}, description string) (int, error) {
		if offset+8 > len(data) {
			return offset, fmt.Errorf("%s length: %w", description, io.ErrUnexpectedEOF)
		}
		compLen := binary.LittleEndian.Uint64(data[offset:])
		offset += 8
		endOffset := offset + int(compLen)
		if endOffset > len(data) {
			return offset, fmt.Errorf("%s data bounds: %w", description, io.ErrUnexpectedEOF)
		}
		// Pass the allocated pointer target directly
		err := serial.TryUnmarshal(target, data[offset:endOffset])
		if err != nil {
			return offset, fmt.Errorf("%s: %w", description, err)
		}
		return endOffset, nil // Return new offset
	}

	var err error

	// Allocate and Read Partitioner
	f.partitioner = &core.RangeBucketer{} // Allocate
	offset, err = readComponent(f.partitioner, "unmarshal partitioner")
	if err != nil {
		return err
	}

	// Allocate and Read SubBucketer (if B is pointer type)
	var zeroB B
	typeB := reflect.TypeOf(zeroB)
	if typeB != nil && typeB.Kind() == reflect.Ptr {
		elemType := typeB.Elem()
		newInstance := reflect.New(elemType)
		if !newInstance.CanInterface() {
			return fmt.Errorf("cannot interface allocated subBucketer pointer for type %v", elemType)
		}
		f.subBucketer = newInstance.Interface().(B) // Assign allocated pointer
	} else {
		f.subBucketer = zeroB // Assign zero value for non-pointer types
	}
	offset, err = readComponent(f.subBucketer, "unmarshal subBucketer") // Pass pointer/value
	if err != nil {
		return err
	}

	// Allocate and Read Pilots (if E is pointer type)
	var zeroE E
	typeE := reflect.TypeOf(zeroE)
	if typeE != nil && typeE.Kind() == reflect.Ptr {
		elemType := typeE.Elem()
		newInstance := reflect.New(elemType)
		if !newInstance.CanInterface() {
			return fmt.Errorf("cannot interface allocated pilots pointer for type %v", elemType)
		}
		f.pilots = newInstance.Interface().(E)
	} else {
		f.pilots = zeroE
	}
	offset, err = readComponent(f.pilots, "unmarshal pilots") // Pass pointer/value
	if err != nil {
		return err
	}

	// Allocate and Read Offsets
	f.offsets = &core.DiffEncoder[*core.CompactEncoder]{} // Allocate specific type
	offset, err = readComponent(f.offsets, "unmarshal offsets")
	if err != nil {
		return err
	}

	// Read Free Slots (allocate only if needed)
	if offset+8 > len(data) {
		return fmt.Errorf("freeSlots length: %w", io.ErrUnexpectedEOF)
	}
	freeSlotsLen := binary.LittleEndian.Uint64(data[offset:])
	offset += 8
	endOffset := offset + int(freeSlotsLen)
	if endOffset > len(data) {
		return fmt.Errorf("freeSlots data bounds: %w", io.ErrUnexpectedEOF)
	}
	if freeSlotsLen > 0 {
		f.freeSlots = core.NewEliasFano()                              // Allocate
		err = serial.TryUnmarshal(f.freeSlots, data[offset:endOffset]) // Pass pointer
		if err != nil {
			return fmt.Errorf("unmarshal freeSlots: %w", err)
		}
	} else {
		f.freeSlots = nil
	}
	offset = endOffset

	if offset != len(data) {
		return fmt.Errorf("extra data after DensePartitionedPHF unmarshal (%d bytes)", len(data)-offset)
	}
	return nil
}
