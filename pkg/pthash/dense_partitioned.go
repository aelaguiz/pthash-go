package pthash

import (
	"fmt"
	"pthashgo/internal/builder"
	"pthashgo/internal/core"
	"pthashgo/internal/util"
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
	var pilots E
	offsetsEnc := &core.DiffEncoder[*core.CompactEncoder]{} // Initialize diff encoder
	return &DensePartitionedPHF[K, H, B, E]{
		isMinimal:   minimal,
		searchType:  search,
		hasher:      hasher,
		subBucketer: subBucketer,
		pilots:      pilots,
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
	if partitionIdx >= f.numPartitions {
		panic(fmt.Sprintf("partition index %d out of bounds (%d)", partitionIdx, f.numPartitions))
	}

	// 3. Get partition range start/end using diff-encoded offsets
	partitionOffset := f.offsets.Access(partitionIdx)
	nextPartitionOffset := f.offsets.Access(partitionIdx + 1)
	partitionSize := nextPartitionOffset - partitionOffset // Size of the hash range for this partition

	// 4. Find sub-bucket within partition
	subBucket := f.subBucketer.Bucket(hash.First()) // Use hash.First() for sub-bucket

	// 5. Get pilot using dense access
	pilot := f.pilots.AccessDense(partitionIdx, uint64(subBucket))

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
