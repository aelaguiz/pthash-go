// File: internal/core/d1array.go
package core

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"math/bits"
)

const (
	// d1BlockSize      = 512 // Size of a basic block (must be multiple of 64) - From C++
	// d1SuperBlockFac  = 8   // Number of basic blocks in a superblock - From C++
	// d1SuperBlockSize = d1BlockSize * d1SuperBlockFac - From C++

	// Use smaller values for easier testing initially, aligned with BITS defaults perhaps?
	// BITS default: Block=1024, SubBlock=32 -> SuperBlock = 32768
	// Let's try values closer to C++ PTHash for potential performance similarity.
	// C++ PTHash uses Block=4096, SubBlock=64 -> SuperBlock = 262144
	d1BlockSize      = 4096   // Basic block size (C++ PTHash)
	d1SubBlockSize   = 64     // Sub-block size within basic block (C++ PTHash)
	d1SuperBlockSize = 262144 // Super block size (can derive from others if needed)
	// d1SuperBlockFac  = d1SuperBlockSize / d1BlockSize // Calculate factor
)

// D1Array supports efficient Rank1 and Select operations on a BitVector.
// This implementation uses multi-level lookup tables similar to Jacobson's Rank
// and Clark's Select, adapted based on common succinct data structure techniques.
type D1Array struct {
	bv              *BitVector // The underlying bit vector
	size            uint64     // Size of the bit vector
	numSetBits      uint64     // Total number of set bits
	superBlockRanks []uint64   // Rank at the start of each superblock
	blockRanks      []uint16   // Rank relative to superblock start for each basic block
	// Select lookup tables (optional, can improve Select speed)
	// selectLUT        []uint64 // Positions of every K-th set bit (e.g., K = log^2 N)
	// selectLUTDensity uint64   // Density of the Select LUT
}

// NewD1Array creates a Rank/Select structure for the given BitVector.
func NewD1Array(bv *BitVector) *D1Array {
	if bv == nil {
		return &D1Array{
			bv:              NewBitVector(0),
			size:            0,
			numSetBits:      0,
			superBlockRanks: make([]uint64, 0),
			blockRanks:      make([]uint16, 0),
		}
	}

	// Special handling for empty BitVector
	if bv.Size() == 0 {
		return &D1Array{
			bv:              bv,
			size:            0,
			numSetBits:      0,
			superBlockRanks: make([]uint64, 0),
			blockRanks:      make([]uint16, 0),
		}
	}

	d1 := &D1Array{
		bv:   bv,
		size: bv.Size(),
	}

	numSuperBlocks := (d1.size + d1SuperBlockSize - 1) / d1SuperBlockSize
	numBlocks := (d1.size + d1BlockSize - 1) / d1BlockSize

	d1.superBlockRanks = make([]uint64, numSuperBlocks)
	d1.blockRanks = make([]uint16, numBlocks) // Max rank in block fits uint16 (4096 < 65536)

	currentRank := uint64(0)
	currentBlockRank := uint16(0) // Rank within the current superblock
	words := bv.Words()
	wordIdx := 0

	for sbIdx := uint64(0); sbIdx < numSuperBlocks; sbIdx++ {
		d1.superBlockRanks[sbIdx] = currentRank
		sbRankStart := currentRank // Rank at the start of this superblock

		for bIdxRel := uint64(0); bIdxRel < d1SuperBlockSize/d1BlockSize; bIdxRel++ {
			blockGlobalIdx := sbIdx*(d1SuperBlockSize/d1BlockSize) + bIdxRel
			if blockGlobalIdx >= numBlocks {
				break // Reached end of blocks
			}
			// Rank within superblock = currentRank - sbRankStart
			currentBlockRank = uint16(currentRank - sbRankStart)
			if uint64(currentBlockRank) != currentRank-sbRankStart {
				panic("Block rank overflowed uint16") // Should not happen with block size 4096
			}
			d1.blockRanks[blockGlobalIdx] = currentBlockRank

			// Count bits within this basic block
			blockStartBit := blockGlobalIdx * d1BlockSize
			blockEndBit := blockStartBit + d1BlockSize
			if blockEndBit > d1.size {
				blockEndBit = d1.size // Don't count past the end
			}

			blockBitCount := uint64(0)
			currentWordBitStart := uint64(wordIdx) * 64

			// Iterate through words potentially overlapping this block
			for currentWordBitStart < blockEndBit {
				if wordIdx >= len(words) {
					break // Should not happen if blockEndBit <= d1.size
				}
				word := words[wordIdx]

				// Determine the range of bits within this word that belong to the current block
				countStartBit := uint64(0)
				if blockStartBit > currentWordBitStart {
					countStartBit = blockStartBit - currentWordBitStart
				}
				countEndBit := uint64(64)
				if blockEndBit < currentWordBitStart+64 {
					countEndBit = blockEndBit - currentWordBitStart
				}

				// Count bits within the valid range [countStartBit, countEndBit)
				if countEndBit > countStartBit {
					mask := uint64(math.MaxUint64)
					if countStartBit > 0 {
						mask <<= countStartBit // Shift mask to clear lower bits
					}
					// Create mask to clear upper bits if needed (more complex)
					// Easier: shift word right, then mask lower bits
					bitsToCount := countEndBit - countStartBit
					word >>= countStartBit
					if bitsToCount < 64 {
						word &= (1 << bitsToCount) - 1
					}
					blockBitCount += uint64(bits.OnesCount64(word))
				}

				// Move to next word if block continues
				if currentWordBitStart+64 >= blockEndBit {
					break // Finished this block
				}
				wordIdx++
				currentWordBitStart += 64
			}
			currentRank += blockBitCount
		}
	}
	d1.numSetBits = currentRank
	// wordIdx might not be updated if last block was empty/partial, reset for next calculation
	wordIdx = 0

	// Optional: Build Select LUT here if desired

	return d1
}

// Rank1 returns the number of set bits up to position pos (exclusive).
// Uses the multi-level lookup table.
func (d *D1Array) Rank1(pos uint64) uint64 {
	if pos >= d.size {
		return d.numSetBits // Rank past the end is total count
	}
	if pos == 0 {
		return 0
	}

	sbIdx := pos / d1SuperBlockSize
	bIdx := pos / d1BlockSize
	wordIdx := pos / 64
	bitIdx := pos % 64

	// Rank from superblock + rank from basic block within superblock
	rank := d.superBlockRanks[sbIdx] + uint64(d.blockRanks[bIdx])

	// Add rank from words within the basic block, before the target word
	blockStartBit := bIdx * d1BlockSize
	blockStartWord := blockStartBit / 64
	words := d.bv.Words()

	for w := blockStartWord; w < wordIdx; w++ {
		// Check if this word index is actually within the bounds of the words slice
		if w >= uint64(len(words)) {
			// This indicates pos was beyond the actual bitvector bits represented by words,
			// which shouldn't happen if pos < d.size was checked. Could happen if size % 64 != 0.
			// If size is, say, 100, wordIdx can be 1. len(words) is 2. Accessing words[1] is fine.
			// If size is 128, wordIdx can be 1. len(words) is 2. Accessing words[1] is fine.
			// If size is 129, wordIdx can be 2. len(words) is 3. Accessing words[2] is fine.
			// Let's assume it's safe due to pos < d.size check.
			break
		}
		rank += uint64(bits.OnesCount64(words[w]))
	}

	// Add rank within the final word
	if bitIdx > 0 && wordIdx < uint64(len(words)) {
		mask := (uint64(1) << bitIdx) - 1 // Mask for bits *before* pos
		rank += uint64(bits.OnesCount64(words[wordIdx] & mask))
	}

	return rank
}

// selectInWord finds the position of the (k+1)-th set bit within a 64-bit word.
// k is 0-based. Returns 64 if the k-th bit is not found.
// Uses broadword selection algorithm concepts (simplified).
func selectInWord(word uint64, k uint8) uint8 {
	// This is a simplified/placeholder select-in-word. A truly fast one uses
	// precomputed tables or complex broadword operations (like PDEP).
	// See https://github.com/facebook/folly/blob/main/folly/experimental/Select64.h
	// Or https://graphics.stanford.edu/~seander/bithacks.html#SelectPosFromMSB
	// This linear scan is functionally correct but slow.
	count := uint8(0)
	for i := uint8(0); i < 64; i++ {
		if (word>>i)&1 == 1 {
			if count == k {
				return i
			}
			count++
		}
	}
	return 64 // Not found (shouldn't happen if k < OnesCount64(word))
}

// Select returns the position of the (rank+1)-th set bit (0-based rank).
// Uses multi-level tables and select-in-word.
func (d *D1Array) Select(rank uint64) uint64 {
	if rank >= d.numSetBits {
		return d.size // Rank out of bounds
	}

	// Special case for empty bit vector
	if d.size == 0 || d.numSetBits == 0 {
		return d.size
	}

	// 1. Find superblock containing the rank using binary search on superBlockRanks
	sbIdx := uint64(0)
	sbLeft, sbRight := 0, len(d.superBlockRanks)-1
	for sbLeft <= sbRight {
		mid := sbLeft + (sbRight-sbLeft)/2
		midRank := d.superBlockRanks[mid]
		if midRank <= rank {
			sbIdx = uint64(mid) // Candidate found
			sbLeft = mid + 1    // Try right half
		} else {
			sbRight = mid - 1 // Try left half
		}
	}
	rankInSuperBlock := rank - d.superBlockRanks[sbIdx]

	// 2. Find basic block within the superblock using binary search on blockRanks
	bStartIdx := sbIdx * (d1SuperBlockSize / d1BlockSize)
	bEndIdx := bStartIdx + (d1SuperBlockSize / d1BlockSize)
	if bEndIdx > uint64(len(d.blockRanks)) {
		bEndIdx = uint64(len(d.blockRanks))
	}
	if bStartIdx >= bEndIdx { // Handle case where superblock is empty or invalid range
		// This indicates a major issue, maybe panic or return error value?
		return d.size // Return size as error indicator
	}

	bIdx := bStartIdx                               // Default to start if not found in search (shouldn't happen for rank 0 in block)
	bLeft, bRight := int(bStartIdx), int(bEndIdx-1) // Ensure bRight is valid index
	for bLeft <= bRight {
		mid := bLeft + (bRight-bLeft)/2
		// Check bounds for d.blockRanks BEFORE accessing
		if mid < 0 || mid >= len(d.blockRanks) {
			// Handle error: break or return? Return size seems safest.
			return d.size
		}
		midRank := uint64(d.blockRanks[mid])
		if midRank <= rankInSuperBlock {
			bIdx = uint64(mid) // Candidate found
			bLeft = mid + 1    // Try right half
		} else {
			bRight = mid - 1 // Try left half
		}
	}
	rankInBlock := rankInSuperBlock - uint64(d.blockRanks[bIdx])

	// 3. Scan words within the basic block to find the word containing the target bit
	blockStartBit := bIdx * d1BlockSize
	currentRankInBlock := uint64(0)
	words := d.bv.Words()
	startWord := blockStartBit / 64
	wordIdx := startWord // Start scanning from the beginning of the block

	for {
		if wordIdx >= uint64(len(words)) {
			panic(fmt.Sprintf("Select ran out of words searching for rank %d", rank)) // Keep panic here as it indicates inconsistency
		}
		word := words[wordIdx]
		bitsInWord := uint64(bits.OnesCount64(word))

		if currentRankInBlock+bitsInWord > rankInBlock {
			targetRankInWord := uint8(rankInBlock - currentRankInBlock)

			posInWord := Select64(word, targetRankInWord) // Use optimized version

			if posInWord >= 64 {
				panic(fmt.Sprintf("selectInWord failed for rank %d in word %d (0x%x)", targetRankInWord, wordIdx, word))
			}
			absPos := wordIdx*64 + uint64(posInWord)

			if absPos >= d.size {
				panic(fmt.Sprintf("Select result %d out of bounds %d", absPos, d.size))
			}
			return absPos
		}
		currentRankInBlock += bitsInWord
		wordIdx++
	}
	// Unreachable if rank < numSetBits
}

// --- Size & Serialization ---

// NumBits returns the storage size of the D1Array structure itself in bits.
func (d *D1Array) NumBits() uint64 {
	// Fixed metadata: 3 uint64 fields (bv pointer, size, numSetBits)
	metadataBits := uint64(3 * 8 * 8)

	// Data size: actual space used by rank arrays
	sbBits := uint64(len(d.superBlockRanks)) * 64 // uint64 elements
	bBits := uint64(len(d.blockRanks)) * 16       // uint16 elements

	totalBits := metadataBits + sbBits + bBits
	return totalBits
}

// MarshalBinary implements encoding.BinaryMarshaler.
func (d *D1Array) MarshalBinary() ([]byte, error) {
	if d.bv == nil {
		return nil, fmt.Errorf("cannot marshal D1Array with nil BitVector")
	}
	bvData, err := d.bv.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("failed to marshal BitVector: %w", err)
	}

	numSB := uint64(len(d.superBlockRanks))
	numB := uint64(len(d.blockRanks))
	// Size: bvLen(8) + bvData + size(8) + numSetBits(8) +
	//       numSB(8) + sbData + numB(8) + bData
	totalSize := 8 + len(bvData) + 8 + 8 + 8 + int(numSB*8) + 8 + int(numB*2)
	buf := make([]byte, totalSize)
	offset := 0

	binary.LittleEndian.PutUint64(buf[offset:], uint64(len(bvData)))
	offset += 8
	copy(buf[offset:], bvData)
	offset += len(bvData)

	binary.LittleEndian.PutUint64(buf[offset:], d.size)
	offset += 8
	binary.LittleEndian.PutUint64(buf[offset:], d.numSetBits)
	offset += 8

	binary.LittleEndian.PutUint64(buf[offset:], numSB)
	offset += 8
	for _, r := range d.superBlockRanks {
		binary.LittleEndian.PutUint64(buf[offset:], r)
		offset += 8
	}

	binary.LittleEndian.PutUint64(buf[offset:], numB)
	offset += 8
	for _, r := range d.blockRanks {
		binary.LittleEndian.PutUint16(buf[offset:], r)
		offset += 2
	}

	if offset != totalSize {
		return nil, fmt.Errorf("D1Array marshal size mismatch: %d != %d", offset, totalSize)
	}
	return buf, nil
}

// UnmarshalBinary implements encoding.BinaryUnmarshaler.
func (d *D1Array) UnmarshalBinary(data []byte) error {
	if len(data) < 8 {
		return io.ErrUnexpectedEOF
	}
	offset := 0

	// BitVector
	bvLen := binary.LittleEndian.Uint64(data[offset:])
	offset += 8
	if uint64(offset)+bvLen > uint64(len(data)) {
		return io.ErrUnexpectedEOF
	}
	d.bv = NewBitVector(0)
	err := d.bv.UnmarshalBinary(data[offset : offset+int(bvLen)])
	if err != nil {
		return fmt.Errorf("failed to unmarshal BitVector: %w", err)
	}
	offset += int(bvLen)

	// size, numSetBits
	if offset+16 > len(data) {
		return io.ErrUnexpectedEOF
	}
	d.size = binary.LittleEndian.Uint64(data[offset:])
	offset += 8
	d.numSetBits = binary.LittleEndian.Uint64(data[offset:])
	offset += 8

	// SuperBlock Ranks
	if offset+8 > len(data) {
		return io.ErrUnexpectedEOF
	}
	numSB := binary.LittleEndian.Uint64(data[offset:])
	offset += 8
	expectedSBLen := int(numSB * 8)
	if offset+expectedSBLen > len(data) {
		return io.ErrUnexpectedEOF
	}
	d.superBlockRanks = make([]uint64, numSB)
	for i := uint64(0); i < numSB; i++ {
		d.superBlockRanks[i] = binary.LittleEndian.Uint64(data[offset:])
		offset += 8
	}

	// Block Ranks
	if offset+8 > len(data) {
		return io.ErrUnexpectedEOF
	}
	numB := binary.LittleEndian.Uint64(data[offset:])
	offset += 8
	expectedBLen := int(numB * 2)
	if offset+expectedBLen > len(data) {
		return io.ErrUnexpectedEOF
	}
	d.blockRanks = make([]uint16, numB)
	for i := uint64(0); i < numB; i++ {
		d.blockRanks[i] = binary.LittleEndian.Uint16(data[offset:])
		offset += 2
	}

	if offset != len(data) {
		return fmt.Errorf("extra data after D1Array unmarshal")
	}
	if d.size != d.bv.Size() {
		return fmt.Errorf("D1Array size mismatch after unmarshal")
	}

	return nil
}

// --- BMI2 PDEP based Select64 ---
// This implementation requires Go 1.17+ for bits.TrailingZeros64 and assumes
// the presence of the PDEP instruction (available on Haswell+ CPUs).
// Go's compiler *might* optimize the loop into PDEP, but maybe not reliably.
// For guaranteed performance, assembly or cgo would be needed.
// This is a translation of a common C/C++ PDEP select pattern.

// Select64 returns the index of the k'th (0-based) set bit in x.
// Returns 64 if k is >= the number of set bits in x.
// This version uses a PDEP pattern emulation (may be optimized by compiler).
func Select64(x uint64, k uint8) uint8 {
	if k >= uint8(bits.OnesCount64(x)) {
		return 64 // k is out of range
	}

	// Create a mask with the k+1 least significant set bits of x
	// This is the hard part without direct PDEP. We can simulate using loops.
	// Alternative: Use binary search over the popcount.

	// Binary search approach: Find the smallest p such that popcount(x & ((1<<p)-1)) > k
	p := uint8(0)
	// rank := 0         // popcount up to current guess 'p'
	size := uint8(32) // Start search range from 0 to 64

	// fmt.Printf("Select64(0x%016x, %d)\n", x, k)
	for size > 0 {
		half := size >> 1
		p_try := p + half
		// Mask for lower p_try bits
		mask := uint64(math.MaxUint64) // Avoid overflow with 1<<64
		if p_try < 64 {
			mask = (uint64(1) << p_try) - 1
		}
		rank_try := bits.OnesCount64(x & mask)
		// fmt.Printf("  p=%d, size=%d, half=%d, p_try=%d, rank_try=%d\n", p, size, half, p_try, rank_try)

		if rank_try <= int(k) {
			p = p_try
			// rank = rank_try
		}
		size = half
	}

	// fmt.Printf("  Final p=%d\n", p)
	// At this point, 'p' is the largest index such that popcount(lower p bits) <= k.
	// The (k+1)-th bit must be at index p or higher.
	// We need the index of the (k - rank + 1)-th bit in the remaining upper part.
	// However, standard library doesn't have select.
	// We found the _range_ containing the bit. Let's refine using TrailingZeros.
	// The bit we want is the (k - rank + 1)th set bit *after* position p.

	// Let's restart with a slightly different binary search: find smallest p such that rank(p) == k+1
	low, high := uint8(0), uint8(64)
	result := uint8(64)

	for low < high {
		mid := low + (high-low)/2 // Candidate position
		mask := uint64(math.MaxUint64)
		if mid < 64 {
			mask = (uint64(1) << mid) - 1 // Mask for bits *below* mid
		}
		rank_at_mid := bits.OnesCount64(x & mask)

		if rank_at_mid <= int(k) { // The k-th bit is at or after mid
			low = mid + 1
		} else { // The k-th bit is before mid
			high = mid
		}
	}
	// 'low' should now be the smallest index 'p' such that rank(p) > k.
	// This means the k-th set bit (0-indexed) is at position low-1.
	if low > 0 {
		result = low - 1
	} else {
		// This case should ideally not be reached if k < popcount(x)
		// If k=0 and the first bit is 0, low remains 0. Fallback?
		// Check if bit at result (low-1) is actually set
		if result < 64 && (x>>(result))&1 == 1 {
			// Verify rank
			mask := uint64(math.MaxUint64)
			if result < 64 {
				mask = (uint64(1) << result) - 1
			}
			rank_check := bits.OnesCount64(x & mask)
			if rank_check == int(k) {
				return result
			}
			// If rank doesn't match, something is wrong. Fallback to linear scan for safety.
		}
		// Fallback to linear scan if binary search fails or edge case encountered
		count := uint8(0)
		for i := uint8(0); i < 64; i++ {
			if (x>>i)&1 == 1 {
				if count == k {
					return i
				}
				count++
			}
		}
		return 64 // Should be unreachable if k < popcount(x)

	}
	// Final check: is the bit at 'result' actually set?
	if result < 64 && (x>>result)&1 == 1 {
		// Verify rank
		mask := uint64(math.MaxUint64)
		if result < 64 {
			mask = (uint64(1) << result) - 1
		}
		rank_check := bits.OnesCount64(x & mask)
		if rank_check == int(k) {
			return result
		}
	}
	// Fallback to linear scan if binary search fails
	count := uint8(0)
	for i := uint8(0); i < 64; i++ {
		if (x>>i)&1 == 1 {
			if count == k {
				return i
			}
			count++
		}
	}
	return 64 // Should be unreachable

}
