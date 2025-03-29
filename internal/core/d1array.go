package core

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"math/bits"
)

const (
	d1BlockSize     = 512 // Size of a basic block (must be multiple of 64)
	d1SuperBlockFac = 8   // Number of basic blocks in a superblock
	d1SuperBlockSize = d1BlockSize * d1SuperBlockFac
)

// D1Array supports Rank1 and Select operations on a BitVector.
// This implementation uses precomputed block counts for efficiency.
type D1Array struct {
	bv              *BitVector // The underlying bit vector
	size            uint64     // Size of the bit vector
	numSetBits      uint64     // Total number of set bits
	superBlockRanks []uint64   // Rank at the start of each superblock
	blockRanks      []uint16   // Rank relative to superblock start for each basic block
}

// NewD1Array creates a Rank/Select structure for the given BitVector.
func NewD1Array(bv *BitVector) *D1Array {
	if bv == nil {
		// Handle nil BitVector gracefully
		return &D1Array{
			bv:              NewBitVector(0), // Create empty
			size:            0,
			numSetBits:      0,
			superBlockRanks: []uint64{},
			blockRanks:      []uint16{},
		}
	}

	d1 := &D1Array{
		bv:   bv,
		size: bv.Size(),
	}

	numSuperBlocks := (d1.size + d1SuperBlockSize - 1) / d1SuperBlockSize
	numBlocks := (d1.size + d1BlockSize - 1) / d1BlockSize

	d1.superBlockRanks = make([]uint64, numSuperBlocks)
	d1.blockRanks = make([]uint16, numBlocks)

	currentRank := uint64(0)
	currentBlockRank := uint16(0) // Rank within the current superblock

	words := bv.Words()
	wordIdx := 0

	for sbIdx := uint64(0); sbIdx < numSuperBlocks; sbIdx++ {
		d1.superBlockRanks[sbIdx] = currentRank
		currentBlockRank = 0 // Reset rank for the new superblock

		for bIdx := uint64(0); bIdx < d1SuperBlockFac; bIdx++ {
			blockGlobalIdx := sbIdx*d1SuperBlockFac + bIdx
			if blockGlobalIdx >= numBlocks {
				break // No more blocks left
			}
			d1.blockRanks[blockGlobalIdx] = currentBlockRank

			// Count bits within this basic block (d1BlockSize bits)
			blockBitCount := uint64(0)
			startWord := (blockGlobalIdx * d1BlockSize) / 64
			endWord := ((blockGlobalIdx + 1) * d1BlockSize) / 64
			if endWord > uint64(len(words)) {
				endWord = uint64(len(words)) // Don't read past end of words slice
			}

			for w := startWord; w < endWord; w++ {
				// Adjust count if block doesn't align perfectly with words
				word := words[w]
				startBit := uint64(0)
				endBit := uint64(64)

				// Handle start boundary
				if w == startWord && (blockGlobalIdx*d1BlockSize)%64 != 0 {
					startBit = (blockGlobalIdx * d1BlockSize) % 64
					word >>= startBit // Align relevant bits to LSB
				}

				// Handle end boundary (and potentially size boundary)
				blockEndBitGlobal := (blockGlobalIdx + 1) * d1BlockSize
				wordEndBitGlobal := (w + 1) * 64
				if blockEndBitGlobal < wordEndBitGlobal && blockEndBitGlobal <= d1.size {
					endBit = blockEndBitGlobal % 64
					if endBit == 0 { endBit = 64 } // If ends on boundary
				}
				// Also handle overall size boundary within the word
				if d1.size < wordEndBitGlobal {
					sizeEndBit := d1.size % 64
					if sizeEndBit == 0 && d1.size > 0 { sizeEndBit = 64 }
					if sizeEndBit < endBit { endBit = sizeEndBit }
				}


				bitsInWordToCount := uint64(0)
				if endBit > startBit {
					bitsInWordToCount = endBit - startBit
					mask := uint64(math.MaxUint64)
					if bitsInWordToCount < 64 {
						mask = (1 << bitsInWordToCount) - 1
					}
					blockBitCount += uint64(bits.OnesCount64(word & mask))
				}
			}


			currentRank += blockBitCount
			if currentBlockRank+uint16(blockBitCount) < currentBlockRank {
				panic("blockRank overflow") // Should not happen with uint16
			}
			currentBlockRank += uint16(blockBitCount)
		}
	}
	d1.numSetBits = currentRank

	return d1
}

// Rank1 returns the number of set bits up to position pos (exclusive).
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

	rank := d.superBlockRanks[sbIdx] + uint64(d.blockRanks[bIdx])

	// Count remaining bits in the last basic block up to wordIdx
	startWordInBlock := (bIdx * d1BlockSize) / 64
	words := d.bv.Words()
	for w := startWordInBlock; w < wordIdx; w++ {
		wordOffsetInBlock := (w * 64) % d1BlockSize
		// Count bits only if they are actually part of the current block
		if wordOffsetInBlock < d1BlockSize { // Basic check
			// Refined boundary checks as in constructor might be needed for perfect accuracy
			rank += uint64(bits.OnesCount64(words[w]))
		}
	}

	// Count remaining bits in the final word up to bitIdx
	if bitIdx > 0 && wordIdx < uint64(len(words)) {
		mask := (uint64(1) << bitIdx) - 1
		rank += uint64(bits.OnesCount64(words[wordIdx] & mask))
	}

	return rank
}

// Select returns the position of the (rank+1)-th set bit (0-based rank).
func (d *D1Array) Select(rank uint64) uint64 {
	if rank >= d.numSetBits {
		return d.size // Rank out of bounds
	}

	// 1. Find the superblock containing the rank using binary search or linear scan
	// Linear scan for simplicity first
	sbIdx := uint64(0)
	for sbIdx+1 < uint64(len(d.superBlockRanks)) && d.superBlockRanks[sbIdx+1] <= rank {
		sbIdx++
	}
	rankInSuperBlock := rank - d.superBlockRanks[sbIdx]

	// 2. Find the basic block within the superblock containing the rank
	bStartIdx := sbIdx * d1SuperBlockFac
	bEndIdx := bStartIdx + d1SuperBlockFac
	if bEndIdx > uint64(len(d.blockRanks)) {
		bEndIdx = uint64(len(d.blockRanks))
	}
	bIdx := bStartIdx
	for bIdx+1 < bEndIdx && uint64(d.blockRanks[bIdx+1]) <= rankInSuperBlock {
		bIdx++
	}
	rankInBlock := rankInSuperBlock - uint64(d.blockRanks[bIdx])

	// 3. Scan words within the basic block to find the exact position
	startBitPos := bIdx * d1BlockSize
	currentRankInBlock := uint64(0)
	words := d.bv.Words()
	startWord := startBitPos / 64

	for w := startWord; ; w++ {
		if w >= uint64(len(words)) {
			break // Should not happen if rank is valid
		}
		word := words[w]
		bitsInWord := uint64(bits.OnesCount64(word))

		if currentRankInBlock+bitsInWord > rankInBlock {
			// The target bit is within this word
			targetRankInWord := rankInBlock - currentRankInBlock
			// Find the (targetRankInWord+1)-th set bit within the word
			// This requires a select-in-word operation. bits.Select is not stdlib.
			// We can simulate it.
			count := uint64(0)
			for bit := uint64(0); bit < 64; bit++ {
				if (word>>bit)&1 == 1 {
					if count == targetRankInWord {
						absPos := w*64 + bit
						// Final check: make sure position is within bounds and rank is correct
						if absPos < d.size && d.Rank1(absPos+1) == rank+1 {
							return absPos
						}
						// If check fails, indicates logic error somewhere
						panic(fmt.Sprintf("Select internal error: pos %d mismatch for rank %d", absPos, rank))
					}
					count++
				}
			}
			// Should have found it if bitsInWord > targetRankInWord
			panic(fmt.Sprintf("Select internal error: bit not found in word %d for rank %d", w, rank))
		}
		currentRankInBlock += bitsInWord
	}

	return d.size // Should be unreachable if rank is valid
}

// NumBits returns the size of the D1Array structure itself.
func (d *D1Array) NumBits() uint64 {
	// Pointer to bv + size + numSetBits + superBlockRanks slice + blockRanks slice
	sbBits := uint64(len(d.superBlockRanks)) * 64
	bBits := uint64(len(d.blockRanks)) * 16 // uint16
	// Rough estimate, not including slice header overheads
	return 64 + 64 + 64 + sbBits + bBits
}

// MarshalBinary / UnmarshalBinary (basic implementation)
func (d *D1Array) MarshalBinary() ([]byte, error) {
	if d.bv == nil {
		return nil, fmt.Errorf("cannot marshal D1Array with nil BitVector")
	}
	bvData, err := d.bv.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("failed to marshal BitVector: %w", err)
	}

	// Size: bvLen(8) + bvData + size(8) + numSetBits(8) +
	//       numSB(8) + sbData + numB(8) + bData
	numSB := uint64(len(d.superBlockRanks))
	numB := uint64(len(d.blockRanks))
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

func (d *D1Array) UnmarshalBinary(data []byte) error {
	if len(data) < 8 { return io.ErrUnexpectedEOF }
	offset := 0
	bvLen := binary.LittleEndian.Uint64(data[offset:])
	offset += 8
	if offset+int(bvLen) > len(data) { return io.ErrUnexpectedEOF }
	d.bv = NewBitVector(0) // Create empty first
	err := d.bv.UnmarshalBinary(data[offset:offset+int(bvLen)])
	if err != nil { return fmt.Errorf("failed to unmarshal BitVector: %w", err) }
	offset += int(bvLen)

	if offset+8+8 > len(data) { return io.ErrUnexpectedEOF }
	d.size = binary.LittleEndian.Uint64(data[offset:])
	offset += 8
	d.numSetBits = binary.LittleEndian.Uint64(data[offset:])
	offset += 8

	if offset+8 > len(data) { return io.ErrUnexpectedEOF }
	numSB := binary.LittleEndian.Uint64(data[offset:])
	offset += 8
	expectedSBLen := int(numSB * 8)
	if offset+expectedSBLen > len(data) { return io.ErrUnexpectedEOF }
	d.superBlockRanks = make([]uint64, numSB)
	for i := uint64(0); i < numSB; i++ {
		d.superBlockRanks[i] = binary.LittleEndian.Uint64(data[offset:])
		offset += 8
	}

	if offset+8 > len(data) { return io.ErrUnexpectedEOF }
	numB := binary.LittleEndian.Uint64(data[offset:])
	offset += 8
	expectedBLen := int(numB * 2)
	if offset+expectedBLen > len(data) { return io.ErrUnexpectedEOF }
	d.blockRanks = make([]uint16, numB)
	for i := uint64(0); i < numB; i++ {
		d.blockRanks[i] = binary.LittleEndian.Uint16(data[offset:])
		offset += 2
	}

	if offset != len(data) { return fmt.Errorf("extra data after D1Array unmarshal") }
	if d.size != d.bv.Size() { return fmt.Errorf("D1Array size mismatch after unmarshal") }

	return nil
}
