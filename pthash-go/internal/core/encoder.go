package core

// Encoder defines the interface for encoding/decoding pilot values.
type Encoder interface {
	// Encode takes a slice of pilot values and encodes them internally.
	Encode(pilots []uint64) error
	// Access retrieves the pilot value at the given index.
	Access(i uint64) uint64
	// NumBits returns the size of the encoded data in bits.
	NumBits() uint64
	// TODO: Add serialization methods later.
	// TODO: Add Name() method?
}

// --- Placeholder Implementations ---

type RiceEncoder struct { /* TODO */ }

func (e *RiceEncoder) Encode(pilots []uint64) error { /* TODO */ return nil }
func (e *RiceEncoder) Access(i uint64) uint64      { /* TODO */ return 0 }
func (e *RiceEncoder) NumBits() uint64            { /* TODO */ return 0 }

type CompactEncoder struct { /* TODO */ }

func (e *CompactEncoder) Encode(pilots []uint64) error { /* TODO */ return nil }
func (e *CompactEncoder) Access(i uint64) uint64      { /* TODO */ return 0 }
func (e *CompactEncoder) NumBits() uint64            { /* TODO */ return 0 }

// EliasFano is needed for minimal PHF free slots. Placeholder.
type EliasFano struct { /* TODO */ }

// TODO: Implement DualEncoder, DenseEncoder interfaces/structs later

// Placeholder diff encoder for dense partitioned offsets
type DiffCompactEncoder struct { /* TODO */ }
