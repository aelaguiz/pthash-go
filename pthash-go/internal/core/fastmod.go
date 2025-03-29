// Package core provides fundamental types and utilities.
package core

import (
	"math/bits"
)

// M64 represents the 128-bit magic constant for 64-bit fastmod.
type M64 [2]uint64 // [0] is Low, [1] is High

// M32 represents the 64-bit magic constant for 32-bit fastmod.
type M32 uint64

// ComputeM32 computes the magic number for 32-bit fast modulus.
// M = ceil( (1<<64) / d ), d > 0
func ComputeM32(d uint32) M32 {
	if d == 0 {
		panic("division by zero")
	}
	// (0xFFFFFFFFFFFFFFFF / d) + 1
	return M32(^uint64(0)/uint64(d) + 1)
}

// FastModU32 computes (a % d) given precomputed M.
// Based on lemire/fastmod C code:
// uint64_t lowbits = M * a;
// return (uint32_t)mul128_u32(lowbits, d);
func FastModU32(a uint32, m M32, d uint32) uint32 {
	lowbits := uint64(m) * uint64(a)
	// Equivalent to C's __uint128_t product = lowbits * d; return uint32_t(product >> 64);
	hi, _ := bits.Mul64(lowbits, uint64(d))
	return uint32(hi)
}

// FastDivU32 computes (a / d) given precomputed M for d > 1.
// Based on lemire/fastmod C code:
// return (uint32_t)mul128_u32(M, a);
func FastDivU32(a uint32, m M32) uint32 {
	// Equivalent to C's __uint128_t product = M * a; return uint32_t(product >> 64);
	hi, _ := bits.Mul64(uint64(m), uint64(a))
	return uint32(hi)
}

// IsDivisible checks if n % d == 0 using precomputed M.
// Based on lemire/fastmod C code:
// return n * M <= M - 1;
func IsDivisible(n uint32, m M32) bool {
	mVal := uint64(m)
	prod := uint64(n) * mVal
	return prod <= mVal-1
}

// ComputeM64 computes the 128-bit magic number for 64-bit fast modulus.
// M = ceil( (1<<128) / d ), d > 0
// This is complex without native 128-bit division.
// We use the logic from https://github.com/lemire/fastmod/pull/19#issuecomment-413074893
// M = floor( ( (1<<128) - 1 ) / d ) + 1
// Requires 128-bit division by 64-bit.
func ComputeM64(d uint64) M64 {
	if d == 0 {
		panic("division by zero")
	}
	// Calculate (2^128 - 1) / d.
	// Let N = 2^128 - 1 = (N_h << 64) | N_l, where N_h = N_l = 2^64 - 1.
	// We want to compute N / d. Using schoolbook division:
	// q_h = N_h / d
	// r_h = N_h % d
	// N' = (r_h << 64) | N_l
	// q_l = N' / d
	// Result q = (q_h << 64) | q_l
	// Go doesn't have Div128by64 directly, but we can use Div64.
	// qh = floor( (2^64-1) / d )
	qh := ^uint64(0) / d
	// rh = (2^64-1) % d = (2^64-1) - qh * d
	rh := ^uint64(0) - qh*d

	// Now compute N' / d where N' = (rh << 64) | (2^64-1)
	// We use bits.Div64 which computes floor( (hi << 64 | lo) / y )
	ql, _ := bits.Div64(rh, ^uint64(0), d)

	// Result is floor( (2^128 - 1) / d ) = (qh << 64) | ql
	// We need M = floor(...) + 1
	var m M64
	m[0], m[1] = bits.Add64(ql, 1, 0) // Add 1 to the low part, propagate carry
	m[1], _ = bits.Add64(qh, m[1], 0) // Add the carry to the high part
	return m
}

// mul128_u64 calculates the high 64 bits of (lowbits * d), where lowbits is 128-bit.
// lowbits = [low_hi << 64 | low_lo]
// product = lowbits * d = (low_hi * d << 64) + (low_lo * d)
// We need the top 64 bits of this 192-bit result.
// product = (Phh << 128) | (Phl << 64) | (Plh << 64) | Pll
// P_high = low_hi * d (128 bits: Phh, Phl)
// P_low = low_lo * d (128 bits: Plh, Pll)
// Add the overlapping parts: middle = Phl + Plh
// Carry from middle affects Phh.
// Result high 64 bits = Phh + carry_from(middle)
func mul128_u64(low_hi, low_lo, d uint64) uint64 {
	phh, phl := bits.Mul64(low_hi, d)
	plh, _ := bits.Mul64(low_lo, d) // Changed pll to _
	_, carry := bits.Add64(phl, plh, 0)
	res, _ := bits.Add64(phh, 0, carry) // Add carry to phh
	return res
}

// FastModU64 computes (a % d) given precomputed M (128-bit).
// Based on lemire/fastmod C code:
// __uint128_t lowbits = M * a; // M is 128-bit
// return mul128_u64(lowbits, d); // mul128_u64 needs high 64 bits of (128-bit * 64-bit)
func FastModU64(a uint64, m M64, d uint64) uint64 {
	// Calculate M * a (128-bit * 64-bit = 192-bit, need lowest 128 bits)
	// M = [m[1] << 64 | m[0]]
	// lowbits = (m[1]*a << 64) + m[0]*a
	p1h, p1l := bits.Mul64(m[0], a) // Low part product
	p0h, _ := bits.Mul64(m[1], a)   // High part product (only need lower 64 bits, p0l is discarded)

	// Add the high part of the low product to the low part of the high product
	// lowbits_lo = p1l
	// lowbits_hi = p1h + p0h (with carry)
	lowbits_hi, _ := bits.Add64(p1h, p0h, 0)

	// If there was a carry when calculating lowbits_hi, it's part of the bits
	// above the 128 we need, so we ignore it.
	// lowbits = [lowbits_hi << 64 | p1l] (conceptually)

	// Now compute mul128_u64(lowbits, d) which needs high 64 bits of lowbits * d
	// lowbits is [lowbits_hi, p1l]
	return mul128_u64(lowbits_hi, p1l, d)
}

// FastDivU64 computes (a / d) given precomputed M (128-bit) for d > 1.
// Based on lemire/fastmod C code:
// return mul128_u64(M, a); // mul128_u64 needs high 64 bits of (128-bit * 64-bit)
func FastDivU64(a uint64, m M64) uint64 {
	// Compute M * a, need high 64 bits. M is [m[1], m[0]].
	// This is exactly what mul128_u64 calculates if lowbits = M.
	// So, low_hi = m[1], low_lo = m[0], d = a
	return mul128_u64(m[1], m[0], a)
}

// NOTE: Signed integer versions (computeM_s32, fastmod_s32, etc.) are omitted
// for Phase 1 brevity but would follow similar porting logic, paying close
// attention to sign extension and signed overflow rules.
