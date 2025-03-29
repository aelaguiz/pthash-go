# pthash-go/script/generate_test_values.py
import sys

def to_u64_pair(val_128):
    """Converts a 128-bit integer to high and low uint64 parts."""
    mask64 = (1 << 64) - 1
    low = val_128 & mask64
    high = (val_128 >> 64) & mask64
    return high, low

def format_go_m64(high, low):
    """Formats high, low pair as Go M64 initializer."""
    return f"core.M64{{0x{low:016x}, 0x{high:016x}}} // Low, High"

def print_m64_test(d):
    """Calculates and prints M for ComputeM64."""
    if d == 0:
        print(f"// d = {d}: Division by zero")
        return

    # M = floor( ( (1<<128) - 1 ) / d ) + 1
    numerator = (1 << 128) - 1
    m_128 = (numerator // d) + 1

    m_high, m_low = to_u64_pair(m_128)
    print(f"// For d = {d}")
    print(f"ExpectedM64_{d} := {format_go_m64(m_high, m_low)}")
    print(f"// M_128 = 0x{m_128:032x}")
    print("-" * 20)


def print_mul128_u64_test(low_hi, low_lo, d):
    """Calculates and prints expected result for mul128_u64."""
    low_128 = (low_hi << 64) | low_lo
    product_192 = low_128 * d
    expected_high_64 = product_192 >> 128 # Get bits 128 through 191

    print(f"// Test case for mul128_u64(0x{low_hi:016x}, 0x{low_lo:016x}, 0x{d:016x})")
    print(f"InputLowHi:  0x{low_hi:016x},")
    print(f"InputLowLo:  0x{low_lo:016x},")
    print(f"InputD:      0x{d:016x},")
    print(f"ExpectedHigh: 0x{expected_high_64:016x},")
    print(f"// Product192 = 0x{product_192:048x}") # Show full product for debug
    print("-" * 20)


def print_fastmod64_test(a, d):
    """Calculates M and expected result for FastModU64."""
    if d == 0:
        print(f"// a = {a}, d = {d}: Division by zero")
        return

    numerator = (1 << 128) - 1
    m_128 = (numerator // d) + 1
    m_high, m_low = to_u64_pair(m_128)

    # Calculate expected modulo using Python's arbitrary precision
    expected_mod = a % d

    # Simulate the FastMod intermediate step: lowbits = M * a (lowest 128 bits)
    lowbits_192 = m_128 * a
    lowbits_128 = lowbits_192 & ((1 << 128) - 1)
    lb_high, lb_low = to_u64_pair(lowbits_128)

    # Simulate the final mul128_u64 step: high 64 bits of lowbits * d
    final_product_192 = lowbits_128 * d
    simulated_fastmod_result = final_product_192 >> 128

    print(f"// Test case for FastModU64({a}, {d})")
    print(f"InputA:      {a},")
    print(f"InputD:      {d},")
    print(f"M:           {format_go_m64(m_high, m_low)},")
    # Optional: include intermediate lowbits for debugging mul128_u64 inside fastmod
    # print(f"// Lowbits128: 0x{lowbits_128:032x} (High: 0x{lb_high:016x}, Low: 0x{lb_low:016x})")
    print(f"ExpectedMod: {expected_mod},")
    # Sanity check if our simulation matches Python's %
    if simulated_fastmod_result != expected_mod:
         print(f"// WARNING: Python simulation of FastMod ({simulated_fastmod_result}) != Python % ({expected_mod})")
    print("-" * 20)


if __name__ == "__main__":
    # --- M64 Values ---
    print("=== Expected M64 Values ===")
    print_m64_test(1)
    print_m64_test(1020)
    print_m64_test((1 << 32) - 1)
    print_m64_test((1 << 63) - 1) # Large prime divisor
    print_m64_test((1 << 64) - 1) # Max uint64 - 1 (causes issues if d=2^64)

    # --- mul128_u64 Values ---
    print("\n=== Expected mul128_u64 High Values ===")
    # Simple cases
    print_mul128_u64_test(0, 1, 5) # 1 * 5 = 5 => high 0
    print_mul128_u64_test(1, 0, 5) # (1<<64) * 5 = 5<<64 => high 5
    print_mul128_u64_test(0, (1<<64)-1, 2) # (2^64-1)*2 = 2^65-2 => high 1
    # Cases with carries
    print_mul128_u64_test(0, (1<<63), 2) # (2^63)*2 = 2^64 => high 1
    print_mul128_u64_test(1, (1<<63), 2) # (2^64 + 2^63) * 2 = 2^65 + 2^64 => high 3
    # Larger values
    print_mul128_u64_test(0xABCDEF0123456789, 0x9876543210FEDCBA, 0x1122334455667788)
    print_mul128_u64_test( (1<<64)-1, (1<<64)-1, (1<<64)-1) # Max inputs

    # --- FastModU64 Values ---
    print("\n=== Expected FastModU64 Values ===")
    # d=1020 from previous test
    print_fastmod64_test(0, 1020)
    print_fastmod64_test(1, 1020)
    print_fastmod64_test(1019, 1020)
    print_fastmod64_test(1020, 1020)
    print_fastmod64_test(1021, 1020)
    print_fastmod64_test(33770903594394249, 1020) # From distribution test
    print_fastmod64_test(49276032860695964, 1020) # From distribution test
    # Large divisor
    large_d = (1 << 63) - 1
    print_fastmod64_test(0, large_d)
    print_fastmod64_test(1234567890123456789, large_d)
    print_fastmod64_test(large_d - 1, large_d)
    print_fastmod64_test(large_d, large_d)
    print_fastmod64_test(large_d + 1, large_d)
    print_fastmod64_test((1 << 64) - 1, large_d)