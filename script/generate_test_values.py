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
    return f"core.M64{{0x{low:016x}, 0x{high:016x}}}" # Low, High

# --- New Go Struct Literal Printers ---

def print_m64_test_go(d):
    """Calculates and prints M as a Go test case struct literal."""
    if d == 0:
        print(f"\t// d = {d}: Division by zero (skipped)")
        return

    numerator = (1 << 128) - 1
    m_128 = (numerator // d) + 1
    m_high, m_low = to_u64_pair(m_128)

    print(f"\t// Test case for d = {d}")
    print(f"\t{{")
    print(f"\t\td:        {d},")
    print(f"\t\texpected: {format_go_m64(m_high, m_low)}, // M_128 = 0x{m_128:032x}")
    print(f"\t}},")
def print_mul128_u64_test_go(name, low_hi, low_lo, d):
    """Calculates and prints expected result for mul128_u64 as a Go test case struct literal."""
    low_128 = (low_hi << 64) | low_lo
    product_192 = low_128 * d
    expected_high_64 = (product_192 >> 128) & ((1 << 64) - 1) # Ensure it fits uint64

    print(f"\t// Test case: {name}")
    print(f"\t{{")
    print(f"\t\tname:         \"{name}\",")
    print(f"\t\tInputLowHi:   0x{low_hi:016x},")
    print(f"\t\tInputLowLo:   0x{low_lo:016x},")
    print(f"\t\tInputD:       0x{d:016x},")
    print(f"\t\texpectedHigh: 0x{expected_high_64:016x}, // Product192 = 0x{product_192:048x}")
    print(f"\t}},")
def print_fastmod64_test_go(a, d):
    """Calculates M and expected result for FastModU64 as a Go test case struct literal."""
    if d == 0:
        print(f"\t// Test case for a = {a}, d = {d}: Division by zero (skipped)")
        return

    numerator = (1 << 128) - 1
    m_128 = (numerator // d) + 1
    m_high, m_low = to_u64_pair(m_128)
    expected_mod = a % d

    # Optional: Simulate fastmod result for sanity check (as before)
    lowbits_192 = m_128 * a
    lowbits_128 = lowbits_192 & ((1 << 128) - 1)
    final_product_192 = lowbits_128 * d
    simulated_fastmod_result = (final_product_192 >> 128) & ((1 << 64) - 1)
    warning = ""
    if simulated_fastmod_result != expected_mod:
         warning = f"// WARNING: Python sim({simulated_fastmod_result}) != Py %({expected_mod})"

    print(f"\t// Test case for a = {a}, d = {d}")
    print(f"\t{{")
    print(f"\t\tInputA:      {a},")
    print(f"\t\tInputD:      {d},")
    print(f"\t\tM:           {format_go_m64(m_high, m_low)},")
    print(f"\t\tExpectedMod: {expected_mod}, {warning}")
    print(f"\t}},")
if __name__ == "__main__":
    # --- M64 Values ---
    print("// === Expected M64 Go Test Cases ===") # Adjusted header
    print_m64_test_go(1)
    print_m64_test_go(1020)
    print_m64_test_go((1 << 32) - 1)
    print_m64_test_go((1 << 63) - 1)
    print_m64_test_go((1 << 64) - 1)
    print("// === END M64 ===") # Add specific end marker

    # --- mul128_u64 Values ---
    print("\n// === Expected mul128_u64 Go Test Cases ===") # Adjusted header
    print_mul128_u64_test_go("1*5", 0, 1, 5)
    print_mul128_u64_test_go("(1<<64)*5", 1, 0, 5)
    print_mul128_u64_test_go("(2^64-1)*2", 0, (1<<64)-1, 2)
    print_mul128_u64_test_go("(2^63)*2", 0, (1<<63), 2)
    print_mul128_u64_test_go("(2^64+2^63)*2", 1, (1<<63), 2)
    print_mul128_u64_test_go("Large values", 0xABCDEF0123456789, 0x9876543210FEDCBA, 0x1122334455667788)
    print_mul128_u64_test_go("Max values", (1<<64)-1, (1<<64)-1, (1<<64)-1)
    print("// === END mul128_u64 ===") # Add specific end marker

    # --- FastModU64 Values ---
    print("\n// === Expected FastModU64 Go Test Cases ===") # Adjusted header
    print_fastmod64_test_go(0, 1020)
    print_fastmod64_test_go(1, 1020)
    print_fastmod64_test_go(1019, 1020)
    print_fastmod64_test_go(1020, 1020)
    print_fastmod64_test_go(1021, 1020)
    print_fastmod64_test_go(33770903594394249, 1020)
    print_fastmod64_test_go(49276032860695964, 1020)
    large_d = (1 << 63) - 1
    print_fastmod64_test_go(0, large_d)
    print_fastmod64_test_go(1234567890123456789, large_d)
    print_fastmod64_test_go(large_d - 1, large_d)
    print_fastmod64_test_go(large_d, large_d)
    print_fastmod64_test_go(large_d + 1, large_d)
    print_fastmod64_test_go((1 << 64) - 1, large_d)
    print("// === END FastModU64 ===") # Add specific end marker
