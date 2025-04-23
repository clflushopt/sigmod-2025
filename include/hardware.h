// Minimal library to check for some hardware features most
// of the following is adapted from [1] and the AMD Programmer's
// manual.'
#pragma once

#include <cassert>
#include <cpuid.h>
#include <cstdint>

// Uses compiler exposed instrinsics should be available on Clang and GCC.
//
// Ref[1]: https://learn.microsoft.com/en-us/cpp/intrinsics/cpuid-cpuidex?view=msvc-170
static inline auto cpuid(uint32_t fn, uint32_t* cpu_info) noexcept {
    // We know that cpu_info is static since we control the caller.
    __cpuid(fn, cpu_info[0], cpu_info[1], cpu_info[2], cpu_info[3]);
}

// Wrapper around CPUID.
union CPUID {
    uint32_t cpu_info[4];

    struct Registers {
        uint32_t eax;
        uint32_t ebx;
        uint32_t ecx;
        uint32_t edx;
    } registers;

    explicit CPUID(uint32_t fn) noexcept { cpuid(fn, cpu_info); }
};

// Returns true if CPU supports rdtscp (fenced equivalent of rdtsc).
inline auto hasRdtscp() noexcept -> bool {
    return (CPUID(0x80000001).registers.edx >> 27) & 1u;
}

// Returns true if CPU supports SSE 4.2.
//
// From the AMD Programmer's Manual :
//
// CRC32 is a SSE4.2 instruction. Support for SSE4.2 instructions is indicated
// by CPUID Fn0000_0001_ECX[SSE42] = 1
inline auto hasSse42() noexcept -> bool {
    return (CPUID(0x1).registers.ecx >> 20) & 1u;
}
