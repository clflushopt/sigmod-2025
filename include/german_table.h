// This file contains the implementation of what I henceforth will call
// German Tables in same way we have German Strings and Swiss Tables.
//
// Since there's no actual public implementation (AFAIK) this has been loosely
// based on "Simple, Efficient and Robust Hash Tables for Join Processing"
// which was published in DaMoN 2024.
#pragma once

#include <cstdint>
#include <nmmintrin.h>

// Compute a single step of CRC32.
static inline auto crc32(uint32_t seed, uint32_t key) -> uint32_t {
    return _mm_crc32_u32(seed, key);
}

inline auto hash32(uint32_t seed, uint32_t key) -> uint64_t {
    // Mixing constant.
    uint64_t k = 0x8648DBDB;
    // CRC32.
    uint32_t crc = crc32(seed, key);
    return crc * ((k << 32) + 1);
}
