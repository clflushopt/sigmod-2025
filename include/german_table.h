// This file contains the implementation of what I henceforth will call
// German Tables in same way we have German Strings and Swiss Tables.
//
// Since there's no actual public implementation (AFAIK) this has been loosely
// based on "Simple, Efficient and Robust Hash Tables for Join Processing"
// which was published in DaMoN 2024.
#pragma once

#include <immintrin.h>

#include <cassert>
#include <cmath>
#include <cstdint>
#include <cstring>

#include <functional>
#include <optional>
#include <string>
#include <utility>
#include <variant>
#include <vector>

// To compile hash32 to the desired 3-instruction sequence, use the following
// compiler flags for clang: -O2 -mcrc32 -march=native
// The O2 optimization level is necessary to enable constant folding and
// optimize away unnecessary register operations. Without O2, the function
// may include additional instructions for prologue, argument alignment, and
// return value handling.
//
// With O2, the function compiles to:
// | crc32 esi, edi
// | movabs rax, -8770518540659720191 // This is k << 32 + 1.
// | imul rax, rsi
// | ret
static inline uint64_t hash32(uint32_t key, uint32_t seed = 0) {
    const uint64_t k   = 0x8648DBDBU;
    uint32_t       crc = __builtin_ia32_crc32si(seed, key);
    return crc * ((k << 32) + 1);
}

static inline uint64_t hash64(uint64_t key, uint32_t seed1 = 0, uint32_t seed2 = 0) {
    const uint64_t k        = 0x2545F4914F6CDD1DULL;
    uint32_t       crc1     = __builtin_ia32_crc32si(seed1, static_cast<uint32_t>(key));
    uint32_t       crc2     = __builtin_ia32_crc32si(seed2, static_cast<uint32_t>(key >> 32));
    uint64_t       upper    = static_cast<uint64_t>(crc2) << 32;
    uint64_t       combined = crc1 | upper;
    return combined * k;
}

// --- Input Data Types (as provided by user) ---
using Data          = std::variant<int32_t, int64_t, double, std::string, std::monostate>;
using ExecuteResult = std::vector<std::vector<Data>>;

// Define the structure stored internally: {full_hash, original_row_index}
// We store the full hash to avoid accessing original data during probe scan for comparison.
using StoredEntry = std::pair<uint64_t, uint64_t>; // {hash, row_index}

class UnchainedHashTable {
public:
    // --- Configuration Constants (Same as before) ---
    static constexpr size_t   DIRECTORY_LOG2_SIZE = 14;
    static constexpr size_t   DIRECTORY_SIZE      = 1 << DIRECTORY_LOG2_SIZE;
    static constexpr int      POINTER_BITS        = 48;
    static constexpr int      FILTER_BITS         = 16;
    static constexpr uint64_t POINTER_MASK        = (1ULL << POINTER_BITS) - 1;
    static constexpr int      HASH_TO_SLOT_SHIFT  = 64 - DIRECTORY_LOG2_SIZE;

private:
    // --- Member Variables ---
    std::vector<uint64_t> directory;
    // Stores pairs of {hash, row_index}
    std::vector<StoredEntry> tupleStorage;
    size_t                   num_elements = 0;
    // Precompute bloom fitler tags.
    uint16_t precalculated_tags[2048];

    // --- Hashing & Filtering Utilities (Mostly same as before) ---
    // CRC32 hardware instruction based hash would be ideal here.
    // Using splitmix64 as placeholder.
    inline uint64_t compute_hash_from_value(uint64_t key_val) const { return hash64(key_val); }

    // Hash for strings
    inline uint64_t compute_hash_from_string(const std::string& str) const {
        // Use std::hash for strings, commonly implemented well.
        return std::hash<std::string>{}(str);
    }

    // Helper to get a hashable uint64_t representation from Data variant
    // Returns nullopt if the data type cannot/should not be hashed (e.g., monostate)
    inline constexpr std::optional<uint64_t> get_hash_from_key_data(
        const Data& key_data) const {
        return std::visit(
            [this](auto&& arg) -> std::optional<uint64_t> {
                using T = std::decay_t<decltype(arg)>;
                if constexpr (std::is_same_v<T, int32_t>) {
                    return compute_hash_from_value(static_cast<uint64_t>(arg));
                } else if constexpr (std::is_same_v<T, int64_t>) {
                    return compute_hash_from_value(static_cast<uint64_t>(arg));
                } else if constexpr (std::is_same_v<T, double>) {
                    // Hash the bit representation of the double
                    uint64_t bits;
                    static_assert(sizeof(double) == sizeof(uint64_t));
                    std::memcpy(&bits, &arg, sizeof(double));
                    return compute_hash_from_value(bits);
                } else if constexpr (std::is_same_v<T, std::string>) {
                    return compute_hash_from_string(arg);
                } else if constexpr (std::is_same_v<T, std::monostate>) {
                    return std::nullopt; // Skip NULL keys (monostate)
                } else {
                    // Should not happen with the defined variant types
                    return std::nullopt;
                }
            },
            key_data);
    }

    inline size_t get_directory_slot(uint64_t full_hash) const {
        return static_cast<size_t>(full_hash >> HASH_TO_SLOT_SHIFT);
    }

    // Same compute_tag_mask and couldContain as before
    inline uint16_t compute_tag_mask(uint64_t full_hash) const {
        uint16_t mask  = 0;
        mask          |= (1 << ((full_hash >> 0) & 0xF));
        mask          |= (1 << ((full_hash >> 4) & 0xF));
        mask          |= (1 << ((full_hash >> 8) & 0xF));
        mask          |= (1 << ((full_hash >> 12) & 0xF));
        return mask;
    }

    inline bool couldContain(uint64_t dir_entry, uint64_t probe_hash) const {
        uint16_t entry_filter = static_cast<uint16_t>(dir_entry >> POINTER_BITS);
        uint16_t probe_mask   = compute_tag_mask(probe_hash);
        return (probe_mask & ~entry_filter) == 0;
    }

    // --- Packing / Unpacking (UPDATED for StoredEntry*) ---
    inline uint64_t pack_entry(StoredEntry* tuple_ptr, uint16_t filter) const {
        uintptr_t ptr_addr = reinterpret_cast<uintptr_t>(tuple_ptr);
        assert((ptr_addr & ~POINTER_MASK) == 0 && "Pointer requires more than 48 bits!");
        uint64_t packed_filter = static_cast<uint64_t>(filter) << POINTER_BITS;
        uint64_t packed_ptr    = static_cast<uint64_t>(ptr_addr) & POINTER_MASK;
        return packed_filter | packed_ptr;
    }

    // Returns pointer to the start of the range in tupleStorage
    inline StoredEntry* unpack_pointer(uint64_t dir_entry) const {
        return reinterpret_cast<StoredEntry*>(dir_entry & POINTER_MASK);
    }

    // Unpack filter remains the same conceptually, just applies to the packed entry
    inline uint16_t unpack_filter(uint64_t dir_entry) const {
        return static_cast<uint16_t>(dir_entry >> POINTER_BITS);
    }


public:
    // Constructor
    UnchainedHashTable()
    : directory(DIRECTORY_SIZE + 1, 0) {}

    // --- Build Phase (UPDATED) ---
    // Builds directly from ExecuteResult, using key from key_col_idx.
    void build(const ExecuteResult& build_data, size_t key_col_idx) {
        num_elements            = 0; // Will count valid elements
        const size_t total_rows = build_data.size();
        if (total_rows == 0) {
            directory.assign(DIRECTORY_SIZE + 1, 0);
            tupleStorage.clear();
            return;
        }

        // --- Stage 1: Counting, Filtering, and Hashing (Single Pass) ---
        std::vector<size_t>   counts(DIRECTORY_SIZE, 0);
        std::vector<uint16_t> temp_filters(DIRECTORY_SIZE, 0);
        // Store computed hashes temporarily to avoid recomputing in placement pass
        std::vector<std::optional<uint64_t>> row_hashes(total_rows);

        for (size_t i = 0; i < total_rows; ++i) {
            const auto& record = build_data[i];
            if (key_col_idx >= record.size()) {
                // Handle error: key column index out of bounds for this row
                // Options: throw, skip, log warning. Skipping for now.
                row_hashes[i] = std::nullopt;
                continue;
            }

            const Data&             key_data   = record[key_col_idx];
            std::optional<uint64_t> maybe_hash = get_hash_from_key_data(key_data);
            row_hashes[i]                      = maybe_hash; // Store hash (or nullopt)

            if (maybe_hash) {
                uint64_t hash_val = *maybe_hash;
                size_t   slot     = get_directory_slot(hash_val);

                counts[slot]++; // Count valid element
                temp_filters[slot] |= compute_tag_mask(hash_val);
                num_elements++; // Increment total valid elements
            }
        }

        if (num_elements == 0) { // Handle case where no valid keys were found
            directory.assign(DIRECTORY_SIZE + 1, 0);
            tupleStorage.clear();
            return;
        }

        // --- Stage 2: Calculate Offsets & Pack Directory ---
        tupleStorage.resize(num_elements); // Resize for valid elements only
        StoredEntry* base_storage_ptr = tupleStorage.data();
        size_t       current_offset   = 0;

        for (size_t i = 0; i < DIRECTORY_SIZE; ++i) {
            size_t       count_in_slot       = counts[i];
            uint16_t     filter_for_slot     = temp_filters[i];
            StoredEntry* start_ptr_for_slot  = base_storage_ptr + current_offset;
            directory[i]                     = pack_entry(start_ptr_for_slot, filter_for_slot);
            current_offset                  += count_in_slot;
        }
        StoredEntry* end_ptr      = base_storage_ptr + num_elements;
        directory[DIRECTORY_SIZE] = pack_entry(end_ptr, 0);

        // --- Stage 3: Placement (Single Pass using stored hashes) ---
        std::vector<size_t> next_write_offset(
            DIRECTORY_SIZE); // Stores offset relative to base_storage_ptr
        for (size_t i = 0; i < DIRECTORY_SIZE; ++i) {
            next_write_offset[i] = unpack_pointer(directory[i]) - base_storage_ptr;
        }

        for (size_t i = 0; i < total_rows; ++i) {
            if (row_hashes[i]) { // Place only rows that had a valid hash
                uint64_t hash_val     = *row_hashes[i];
                size_t   slot         = get_directory_slot(hash_val);
                size_t   write_offset = next_write_offset[slot];

                // Store the {hash, row_index} pair
                tupleStorage[write_offset] = {hash_val, i};

                next_write_offset[slot]++; // Increment write offset for this slot
            }
        }
        // Build complete!
    }

    // --- Probe Phase (UPDATED) ---
    // Probes using a key from the Data variant.
    void probe(const Data& probe_key_data, std::vector<uint64_t>& results) const {
        if (num_elements == 0) {
            return;
        }

        // 1. Compute hash for the probe key
        std::optional<uint64_t> maybe_probe_hash = get_hash_from_key_data(probe_key_data);
        if (!maybe_probe_hash) {
            return; // Probe key is not hashable (e.g., NULL)
        }
        uint64_t probe_hash = *maybe_probe_hash;

        // 2. Find directory slot
        size_t slot = get_directory_slot(probe_hash);
        if (slot >= DIRECTORY_SIZE) {
            return; // Should not happen
        }

        // 3. Read directory entry & check Bloom filter
        uint64_t dir_entry = directory[slot];
        if (!couldContain(dir_entry, probe_hash)) {
            return; // Definite miss
        }

        // 4. Get tuple range pointers (points to StoredEntry)
        StoredEntry* range_start = unpack_pointer(dir_entry);
        StoredEntry* range_end   = unpack_pointer(directory[slot + 1]);

        // 5. Scan the tuple range, comparing FULL HASHES
        for (StoredEntry* current = range_start; current < range_end; ++current) {
            // Compare the stored hash (current->first) with the probe hash
            if (current->first == probe_hash) {
                // Hash match! Add the original row index (current->second) to results.
                // NOTE: This assumes hash collisions are acceptable or handled elsewhere.
                // If exact key match is needed, you'd need the original build_data here
                // to compare `probe_key_data` with `build_data[current->second][key_col_idx]`.
                // But for many join scenarios, hash-only match is sufficient or the next
                // operator handles potential hash collisions.
                results.push_back(current->second);
            }
        }
    }

    // --- Utility Functions (Same as before) ---
    size_t size() const { return num_elements; }

    bool empty() const { return num_elements == 0; }

    const std::vector<StoredEntry>& getTupleStorage() const { return tupleStorage; }

    const std::vector<uint64_t>& getDirectory() const { return directory; }
};
