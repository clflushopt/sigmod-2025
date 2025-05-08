#include "attribute.h"
#include "fmt/base.h"
#include "statement.h"
#include <cassert>
#include <columnar_exec.h>
#include <cstddef>
#include <cstdint>
#include <hardware__talos.h>
#include <mutex>
#include <par_german_table.h>
#include <parallel_hashmap/phmap.h>
#include <plan.h>
#include <pthread.h>
#include <string>
#include <thread>
#include <tuple>
#include <unordered_map>
#include <vector>

using phmap::flat_hash_map;
using ExecuteResult = std::vector<std::vector<Data>>;
using OutputAttrs   = std::vector<std::tuple<size_t, DataType>>;

// PartionedHashTable is a vector of unordered maps, where each map represents a
// a single partition that maps a single key to a vector of row indices.
template <typename T>
using PartitionedHashTable = std::vector<flat_hash_map<uint64_t, std::vector<size_t>>>;


// NumPartitions is the number of partitions in the hash table, this is set to the number
// of cores available on the system.
constexpr size_t NumPartitions = 32;

// Returns the offset within a page of a value of type T where T is restricted
// to fixed length types since those are the only ones we expect to be used in
// the hash join.
template <typename T>
constexpr size_t get_fixed_data_offset() {
    if constexpr (sizeof(T) == 4) {
        return 4;
    } else {
        return 8;
    }
}

static inline bool get_bitmap(const uint8_t* bitmap, uint16_t idx) {
    auto byte_idx = idx / 8;
    auto bit      = idx % 8;
    return bitmap[byte_idx] & (1u << bit);
}

// --- Parallel Build Phase ---
template <typename T>
static void build_worker(size_t thread_id,        // Thread ID for pinning
    const std::vector<Page*>&   pages_for_thread, // Pages assigned to this thread
    size_t                      start_row_offset, // Global row index offset for the first page
    PartitionedHashTable<T>&    ht_partitions,    // Shared partitioned hash table
    std::vector<std::mutex>&    partition_mutexes // Mutexes for each partition
) {
    size_t                  current_row = start_row_offset;
    constexpr size_t        data_offset = get_fixed_data_offset<T>();
    PartitionedHashTable<T> local_partitions(NumPartitions);
    std::hash<T>            hasher; // Hash function object

    for (const auto* page: pages_for_thread) {
        uint16_t numrows = *reinterpret_cast<const uint16_t*>(page->data);
        if (numrows == 0) {
            continue;
        }

        const T*       values = reinterpret_cast<const T*>(page->data + data_offset);
        const uint8_t* bitmap =
            reinterpret_cast<const uint8_t*>(page->data + PAGE_SIZE - (numrows + 7) / 8);
        size_t value_idx = 0; // Index for packed non-NULL values

        for (uint16_t i = 0; i < numrows; ++i) {
            bool is_not_null = get_bitmap(bitmap, i);  // Assume get_bitmap exists
            if (is_not_null) {
                const T& key = values[value_idx];      // Get the key
                size_t   part_idx =
                    hasher(key) & (NumPartitions - 1); // Calculate partition index

                // Insert into thread local partition.
                local_partitions[part_idx][key].emplace_back(current_row);

                value_idx++; // Increment only for non-NULL
            }
            current_row++;   // Increment global row index for every slot
        }
    }

    // Merge thread local results into the shared partitioned hash table.
    for (size_t p_idx = 0; p_idx < NumPartitions; ++p_idx) {
        if (!local_partitions[p_idx].empty()) {
            // Acquire lock for the specific global partition.
            std::lock_guard<std::mutex> lock(partition_mutexes[p_idx]);
            // Merge local partition into the global partition.
            auto& partition = ht_partitions[p_idx];
            for (const auto& [key, rows]: local_partitions[p_idx]) {
                partition[key].insert(partition[key].end(), rows.begin(), rows.end());
            }
        }
    }
}

template <typename T>
static void hash_join_build_partitioned(const ColumnarTable& table,
    size_t                                                   join_col,
    PartitionedHashTable<T>& ht_partitions,    // Output: partitioned hash table
    std::vector<std::mutex>& partition_mutexes // Output: mutexes (created here)
) {
    const auto&  column      = table.columns[join_col];
    unsigned int num_threads = std::thread::hardware_concurrency();

    ht_partitions.resize(NumPartitions);

    std::vector<std::thread> threads;
    threads.reserve(num_threads);

    size_t total_pages      = column.pages.size();
    size_t pages_per_thread = (total_pages + num_threads - 1) / num_threads; // Ceiling division
    size_t start_page_idx   = 0;

    // Pre-calculate row offsets for each chunk start (optional but cleaner)
    std::vector<size_t> page_start_rows(total_pages + 1, 0);
    for (size_t i = 0; i < total_pages; ++i) {
        uint16_t numrows       = *reinterpret_cast<const uint16_t*>(column.pages[i]->data);
        page_start_rows[i + 1] = page_start_rows[i] + numrows;
    }


    for (unsigned int i = 0; i < num_threads; ++i) {
        size_t end_page_idx = std::min(start_page_idx + pages_per_thread, total_pages);
        if (start_page_idx >= end_page_idx) {
            break; // No more pages
        }

        // Create a sub-vector of page pointers for the thread
        std::vector<Page*> pages_for_thread(column.pages.begin() + start_page_idx,
            column.pages.begin() + end_page_idx);

        size_t start_row_for_thread = page_start_rows[start_page_idx];

        // Build the thread object and pin it to a core explicitly.
        threads.emplace_back(build_worker<T>,
            i,                           // Pin to core
            std::move(pages_for_thread), // Pass pages chunk
            start_row_for_thread,        // Pass starting global row index
            std::ref(ht_partitions),     // Pass reference to shared partitions
            std::ref(partition_mutexes)  // Pass reference to shared mutexes
        );

        start_page_idx = end_page_idx;
    }

    // Wait for all build threads to complete
    for (auto& t: threads) {
        if (t.joinable()) {
            t.join();
        }
    }
}

// --- Parallel Probe Phase ---
template <typename T>
static void probe_worker(size_t             thread_id, // Thread ID for pinning
    const std::vector<Page*>&               pages_for_thread,
    size_t                                  start_row_offset,
    const PartitionedHashTable<T>&          ht_partitions, // Read-only access
    std::vector<std::pair<size_t, size_t>>& thread_matches // Output for this thread
) {
    // Pin this thread to a core.
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(thread_id, &cpuset);
    pthread_t thread = pthread_self();
    pthread_setaffinity_np(thread, sizeof(cpu_set_t), &cpuset);

    size_t           current_row = start_row_offset;
    constexpr size_t data_offset = get_fixed_data_offset<T>();
    std::hash<T>     hasher;

    for (const auto* page: pages_for_thread) {
        uint16_t numrows = *reinterpret_cast<const uint16_t*>(page->data);
        if (numrows == 0) {
            continue;
        }

        const T*       values = reinterpret_cast<const T*>(page->data + data_offset);
        const uint8_t* bitmap =
            reinterpret_cast<const uint8_t*>(page->data + PAGE_SIZE - (numrows + 7) / 8);
        size_t value_idx = 0;

        for (uint16_t i = 0; i < numrows; ++i) {
            bool is_not_null = get_bitmap(bitmap, i); // Assume get_bitmap exists
            if (is_not_null) {
                const T& key      = values[value_idx];
                size_t   part_idx = hasher(key) & (NumPartitions - 1);

                // Probe the *specific* partition (no lock needed for read)
                const auto& partition = ht_partitions[part_idx];
                auto        it        = partition.find(key);

                if (it != partition.end()) {
                    // Found matches in the build table partition
                    for (size_t build_row: it->second) {
                        thread_matches.emplace_back(current_row, build_row);
                    }
                }
                value_idx++; // Increment only for non-NULL
            }
            current_row++;   // Increment global row index
        }
    }
}

template <typename T>
static void hash_join_probe_partitioned(const ColumnarTable& table,
    size_t                                                   join_col,
    const PartitionedHashTable<T>&          ht_partitions, // Input: Pre-built partitions
    std::vector<std::pair<size_t, size_t>>& matches        // Output: All matches
) {
    const auto&  column      = table.columns[join_col];
    unsigned int num_threads = std::thread::hardware_concurrency();
    if (num_threads == 0) {
        num_threads = 1;
    }

    std::vector<std::thread> threads;
    threads.reserve(num_threads);
    // Create a vector to hold results from each thread
    std::vector<std::vector<std::pair<size_t, size_t>>> thread_results(num_threads);

    size_t total_pages      = column.pages.size();
    size_t pages_per_thread = (total_pages + num_threads - 1) / num_threads;
    size_t start_page_idx   = 0;

    // Pre-calculate row offsets for each chunk start (same logic as build)
    std::vector<size_t> page_start_rows(total_pages + 1, 0);
    for (size_t i = 0; i < total_pages; ++i) {
        uint16_t numrows       = *reinterpret_cast<const uint16_t*>(column.pages[i]->data);
        page_start_rows[i + 1] = page_start_rows[i] + numrows;
    }

    for (unsigned int i = 0; i < num_threads; ++i) {
        size_t end_page_idx = std::min(start_page_idx + pages_per_thread, total_pages);
        if (start_page_idx >= end_page_idx) {
            break;
        }

        std::vector<Page*> pages_for_thread(column.pages.begin() + start_page_idx,
            column.pages.begin() + end_page_idx);
        size_t             start_row_for_thread = page_start_rows[start_page_idx];


        threads.emplace_back(probe_worker<T>,
            i, // Pin to core
            std::move(pages_for_thread),
            start_row_for_thread,
            std::cref(ht_partitions),   // Pass const reference (read-only)
            std::ref(thread_results[i]) // Pass reference to thread's result vector
        );
        start_page_idx = end_page_idx;
    }

    // Wait for all probe threads to complete
    size_t total_matches = 0;
    for (auto& t: threads) {
        if (t.joinable()) {
            t.join();
        }
    }

    // Aggregate results from all threads
    // Pre-calculate total size for efficiency
    for (const auto& results: thread_results) {
        total_matches += results.size();
    }
    matches.reserve(total_matches); // Reserve space in final vector
    for (const auto& results: thread_results) {
        matches.insert(matches.end(), results.begin(), results.end());
    }
}

ColumnarTable ColumnarExecutor::execute_impl(const Plan& plan, size_t node_idx) {
    auto& node = plan.nodes[node_idx];
    return std::visit(
        [&](const auto& value) {
            using T = std::decay_t<decltype(value)>;
            if constexpr (std::is_same_v<T, JoinNode>) {
                return execute_join(plan, value, node.output_attrs);
            } else {
                return execute_scan(plan, value, node.output_attrs);
            }
        },
        node.data);
}

template <typename T>
static void hash_join_build(const ColumnarTable& table,
    size_t                                       join_col,
    flat_hash_map<T, std::vector<size_t>>&       ht) {
    auto&  column = table.columns[join_col];
    size_t row    = 0;

    // Iterate over each page and build the hash table from the
    // column data.
    for (auto* page: column.pages) {
        uint16_t numrows     = *reinterpret_cast<uint16_t*>(page->data);
        size_t   data_offset = 0;

        // Since we only expect joins on fixed length types we can
        // be sure we don't need to worry about strings which can
        // span multiple pages.
        if constexpr (sizeof(T) == 4) {
            data_offset = 4;
        } else {
            data_offset = 8;
        }

        auto*  values = reinterpret_cast<T*>(page->data + data_offset);
        auto*  bitmap = reinterpret_cast<uint8_t*>(page->data + PAGE_SIZE - (numrows + 7) / 8);
        size_t value_idx = 0;

        for (uint16_t i = 0; i < numrows; i++) {
            bool is_not_null = get_bitmap(bitmap, i);
            if (is_not_null) {
                ht[values[value_idx]].emplace_back(row);
                value_idx++;
            }
            row++;
        }
    }
}

template <typename T>
static void hash_join_probe(const ColumnarTable& table,
    size_t                                       join_col,
    const flat_hash_map<T, std::vector<size_t>>& ht,
    std::vector<std::pair<size_t, size_t>>&      matches) {
    auto&  column = table.columns[join_col];
    size_t row    = 0;

    // Iterate over each page and build the hash table from the
    // column data.
    for (auto* page: column.pages) {
        uint16_t numrows     = *reinterpret_cast<uint16_t*>(page->data);
        size_t   data_offset = 0;

        // Since we only expect joins on fixed length types we can
        // be sure we don't need to worry about strings which can
        // span multiple pages.
        if constexpr (sizeof(T) == 4) {
            data_offset = 4;
        } else {
            data_offset = 8;
        }
        // Since we only expect joins on fixed length types we can
        // be sure we don't need to worry about strings which can
        // span multiple pages.
        auto*  values    = reinterpret_cast<T*>(page->data + data_offset);
        size_t value_idx = 0;
        auto*  bitmap = reinterpret_cast<uint8_t*>(page->data + PAGE_SIZE - (numrows + 7) / 8);

        for (uint16_t i = 0; i < numrows; i++) {
            bool is_not_null = get_bitmap(bitmap, i);
            if (is_not_null) {
                auto it = ht.find(values[value_idx]);
                if (it != ht.end()) {
                    for (auto match_row: it->second) {
                        matches.emplace_back(row, match_row);
                    }
                }
            }
            row++;
            if (is_not_null) {
                value_idx++;
            }
        }
    }
}

template <typename T>
std::vector<std::optional<T>> extract_values_for_column(const ColumnarTable& table,
    size_t                                                                   column_idx) {
    std::vector<std::optional<T>> values(table.num_rows);

    const auto& column  = table.columns[column_idx];
    size_t      row_idx = 0;

    for (auto* page: column.pages) {
        switch (column.type) {
        case DataType::INT32: {
            if constexpr (!std::is_same_v<T, int32_t>) {
                continue;
            }

            auto  num_rows   = *reinterpret_cast<uint16_t*>(page->data);
            auto* data_begin = reinterpret_cast<int32_t*>(page->data + 4);
            auto* bitmap =
                reinterpret_cast<uint8_t*>(page->data + PAGE_SIZE - (num_rows + 7) / 8);
            uint16_t data_idx = 0;
            for (uint16_t i = 0; i < num_rows; ++i) {
                if (get_bitmap(bitmap, i)) {
                    auto value = data_begin[data_idx++];
                    if (row_idx >= table.num_rows) {
                        throw std::runtime_error("row_idx");
                    }
                    // Use is_same to appropriately cast the value and handle template issues.
                    if constexpr (std::is_same_v<T, int32_t>) {
                        values[row_idx++] = value;
                    } else if constexpr (std::is_same_v<T, int64_t>) {
                        values[row_idx++] = static_cast<int64_t>(value);
                    } else if constexpr (std::is_same_v<T, double>) {
                        values[row_idx++] = static_cast<double>(value);
                    }
                } else {
                    values[row_idx] = std::nullopt;
                    ++row_idx;
                }
            }
            break;
        }
        case DataType::INT64: {
            if constexpr (!std::is_same_v<T, int64_t>) {
                continue;
            }

            auto  num_rows   = *reinterpret_cast<uint16_t*>(page->data);
            auto* data_begin = reinterpret_cast<int64_t*>(page->data + 8);
            auto* bitmap =
                reinterpret_cast<uint8_t*>(page->data + PAGE_SIZE - (num_rows + 7) / 8);
            uint16_t data_idx = 0;
            for (uint16_t i = 0; i < num_rows; ++i) {
                if (get_bitmap(bitmap, i)) {
                    auto value = data_begin[data_idx++];
                    if (row_idx >= table.num_rows) {
                        throw std::runtime_error("row_idx");
                    }

                    // Use is_same to appropriately cast the value and handle template issues.
                    if constexpr (std::is_same_v<T, int32_t>) {
                        values[row_idx++] = static_cast<int32_t>(value);
                    } else if constexpr (std::is_same_v<T, int64_t>) {
                        values[row_idx++] = value;
                    } else if constexpr (std::is_same_v<T, double>) {
                        values[row_idx++] = static_cast<double>(value);
                    }
                } else {
                    values[row_idx] = std::nullopt;
                    ++row_idx;
                }
            }
            break;
        }
        case DataType::FP64: {
            if constexpr (!std::is_same_v<T, double>) {
                continue;
            }

            auto  num_rows   = *reinterpret_cast<uint16_t*>(page->data);
            auto* data_begin = reinterpret_cast<double*>(page->data + 8);
            auto* bitmap =
                reinterpret_cast<uint8_t*>(page->data + PAGE_SIZE - (num_rows + 7) / 8);
            uint16_t data_idx = 0;
            for (uint16_t i = 0; i < num_rows; ++i) {
                if (get_bitmap(bitmap, i)) {
                    auto value = data_begin[data_idx++];
                    if (row_idx >= table.num_rows) {
                        throw std::runtime_error("row_idx");
                    }

                    // Use is_same to appropriately cast the value and handle template issues.
                    if constexpr (std::is_same_v<T, int32_t>) {
                        values[row_idx++] = static_cast<int32_t>(value);
                    } else if constexpr (std::is_same_v<T, int64_t>) {
                        values[row_idx++] = static_cast<int64_t>(value);
                    } else if constexpr (std::is_same_v<T, double>) {
                        values[row_idx++] = value;
                    }

                } else {
                    values[row_idx] = std::nullopt;
                    ++row_idx;
                }
            }
            break;
        }
        case DataType::VARCHAR: {
            if constexpr (!std::is_same_v<T, std::string>) {
                continue;
            }

            auto num_rows = *reinterpret_cast<uint16_t*>(page->data);
            if (num_rows == 0xffff) {
                auto        num_chars  = *reinterpret_cast<uint16_t*>(page->data + 2);
                auto*       data_begin = reinterpret_cast<char*>(page->data + 4);
                std::string value{data_begin, data_begin + num_chars};
                if (row_idx >= table.num_rows) {
                    throw std::runtime_error("row_idx");
                }

                // Use is_same to appropriately move the string.
                if constexpr (std::is_same_v<T, std::string>) {
                    values[row_idx++] = std::move(value);
                }

            } else if (num_rows == 0xfffe) {
                auto  num_chars  = *reinterpret_cast<uint16_t*>(page->data + 2);
                auto* data_begin = reinterpret_cast<char*>(page->data + 4);
                if (row_idx > 0 && values[row_idx - 1].has_value()) {
                    if constexpr (std::is_same_v<T, std::string>) {
                        // Append the string to the previous value

                        values[row_idx - 1]->insert(values[row_idx - 1]->end(),
                            data_begin,
                            data_begin + num_chars);
                    }
                } else {
                    throw std::runtime_error("long string page 0xfffe must follow a string");
                }
            } else {
                auto  num_non_null = *reinterpret_cast<uint16_t*>(page->data + 2);
                auto* offset_begin = reinterpret_cast<uint16_t*>(page->data + 4);
                auto* data_begin   = reinterpret_cast<char*>(page->data + 4 + num_non_null * 2);
                auto* string_begin = data_begin;
                auto* bitmap =
                    reinterpret_cast<uint8_t*>(page->data + PAGE_SIZE - (num_rows + 7) / 8);
                uint16_t data_idx = 0;
                for (uint16_t i = 0; i < num_rows; ++i) {
                    if (get_bitmap(bitmap, i)) {
                        auto        offset = offset_begin[data_idx++];
                        std::string value{string_begin, data_begin + offset};
                        string_begin = data_begin + offset;
                        if (row_idx >= table.num_rows) {
                            throw std::runtime_error("row_idx");
                        }

                        if constexpr (std::is_same_v<T, std::string>) {
                            // Use is_same to appropriately move the string.
                            values[row_idx++] = std::move(value);
                        }
                    } else {
                        values[row_idx] = std::nullopt;
                        ++row_idx;
                    }
                }
            }
            break;
        }
        }
    }
    return values;
}

ColumnarTable build_result_columns(const std::vector<std::pair<size_t, size_t>>& matches,
    const std::vector<std::tuple<size_t, DataType>>&                             output_attrs,
    const ColumnarTable&                                                         left_result,
    const ColumnarTable&                                                         right_result,
    bool                                                                         build_left) {
    ColumnarTable result;
    result.num_rows = matches.size();
    result.columns.reserve(output_attrs.size());

    // For each output column
    for (auto [attr_idx, type]: output_attrs) {
        // Determine source column
        bool   from_left;
        size_t source_idx;

        if (attr_idx < left_result.columns.size()) {
            from_left  = true;
            source_idx = attr_idx;
        } else {
            from_left  = false;
            source_idx = attr_idx - left_result.columns.size();
        }

        const auto& source_result = from_left ? left_result : right_result;

        // Create a new column of the appropriate type
        result.columns.emplace_back(type);
        auto& dest_column = result.columns.back();

        // Create the appropriate column inserter and extract values based on type
        switch (type) {
        case DataType::INT32: {
            auto inserter   = ColumnInserter<int32_t>(dest_column);
            auto row_values = extract_values_for_column<int32_t>(source_result, source_idx);

            // Insert values based on matches
            for (const auto& [left_row, right_row]: matches) {
                size_t row_idx = from_left ? left_row : right_row;
                if (row_idx < row_values.size() && row_values[row_idx].has_value()) {
                    inserter.insert(row_values[row_idx].value());
                } else {
                    inserter.insert_null();
                }
            }

            inserter.finalize();
            break;
        }
        case DataType::INT64: {
            auto inserter   = ColumnInserter<int64_t>(dest_column);
            auto row_values = extract_values_for_column<int64_t>(source_result, source_idx);

            // Insert values based on matches
            for (const auto& [left_row, right_row]: matches) {
                size_t row_idx = from_left ? left_row : right_row;
                if (row_idx < row_values.size() && row_values[row_idx].has_value()) {
                    inserter.insert(row_values[row_idx].value());
                } else {
                    inserter.insert_null();
                }
            }

            inserter.finalize();
            break;
        }
        case DataType::FP64: {
            auto inserter   = ColumnInserter<double>(dest_column);
            auto row_values = extract_values_for_column<double>(source_result, source_idx);

            // Insert values based on matches
            for (const auto& [left_row, right_row]: matches) {
                size_t row_idx = from_left ? left_row : right_row;
                if (row_idx < row_values.size() && row_values[row_idx].has_value()) {
                    inserter.insert(row_values[row_idx].value());
                } else {
                    inserter.insert_null();
                }
            }

            inserter.finalize();
            break;
        }
        case DataType::VARCHAR: {
            auto inserter   = ColumnInserter<std::string>(dest_column);
            auto row_values = extract_values_for_column<std::string>(source_result, source_idx);

            // Insert values based on matches
            for (const auto& [left_row, right_row]: matches) {
                size_t row_idx = from_left ? left_row : right_row;
                if (row_idx < row_values.size() && row_values[row_idx].has_value()) {
                    inserter.insert(row_values[row_idx].value());
                } else {
                    inserter.insert_null();
                }
            }

            inserter.finalize();
            break;
        }
        }
    }

    return result;
}

template <typename T>
static ColumnarTable execute_join_impl(const ColumnarTable& build_table,
    const ColumnarTable&                                    probe_table,
    size_t                                                  build_join_col,
    size_t                                                  probe_join_col,
    const OutputAttrs&                                      output_attrs,
    const ColumnarTable&                                    left_results,
    const ColumnarTable&                                    right_results) {
    flat_hash_map<T, std::vector<size_t>> ht;

    // Step 1: Build the hash table from the build table.
    hash_join_build<T>(build_table, build_join_col, ht);
    // Step 2: Probe the hash table with the probe table.
    std::vector<std::pair<size_t, size_t>> matches;
    hash_join_probe<T>(probe_table, probe_join_col, ht, matches);
    matches.reserve(build_table.num_rows);
    // Step 3: Build the result columns based on the matches.
    ColumnarTable result = build_result_columns(matches,
        output_attrs,
        left_results,
        right_results,
        build_table.num_rows == left_results.num_rows);
    // Step 4: Return the result.
    return result;
}

ColumnarTable ColumnarExecutor::execute_scan(const Plan& plan,
    const ScanNode&                                      scan,
    const OutputAttrs&                                   output_attrs) {
    auto  table_id = scan.base_table_id;
    auto& input    = plan.inputs[table_id];

    ColumnarTable results;
    results.num_rows = input.num_rows;
    results.columns.reserve(output_attrs.size());

    // For each output attribute, copy the corresponding column.
    for (auto [source_col_idx, type]: output_attrs) {
        assert(source_col_idx < input.columns.size());

        auto& source_column = input.columns[source_col_idx];
        results.columns.emplace_back(type);
        auto& dest_column = results.columns.back();

        // TODO: if copying is expensive we can do an std::move here.
        for (auto* page: source_column.pages) {
            auto* dest_page = dest_column.new_page();
            std::memcpy(dest_page->data, page->data, PAGE_SIZE);
        }
    }

    return results;
}

ColumnarTable ColumnarExecutor::execute_join(const Plan& plan,
    const JoinNode&                                      join,
    const OutputAttrs&                                   output_attrs) {
    auto left  = join.left;
    auto right = join.right;

    // Recursively execute child nodes
    auto left_result  = execute_impl(plan, left);
    auto right_result = execute_impl(plan, right);

    auto left_join_col  = join.left_attr;
    auto right_join_col = join.right_attr;

    auto& build_table = join.build_left ? left_result : right_result;
    auto& probe_table = join.build_left ? right_result : left_result;

    auto build_join_col = join.build_left ? left_join_col : right_join_col;
    auto probe_join_col = join.build_left ? right_join_col : left_join_col;

    DataType join_col_type;

    if (join.build_left) {
        join_col_type = std::get<1>(plan.nodes[left].output_attrs[build_join_col]);
    } else {
        join_col_type = std::get<1>(plan.nodes[right].output_attrs[build_join_col]);
    }

    std::vector<std::mutex>                partition_mutexes(NumPartitions);
    std::vector<std::pair<size_t, size_t>> matches;

    switch (join_col_type) {
    case DataType::INT32: {
        auto partitioned_hash_table_int32 = PartitionedHashTable<int32_t>(NumPartitions);
        hash_join_build_partitioned<int32_t>(build_table,
            build_join_col,
            std::ref(partitioned_hash_table_int32),
            partition_mutexes);
        hash_join_probe_partitioned<int32_t>(probe_table,
            probe_join_col,
            std::cref(partitioned_hash_table_int32),
            matches);
        ColumnarTable result = build_result_columns(matches,
            output_attrs,
            left_result,
            right_result,
            build_table.num_rows == left_result.num_rows);
        // Step 4: Return the result.
        return result;
    }
    case DataType::INT64: {
        auto partitioned_hash_table_int64 = PartitionedHashTable<int64_t>(NumPartitions);
        hash_join_build_partitioned<int64_t>(build_table,
            build_join_col,
            std::ref(partitioned_hash_table_int64),
            partition_mutexes);
        hash_join_probe_partitioned<int64_t>(probe_table,
            probe_join_col,
            std::cref(partitioned_hash_table_int64),
            matches);
        ColumnarTable result = build_result_columns(matches,
            output_attrs,
            left_result,
            right_result,
            build_table.num_rows == left_result.num_rows);
        // Step 4: Return the result.
        return result;
    }
    case DataType::FP64: {
        auto partitioned_hash_table_double = PartitionedHashTable<double>(NumPartitions);
        hash_join_build_partitioned<double>(build_table,
            build_join_col,
            std::ref(partitioned_hash_table_double),
            partition_mutexes);
        hash_join_probe_partitioned<double>(probe_table,
            probe_join_col,
            std::cref(partitioned_hash_table_double),
            matches);
        ColumnarTable result = build_result_columns(matches,
            output_attrs,
            left_result,
            right_result,
            build_table.num_rows == left_result.num_rows);
        // Step 4: Return the result.
        return result;
    }
    case DataType::VARCHAR:
    default:
        throw std::runtime_error(
            fmt::format("Unsupported data type for join column: {}", DataType::VARCHAR));
    }
}
