#include "columnar_exec.h"
#include <hardware__talos.h>

#include <plan.h>
#include <table.h>
#include <german_table.h>
#include <hardware__talos.h>

namespace Contest {

enum class HashJoinAlgorithm {
    Baseline,
    German
};

using ExecuteResult = std::vector<std::vector<Data>>;

ExecuteResult execute_impl(const Plan& plan, size_t node_idx);

struct BaselineJoinAlgorithm {
    bool                                             build_left;
    ExecuteResult&                                   left;
    ExecuteResult&                                   right;
    ExecuteResult&                                   results;
    size_t                                           left_col, right_col;
    const std::vector<std::tuple<size_t, DataType>>& output_attrs;

    template <class T>
    auto run() {
        namespace views = ranges::views;
        std::unordered_map<T, std::vector<size_t>> hash_table;

        if (build_left) {
            // Build phase.
            for (auto&& [idx, record]: left | views::enumerate) {
                // This is just a type-matching visitor except it's limited to
                // the template type.
                //
                // Key is the row, column entry. The row being `record` we
                // are currently processing, and the column being indexed
                // by `left_col`.
                std::visit(
                    [&hash_table, idx = idx](const auto& key) {
                        // Here we capture the type of the entry (this is just one)
                        using Tk = std::decay_t<decltype(key)>;
                        // If the type of the entry is the same as the template
                        // type, we can insert it into the hash table.
                        if constexpr (std::is_same_v<Tk, T>) {
                            // If the key is not in the hash table, we insert it
                            // with the current index. Otherwise, we just push
                            // the index into the vector of indices.
                            //
                            // The end of the build phase yields a mapping of values
                            // and their corresponding indices in the left table.
                            if (auto itr = hash_table.find(key); itr == hash_table.end()) {
                                hash_table.emplace(key, std::vector<size_t>(1, idx));
                            } else {
                                itr->second.push_back(idx);
                            }
                        } else if constexpr (not std::is_same_v<Tk, std::monostate>) {
                            throw std::runtime_error("wrong type of field");
                        }
                    },
                    record[left_col]);
            }
            // Probe phase.
            for (auto& right_record: right) {
                // Again, iterate on every record and this time our typed-visitor
                // emplaces the records in the results vector.
                std::visit(
                    [&](const auto& key) {
                        using Tk = std::decay_t<decltype(key)>;
                        if constexpr (std::is_same_v<Tk, T>) {
                            if (auto itr = hash_table.find(key); itr != hash_table.end()) {
                                for (auto left_idx: itr->second) {
                                    auto&             left_record = left[left_idx];
                                    std::vector<Data> new_record;
                                    new_record.reserve(output_attrs.size());
                                    for (auto [col_idx, _]: output_attrs) {
                                        if (col_idx < left_record.size()) {
                                            new_record.emplace_back(left_record[col_idx]);
                                        } else {
                                            new_record.emplace_back(
                                                right_record[col_idx - left_record.size()]);
                                        }
                                    }
                                    results.emplace_back(std::move(new_record));
                                }
                            }
                        } else if constexpr (not std::is_same_v<Tk, std::monostate>) {
                            throw std::runtime_error("wrong type of field");
                        }
                    },
                    right_record[right_col]);
            }
        } else {
            for (auto&& [idx, record]: right | views::enumerate) {
                std::visit(
                    [&hash_table, idx = idx](const auto& key) {
                        using Tk = std::decay_t<decltype(key)>;
                        if constexpr (std::is_same_v<Tk, T>) {
                            if (auto itr = hash_table.find(key); itr == hash_table.end()) {
                                hash_table.emplace(key, std::vector<size_t>(1, idx));
                            } else {
                                itr->second.push_back(idx);
                            }
                        } else if constexpr (not std::is_same_v<Tk, std::monostate>) {
                            throw std::runtime_error("wrong type of field");
                        }
                    },
                    record[right_col]);
            }
            for (auto& left_record: left) {
                std::visit(
                    [&](const auto& key) {
                        using Tk = std::decay_t<decltype(key)>;
                        if constexpr (std::is_same_v<Tk, T>) {
                            if (auto itr = hash_table.find(key); itr != hash_table.end()) {
                                for (auto right_idx: itr->second) {
                                    auto&             right_record = right[right_idx];
                                    std::vector<Data> new_record;
                                    new_record.reserve(output_attrs.size());
                                    for (auto [col_idx, _]: output_attrs) {
                                        if (col_idx < left_record.size()) {
                                            new_record.emplace_back(left_record[col_idx]);
                                        } else {
                                            new_record.emplace_back(
                                                right_record[col_idx - left_record.size()]);
                                        }
                                    }
                                    results.emplace_back(std::move(new_record));
                                }
                            }
                        } else if constexpr (not std::is_same_v<Tk, std::monostate>) {
                            throw std::runtime_error("wrong type of field");
                        }
                    },
                    left_record[left_col]);
            }
        }
    }
};

struct GermanJoinAlgorithm {
    bool                                             build_left;
    ExecuteResult&                                   left;
    ExecuteResult&                                   right;
    ExecuteResult&                                   results;
    size_t                                           left_col, right_col;
    const std::vector<std::tuple<size_t, DataType>>& output_attrs;

    template <class T>
    auto run() {
        UnchainedHashTable hash_table;

        if (build_left) {
            // Build phase.
            hash_table.build(left, left_col);
            // Probe phase.
            for (auto& right_record: right) {
                // Again, iterate on every record and this time our typed-visitor
                // emplaces the records in the results vector.
                std::visit(
                    [&](const auto& key) {
                        using Tk = std::decay_t<decltype(key)>;
                        std::vector<uint64_t> values;

                        if constexpr (std::is_same_v<Tk, T>) {
                            // Probe the hash table with the key from the right record
                            hash_table.probe(key, values);

                            // If the key is found, process the matching left records.
                            if (values.size() > 0) {
                                for (auto left_idx: values) {
                                    auto&             left_record = left[left_idx];
                                    std::vector<Data> new_record;
                                    new_record.reserve(output_attrs.size());
                                    for (auto [col_idx, _]: output_attrs) {
                                        if (col_idx < left_record.size()) {
                                            new_record.emplace_back(left_record[col_idx]);
                                        } else {
                                            new_record.emplace_back(
                                                right_record[col_idx - left_record.size()]);
                                        }
                                    }
                                    results.emplace_back(std::move(new_record));
                                }
                            }
                        } else if constexpr (not std::is_same_v<Tk, std::monostate>) {
                            throw std::runtime_error("wrong type of field");
                        }
                    },
                    right_record[right_col]);
            }
        } else {
            // Build phase.
            hash_table.build(right, right_col);

            // Probe phase.
            for (auto& left_record: left) {
                std::visit(
                    [&](const auto& key) {
                        using Tk = std::decay_t<decltype(key)>;
                        std::vector<uint64_t> values;
                        // Probe the hash table with the key from the left record
                        if constexpr (std::is_same_v<Tk, T>) {
                            hash_table.probe(key, values);
                            if (values.size() > 0) {
                                for (auto right_idx: values) {
                                    auto&             right_record = right[right_idx];
                                    std::vector<Data> new_record;
                                    new_record.reserve(output_attrs.size());
                                    for (auto [col_idx, _]: output_attrs) {
                                        if (col_idx < left_record.size()) {
                                            new_record.emplace_back(left_record[col_idx]);
                                        } else {
                                            new_record.emplace_back(
                                                right_record[col_idx - left_record.size()]);
                                        }
                                    }
                                    results.emplace_back(std::move(new_record));
                                }
                            }
                        } else if constexpr (not std::is_same_v<Tk, std::monostate>) {
                            throw std::runtime_error("wrong type of field");
                        }
                    },
                    left_record[left_col]);
            }
        }
    }
};

ExecuteResult execute_hash_join(const Plan&          plan,
    const JoinNode&                                  join,
    const std::vector<std::tuple<size_t, DataType>>& output_attrs,
    const HashJoinAlgorithm&                         algorithm) {
    auto                           left_idx    = join.left;
    auto                           right_idx   = join.right;
    auto&                          left_node   = plan.nodes[left_idx];
    auto&                          right_node  = plan.nodes[right_idx];
    auto&                          left_types  = left_node.output_attrs;
    auto&                          right_types = right_node.output_attrs;
    auto                           left        = execute_impl(plan, left_idx);
    auto                           right       = execute_impl(plan, right_idx);
    std::vector<std::vector<Data>> results;

    if (algorithm == HashJoinAlgorithm::Baseline) {
        BaselineJoinAlgorithm join_algorithm{.build_left = join.build_left,
            .left                                        = left,
            .right                                       = right,
            .results                                     = results,
            .left_col                                    = join.left_attr,
            .right_col                                   = join.right_attr,
            .output_attrs                                = output_attrs};
        if (join.build_left) {
            switch (std::get<1>(left_types[join.left_attr])) {
            case DataType::INT32:   join_algorithm.run<int32_t>(); break;
            case DataType::INT64:   join_algorithm.run<int64_t>(); break;
            case DataType::FP64:    join_algorithm.run<double>(); break;
            case DataType::VARCHAR: join_algorithm.run<std::string>(); break;
            }
        } else {
            switch (std::get<1>(right_types[join.right_attr])) {
            case DataType::INT32:   join_algorithm.run<int32_t>(); break;
            case DataType::INT64:   join_algorithm.run<int64_t>(); break;
            case DataType::FP64:    join_algorithm.run<double>(); break;
            case DataType::VARCHAR: join_algorithm.run<std::string>(); break;
            }
        }
    } else if (algorithm == HashJoinAlgorithm::German) {
        // German join algorithm
        GermanJoinAlgorithm german_join_algorithm{.build_left = join.build_left,
            .left                                             = left,
            .right                                            = right,
            .results                                          = results,
            .left_col                                         = join.left_attr,
            .right_col                                        = join.right_attr,
            .output_attrs                                     = output_attrs};
        if (join.build_left) {
            switch (std::get<1>(left_types[join.left_attr])) {
            case DataType::INT32:   german_join_algorithm.run<int32_t>(); break;
            case DataType::INT64:   german_join_algorithm.run<int64_t>(); break;
            case DataType::FP64:    german_join_algorithm.run<double>(); break;
            case DataType::VARCHAR: german_join_algorithm.run<std::string>(); break;
            }
        } else {
            switch (std::get<1>(right_types[join.right_attr])) {
            case DataType::INT32:   german_join_algorithm.run<int32_t>(); break;
            case DataType::INT64:   german_join_algorithm.run<int64_t>(); break;
            case DataType::FP64:    german_join_algorithm.run<double>(); break;
            case DataType::VARCHAR: german_join_algorithm.run<std::string>(); break;
            }
        }

    } else {
        throw std::runtime_error("Unknown join algorithm");
    }

    return results;
}

ExecuteResult execute_scan(const Plan&               plan,
    const ScanNode&                                  scan,
    const std::vector<std::tuple<size_t, DataType>>& output_attrs) {
    auto                           table_id = scan.base_table_id;
    auto&                          input    = plan.inputs[table_id];
    auto                           table    = Table::from_columnar(input);
    std::vector<std::vector<Data>> results;
    results.reserve(table.number_rows());
    for (auto& record: table.table()) {
        std::vector<Data> new_record;
        // Projection.
        new_record.reserve(output_attrs.size());
        for (auto [col_idx, _]: output_attrs) {
            new_record.emplace_back(record[col_idx]);
        }
        results.emplace_back(std::move(new_record));
    }
    return results;
}

ExecuteResult execute_impl(const Plan& plan, size_t node_idx) {
    auto& node = plan.nodes[node_idx];
    return std::visit(
        [&](const auto& value) {
            using T = std::decay_t<decltype(value)>;
            if constexpr (std::is_same_v<T, JoinNode>) {
                return execute_hash_join(plan,
                    value,
                    node.output_attrs,
                    HashJoinAlgorithm::German);
            } else {
                return execute_scan(plan, value, node.output_attrs);
            }
        },
        node.data);
}

ColumnarTable execute(const Plan& plan, [[maybe_unused]] void* context) {
    // Baseline.
    //
    // namespace views = ranges::views;
    // auto ret        = execute_impl(plan, plan.root);
    // auto ret_types  = plan.nodes[plan.root].output_attrs
    //                | views::transform([](const auto& v) { return std::get<1>(v); })
    //                | ranges::to<std::vector<DataType>>();
    // Table table{std::move(ret), std::move(ret_types)};
    // return table.to_columnar();
    ColumnarExecutor executor;
    return executor.execute_impl(plan, plan.root);
}

void* build_context() {
    return nullptr;
}

void destroy_context([[maybe_unused]] void* context) {}

} // namespace Contest
