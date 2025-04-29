#pragma once

#include <plan.h>

using OutputAttrs = std::vector<std::tuple<size_t, DataType>>;

// ColumnarExecutor implements the execution pipeline using a purely
// columnar approach.
struct ColumnarExecutor {
    ColumnarExecutor() = default;

    // Execute the pipeline and return the result.
    ColumnarTable execute_impl(const Plan& plan, size_t node_idx);

    // Execute a scan node and return the result.
    ColumnarTable
    execute_scan(const Plan& plan, const ScanNode& scan, const OutputAttrs& output_attrs);
    // Execute a join node and return the result.
    ColumnarTable
    execute_join(const Plan& plan, const JoinNode& join, const OutputAttrs& output_attrs);
};
