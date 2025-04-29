#pragma once

#include <fmt/core.h>
#include <fmt/format.h>

namespace logging {
inline void log(const std::string& message) {
    fmt::print("{}\n", message);
}

#define LOG_INFO(msg) logging::log(fmt::format("INFO: {}", msg))

} // namespace logging
