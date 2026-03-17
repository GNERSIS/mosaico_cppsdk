// src/flight/types.hpp
#pragma once

#include <arrow/type_fwd.h>

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace mosaico {

struct SequenceInfo {
    std::string name;
    int64_t min_ts_ns = 0;
    int64_t max_ts_ns = 0;
    int64_t total_size_bytes = 0;
    std::unordered_map<std::string, std::string> user_metadata;
};

struct TopicInfo {
    std::string topic_name;
    std::string ontology_tag;
    std::shared_ptr<arrow::Schema> schema;
    std::unordered_map<std::string, std::string> user_metadata;
    std::string ticket_bytes;
    int64_t min_ts_ns = 0;
    int64_t max_ts_ns = 0;
};

struct TimeRange {
    std::optional<int64_t> start_ns;
    std::optional<int64_t> end_ns;
};

struct PullResult {
    std::shared_ptr<arrow::Schema> schema;
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
};

using ProgressCallback = std::function<void(int64_t rows, int64_t bytes)>;

} // namespace mosaico
