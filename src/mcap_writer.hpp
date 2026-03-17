#pragma once

#include <arrow/status.h>
#include <arrow/type_fwd.h>

#include <memory>
#include <string>
#include <vector>

// Data for one topic to write into the MCAP file.
struct TopicBatches {
  std::string topic_name;
  std::shared_ptr<arrow::Schema> schema;
  std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
};

// Write topics to an MCAP file with JSON-encoded messages and zstd compression.
// The timestamp_ns column (if present) is used as the MCAP message logTime.
// Other columns are serialized as a flat JSON object.
arrow::Status writeMcap(const std::string& output_path,
                        const std::vector<TopicBatches>& topics);
