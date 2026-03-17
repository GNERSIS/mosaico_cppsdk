#include "mcap_writer.hpp"

#include <mcap/writer.hpp>
#include <nlohmann/json.hpp>

#include <arrow/api.h>

#include <iostream>
#include <sstream>

using json = nlohmann::json;

namespace {

// Convert an Arrow schema to a JSON Schema string for the MCAP schema attachment.
std::string arrowSchemaToJsonSchema(const std::shared_ptr<arrow::Schema>& schema) {
  json properties = json::object();
  for (int i = 0; i < schema->num_fields(); ++i) {
    const auto& field = schema->field(i);
    if (field->name() == "timestamp_ns") continue;  // excluded from message body

    json prop;
    switch (field->type()->id()) {
      case arrow::Type::INT8:
      case arrow::Type::INT16:
      case arrow::Type::INT32:
      case arrow::Type::INT64:
      case arrow::Type::UINT8:
      case arrow::Type::UINT16:
      case arrow::Type::UINT32:
      case arrow::Type::UINT64:
      case arrow::Type::FLOAT:
      case arrow::Type::DOUBLE:
        prop = {{"type", "number"}};
        break;
      case arrow::Type::BOOL:
        prop = {{"type", "boolean"}};
        break;
      case arrow::Type::STRING:
        prop = {{"type", "string"}};
        break;
      default:
        prop = {{"type", "string"}};  // fallback
        break;
    }
    properties[field->name()] = prop;
  }
  json schema_obj = {{"type", "object"}, {"properties", properties}};
  return schema_obj.dump();
}

// Extract a scalar value from an Arrow array at row index as a JSON value.
json arrowValueToJson(const std::shared_ptr<arrow::Array>& arr, int64_t row) {
  if (arr->IsNull(row)) return json(nullptr);

  switch (arr->type_id()) {
    case arrow::Type::DOUBLE:
      return std::static_pointer_cast<arrow::DoubleArray>(arr)->Value(row);
    case arrow::Type::FLOAT:
      return static_cast<double>(
          std::static_pointer_cast<arrow::FloatArray>(arr)->Value(row));
    case arrow::Type::INT64:
      return std::static_pointer_cast<arrow::Int64Array>(arr)->Value(row);
    case arrow::Type::INT32:
      return std::static_pointer_cast<arrow::Int32Array>(arr)->Value(row);
    case arrow::Type::INT16:
      return std::static_pointer_cast<arrow::Int16Array>(arr)->Value(row);
    case arrow::Type::INT8:
      return std::static_pointer_cast<arrow::Int8Array>(arr)->Value(row);
    case arrow::Type::UINT64:
      return std::static_pointer_cast<arrow::UInt64Array>(arr)->Value(row);
    case arrow::Type::UINT32:
      return std::static_pointer_cast<arrow::UInt32Array>(arr)->Value(row);
    case arrow::Type::UINT16:
      return std::static_pointer_cast<arrow::UInt16Array>(arr)->Value(row);
    case arrow::Type::UINT8:
      return std::static_pointer_cast<arrow::UInt8Array>(arr)->Value(row);
    case arrow::Type::BOOL:
      return std::static_pointer_cast<arrow::BooleanArray>(arr)->Value(row);
    case arrow::Type::STRING:
      return std::static_pointer_cast<arrow::StringArray>(arr)->GetString(row);
    default:
      return json(nullptr);
  }
}

}  // namespace

arrow::Status writeMcap(const std::string& output_path,
                        const std::vector<TopicBatches>& topics) {
  mcap::McapWriter writer;
  auto opts = mcap::McapWriterOptions("mosaico_cli");
  opts.compression = mcap::Compression::None;

  auto status = writer.open(output_path, opts);
  if (!status.ok()) {
    return arrow::Status::IOError("Failed to open MCAP for writing: ", status.message);
  }

  for (const auto& topic : topics) {
    // Register schema
    std::string json_schema_str = arrowSchemaToJsonSchema(topic.schema);
    mcap::Schema mcap_schema(topic.topic_name, "jsonschema", json_schema_str);
    writer.addSchema(mcap_schema);

    // Register channel
    mcap::Channel channel(topic.topic_name, "json", mcap_schema.id);
    writer.addChannel(channel);

    // Find timestamp column
    int ts_idx = topic.schema->GetFieldIndex("timestamp_ns");
    if (ts_idx < 0) ts_idx = topic.schema->GetFieldIndex("recording_timestamp_ns");

    uint32_t seq = 0;
    for (const auto& batch : topic.batches) {
      for (int64_t row = 0; row < batch->num_rows(); ++row) {
        // Get timestamp
        uint64_t log_time = 0;
        if (ts_idx >= 0) {
          auto ts_arr = std::static_pointer_cast<arrow::Int64Array>(batch->column(ts_idx));
          log_time = static_cast<uint64_t>(ts_arr->Value(row));
        }

        // Build JSON body from all non-timestamp columns
        json body = json::object();
        for (int col = 0; col < batch->num_columns(); ++col) {
          if (col == ts_idx) continue;
          const auto& name = batch->schema()->field(col)->name();
          body[name] = arrowValueToJson(batch->column(col), row);
        }

        std::string serialized = body.dump();
        mcap::Message msg;
        msg.channelId = channel.id;
        msg.sequence = seq++;
        msg.logTime = log_time;
        msg.publishTime = log_time;
        msg.data = reinterpret_cast<const std::byte*>(serialized.data());
        msg.dataSize = serialized.size();

        auto write_status = writer.write(msg);
        if (!write_status.ok()) {
          writer.close();
          return arrow::Status::IOError("MCAP write failed: ", write_status.message);
        }
      }
    }
  }

  writer.close();
  return arrow::Status::OK();
}
