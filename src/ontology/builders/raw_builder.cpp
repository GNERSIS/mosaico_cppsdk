#include "ontology/builders/raw_builder.hpp"

#include <arrow/builder.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>

#include <algorithm>
#include <string>
#include <vector>

namespace {

// Map a FieldValue to its corresponding Arrow data type.
std::shared_ptr<arrow::DataType> fieldValueToArrowType(const FieldValue& fv) {
    return std::visit(
        [](const auto& v) -> std::shared_ptr<arrow::DataType> {
            using T = std::decay_t<decltype(v)>;
            if constexpr (std::is_same_v<T, bool>) {
                return arrow::boolean();
            } else if constexpr (std::is_same_v<T, int8_t>) {
                return arrow::int8();
            } else if constexpr (std::is_same_v<T, int16_t>) {
                return arrow::int16();
            } else if constexpr (std::is_same_v<T, int32_t>) {
                return arrow::int32();
            } else if constexpr (std::is_same_v<T, int64_t>) {
                return arrow::int64();
            } else if constexpr (std::is_same_v<T, uint8_t>) {
                return arrow::uint8();
            } else if constexpr (std::is_same_v<T, uint16_t>) {
                return arrow::uint16();
            } else if constexpr (std::is_same_v<T, uint32_t>) {
                return arrow::uint32();
            } else if constexpr (std::is_same_v<T, uint64_t>) {
                return arrow::uint64();
            } else if constexpr (std::is_same_v<T, float>) {
                return arrow::float32();
            } else if constexpr (std::is_same_v<T, double>) {
                return arrow::float64();
            } else if constexpr (std::is_same_v<T, std::string>) {
                return arrow::utf8();
            } else if constexpr (std::is_same_v<T, std::vector<uint8_t>>) {
                return arrow::binary();
            } else if constexpr (std::is_same_v<T, std::vector<double>>) {
                return arrow::list(arrow::float64());
            } else if constexpr (std::is_same_v<T, std::vector<std::string>>) {
                return arrow::list(arrow::utf8());
            } else if constexpr (std::is_same_v<T, std::vector<FieldMap>>) {
                // Serialize as JSON string for raw passthrough
                return arrow::utf8();
            } else {
                return arrow::utf8();
            }
        },
        fv);
}

// Append a single FieldValue to an ArrayBuilder.
arrow::Status appendValue(arrow::ArrayBuilder* builder, const FieldValue& fv) {
    return std::visit(
        [builder](const auto& v) -> arrow::Status {
            using T = std::decay_t<decltype(v)>;
            if constexpr (std::is_same_v<T, bool>) {
                return static_cast<arrow::BooleanBuilder*>(builder)->Append(v);
            } else if constexpr (std::is_same_v<T, int8_t>) {
                return static_cast<arrow::Int8Builder*>(builder)->Append(v);
            } else if constexpr (std::is_same_v<T, int16_t>) {
                return static_cast<arrow::Int16Builder*>(builder)->Append(v);
            } else if constexpr (std::is_same_v<T, int32_t>) {
                return static_cast<arrow::Int32Builder*>(builder)->Append(v);
            } else if constexpr (std::is_same_v<T, int64_t>) {
                return static_cast<arrow::Int64Builder*>(builder)->Append(v);
            } else if constexpr (std::is_same_v<T, uint8_t>) {
                return static_cast<arrow::UInt8Builder*>(builder)->Append(v);
            } else if constexpr (std::is_same_v<T, uint16_t>) {
                return static_cast<arrow::UInt16Builder*>(builder)->Append(v);
            } else if constexpr (std::is_same_v<T, uint32_t>) {
                return static_cast<arrow::UInt32Builder*>(builder)->Append(v);
            } else if constexpr (std::is_same_v<T, uint64_t>) {
                return static_cast<arrow::UInt64Builder*>(builder)->Append(v);
            } else if constexpr (std::is_same_v<T, float>) {
                return static_cast<arrow::FloatBuilder*>(builder)->Append(v);
            } else if constexpr (std::is_same_v<T, double>) {
                return static_cast<arrow::DoubleBuilder*>(builder)->Append(v);
            } else if constexpr (std::is_same_v<T, std::string>) {
                return static_cast<arrow::StringBuilder*>(builder)->Append(v);
            } else if constexpr (std::is_same_v<T, std::vector<uint8_t>>) {
                return static_cast<arrow::BinaryBuilder*>(builder)->Append(
                    reinterpret_cast<const char*>(v.data()), static_cast<int32_t>(v.size()));
            } else if constexpr (std::is_same_v<T, std::vector<double>>) {
                auto* list_builder = static_cast<arrow::ListBuilder*>(builder);
                auto* value_builder = static_cast<arrow::DoubleBuilder*>(list_builder->value_builder());
                ARROW_RETURN_NOT_OK(list_builder->Append());
                return value_builder->AppendValues(v.data(), static_cast<int64_t>(v.size()));
            } else if constexpr (std::is_same_v<T, std::vector<std::string>>) {
                auto* list_builder = static_cast<arrow::ListBuilder*>(builder);
                auto* value_builder = static_cast<arrow::StringBuilder*>(list_builder->value_builder());
                ARROW_RETURN_NOT_OK(list_builder->Append());
                for (const auto& s : v) {
                    ARROW_RETURN_NOT_OK(value_builder->Append(s));
                }
                return arrow::Status::OK();
            } else if constexpr (std::is_same_v<T, std::vector<FieldMap>>) {
                // Serialize as a minimal JSON-like string for raw passthrough
                return static_cast<arrow::StringBuilder*>(builder)->Append("[FieldMap]");
            } else {
                return arrow::Status::OK();
            }
        },
        fv);
}

}  // namespace

std::string RawBuilder::ontologyTag() const {
    return "raw";
}

std::shared_ptr<arrow::Schema> RawBuilder::schema() const {
    return schema_;
}

arrow::Status RawBuilder::append(const FieldMap& fields) {
    if (!schema_inferred_) {
        ARROW_RETURN_NOT_OK(inferSchema(fields));
        schema_inferred_ = true;
    }
    ARROW_RETURN_NOT_OK(appendToBuilders(fields));
    ++row_count_;
    return arrow::Status::OK();
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> RawBuilder::flush() {
    if (row_count_ == 0) {
        // Return an empty batch if schema has been inferred, otherwise nullptr
        if (schema_inferred_) {
            std::vector<std::shared_ptr<arrow::Array>> arrays;
            arrays.reserve(builders_.size());
            for (auto& b : builders_) {
                std::shared_ptr<arrow::Array> arr;
                ARROW_RETURN_NOT_OK(b->Finish(&arr));
                arrays.push_back(std::move(arr));
            }
            // Recreate builders for reuse
            builders_.clear();
            for (const auto& field : schema_->fields()) {
                std::unique_ptr<arrow::ArrayBuilder> new_builder;
                ARROW_RETURN_NOT_OK(arrow::MakeBuilder(arrow::default_memory_pool(), field->type(), &new_builder));
                builders_.push_back(std::move(new_builder));
            }
            return arrow::RecordBatch::Make(schema_, 0, arrays);
        }
        return nullptr;
    }

    std::vector<std::shared_ptr<arrow::Array>> arrays;
    arrays.reserve(builders_.size());
    for (auto& b : builders_) {
        std::shared_ptr<arrow::Array> arr;
        ARROW_RETURN_NOT_OK(b->Finish(&arr));
        arrays.push_back(std::move(arr));
    }

    int64_t num_rows = row_count_;
    row_count_ = 0;

    // Recreate builders so they're ready for the next batch
    builders_.clear();
    for (const auto& field : schema_->fields()) {
        std::unique_ptr<arrow::ArrayBuilder> new_builder;
        ARROW_RETURN_NOT_OK(arrow::MakeBuilder(arrow::default_memory_pool(), field->type(), &new_builder));
        builders_.push_back(std::move(new_builder));
    }

    return arrow::RecordBatch::Make(schema_, num_rows, arrays);
}

bool RawBuilder::shouldFlush() const {
    return row_count_ >= kBatchSize;
}

arrow::Status RawBuilder::inferSchema(const FieldMap& fields) {
    // Collect and sort keys alphabetically
    column_names_.clear();
    column_names_.reserve(fields.fields.size());
    for (const auto& [key, _] : fields.fields) {
        column_names_.push_back(key);
    }
    std::sort(column_names_.begin(), column_names_.end());

    // Build Arrow schema fields
    std::vector<std::shared_ptr<arrow::Field>> arrow_fields;
    arrow_fields.reserve(column_names_.size());
    for (const auto& name : column_names_) {
        const auto& fv = fields.fields.at(name);
        auto type = fieldValueToArrowType(fv);
        arrow_fields.push_back(arrow::field(name, type));
    }

    schema_ = arrow::schema(arrow_fields);

    // Create builders
    builders_.clear();
    builders_.reserve(column_names_.size());
    for (const auto& arrow_field : schema_->fields()) {
        std::unique_ptr<arrow::ArrayBuilder> builder;
        ARROW_RETURN_NOT_OK(arrow::MakeBuilder(arrow::default_memory_pool(), arrow_field->type(), &builder));
        builders_.push_back(std::move(builder));
    }

    return arrow::Status::OK();
}

arrow::Status RawBuilder::appendToBuilders(const FieldMap& fields) {
    for (size_t i = 0; i < column_names_.size(); ++i) {
        const auto& name = column_names_[i];
        auto it = fields.fields.find(name);
        if (it == fields.fields.end()) {
            // Field missing in this row — append null
            ARROW_RETURN_NOT_OK(builders_[i]->AppendNull());
        } else {
            ARROW_RETURN_NOT_OK(appendValue(builders_[i].get(), it->second));
        }
    }
    return arrow::Status::OK();
}
