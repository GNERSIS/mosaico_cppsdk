#pragma once
#include "ontology/ontology_builder.hpp"
#include <arrow/api.h>

class RawBuilder : public OntologyBuilder {
 public:
    std::string ontologyTag() const override;
    std::shared_ptr<arrow::Schema> schema() const override;
    arrow::Status append(const FieldMap& fields) override;
    arrow::Result<std::shared_ptr<arrow::RecordBatch>> flush() override;
    bool shouldFlush() const override;

 private:
    static constexpr int64_t kBatchSize = 65536;

    bool schema_inferred_ = false;
    std::shared_ptr<arrow::Schema> schema_;
    std::vector<std::string> column_names_;  // sorted alphabetically
    std::vector<std::unique_ptr<arrow::ArrayBuilder>> builders_;
    int64_t row_count_ = 0;

    arrow::Status inferSchema(const FieldMap& fields);
    arrow::Status appendToBuilders(const FieldMap& fields);
};
