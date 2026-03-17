#include <gtest/gtest.h>
#include "ontology/builders/std_type_builder.hpp"
#include "ontology/field_map.hpp"

namespace {

TEST(StdTypeBuilder, StringType) {
    StdTypeBuilder<arrow::StringType> builder("string");
    FieldMap fm;
    fm.fields["_log_time_ns"] = uint64_t{100000000000};
    fm.fields["data"] = std::string("hello world");

    ASSERT_TRUE(builder.append(fm).ok());
    auto batch = builder.flush().ValueOrDie();
    ASSERT_NE(batch, nullptr);
    EXPECT_EQ(batch->num_rows(), 1);
    EXPECT_EQ(batch->schema()->field(2)->name(), "data");
    EXPECT_EQ(batch->schema()->field(2)->type()->id(), arrow::Type::STRING);

    auto arr = std::static_pointer_cast<arrow::StringArray>(batch->column(2));
    EXPECT_EQ(arr->GetString(0), "hello world");
}

TEST(StdTypeBuilder, BoolType) {
    StdTypeBuilder<arrow::BooleanType> builder("boolean");
    FieldMap fm;
    fm.fields["_log_time_ns"] = uint64_t{100000000000};
    fm.fields["data"] = true;

    ASSERT_TRUE(builder.append(fm).ok());
    auto batch = builder.flush().ValueOrDie();
    EXPECT_EQ(batch->num_rows(), 1);
    auto arr = std::static_pointer_cast<arrow::BooleanArray>(batch->column(2));
    EXPECT_TRUE(arr->Value(0));
}

TEST(StdTypeBuilder, Int32Type) {
    StdTypeBuilder<arrow::Int32Type> builder("integer32");
    FieldMap fm;
    fm.fields["_log_time_ns"] = uint64_t{100000000000};
    fm.fields["data"] = int32_t{42};

    ASSERT_TRUE(builder.append(fm).ok());
    auto batch = builder.flush().ValueOrDie();
    EXPECT_EQ(batch->num_rows(), 1);
    auto arr = std::static_pointer_cast<arrow::Int32Array>(batch->column(2));
    EXPECT_EQ(arr->Value(0), 42);
}

TEST(StdTypeBuilder, Float32Type) {
    StdTypeBuilder<arrow::FloatType> builder("floating32");
    FieldMap fm;
    fm.fields["_log_time_ns"] = uint64_t{100000000000};
    fm.fields["data"] = 3.14f;

    ASSERT_TRUE(builder.append(fm).ok());
    auto batch = builder.flush().ValueOrDie();
    EXPECT_EQ(batch->num_rows(), 1);
    auto arr = std::static_pointer_cast<arrow::FloatArray>(batch->column(2));
    EXPECT_FLOAT_EQ(arr->Value(0), 3.14f);
}

TEST(StdTypeBuilder, Float64Type) {
    StdTypeBuilder<arrow::DoubleType> builder("floating64");
    FieldMap fm;
    fm.fields["_log_time_ns"] = uint64_t{100000000000};
    fm.fields["data"] = 2.71828;

    ASSERT_TRUE(builder.append(fm).ok());
    auto batch = builder.flush().ValueOrDie();
    EXPECT_EQ(batch->num_rows(), 1);
    auto arr = std::static_pointer_cast<arrow::DoubleArray>(batch->column(2));
    EXPECT_DOUBLE_EQ(arr->Value(0), 2.71828);
}

TEST(StdTypeBuilder, OntologyTag) {
    StdTypeBuilder<arrow::StringType> builder("string");
    EXPECT_EQ(builder.ontologyTag(), "string");
}

TEST(StdTypeBuilder, SchemaHasHeader) {
    StdTypeBuilder<arrow::StringType> builder("string");
    FieldMap fm;
    fm.fields["_log_time_ns"] = uint64_t{100000000000};
    fm.fields["data"] = std::string("test");
    fm.fields["header.stamp.sec"] = int32_t{100};
    fm.fields["header.stamp.nanosec"] = uint32_t{0};

    ASSERT_TRUE(builder.append(fm).ok());
    auto batch = builder.flush().ValueOrDie();
    EXPECT_EQ(batch->schema()->field(3)->name(), "header");
}

}  // namespace
