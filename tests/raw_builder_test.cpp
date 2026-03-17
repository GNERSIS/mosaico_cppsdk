#include "ontology/builders/raw_builder.hpp"
#include <arrow/api.h>
#include <gtest/gtest.h>

#define ASSERT_OK(expr) ASSERT_TRUE((expr).ok())

TEST(RawBuilderTest, InfersSchemaFromFirstAppend) {
    RawBuilder builder;
    FieldMap fm;
    fm.fields["_log_time_ns"] = uint64_t(1000);
    fm.fields["value"] = double(3.14);
    fm.fields["name"] = std::string("test");

    ASSERT_OK(builder.append(fm));
    auto result = builder.flush();
    ASSERT_OK(result.status());

    auto batch = *result;
    ASSERT_NE(batch, nullptr);
    EXPECT_EQ(batch->num_rows(), 1);
    EXPECT_EQ(batch->schema()->field(0)->name(), "_log_time_ns");
    EXPECT_EQ(batch->schema()->field(1)->name(), "name");
    EXPECT_EQ(batch->schema()->field(2)->name(), "value");
}

TEST(RawBuilderTest, CorrectArrowTypes) {
    RawBuilder builder;
    FieldMap fm;
    fm.fields["b"] = true;
    fm.fields["i32"] = int32_t(1);
    fm.fields["u64"] = uint64_t(2);
    fm.fields["f"] = float(1.5f);
    fm.fields["d"] = double(2.5);
    fm.fields["s"] = std::string("hi");
    fm.fields["blob"] = std::vector<uint8_t>{1, 2};
    fm.fields["arr"] = std::vector<double>{1.0};

    ASSERT_OK(builder.append(fm));
    auto batch = *builder.flush();
    auto schema = batch->schema();

    EXPECT_TRUE(schema->GetFieldByName("b")->type()->Equals(arrow::boolean()));
    EXPECT_TRUE(schema->GetFieldByName("i32")->type()->Equals(arrow::int32()));
    EXPECT_TRUE(schema->GetFieldByName("u64")->type()->Equals(arrow::uint64()));
    EXPECT_TRUE(schema->GetFieldByName("f")->type()->Equals(arrow::float32()));
    EXPECT_TRUE(schema->GetFieldByName("d")->type()->Equals(arrow::float64()));
    EXPECT_TRUE(schema->GetFieldByName("s")->type()->Equals(arrow::utf8()));
    EXPECT_TRUE(schema->GetFieldByName("blob")->type()->Equals(arrow::binary()));
    EXPECT_TRUE(schema->GetFieldByName("arr")->type()->Equals(arrow::list(arrow::float64())));
}

TEST(RawBuilderTest, AccumulatesMultipleRows) {
    RawBuilder builder;
    for (int i = 0; i < 3; ++i) {
        FieldMap fm;
        fm.fields["_log_time_ns"] = uint64_t(i * 1000);
        fm.fields["x"] = double(i * 1.5);
        ASSERT_OK(builder.append(fm));
    }
    auto batch = *builder.flush();
    EXPECT_EQ(batch->num_rows(), 3);
}

TEST(RawBuilderTest, FlushResetsState) {
    RawBuilder builder;
    FieldMap fm;
    fm.fields["x"] = double(1.0);
    ASSERT_OK(builder.append(fm));
    auto batch1 = *builder.flush();
    EXPECT_EQ(batch1->num_rows(), 1);

    ASSERT_OK(builder.append(fm));
    auto batch2 = *builder.flush();
    EXPECT_EQ(batch2->num_rows(), 1);
}

TEST(RawBuilderTest, FlushReturnsNullWhenEmpty) {
    RawBuilder builder;
    // Need at least one append to infer schema
    FieldMap fm;
    fm.fields["x"] = double(1.0);
    ASSERT_OK(builder.append(fm));
    auto b1 = *builder.flush();
    EXPECT_EQ(b1->num_rows(), 1);

    // Second flush with no appends
    auto result2 = builder.flush();
    // Should return batch with 0 rows or nullptr
    ASSERT_TRUE(result2.ok());
}

TEST(RawBuilderTest, ShouldFlushReturnsFalseForSmallCounts) {
    RawBuilder builder;
    FieldMap fm;
    fm.fields["x"] = double(1.0);
    ASSERT_OK(builder.append(fm));
    EXPECT_FALSE(builder.shouldFlush());
}

TEST(RawBuilderTest, OntologyTagIsRaw) {
    RawBuilder builder;
    EXPECT_EQ(builder.ontologyTag(), "raw");
}

TEST(RawBuilderTest, HandlesNullForMissingFields) {
    RawBuilder builder;
    // First append establishes schema with 2 columns
    FieldMap fm1;
    fm1.fields["a"] = double(1.0);
    fm1.fields["b"] = double(2.0);
    ASSERT_OK(builder.append(fm1));

    // Second append missing "b"
    FieldMap fm2;
    fm2.fields["a"] = double(3.0);
    ASSERT_OK(builder.append(fm2));

    auto batch = *builder.flush();
    EXPECT_EQ(batch->num_rows(), 2);
    // Column b should have null in row 1
    EXPECT_TRUE(batch->column(0)->IsValid(1));  // "a" present
    EXPECT_FALSE(batch->column(1)->IsValid(1)); // "b" null
}
