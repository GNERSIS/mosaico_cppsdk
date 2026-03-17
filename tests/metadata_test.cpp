#include "flight/metadata.hpp"
#include <arrow/api.h>
#include <gtest/gtest.h>

TEST(MetadataTest, ExtractOntologyTagFromProperties) {
    auto kv = arrow::KeyValueMetadata::Make(
        {"mosaico:properties"}, {R"({"ontology_tag": "imu"})"});
    auto tag = mosaico::extractOntologyTag(kv);
    ASSERT_TRUE(tag.has_value());
    EXPECT_EQ(*tag, "imu");
}

TEST(MetadataTest, ExtractOntologyTagMissing) {
    auto kv = arrow::KeyValueMetadata::Make({"other_key"}, {"value"});
    EXPECT_FALSE(mosaico::extractOntologyTag(kv).has_value());
}

TEST(MetadataTest, ExtractOntologyTagNullMetadata) {
    EXPECT_FALSE(mosaico::extractOntologyTag(nullptr).has_value());
}

TEST(MetadataTest, ExtractOntologyTagInvalidJson) {
    auto kv = arrow::KeyValueMetadata::Make(
        {"mosaico:properties"}, {"not json"});
    EXPECT_FALSE(mosaico::extractOntologyTag(kv).has_value());
}

TEST(MetadataTest, ExtractUserMetadata) {
    auto kv = arrow::KeyValueMetadata::Make(
        {"mosaico:user_metadata"},
        {R"({"vehicle": "nissan", "location": "zala"})"});
    auto meta = mosaico::extractUserMetadata(kv);
    EXPECT_EQ(meta["vehicle"], "nissan");
    EXPECT_EQ(meta["location"], "zala");
}

TEST(MetadataTest, ExtractUserMetadataEmpty) {
    EXPECT_TRUE(mosaico::extractUserMetadata(nullptr).empty());
}

TEST(MetadataTest, DetectOntologyTagImu) {
    auto schema = arrow::schema({
        arrow::field("timestamp_ns", arrow::int64()),
        arrow::field("acceleration", arrow::struct_({
            arrow::field("x", arrow::float64()),
            arrow::field("y", arrow::float64()),
            arrow::field("z", arrow::float64()),
        })),
        arrow::field("angular_velocity", arrow::struct_({
            arrow::field("x", arrow::float64()),
            arrow::field("y", arrow::float64()),
            arrow::field("z", arrow::float64()),
        })),
    });
    auto tag = mosaico::detectOntologyTag(schema);
    ASSERT_TRUE(tag.has_value());
    EXPECT_EQ(*tag, "imu");
}

TEST(MetadataTest, DetectOntologyTagPose) {
    auto schema = arrow::schema({
        arrow::field("timestamp_ns", arrow::int64()),
        arrow::field("position", arrow::struct_({
            arrow::field("x", arrow::float64()),
        })),
        arrow::field("orientation", arrow::struct_({
            arrow::field("x", arrow::float64()),
        })),
    });
    EXPECT_EQ(*mosaico::detectOntologyTag(schema), "pose");
}

TEST(MetadataTest, DetectOntologyTagVelocity) {
    auto schema = arrow::schema({
        arrow::field("timestamp_ns", arrow::int64()),
        arrow::field("linear", arrow::struct_({
            arrow::field("x", arrow::float64()),
        })),
        arrow::field("angular", arrow::struct_({
            arrow::field("x", arrow::float64()),
        })),
    });
    EXPECT_EQ(*mosaico::detectOntologyTag(schema), "velocity");
}

TEST(MetadataTest, DetectOntologyTagUnknown) {
    auto schema = arrow::schema({
        arrow::field("timestamp_ns", arrow::int64()),
        arrow::field("mystery", arrow::float64()),
    });
    EXPECT_FALSE(mosaico::detectOntologyTag(schema).has_value());
}
