#include "ontology/field_map.hpp"
#include <gtest/gtest.h>

TEST(FieldMapTest, StoreAndRetrieveScalars) {
    FieldMap fm;
    fm.fields["accel.x"] = double(9.81);
    fm.fields["count"] = int32_t(42);
    fm.fields["active"] = true;

    ASSERT_TRUE(std::holds_alternative<double>(fm.fields.at("accel.x")));
    EXPECT_DOUBLE_EQ(std::get<double>(fm.fields.at("accel.x")), 9.81);
    EXPECT_EQ(std::get<int32_t>(fm.fields.at("count")), 42);
    EXPECT_TRUE(std::get<bool>(fm.fields.at("active")));
}

TEST(FieldMapTest, StoreStrings) {
    FieldMap fm;
    fm.fields["frame_id"] = std::string("base_link");
    EXPECT_EQ(std::get<std::string>(fm.fields.at("frame_id")), "base_link");
}

TEST(FieldMapTest, StoreBinaryBlob) {
    FieldMap fm;
    fm.fields["data"] = std::vector<uint8_t>{0x01, 0x02, 0xFF};
    auto& blob = std::get<std::vector<uint8_t>>(fm.fields.at("data"));
    ASSERT_EQ(blob.size(), 3u);
    EXPECT_EQ(blob[2], 0xFF);
}

TEST(FieldMapTest, StoreDoubleArray) {
    FieldMap fm;
    fm.fields["covariance"] = std::vector<double>{1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0};
    auto& arr = std::get<std::vector<double>>(fm.fields.at("covariance"));
    EXPECT_EQ(arr.size(), 9u);
}

TEST(FieldMapTest, StoreNestedFieldMaps) {
    FieldMap inner;
    inner.fields["translation.x"] = double(1.0);
    inner.fields["rotation.w"] = double(1.0);

    FieldMap fm;
    fm.fields["transforms"] = std::vector<FieldMap>{inner};

    auto& vec = std::get<std::vector<FieldMap>>(fm.fields.at("transforms"));
    ASSERT_EQ(vec.size(), 1u);
    EXPECT_DOUBLE_EQ(std::get<double>(vec[0].fields.at("translation.x")), 1.0);
}

TEST(FieldMapTest, StoreAllIntegerTypes) {
    FieldMap fm;
    fm.fields["i8"] = int8_t(-1);
    fm.fields["i16"] = int16_t(-256);
    fm.fields["i32"] = int32_t(-65536);
    fm.fields["i64"] = int64_t(-1LL << 40);
    fm.fields["u8"] = uint8_t(255);
    fm.fields["u16"] = uint16_t(65535);
    fm.fields["u32"] = uint32_t(0xFFFFFFFF);
    fm.fields["u64"] = uint64_t(0xFFFFFFFFFFFFFFFF);

    EXPECT_EQ(std::get<int8_t>(fm.fields.at("i8")), -1);
    EXPECT_EQ(std::get<uint64_t>(fm.fields.at("u64")), 0xFFFFFFFFFFFFFFFF);
}

TEST(FieldMapTest, ResolvePrefixFindsDeepest) {
    FieldMap fm;
    fm.fields["pose.pose.position.x"] = double(1.0);
    auto prefix = resolvePrefix(fm, "position.x", {"pose.pose.", "pose.", ""});
    EXPECT_EQ(prefix, "pose.pose.");
}

TEST(FieldMapTest, ResolvePrefixFallsBack) {
    FieldMap fm;
    fm.fields["position.x"] = double(1.0);
    auto prefix = resolvePrefix(fm, "position.x", {"pose.pose.", "pose.", ""});
    EXPECT_EQ(prefix, "");
}

TEST(FieldMapTest, ResolvePrefixReturnsLastCandidateOnMiss) {
    FieldMap fm;
    auto prefix = resolvePrefix(fm, "position.x", {"pose.pose.", "pose.", ""});
    EXPECT_EQ(prefix, "");
}
