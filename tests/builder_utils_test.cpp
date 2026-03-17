#include "ontology/builders/builder_utils.hpp"
#include <gtest/gtest.h>

TEST(BuilderUtilsTest, ExtractTimestampFromHeaderStamp) {
    FieldMap fm;
    fm.fields["header.stamp.sec"] = int32_t(1710000000);
    fm.fields["header.stamp.nanosec"] = uint32_t(500000000);
    fm.fields["_log_time_ns"] = uint64_t(1710000000000000000ULL);

    auto [ts, rec_ts] = extractTimestamps(fm);
    EXPECT_EQ(ts, 1710000000500000000LL);
    EXPECT_EQ(rec_ts, 1710000000000000000ULL);
}

TEST(BuilderUtilsTest, ExtractTimestampFallbackToLogTime) {
    FieldMap fm;
    fm.fields["_log_time_ns"] = uint64_t(1710000000000000000ULL);

    auto [ts, rec_ts] = extractTimestamps(fm);
    EXPECT_EQ(ts, 1710000000000000000LL);
    EXPECT_EQ(rec_ts, 1710000000000000000ULL);
}

TEST(BuilderUtilsTest, ExtractTimestampNoData) {
    FieldMap fm;
    auto [ts, rec_ts] = extractTimestamps(fm);
    EXPECT_EQ(ts, 0);
    EXPECT_EQ(rec_ts, 0);
}

TEST(BuilderUtilsTest, IsValidCovarianceRejectsAllZeros) {
    std::vector<double> cov(9, 0.0);
    EXPECT_FALSE(isValidCovariance(cov));
}

TEST(BuilderUtilsTest, IsValidCovarianceRejectsMinusOne) {
    std::vector<double> cov = {-1.0, 0, 0, 0, 0, 0, 0, 0, 0};
    EXPECT_FALSE(isValidCovariance(cov));
}

TEST(BuilderUtilsTest, IsValidCovarianceAcceptsValid) {
    std::vector<double> cov = {0.01, 0, 0, 0, 0.01, 0, 0, 0, 0.01};
    EXPECT_TRUE(isValidCovariance(cov));
}

TEST(BuilderUtilsTest, IsValidCovarianceRejectsEmpty) {
    EXPECT_FALSE(isValidCovariance({}));
}

TEST(BuilderUtilsTest, GetDoublePresent) {
    FieldMap fm;
    fm.fields["x"] = double(3.14);
    auto v = getDouble(fm, "x");
    ASSERT_TRUE(v.has_value());
    EXPECT_DOUBLE_EQ(*v, 3.14);
}

TEST(BuilderUtilsTest, GetDoubleMissing) {
    FieldMap fm;
    EXPECT_FALSE(getDouble(fm, "x").has_value());
}

TEST(BuilderUtilsTest, GetDoubleWrongType) {
    FieldMap fm;
    fm.fields["x"] = int32_t(42);
    EXPECT_FALSE(getDouble(fm, "x").has_value());
}

TEST(BuilderUtilsTest, GetStringPresent) {
    FieldMap fm;
    fm.fields["name"] = std::string("hello");
    EXPECT_EQ(*getString(fm, "name"), "hello");
}

TEST(BuilderUtilsTest, GetDoubleArrayPresent) {
    FieldMap fm;
    fm.fields["cov"] = std::vector<double>{1.0, 2.0};
    auto* arr = getDoubleArray(fm, "cov");
    ASSERT_NE(arr, nullptr);
    EXPECT_EQ(arr->size(), 2u);
}

TEST(BuilderUtilsTest, GetDoubleArrayMissing) {
    FieldMap fm;
    EXPECT_EQ(getDoubleArray(fm, "cov"), nullptr);
}
