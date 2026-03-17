#include <gtest/gtest.h>
#include "ontology/builders/gps_builders.hpp"
#include "ontology/field_map.hpp"

namespace {

TEST(GpsBuilder, BasicNavSatFix) {
    GpsBuilder builder;
    FieldMap fm;
    fm.fields["_log_time_ns"] = uint64_t{100000000000};
    fm.fields["header.stamp.sec"] = int32_t{100};
    fm.fields["header.stamp.nanosec"] = uint32_t{0};
    fm.fields["header.frame_id"] = std::string("gps");
    fm.fields["latitude"] = 45.5;
    fm.fields["longitude"] = -122.5;
    fm.fields["altitude"] = 100.0;
    fm.fields["status.status"] = int8_t{1};
    fm.fields["status.service"] = uint16_t{1};

    ASSERT_TRUE(builder.append(fm).ok());
    auto batch = builder.flush().ValueOrDie();
    ASSERT_NE(batch, nullptr);
    EXPECT_EQ(batch->num_rows(), 1);
    EXPECT_EQ(batch->schema()->field(2)->name(), "position");
    EXPECT_EQ(batch->schema()->field(4)->name(), "status");
}

TEST(GpsBuilder, WithPositionCovariance) {
    GpsBuilder builder;
    FieldMap fm;
    fm.fields["_log_time_ns"] = uint64_t{100000000000};
    fm.fields["latitude"] = 45.5;
    fm.fields["longitude"] = -122.5;
    fm.fields["altitude"] = 100.0;
    fm.fields["status.status"] = int8_t{0};
    fm.fields["status.service"] = uint16_t{1};
    fm.fields["position_covariance"] = std::vector<double>{1.0, 0, 0, 0, 1.0, 0, 0, 0, 1.0};

    ASSERT_TRUE(builder.append(fm).ok());
    auto batch = builder.flush().ValueOrDie();
    EXPECT_EQ(batch->num_rows(), 1);
}

TEST(GpsBuilder, NoStatus) {
    GpsBuilder builder;
    FieldMap fm;
    fm.fields["_log_time_ns"] = uint64_t{100000000000};
    fm.fields["latitude"] = 45.5;
    fm.fields["longitude"] = -122.5;
    fm.fields["altitude"] = 100.0;

    ASSERT_TRUE(builder.append(fm).ok());
    auto batch = builder.flush().ValueOrDie();
    EXPECT_EQ(batch->num_rows(), 1);
    // status should be null
    EXPECT_TRUE(batch->column(4)->IsNull(0));
}

TEST(GpsBuilder, OntologyTag) {
    GpsBuilder builder;
    EXPECT_EQ(builder.ontologyTag(), "gps");
}

TEST(GpsStatusBuilder, BasicStatus) {
    GpsStatusBuilder builder;
    FieldMap fm;
    fm.fields["_log_time_ns"] = uint64_t{100000000000};
    fm.fields["status"] = int8_t{2};
    fm.fields["service"] = uint16_t{3};

    ASSERT_TRUE(builder.append(fm).ok());
    auto batch = builder.flush().ValueOrDie();
    EXPECT_EQ(batch->num_rows(), 1);
    EXPECT_EQ(batch->schema()->field(2)->name(), "status");
    EXPECT_EQ(batch->schema()->field(3)->name(), "service");
}

}  // namespace
