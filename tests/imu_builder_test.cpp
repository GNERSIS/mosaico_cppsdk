#include <gtest/gtest.h>
#include "ontology/builders/imu_builder.hpp"
#include "ontology/field_map.hpp"

namespace {

FieldMap makeImuFields() {
    FieldMap fm;
    fm.fields["header.stamp.sec"] = int32_t{100};
    fm.fields["header.stamp.nanosec"] = uint32_t{0};
    fm.fields["header.frame_id"] = std::string("imu_link");
    fm.fields["_log_time_ns"] = uint64_t{100000000000};

    fm.fields["linear_acceleration.x"] = 0.1;
    fm.fields["linear_acceleration.y"] = 0.2;
    fm.fields["linear_acceleration.z"] = 9.81;
    fm.fields["angular_velocity.x"] = 0.01;
    fm.fields["angular_velocity.y"] = 0.02;
    fm.fields["angular_velocity.z"] = 0.03;
    return fm;
}

TEST(ImuBuilder, BasicNoOrientation) {
    ImuBuilder builder;
    FieldMap fm = makeImuFields();

    // No orientation, no covariance
    ASSERT_TRUE(builder.append(fm).ok());
    auto batch = builder.flush().ValueOrDie();
    ASSERT_NE(batch, nullptr);
    EXPECT_EQ(batch->num_rows(), 1);
    EXPECT_EQ(batch->schema()->field(2)->name(), "acceleration");
    EXPECT_EQ(batch->schema()->field(3)->name(), "angular_velocity");
    EXPECT_EQ(batch->schema()->field(4)->name(), "orientation");
    // orientation should be null
    EXPECT_TRUE(batch->column(4)->IsNull(0));
}

TEST(ImuBuilder, WithOrientation) {
    ImuBuilder builder;
    FieldMap fm = makeImuFields();
    fm.fields["orientation.x"] = 0.0;
    fm.fields["orientation.y"] = 0.0;
    fm.fields["orientation.z"] = 0.0;
    fm.fields["orientation.w"] = 1.0;
    // covariance[0] != -1 means orientation is available
    fm.fields["orientation_covariance"] = std::vector<double>{0.01, 0, 0, 0, 0.01, 0, 0, 0, 0.01};

    ASSERT_TRUE(builder.append(fm).ok());
    auto batch = builder.flush().ValueOrDie();
    EXPECT_EQ(batch->num_rows(), 1);
    // orientation should NOT be null
    EXPECT_FALSE(batch->column(4)->IsNull(0));
}

TEST(ImuBuilder, OrientationUnavailable) {
    ImuBuilder builder;
    FieldMap fm = makeImuFields();
    fm.fields["orientation.x"] = 0.0;
    fm.fields["orientation.y"] = 0.0;
    fm.fields["orientation.z"] = 0.0;
    fm.fields["orientation.w"] = 0.0;
    // covariance[0] == -1 means orientation data unavailable
    fm.fields["orientation_covariance"] = std::vector<double>{-1, 0, 0, 0, 0, 0, 0, 0, 0};

    ASSERT_TRUE(builder.append(fm).ok());
    auto batch = builder.flush().ValueOrDie();
    EXPECT_EQ(batch->num_rows(), 1);
    EXPECT_TRUE(batch->column(4)->IsNull(0));
}

TEST(ImuBuilder, WithAccelerationCovariance) {
    ImuBuilder builder;
    FieldMap fm = makeImuFields();
    fm.fields["linear_acceleration_covariance"] = std::vector<double>{0.1, 0, 0, 0, 0.1, 0, 0, 0, 0.1};
    fm.fields["angular_velocity_covariance"] = std::vector<double>{0.05, 0, 0, 0, 0.05, 0, 0, 0, 0.05};

    ASSERT_TRUE(builder.append(fm).ok());
    auto batch = builder.flush().ValueOrDie();
    EXPECT_EQ(batch->num_rows(), 1);
    // Both acceleration and angular_velocity should have covariance
}

TEST(ImuBuilder, MultipleRows) {
    ImuBuilder builder;
    for (int i = 0; i < 5; ++i) {
        FieldMap fm = makeImuFields();
        fm.fields["linear_acceleration.x"] = static_cast<double>(i);
        ASSERT_TRUE(builder.append(fm).ok());
    }
    auto batch = builder.flush().ValueOrDie();
    EXPECT_EQ(batch->num_rows(), 5);
}

TEST(ImuBuilder, OntologyTag) {
    ImuBuilder builder;
    EXPECT_EQ(builder.ontologyTag(), "imu");
}

}  // namespace
