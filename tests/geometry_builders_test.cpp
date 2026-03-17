#include <gtest/gtest.h>
#include "ontology/builders/geometry_builders.hpp"
#include "ontology/builders/compound_geometry_builders.hpp"
#include "ontology/field_map.hpp"

namespace {

FieldMap makeTimestampFields() {
    FieldMap fm;
    fm.fields["header.stamp.sec"] = int32_t{100};
    fm.fields["header.stamp.nanosec"] = uint32_t{500};
    fm.fields["header.frame_id"] = std::string("base_link");
    fm.fields["_log_time_ns"] = uint64_t{100000000500};
    return fm;
}

TEST(Vector3dBuilder, BasicAppendFlush) {
    Vector3dBuilder builder;
    FieldMap fm = makeTimestampFields();
    fm.fields["x"] = 1.0;
    fm.fields["y"] = 2.0;
    fm.fields["z"] = 3.0;

    ASSERT_TRUE(builder.append(fm).ok());
    auto result = builder.flush();
    ASSERT_TRUE(result.ok());
    auto batch = result.ValueOrDie();
    ASSERT_NE(batch, nullptr);
    EXPECT_EQ(batch->num_rows(), 1);
    EXPECT_EQ(batch->schema()->num_fields(), 8); // ts, rec_ts, x, y, z, header, cov, cov_type

    // Check column names
    EXPECT_EQ(batch->schema()->field(0)->name(), "timestamp_ns");
    EXPECT_EQ(batch->schema()->field(2)->name(), "x");
    EXPECT_EQ(batch->schema()->field(3)->name(), "y");
    EXPECT_EQ(batch->schema()->field(4)->name(), "z");

    // Check data values
    auto x_arr = std::static_pointer_cast<arrow::DoubleArray>(batch->column(2));
    EXPECT_DOUBLE_EQ(x_arr->Value(0), 1.0);
    auto y_arr = std::static_pointer_cast<arrow::DoubleArray>(batch->column(3));
    EXPECT_DOUBLE_EQ(y_arr->Value(0), 2.0);
    auto z_arr = std::static_pointer_cast<arrow::DoubleArray>(batch->column(4));
    EXPECT_DOUBLE_EQ(z_arr->Value(0), 3.0);
}

TEST(Vector3dBuilder, StampedUnwrap) {
    Vector3dBuilder builder;
    FieldMap fm = makeTimestampFields();
    fm.fields["vector.x"] = 4.0;
    fm.fields["vector.y"] = 5.0;
    fm.fields["vector.z"] = 6.0;

    ASSERT_TRUE(builder.append(fm).ok());
    auto batch = builder.flush().ValueOrDie();
    auto x_arr = std::static_pointer_cast<arrow::DoubleArray>(batch->column(2));
    EXPECT_DOUBLE_EQ(x_arr->Value(0), 4.0);
}

TEST(Point3dBuilder, BasicFields) {
    Point3dBuilder builder;
    FieldMap fm = makeTimestampFields();
    fm.fields["x"] = 10.0;
    fm.fields["y"] = 20.0;
    fm.fields["z"] = 30.0;

    ASSERT_TRUE(builder.append(fm).ok());
    auto batch = builder.flush().ValueOrDie();
    EXPECT_EQ(batch->num_rows(), 1);
    EXPECT_EQ(batch->schema()->field(2)->name(), "x");
}

TEST(QuaternionBuilder, BasicFields) {
    QuaternionBuilder builder;
    FieldMap fm = makeTimestampFields();
    fm.fields["x"] = 0.0;
    fm.fields["y"] = 0.0;
    fm.fields["z"] = 0.0;
    fm.fields["w"] = 1.0;

    ASSERT_TRUE(builder.append(fm).ok());
    auto batch = builder.flush().ValueOrDie();
    EXPECT_EQ(batch->num_rows(), 1);
    EXPECT_EQ(batch->schema()->num_fields(), 9); // ts, rec_ts, x, y, z, w, header, cov, cov_type
    auto w_arr = std::static_pointer_cast<arrow::DoubleArray>(batch->column(5));
    EXPECT_DOUBLE_EQ(w_arr->Value(0), 1.0);
}

TEST(PoseBuilder, BasicPose) {
    PoseBuilder builder;
    FieldMap fm = makeTimestampFields();
    fm.fields["position.x"] = 1.0;
    fm.fields["position.y"] = 2.0;
    fm.fields["position.z"] = 3.0;
    fm.fields["orientation.x"] = 0.0;
    fm.fields["orientation.y"] = 0.0;
    fm.fields["orientation.z"] = 0.0;
    fm.fields["orientation.w"] = 1.0;

    ASSERT_TRUE(builder.append(fm).ok());
    auto batch = builder.flush().ValueOrDie();
    EXPECT_EQ(batch->num_rows(), 1);
    EXPECT_EQ(batch->schema()->field(2)->name(), "position");
    EXPECT_EQ(batch->schema()->field(3)->name(), "orientation");
}

TEST(PoseBuilder, StampedUnwrap) {
    PoseBuilder builder;
    FieldMap fm = makeTimestampFields();
    fm.fields["pose.position.x"] = 10.0;
    fm.fields["pose.position.y"] = 20.0;
    fm.fields["pose.position.z"] = 30.0;
    fm.fields["pose.orientation.x"] = 0.0;
    fm.fields["pose.orientation.y"] = 0.0;
    fm.fields["pose.orientation.z"] = 0.707;
    fm.fields["pose.orientation.w"] = 0.707;

    ASSERT_TRUE(builder.append(fm).ok());
    auto batch = builder.flush().ValueOrDie();
    EXPECT_EQ(batch->num_rows(), 1);
}

TEST(VelocityBuilder, BasicTwist) {
    VelocityBuilder builder;
    FieldMap fm = makeTimestampFields();
    fm.fields["linear.x"] = 1.0;
    fm.fields["linear.y"] = 0.0;
    fm.fields["linear.z"] = 0.0;
    fm.fields["angular.x"] = 0.0;
    fm.fields["angular.y"] = 0.0;
    fm.fields["angular.z"] = 0.5;

    ASSERT_TRUE(builder.append(fm).ok());
    auto batch = builder.flush().ValueOrDie();
    EXPECT_EQ(batch->num_rows(), 1);
    EXPECT_EQ(batch->schema()->field(2)->name(), "linear");
    EXPECT_EQ(batch->schema()->field(3)->name(), "angular");
}

TEST(AccelerationBuilder, Basic) {
    AccelerationBuilder builder;
    FieldMap fm = makeTimestampFields();
    fm.fields["linear.x"] = 0.0;
    fm.fields["linear.y"] = 0.0;
    fm.fields["linear.z"] = 9.81;
    fm.fields["angular.x"] = 0.1;
    fm.fields["angular.y"] = 0.0;
    fm.fields["angular.z"] = 0.0;

    ASSERT_TRUE(builder.append(fm).ok());
    auto batch = builder.flush().ValueOrDie();
    EXPECT_EQ(batch->num_rows(), 1);
}

TEST(TransformBuilder, BasicTransform) {
    TransformBuilder builder;
    FieldMap fm = makeTimestampFields();
    fm.fields["translation.x"] = 1.0;
    fm.fields["translation.y"] = 0.0;
    fm.fields["translation.z"] = 0.5;
    fm.fields["rotation.x"] = 0.0;
    fm.fields["rotation.y"] = 0.0;
    fm.fields["rotation.z"] = 0.0;
    fm.fields["rotation.w"] = 1.0;
    fm.fields["child_frame_id"] = std::string("child");

    ASSERT_TRUE(builder.append(fm).ok());
    auto batch = builder.flush().ValueOrDie();
    EXPECT_EQ(batch->num_rows(), 1);
    EXPECT_EQ(batch->schema()->field(4)->name(), "target_frame_id");
}

TEST(ForceTorqueBuilder, BasicWrench) {
    ForceTorqueBuilder builder;
    FieldMap fm = makeTimestampFields();
    fm.fields["force.x"] = 10.0;
    fm.fields["force.y"] = 0.0;
    fm.fields["force.z"] = -9.81;
    fm.fields["torque.x"] = 0.0;
    fm.fields["torque.y"] = 0.5;
    fm.fields["torque.z"] = 0.0;

    ASSERT_TRUE(builder.append(fm).ok());
    auto batch = builder.flush().ValueOrDie();
    EXPECT_EQ(batch->num_rows(), 1);
    EXPECT_EQ(batch->schema()->field(2)->name(), "force");
    EXPECT_EQ(batch->schema()->field(3)->name(), "torque");
}

TEST(PoseBuilder, WithCovariance) {
    PoseBuilder builder;
    FieldMap fm = makeTimestampFields();
    fm.fields["position.x"] = 1.0;
    fm.fields["position.y"] = 2.0;
    fm.fields["position.z"] = 3.0;
    fm.fields["orientation.x"] = 0.0;
    fm.fields["orientation.y"] = 0.0;
    fm.fields["orientation.z"] = 0.0;
    fm.fields["orientation.w"] = 1.0;
    fm.fields["covariance"] = std::vector<double>(36, 0.1);

    ASSERT_TRUE(builder.append(fm).ok());
    auto batch = builder.flush().ValueOrDie();
    EXPECT_EQ(batch->num_rows(), 1);
    // covariance column is not null
    EXPECT_FALSE(batch->column(5)->IsNull(0));
}

}  // namespace
