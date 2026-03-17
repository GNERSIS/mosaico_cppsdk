#include <gtest/gtest.h>
#include "ontology/builders/nav_builders.hpp"
#include "ontology/field_map.hpp"

namespace {

TEST(MotionStateBuilder, BasicOdometry) {
    MotionStateBuilder builder;
    FieldMap fm;
    fm.fields["_log_time_ns"] = uint64_t{100000000000};
    fm.fields["header.stamp.sec"] = int32_t{100};
    fm.fields["header.stamp.nanosec"] = uint32_t{0};
    fm.fields["header.frame_id"] = std::string("odom");
    fm.fields["child_frame_id"] = std::string("base_link");
    fm.fields["pose.pose.position.x"] = 1.0;
    fm.fields["pose.pose.position.y"] = 2.0;
    fm.fields["pose.pose.position.z"] = 0.0;
    fm.fields["pose.pose.orientation.x"] = 0.0;
    fm.fields["pose.pose.orientation.y"] = 0.0;
    fm.fields["pose.pose.orientation.z"] = 0.0;
    fm.fields["pose.pose.orientation.w"] = 1.0;
    fm.fields["twist.twist.linear.x"] = 1.5;
    fm.fields["twist.twist.linear.y"] = 0.0;
    fm.fields["twist.twist.linear.z"] = 0.0;
    fm.fields["twist.twist.angular.x"] = 0.0;
    fm.fields["twist.twist.angular.y"] = 0.0;
    fm.fields["twist.twist.angular.z"] = 0.1;

    ASSERT_TRUE(builder.append(fm).ok());
    auto batch = builder.flush().ValueOrDie();
    ASSERT_NE(batch, nullptr);
    EXPECT_EQ(batch->num_rows(), 1);
    EXPECT_EQ(batch->schema()->field(2)->name(), "pose");
    EXPECT_EQ(batch->schema()->field(3)->name(), "velocity");
    EXPECT_EQ(batch->schema()->field(4)->name(), "target_frame_id");
    EXPECT_EQ(batch->schema()->field(5)->name(), "acceleration");
    // acceleration should be null
    EXPECT_TRUE(batch->column(5)->IsNull(0));
}

TEST(MotionStateBuilder, OntologyTag) {
    MotionStateBuilder builder;
    EXPECT_EQ(builder.ontologyTag(), "motion_state");
}

TEST(FrameTransformBuilder, SingleTransform) {
    FrameTransformBuilder builder;
    FieldMap fm;
    fm.fields["_log_time_ns"] = uint64_t{100000000000};
    fm.fields["header.stamp.sec"] = int32_t{100};
    fm.fields["header.stamp.nanosec"] = uint32_t{0};
    fm.fields["header.frame_id"] = std::string("map");

    FieldMap tf;
    tf.fields["header.stamp.sec"] = int32_t{100};
    tf.fields["header.stamp.nanosec"] = uint32_t{0};
    tf.fields["header.frame_id"] = std::string("map");
    tf.fields["child_frame_id"] = std::string("base_link");
    tf.fields["transform.translation.x"] = 1.0;
    tf.fields["transform.translation.y"] = 0.0;
    tf.fields["transform.translation.z"] = 0.0;
    tf.fields["transform.rotation.x"] = 0.0;
    tf.fields["transform.rotation.y"] = 0.0;
    tf.fields["transform.rotation.z"] = 0.0;
    tf.fields["transform.rotation.w"] = 1.0;

    fm.fields["transforms"] = std::vector<FieldMap>{tf};

    ASSERT_TRUE(builder.append(fm).ok());
    auto batch = builder.flush().ValueOrDie();
    ASSERT_NE(batch, nullptr);
    EXPECT_EQ(batch->num_rows(), 1);
    EXPECT_EQ(batch->schema()->field(2)->name(), "translation");
    EXPECT_EQ(batch->schema()->field(4)->name(), "target_frame_id");
}

TEST(FrameTransformBuilder, MultipleTransforms) {
    FrameTransformBuilder builder;
    FieldMap fm;
    fm.fields["_log_time_ns"] = uint64_t{100000000000};

    std::vector<FieldMap> transforms;
    for (int i = 0; i < 3; ++i) {
        FieldMap tf;
        tf.fields["header.stamp.sec"] = int32_t{100 + i};
        tf.fields["header.stamp.nanosec"] = uint32_t{0};
        tf.fields["child_frame_id"] = std::string("link_" + std::to_string(i));
        tf.fields["transform.translation.x"] = static_cast<double>(i);
        tf.fields["transform.translation.y"] = 0.0;
        tf.fields["transform.translation.z"] = 0.0;
        tf.fields["transform.rotation.x"] = 0.0;
        tf.fields["transform.rotation.y"] = 0.0;
        tf.fields["transform.rotation.z"] = 0.0;
        tf.fields["transform.rotation.w"] = 1.0;
        transforms.push_back(tf);
    }
    fm.fields["transforms"] = transforms;

    ASSERT_TRUE(builder.append(fm).ok());
    auto batch = builder.flush().ValueOrDie();
    ASSERT_NE(batch, nullptr);
    EXPECT_EQ(batch->num_rows(), 3);
}

TEST(FrameTransformBuilder, OntologyTag) {
    FrameTransformBuilder builder;
    EXPECT_EQ(builder.ontologyTag(), "frame_transform");
}

}  // namespace
