#include "ontology/ontology_registry.hpp"
#include "ontology/tag_resolver.hpp"
#include <gtest/gtest.h>

// Minimal stub builder for testing
class StubBuilder : public OntologyBuilder {
 public:
    explicit StubBuilder(std::string tag) : tag_(std::move(tag)) {}
    std::string ontologyTag() const override { return tag_; }
    std::shared_ptr<arrow::Schema> schema() const override { return nullptr; }
    arrow::Status append(const FieldMap&) override { return arrow::Status::OK(); }
    arrow::Result<std::shared_ptr<arrow::RecordBatch>> flush() override { return nullptr; }
    bool shouldFlush() const override { return false; }
 private:
    std::string tag_;
};

TEST(OntologyRegistryTest, FindReturnsNullptrForUnknown) {
    OntologyRegistry reg;
    EXPECT_EQ(reg.find("nonexistent"), nullptr);
}

TEST(OntologyRegistryTest, FindReturnsBuilderAfterAdd) {
    OntologyRegistry reg;
    reg.add("imu", []{ return std::make_unique<StubBuilder>("imu"); });
    auto* b = reg.find("imu");
    ASSERT_NE(b, nullptr);
    EXPECT_EQ(b->ontologyTag(), "imu");
}

TEST(OntologyRegistryTest, FindReturnsSameInstanceOnSecondCall) {
    OntologyRegistry reg;
    reg.add("imu", []{ return std::make_unique<StubBuilder>("imu"); });
    auto* b1 = reg.find("imu");
    auto* b2 = reg.find("imu");
    EXPECT_EQ(b1, b2);
}

TEST(TagResolverTest, ResolvesKnownTag) {
    TagResolver r;
    r.map("sensor_msgs/msg/Imu", "imu");
    auto result = r.resolve("sensor_msgs/msg/Imu");
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(*result, "imu");
}

TEST(TagResolverTest, ReturnsNulloptForUnknown) {
    TagResolver r;
    auto result = r.resolve("unknown/Type");
    EXPECT_FALSE(result.has_value());
}

TEST(TagResolverTest, MultipleSourceTagsSameOntologyTag) {
    TagResolver r;
    r.map("sensor_msgs/msg/Imu", "imu");
    r.map("sensor_msgs/Imu", "imu");
    r.map("sensor_msgs.msg.Imu", "imu");
    EXPECT_EQ(*r.resolve("sensor_msgs/msg/Imu"), "imu");
    EXPECT_EQ(*r.resolve("sensor_msgs/Imu"), "imu");
    EXPECT_EQ(*r.resolve("sensor_msgs.msg.Imu"), "imu");
}

TEST(TagResolverTest, CreateRosTagResolverHasImu) {
    auto r = createRosTagResolver();
    EXPECT_TRUE(r.resolve("sensor_msgs/msg/Imu").has_value());
    EXPECT_TRUE(r.resolve("sensor_msgs/Imu").has_value());
    EXPECT_TRUE(r.resolve("sensor_msgs.msg.Imu").has_value());
}

TEST(TagResolverTest, CreateRosTagResolverHasGeometryVariants) {
    auto r = createRosTagResolver();
    EXPECT_EQ(*r.resolve("geometry_msgs/msg/Pose"), "pose");
    EXPECT_EQ(*r.resolve("geometry_msgs/msg/PoseStamped"), "pose");
    EXPECT_EQ(*r.resolve("geometry_msgs/msg/PoseWithCovariance"), "pose");
    EXPECT_EQ(*r.resolve("geometry_msgs/msg/PoseWithCovarianceStamped"), "pose");
}

TEST(TagResolverTest, CreateRosTagResolverHasStdMsgs) {
    auto r = createRosTagResolver();
    EXPECT_EQ(*r.resolve("std_msgs/msg/Float32"), "floating32");
    EXPECT_EQ(*r.resolve("std_msgs/msg/String"), "string");
}
