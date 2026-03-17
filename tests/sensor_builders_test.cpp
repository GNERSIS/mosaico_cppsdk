#include <gtest/gtest.h>
#include "ontology/builders/sensor_builders.hpp"
#include "ontology/field_map.hpp"

namespace {

TEST(ImageBuilder, BasicImage) {
    ImageBuilder builder;
    FieldMap fm;
    fm.fields["_log_time_ns"] = uint64_t{100000000000};
    fm.fields["header.stamp.sec"] = int32_t{100};
    fm.fields["header.stamp.nanosec"] = uint32_t{0};
    fm.fields["width"] = int32_t{640};
    fm.fields["height"] = int32_t{480};
    fm.fields["step"] = int32_t{1920};
    fm.fields["encoding"] = std::string("bgr8");
    fm.fields["data"] = std::vector<uint8_t>{0, 1, 2, 3};

    ASSERT_TRUE(builder.append(fm).ok());
    auto batch = builder.flush().ValueOrDie();
    ASSERT_NE(batch, nullptr);
    EXPECT_EQ(batch->num_rows(), 1);
    EXPECT_EQ(batch->schema()->field(2)->name(), "data");
    EXPECT_EQ(batch->schema()->field(3)->name(), "format");
    EXPECT_EQ(batch->schema()->field(4)->name(), "width");
}

TEST(ImageBuilder, OntologyTag) {
    ImageBuilder builder;
    EXPECT_EQ(builder.ontologyTag(), "image");
}

TEST(CompressedImageBuilder, BasicCompressed) {
    CompressedImageBuilder builder;
    FieldMap fm;
    fm.fields["_log_time_ns"] = uint64_t{100000000000};
    fm.fields["format"] = std::string("jpeg");
    fm.fields["data"] = std::vector<uint8_t>{0xFF, 0xD8, 0xFF};

    ASSERT_TRUE(builder.append(fm).ok());
    auto batch = builder.flush().ValueOrDie();
    EXPECT_EQ(batch->num_rows(), 1);
    EXPECT_EQ(batch->schema()->field(2)->name(), "data");
    EXPECT_EQ(batch->schema()->field(3)->name(), "format");
}

TEST(CameraInfoBuilder, BasicCameraInfo) {
    CameraInfoBuilder builder;
    FieldMap fm;
    fm.fields["_log_time_ns"] = uint64_t{100000000000};
    fm.fields["height"] = uint32_t{480};
    fm.fields["width"] = uint32_t{640};
    fm.fields["distortion_model"] = std::string("plumb_bob");
    fm.fields["d"] = std::vector<double>{0, 0, 0, 0, 0};
    fm.fields["k"] = std::vector<double>{1, 0, 320, 0, 1, 240, 0, 0, 1};
    fm.fields["r"] = std::vector<double>{1, 0, 0, 0, 1, 0, 0, 0, 1};
    fm.fields["p"] = std::vector<double>{1, 0, 320, 0, 0, 1, 240, 0, 0, 0, 1, 0};
    fm.fields["binning_x"] = uint32_t{1};
    fm.fields["binning_y"] = uint32_t{1};
    fm.fields["roi.x_offset"] = uint32_t{0};
    fm.fields["roi.y_offset"] = uint32_t{0};
    fm.fields["roi.height"] = uint32_t{480};
    fm.fields["roi.width"] = uint32_t{640};
    fm.fields["roi.do_rectify"] = false;

    ASSERT_TRUE(builder.append(fm).ok());
    auto batch = builder.flush().ValueOrDie();
    EXPECT_EQ(batch->num_rows(), 1);
    EXPECT_EQ(batch->schema()->field(2)->name(), "height");
    EXPECT_EQ(batch->schema()->field(3)->name(), "width");
}

TEST(BatteryStateBuilder, BasicBattery) {
    BatteryStateBuilder builder;
    FieldMap fm;
    fm.fields["_log_time_ns"] = uint64_t{100000000000};
    fm.fields["voltage"] = float{12.6f};
    fm.fields["temperature"] = float{25.0f};
    fm.fields["current"] = float{1.5f};
    fm.fields["charge"] = float{50.0f};
    fm.fields["capacity"] = float{100.0f};
    fm.fields["design_capacity"] = float{100.0f};
    fm.fields["percentage"] = float{0.5f};
    fm.fields["power_supply_status"] = uint8_t{1};
    fm.fields["power_supply_health"] = uint8_t{1};
    fm.fields["power_supply_technology"] = uint8_t{2};
    fm.fields["present"] = true;
    fm.fields["location"] = std::string("slot_0");
    fm.fields["serial_number"] = std::string("SN12345");
    fm.fields["cell_voltage"] = std::vector<double>{3.7, 3.7, 3.7};
    fm.fields["cell_temperature"] = std::vector<double>{25.0, 25.1, 25.2};

    ASSERT_TRUE(builder.append(fm).ok());
    auto batch = builder.flush().ValueOrDie();
    EXPECT_EQ(batch->num_rows(), 1);
    EXPECT_EQ(batch->schema()->field(2)->name(), "voltage");
}

TEST(BatteryStateBuilder, OntologyTag) {
    BatteryStateBuilder builder;
    EXPECT_EQ(builder.ontologyTag(), "battery_state");
}

TEST(RobotJointBuilder, BasicJointState) {
    RobotJointBuilder builder;
    FieldMap fm;
    fm.fields["_log_time_ns"] = uint64_t{100000000000};
    fm.fields["header.stamp.sec"] = int32_t{100};
    fm.fields["header.stamp.nanosec"] = uint32_t{0};
    fm.fields["name"] = std::vector<std::string>{"joint1", "joint2"};
    fm.fields["position"] = std::vector<double>{0.1, 0.2};
    fm.fields["velocity"] = std::vector<double>{0.01, 0.02};
    fm.fields["effort"] = std::vector<double>{1.0, 2.0};

    ASSERT_TRUE(builder.append(fm).ok());
    auto batch = builder.flush().ValueOrDie();
    EXPECT_EQ(batch->num_rows(), 1);
    EXPECT_EQ(batch->schema()->field(2)->name(), "names");
    EXPECT_EQ(batch->schema()->field(3)->name(), "positions");
}

TEST(NmeaSentenceBuilder, BasicSentence) {
    NmeaSentenceBuilder builder;
    FieldMap fm;
    fm.fields["_log_time_ns"] = uint64_t{100000000000};
    fm.fields["sentence"] = std::string("$GPGGA,123519,4807.038,N,01131.000,E,1,08,0.9,545.4,M*47");

    ASSERT_TRUE(builder.append(fm).ok());
    auto batch = builder.flush().ValueOrDie();
    EXPECT_EQ(batch->num_rows(), 1);
    EXPECT_EQ(batch->schema()->field(2)->name(), "sentence");
}

TEST(RoiBuilder, BasicRoi) {
    RoiBuilder builder;
    FieldMap fm;
    fm.fields["_log_time_ns"] = uint64_t{100000000000};
    fm.fields["x_offset"] = uint32_t{10};
    fm.fields["y_offset"] = uint32_t{20};
    fm.fields["height"] = uint32_t{100};
    fm.fields["width"] = uint32_t{200};
    fm.fields["do_rectify"] = false;

    ASSERT_TRUE(builder.append(fm).ok());
    auto batch = builder.flush().ValueOrDie();
    EXPECT_EQ(batch->num_rows(), 1);
    EXPECT_EQ(batch->schema()->field(2)->name(), "offset");
    EXPECT_EQ(batch->schema()->field(3)->name(), "height");
    EXPECT_EQ(batch->schema()->field(4)->name(), "width");
}

}  // namespace
