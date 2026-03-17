// tests/tools/msg_parser_test.cpp
#include "decoders/msg_parser.hpp"

#include <arrow/api.h>
#include <gtest/gtest.h>

using namespace msg;

TEST(MsgParser, SimplePrimitives) {
  auto schema = parseMsgDef("test/Simple", "float64 x\nfloat64 y\nstring label\n");
  ASSERT_NE(schema, nullptr);
  ASSERT_EQ(schema->fields.size(), 3u);
  EXPECT_EQ(schema->fields[0].name, "x");
  EXPECT_EQ(schema->fields[0].primitive, PrimitiveType::kFloat64);
  EXPECT_EQ(schema->fields[1].name, "y");
  EXPECT_EQ(schema->fields[1].primitive, PrimitiveType::kFloat64);
  EXPECT_EQ(schema->fields[2].name, "label");
  EXPECT_EQ(schema->fields[2].primitive, PrimitiveType::kString);
}

TEST(MsgParser, FixedArray) {
  auto schema = parseMsgDef("test/Arr", "float64[9] covariance\n");
  ASSERT_NE(schema, nullptr);
  ASSERT_EQ(schema->fields.size(), 1u);
  EXPECT_EQ(schema->fields[0].name, "covariance");
  EXPECT_EQ(schema->fields[0].array_size, 9);
  EXPECT_EQ(schema->fields[0].primitive, PrimitiveType::kFloat64);
}

TEST(MsgParser, DynamicArray) {
  auto schema = parseMsgDef("test/DynArr", "uint8[] data\n");
  ASSERT_NE(schema, nullptr);
  ASSERT_EQ(schema->fields.size(), 1u);
  EXPECT_EQ(schema->fields[0].name, "data");
  EXPECT_EQ(schema->fields[0].array_size, -1);
  EXPECT_EQ(schema->fields[0].primitive, PrimitiveType::kUint8);
}

TEST(MsgParser, Comments) {
  auto schema = parseMsgDef("test/Cmt",
      "# This is a comment\nfloat64 x  # inline comment\n");
  ASSERT_NE(schema, nullptr);
  ASSERT_EQ(schema->fields.size(), 1u);
  EXPECT_EQ(schema->fields[0].name, "x");
  EXPECT_EQ(schema->fields[0].primitive, PrimitiveType::kFloat64);
}

TEST(MsgParser, DefaultValue) {
  auto schema = parseMsgDef("test/Def", "float64 w 1\n");
  ASSERT_NE(schema, nullptr);
  ASSERT_EQ(schema->fields.size(), 1u);
  EXPECT_EQ(schema->fields[0].name, "w");
  EXPECT_EQ(schema->fields[0].primitive, PrimitiveType::kFloat64);
  EXPECT_EQ(schema->fields[0].default_value, "1");
}

TEST(MsgParser, NestedType) {
  std::string text =
      "geometry_msgs/Vector3 position\n"
      "================================================================================\n"
      "MSG: geometry_msgs/Vector3\n"
      "float64 x\nfloat64 y\nfloat64 z\n";
  auto schema = parseMsgDef("test/Nested", text);
  ASSERT_NE(schema, nullptr);
  ASSERT_EQ(schema->fields.size(), 1u);
  EXPECT_EQ(schema->fields[0].nested_type, "geometry_msgs/Vector3");
  EXPECT_NE(schema->nested.find("geometry_msgs/Vector3"), schema->nested.end());
  EXPECT_EQ(schema->nested["geometry_msgs/Vector3"]->fields.size(), 3u);
}

TEST(MsgParser, BuiltinTime) {
  std::string text =
      "builtin_interfaces/Time stamp\nstring frame_id\n"
      "================================================================================\n"
      "MSG: builtin_interfaces/Time\n"
      "int32 sec\nuint32 nanosec\n";
  auto schema = parseMsgDef("std_msgs/Header", text);
  ASSERT_NE(schema, nullptr);
  ASSERT_EQ(schema->fields.size(), 2u);
  EXPECT_NE(schema->nested.find("builtin_interfaces/Time"), schema->nested.end());
  auto& time_schema = schema->nested["builtin_interfaces/Time"];
  ASSERT_EQ(time_schema->fields.size(), 2u);
  EXPECT_EQ(time_schema->fields[0].name, "sec");
  EXPECT_EQ(time_schema->fields[0].primitive, PrimitiveType::kInt32);
  EXPECT_EQ(time_schema->fields[1].name, "nanosec");
  EXPECT_EQ(time_schema->fields[1].primitive, PrimitiveType::kUint32);
}

TEST(MsgParser, ToArrowSchema) {
  std::string text = "float64 x\nfloat64 y\nstring name\n";
  auto schema = parseMsgDef("test/Simple", text);
  ASSERT_NE(schema, nullptr);
  auto arrow_schema = msgSchemaToArrow(*schema);
  ASSERT_NE(arrow_schema, nullptr);
  ASSERT_EQ(arrow_schema->num_fields(), 4);  // timestamp_ns + 3 fields
  EXPECT_EQ(arrow_schema->field(0)->name(), "timestamp_ns");
  EXPECT_EQ(arrow_schema->field(0)->type()->id(), arrow::Type::INT64);
  EXPECT_EQ(arrow_schema->field(1)->name(), "x");
  EXPECT_EQ(arrow_schema->field(1)->type()->id(), arrow::Type::DOUBLE);
  EXPECT_EQ(arrow_schema->field(2)->name(), "y");
  EXPECT_EQ(arrow_schema->field(2)->type()->id(), arrow::Type::DOUBLE);
  EXPECT_EQ(arrow_schema->field(3)->name(), "name");
  EXPECT_EQ(arrow_schema->field(3)->type()->id(), arrow::Type::STRING);
}

TEST(MsgParser, ToArrowNestedStruct) {
  std::string text =
      "geometry_msgs/Vector3 pos\n"
      "================================================================================\n"
      "MSG: geometry_msgs/Vector3\n"
      "float64 x\nfloat64 y\nfloat64 z\n";
  auto schema = parseMsgDef("test/Nested", text);
  ASSERT_NE(schema, nullptr);
  auto arrow_schema = msgSchemaToArrow(*schema);
  ASSERT_NE(arrow_schema, nullptr);
  // timestamp_ns + pos (struct)
  ASSERT_EQ(arrow_schema->num_fields(), 2);
  EXPECT_EQ(arrow_schema->field(0)->name(), "timestamp_ns");
  EXPECT_EQ(arrow_schema->field(1)->name(), "pos");
  EXPECT_EQ(arrow_schema->field(1)->type()->id(), arrow::Type::STRUCT);

  // Verify struct children
  auto struct_type =
      std::static_pointer_cast<arrow::StructType>(arrow_schema->field(1)->type());
  ASSERT_EQ(struct_type->num_fields(), 3);
  EXPECT_EQ(struct_type->field(0)->name(), "x");
  EXPECT_EQ(struct_type->field(0)->type()->id(), arrow::Type::DOUBLE);
  EXPECT_EQ(struct_type->field(1)->name(), "y");
  EXPECT_EQ(struct_type->field(2)->name(), "z");
}

TEST(MsgParser, RealImuSchema) {
  // The actual schema from nissan_zala_50_zeg_4_0.mcap
  std::string text = R"(std_msgs/Header header
geometry_msgs/Quaternion orientation
float64[9] orientation_covariance
geometry_msgs/Vector3 angular_velocity
float64[9] angular_velocity_covariance
geometry_msgs/Vector3 linear_acceleration
float64[9] linear_acceleration_covariance
================================================================================
MSG: geometry_msgs/Quaternion
float64 x 0
float64 y 0
float64 z 0
float64 w 1
================================================================================
MSG: geometry_msgs/Vector3
float64 x
float64 y
float64 z
================================================================================
MSG: std_msgs/Header
builtin_interfaces/Time stamp
string frame_id
================================================================================
MSG: builtin_interfaces/Time
int32 sec
uint32 nanosec)";

  auto schema = parseMsgDef("sensor_msgs/msg/Imu", text);
  ASSERT_NE(schema, nullptr);
  EXPECT_EQ(schema->full_name, "sensor_msgs/msg/Imu");
  ASSERT_EQ(schema->fields.size(), 7u);

  // Verify field names and types
  EXPECT_EQ(schema->fields[0].name, "header");
  EXPECT_EQ(schema->fields[0].nested_type, "std_msgs/Header");

  EXPECT_EQ(schema->fields[1].name, "orientation");
  EXPECT_EQ(schema->fields[1].nested_type, "geometry_msgs/Quaternion");

  EXPECT_EQ(schema->fields[2].name, "orientation_covariance");
  EXPECT_EQ(schema->fields[2].primitive, PrimitiveType::kFloat64);
  EXPECT_EQ(schema->fields[2].array_size, 9);

  EXPECT_EQ(schema->fields[3].name, "angular_velocity");
  EXPECT_EQ(schema->fields[3].nested_type, "geometry_msgs/Vector3");

  // Verify nested resolution
  EXPECT_NE(schema->nested.find("std_msgs/Header"), schema->nested.end());
  EXPECT_NE(schema->nested.find("geometry_msgs/Quaternion"),
            schema->nested.end());
  EXPECT_NE(schema->nested.find("geometry_msgs/Vector3"), schema->nested.end());

  // Verify Header has nested Time
  auto& header = schema->nested["std_msgs/Header"];
  ASSERT_EQ(header->fields.size(), 2u);
  EXPECT_EQ(header->fields[0].nested_type, "builtin_interfaces/Time");
  EXPECT_NE(header->nested.find("builtin_interfaces/Time"),
            header->nested.end());

  // Verify Quaternion default values
  auto& quat = schema->nested["geometry_msgs/Quaternion"];
  ASSERT_EQ(quat->fields.size(), 4u);
  EXPECT_EQ(quat->fields[3].name, "w");
  EXPECT_EQ(quat->fields[3].default_value, "1");

  // Convert to Arrow and verify
  auto arrow_schema = msgSchemaToArrow(*schema);
  ASSERT_NE(arrow_schema, nullptr);
  // timestamp_ns + 7 fields = 8
  ASSERT_EQ(arrow_schema->num_fields(), 8);

  EXPECT_EQ(arrow_schema->field(0)->name(), "timestamp_ns");
  EXPECT_EQ(arrow_schema->field(0)->type()->id(), arrow::Type::INT64);

  // header -> struct
  EXPECT_EQ(arrow_schema->field(1)->name(), "header");
  EXPECT_EQ(arrow_schema->field(1)->type()->id(), arrow::Type::STRUCT);
  auto header_type =
      std::static_pointer_cast<arrow::StructType>(arrow_schema->field(1)->type());
  ASSERT_EQ(header_type->num_fields(), 2);
  EXPECT_EQ(header_type->field(0)->name(), "stamp");
  EXPECT_EQ(header_type->field(0)->type()->id(), arrow::Type::STRUCT);
  EXPECT_EQ(header_type->field(1)->name(), "frame_id");
  EXPECT_EQ(header_type->field(1)->type()->id(), arrow::Type::STRING);

  // stamp -> struct(sec: int32, nanosec: uint32)
  auto stamp_type =
      std::static_pointer_cast<arrow::StructType>(header_type->field(0)->type());
  ASSERT_EQ(stamp_type->num_fields(), 2);
  EXPECT_EQ(stamp_type->field(0)->name(), "sec");
  EXPECT_EQ(stamp_type->field(0)->type()->id(), arrow::Type::INT32);
  EXPECT_EQ(stamp_type->field(1)->name(), "nanosec");
  EXPECT_EQ(stamp_type->field(1)->type()->id(), arrow::Type::UINT32);

  // orientation -> struct(x, y, z, w)
  EXPECT_EQ(arrow_schema->field(2)->name(), "orientation");
  EXPECT_EQ(arrow_schema->field(2)->type()->id(), arrow::Type::STRUCT);
  auto orient_type =
      std::static_pointer_cast<arrow::StructType>(arrow_schema->field(2)->type());
  ASSERT_EQ(orient_type->num_fields(), 4);

  // orientation_covariance -> fixed_size_list(float64, 9)
  EXPECT_EQ(arrow_schema->field(3)->name(), "orientation_covariance");
  EXPECT_EQ(arrow_schema->field(3)->type()->id(), arrow::Type::FIXED_SIZE_LIST);
  auto fsl_type = std::static_pointer_cast<arrow::FixedSizeListType>(
      arrow_schema->field(3)->type());
  EXPECT_EQ(fsl_type->list_size(), 9);
  EXPECT_EQ(fsl_type->value_type()->id(), arrow::Type::DOUBLE);

  // angular_velocity -> struct
  EXPECT_EQ(arrow_schema->field(4)->name(), "angular_velocity");
  EXPECT_EQ(arrow_schema->field(4)->type()->id(), arrow::Type::STRUCT);

  // angular_velocity_covariance -> fixed_size_list(float64, 9)
  EXPECT_EQ(arrow_schema->field(5)->name(), "angular_velocity_covariance");
  EXPECT_EQ(arrow_schema->field(5)->type()->id(), arrow::Type::FIXED_SIZE_LIST);

  // linear_acceleration -> struct
  EXPECT_EQ(arrow_schema->field(6)->name(), "linear_acceleration");
  EXPECT_EQ(arrow_schema->field(6)->type()->id(), arrow::Type::STRUCT);

  // linear_acceleration_covariance -> fixed_size_list(float64, 9)
  EXPECT_EQ(arrow_schema->field(7)->name(), "linear_acceleration_covariance");
  EXPECT_EQ(arrow_schema->field(7)->type()->id(), arrow::Type::FIXED_SIZE_LIST);
}

TEST(MsgParser, ByteAndCharAliases) {
  auto schema = parseMsgDef("test/Aliases", "byte b\nchar c\n");
  ASSERT_NE(schema, nullptr);
  ASSERT_EQ(schema->fields.size(), 2u);
  EXPECT_EQ(schema->fields[0].primitive, PrimitiveType::kUint8);
  EXPECT_EQ(schema->fields[1].primitive, PrimitiveType::kUint8);
}

TEST(MsgParser, EmptyInput) {
  auto schema = parseMsgDef("test/Empty", "");
  ASSERT_NE(schema, nullptr);
  EXPECT_EQ(schema->fields.size(), 0u);
}

TEST(MsgParser, CommentsOnly) {
  auto schema = parseMsgDef("test/CommentsOnly",
      "# Just comments\n# Nothing else\n");
  ASSERT_NE(schema, nullptr);
  EXPECT_EQ(schema->fields.size(), 0u);
}

TEST(MsgParser, FixedArrayArrow) {
  auto schema = parseMsgDef("test/FArr", "float64[3] vec\n");
  ASSERT_NE(schema, nullptr);
  auto arrow_schema = msgSchemaToArrow(*schema);
  ASSERT_NE(arrow_schema, nullptr);
  // timestamp_ns + vec
  ASSERT_EQ(arrow_schema->num_fields(), 2);
  EXPECT_EQ(arrow_schema->field(1)->type()->id(), arrow::Type::FIXED_SIZE_LIST);
  auto fsl = std::static_pointer_cast<arrow::FixedSizeListType>(
      arrow_schema->field(1)->type());
  EXPECT_EQ(fsl->list_size(), 3);
  EXPECT_EQ(fsl->value_type()->id(), arrow::Type::DOUBLE);
}

TEST(MsgParser, DynamicArrayArrow) {
  auto schema = parseMsgDef("test/DArr", "uint8[] data\n");
  ASSERT_NE(schema, nullptr);
  auto arrow_schema = msgSchemaToArrow(*schema);
  ASSERT_NE(arrow_schema, nullptr);
  ASSERT_EQ(arrow_schema->num_fields(), 2);
  EXPECT_EQ(arrow_schema->field(1)->type()->id(), arrow::Type::LIST);
  auto list_type =
      std::static_pointer_cast<arrow::ListType>(arrow_schema->field(1)->type());
  EXPECT_EQ(list_type->value_type()->id(), arrow::Type::UINT8);
}
