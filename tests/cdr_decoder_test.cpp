// tests/tools/cdr_decoder_test.cpp
#include "decoders/cdr_decoder.hpp"
#include "ontology/field_map.hpp"

#include <fastcdr/Cdr.h>
#include <fastcdr/FastBuffer.h>

#include <gtest/gtest.h>

#include <cstddef>
#include <cstring>
#include <vector>

#define ASSERT_OK(expr)                                    \
  do {                                                     \
    auto _s = (expr);                                      \
    ASSERT_TRUE(_s.ok()) << _s.ToString();                 \
  } while (0)

namespace {

// Helper: build a CDR v1 little-endian payload by serializing values.
// Returns a byte vector including the 4-byte encapsulation header.
class CdrPayloadBuilder {
 public:
  CdrPayloadBuilder()
      : buffer_(buf_, sizeof(buf_)),
        cdr_(buffer_, eprosima::fastcdr::Cdr::LITTLE_ENDIANNESS,
             eprosima::fastcdr::CdrVersion::XCDRv1) {}

  template <typename T>
  CdrPayloadBuilder& write(const T& val) {
    cdr_ << val;
    return *this;
  }

  // Returns vector with 4-byte encapsulation header prepended.
  std::vector<std::byte> finish() {
    std::vector<std::byte> result;
    // CDR v1 little-endian encapsulation header
    result.push_back(std::byte{0x00});
    result.push_back(std::byte{0x01});
    result.push_back(std::byte{0x00});
    result.push_back(std::byte{0x00});

    auto serialized_size = cdr_.get_serialized_data_length();
    auto* base = reinterpret_cast<const std::byte*>(buffer_.getBuffer());
    result.insert(result.end(), base, base + serialized_size);
    return result;
  }

 private:
  char buf_[4096]{};
  eprosima::fastcdr::FastBuffer buffer_;
  eprosima::fastcdr::Cdr cdr_;
};

DecoderContext makeContext(const std::string& schema_name,
                          const std::string& msg_def) {
  DecoderContext ctx;
  ctx.schema_name = schema_name;
  ctx.schema_encoding = "ros2msg";
  ctx.schema_data.resize(msg_def.size());
  std::memcpy(ctx.schema_data.data(), msg_def.data(), msg_def.size());
  return ctx;
}

}  // namespace

// ---------------------------------------------------------------------------
// Test 1: Simple primitives (float64 x, float64 y)
// ---------------------------------------------------------------------------
TEST(CdrDecoder, SimplePrimitives) {
  CdrDecoder dec;
  auto ctx = makeContext("test/Point", "float64 x\nfloat64 y\n");
  ASSERT_OK(dec.prepare(ctx));

  auto payload = CdrPayloadBuilder().write(1.5).write(2.5).finish();

  FieldMap fm;
  ASSERT_OK(dec.decode(payload.data(),
                       static_cast<uint64_t>(payload.size()), ctx, fm));

  ASSERT_EQ(fm.fields.count("x"), 1u);
  ASSERT_EQ(fm.fields.count("y"), 1u);
  EXPECT_DOUBLE_EQ(std::get<double>(fm.fields.at("x")), 1.5);
  EXPECT_DOUBLE_EQ(std::get<double>(fm.fields.at("y")), 2.5);
}

// ---------------------------------------------------------------------------
// Test 2: Nested struct (Vector3 with x, y, z)
// ---------------------------------------------------------------------------
TEST(CdrDecoder, NestedStruct) {
  CdrDecoder dec;
  std::string msg_def =
      "geometry_msgs/Vector3 position\n"
      "================================================================================\n"
      "MSG: geometry_msgs/Vector3\n"
      "float64 x\nfloat64 y\nfloat64 z\n";

  auto ctx = makeContext("test/Pose", msg_def);
  ASSERT_OK(dec.prepare(ctx));

  auto payload =
      CdrPayloadBuilder().write(1.0).write(2.0).write(3.0).finish();

  FieldMap fm;
  ASSERT_OK(dec.decode(payload.data(),
                       static_cast<uint64_t>(payload.size()), ctx, fm));

  // Nested struct flattened with dot-path keys
  ASSERT_EQ(fm.fields.count("position.x"), 1u);
  ASSERT_EQ(fm.fields.count("position.y"), 1u);
  ASSERT_EQ(fm.fields.count("position.z"), 1u);
  EXPECT_DOUBLE_EQ(std::get<double>(fm.fields.at("position.x")), 1.0);
  EXPECT_DOUBLE_EQ(std::get<double>(fm.fields.at("position.y")), 2.0);
  EXPECT_DOUBLE_EQ(std::get<double>(fm.fields.at("position.z")), 3.0);
}

// ---------------------------------------------------------------------------
// Test 3: Fixed array (float64[3])
// ---------------------------------------------------------------------------
TEST(CdrDecoder, FixedArray) {
  CdrDecoder dec;
  auto ctx = makeContext("test/Cov", "float64[3] values\n");
  ASSERT_OK(dec.prepare(ctx));

  auto payload =
      CdrPayloadBuilder().write(10.0).write(20.0).write(30.0).finish();

  FieldMap fm;
  ASSERT_OK(dec.decode(payload.data(),
                       static_cast<uint64_t>(payload.size()), ctx, fm));

  ASSERT_EQ(fm.fields.count("values"), 1u);
  const auto& vals = std::get<std::vector<double>>(fm.fields.at("values"));
  ASSERT_EQ(vals.size(), 3u);
  EXPECT_DOUBLE_EQ(vals[0], 10.0);
  EXPECT_DOUBLE_EQ(vals[1], 20.0);
  EXPECT_DOUBLE_EQ(vals[2], 30.0);
}

// ---------------------------------------------------------------------------
// Test 4: String field
// ---------------------------------------------------------------------------
TEST(CdrDecoder, StringField) {
  CdrDecoder dec;
  auto ctx = makeContext("test/Msg", "string data\nfloat64 value\n");
  ASSERT_OK(dec.prepare(ctx));

  auto payload =
      CdrPayloadBuilder()
          .write(std::string("hello"))
          .write(42.0)
          .finish();

  FieldMap fm;
  ASSERT_OK(dec.decode(payload.data(),
                       static_cast<uint64_t>(payload.size()), ctx, fm));

  ASSERT_EQ(fm.fields.count("data"), 1u);
  ASSERT_EQ(fm.fields.count("value"), 1u);
  EXPECT_EQ(std::get<std::string>(fm.fields.at("data")), "hello");
  EXPECT_DOUBLE_EQ(std::get<double>(fm.fields.at("value")), 42.0);
}

// ---------------------------------------------------------------------------
// Test 5: Dynamic array (uint8[] data)
// ---------------------------------------------------------------------------
TEST(CdrDecoder, DynamicArray) {
  CdrDecoder dec;
  auto ctx = makeContext("test/Bytes", "uint8[] data\n");
  ASSERT_OK(dec.prepare(ctx));

  // CDR: uint32 length=3, then 3 uint8 values
  auto payload = CdrPayloadBuilder()
                     .write(static_cast<uint32_t>(3))
                     .write(static_cast<uint8_t>(0xAA))
                     .write(static_cast<uint8_t>(0xBB))
                     .write(static_cast<uint8_t>(0xCC))
                     .finish();

  FieldMap fm;
  ASSERT_OK(dec.decode(payload.data(),
                       static_cast<uint64_t>(payload.size()), ctx, fm));

  ASSERT_EQ(fm.fields.count("data"), 1u);
  const auto& vals = std::get<std::vector<uint8_t>>(fm.fields.at("data"));
  ASSERT_EQ(vals.size(), 3u);
  EXPECT_EQ(vals[0], 0xAA);
  EXPECT_EQ(vals[1], 0xBB);
  EXPECT_EQ(vals[2], 0xCC);
}

// ---------------------------------------------------------------------------
// Test 6: Integer types (bool, int32, uint16)
// ---------------------------------------------------------------------------
TEST(CdrDecoder, IntegerTypes) {
  CdrDecoder dec;
  auto ctx =
      makeContext("test/Mixed", "bool flag\nint32 count\nuint16 id\n");
  ASSERT_OK(dec.prepare(ctx));

  auto payload = CdrPayloadBuilder()
                     .write(true)
                     .write(static_cast<int32_t>(-42))
                     .write(static_cast<uint16_t>(1000))
                     .finish();

  FieldMap fm;
  ASSERT_OK(dec.decode(payload.data(),
                       static_cast<uint64_t>(payload.size()), ctx, fm));

  ASSERT_EQ(fm.fields.count("flag"), 1u);
  ASSERT_EQ(fm.fields.count("count"), 1u);
  ASSERT_EQ(fm.fields.count("id"), 1u);
  EXPECT_TRUE(std::get<bool>(fm.fields.at("flag")));
  EXPECT_EQ(std::get<int32_t>(fm.fields.at("count")), -42);
  EXPECT_EQ(std::get<uint16_t>(fm.fields.at("id")), 1000);
}

// ---------------------------------------------------------------------------
// Test 7: Real IMU .msg schema
// ---------------------------------------------------------------------------
TEST(CdrDecoder, RealImuSchema) {
  CdrDecoder dec;
  std::string msg_def = R"(std_msgs/Header header
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

  auto ctx = makeContext("sensor_msgs/msg/Imu", msg_def);
  ASSERT_OK(dec.prepare(ctx));

  // Build CDR payload matching the IMU message layout
  CdrPayloadBuilder builder;
  // header.stamp
  builder.write(static_cast<int32_t>(1234));
  builder.write(static_cast<uint32_t>(5678));
  // header.frame_id
  builder.write(std::string("imu_link"));
  // orientation
  builder.write(0.0).write(0.0).write(0.0).write(1.0);
  // orientation_covariance[9]
  for (int i = 0; i < 9; ++i) builder.write(0.1);
  // angular_velocity
  builder.write(0.01).write(0.02).write(0.03);
  // angular_velocity_covariance[9]
  for (int i = 0; i < 9; ++i) builder.write(0.2);
  // linear_acceleration
  builder.write(0.1).write(0.2).write(9.8);
  // linear_acceleration_covariance[9]
  for (int i = 0; i < 9; ++i) builder.write(0.3);

  auto payload = builder.finish();

  FieldMap fm;
  ASSERT_OK(dec.decode(payload.data(),
                       static_cast<uint64_t>(payload.size()), ctx, fm));

  // Verify header fields (nested struct flattened with dot-paths)
  EXPECT_EQ(std::get<int32_t>(fm.fields.at("header.stamp.sec")), 1234);
  EXPECT_EQ(std::get<uint32_t>(fm.fields.at("header.stamp.nanosec")), 5678u);
  EXPECT_EQ(std::get<std::string>(fm.fields.at("header.frame_id")), "imu_link");

  // Verify orientation (nested struct flattened)
  EXPECT_DOUBLE_EQ(std::get<double>(fm.fields.at("orientation.x")), 0.0);
  EXPECT_DOUBLE_EQ(std::get<double>(fm.fields.at("orientation.w")), 1.0);

  // Verify orientation_covariance (fixed array -> vector<double>)
  const auto& orient_cov =
      std::get<std::vector<double>>(fm.fields.at("orientation_covariance"));
  ASSERT_EQ(orient_cov.size(), 9u);
  EXPECT_DOUBLE_EQ(orient_cov[0], 0.1);

  // Verify angular_velocity
  EXPECT_DOUBLE_EQ(std::get<double>(fm.fields.at("angular_velocity.x")), 0.01);
  EXPECT_DOUBLE_EQ(std::get<double>(fm.fields.at("angular_velocity.y")), 0.02);
  EXPECT_DOUBLE_EQ(std::get<double>(fm.fields.at("angular_velocity.z")), 0.03);

  // Verify linear_acceleration
  EXPECT_DOUBLE_EQ(
      std::get<double>(fm.fields.at("linear_acceleration.x")), 0.1);
  EXPECT_DOUBLE_EQ(
      std::get<double>(fm.fields.at("linear_acceleration.z")), 9.8);

  // Verify linear_acceleration_covariance
  const auto& lin_cov =
      std::get<std::vector<double>>(fm.fields.at("linear_acceleration_covariance"));
  ASSERT_EQ(lin_cov.size(), 9u);
  EXPECT_DOUBLE_EQ(lin_cov[0], 0.3);
}

// ---------------------------------------------------------------------------
// Test 8: Too-short message
// ---------------------------------------------------------------------------
TEST(CdrDecoder, TooShortMessage) {
  CdrDecoder dec;
  auto ctx = makeContext("test/X", "float64 x\n");
  ASSERT_OK(dec.prepare(ctx));

  // Only 2 bytes -- less than 4-byte encapsulation header
  std::byte tiny[] = {std::byte{0x00}, std::byte{0x01}};
  FieldMap fm;
  auto status = dec.decode(tiny, 2, ctx, fm);
  EXPECT_FALSE(status.ok());
}

// ---------------------------------------------------------------------------
// Test 9: Decode without prepare returns error
// ---------------------------------------------------------------------------
TEST(CdrDecoder, NoPrepare) {
  CdrDecoder dec;
  DecoderContext ctx;
  auto payload = CdrPayloadBuilder().write(1.0).finish();
  FieldMap fm;
  auto status = dec.decode(payload.data(),
                           static_cast<uint64_t>(payload.size()), ctx, fm);
  EXPECT_FALSE(status.ok());
}
