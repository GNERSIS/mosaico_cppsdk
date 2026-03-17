// tests/tools/ros1_decoder_test.cpp
#include "decoders/ros1_decoder.hpp"
#include "ontology/field_map.hpp"

#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

#define ASSERT_OK(expr)                                    \
  do {                                                     \
    auto _s = (expr);                                      \
    ASSERT_TRUE(_s.ok()) << _s.ToString();                 \
  } while (0)

namespace {

// ---------------------------------------------------------------------------
// Helper: pack values sequentially into a byte buffer (little-endian, no
// alignment padding) -- mimicking ROS1 serialization.
// ---------------------------------------------------------------------------
class Ros1PayloadBuilder {
 public:
  template <typename T>
  Ros1PayloadBuilder& write(const T& val) {
    const auto* p = reinterpret_cast<const std::byte*>(&val);
    buf_.insert(buf_.end(), p, p + sizeof(T));
    return *this;
  }

  // ROS1 string: uint32 length + UTF-8 bytes (no null terminator)
  Ros1PayloadBuilder& writeString(const std::string& s) {
    uint32_t len = static_cast<uint32_t>(s.size());
    write(len);
    const auto* p = reinterpret_cast<const std::byte*>(s.data());
    buf_.insert(buf_.end(), p, p + s.size());
    return *this;
  }

  const std::vector<std::byte>& finish() const { return buf_; }

 private:
  std::vector<std::byte> buf_;
};

DecoderContext makeContext(const std::string& schema_name,
                          const std::string& msg_def) {
  DecoderContext ctx;
  ctx.schema_name = schema_name;
  ctx.schema_encoding = "ros1msg";
  ctx.schema_data.resize(msg_def.size());
  std::memcpy(ctx.schema_data.data(), msg_def.data(), msg_def.size());
  return ctx;
}

}  // namespace

// ---------------------------------------------------------------------------
// Test 1: Simple primitives (float64 x, float64 y)
// ---------------------------------------------------------------------------
TEST(Ros1Decoder, SimplePrimitives) {
  Ros1Decoder dec;
  auto ctx = makeContext("test/Point", "float64 x\nfloat64 y\n");
  ASSERT_OK(dec.prepare(ctx));

  auto payload = Ros1PayloadBuilder().write(1.5).write(2.5).finish();

  FieldMap fm;
  ASSERT_OK(dec.decode(payload.data(),
                       static_cast<uint64_t>(payload.size()), ctx, fm));

  ASSERT_EQ(fm.fields.count("x"), 1u);
  ASSERT_EQ(fm.fields.count("y"), 1u);
  EXPECT_DOUBLE_EQ(std::get<double>(fm.fields.at("x")), 1.5);
  EXPECT_DOUBLE_EQ(std::get<double>(fm.fields.at("y")), 2.5);
}

// ---------------------------------------------------------------------------
// Test 2: String field (uint32 length + bytes, no null terminator)
// ---------------------------------------------------------------------------
TEST(Ros1Decoder, StringField) {
  Ros1Decoder dec;
  auto ctx = makeContext("test/Msg", "string data\nfloat64 value\n");
  ASSERT_OK(dec.prepare(ctx));

  auto payload =
      Ros1PayloadBuilder().writeString("hello").write(42.0).finish();

  FieldMap fm;
  ASSERT_OK(dec.decode(payload.data(),
                       static_cast<uint64_t>(payload.size()), ctx, fm));

  EXPECT_EQ(std::get<std::string>(fm.fields.at("data")), "hello");
  EXPECT_DOUBLE_EQ(std::get<double>(fm.fields.at("value")), 42.0);
}

// ---------------------------------------------------------------------------
// Test 3: String with no null terminator -- length bytes only
// ---------------------------------------------------------------------------
TEST(Ros1Decoder, StringNoNullTerminator) {
  Ros1Decoder dec;
  auto ctx = makeContext("test/Label", "string label\n");
  ASSERT_OK(dec.prepare(ctx));

  // Manually craft: length=3, then "abc" -- no null byte appended
  std::vector<std::byte> payload;
  uint32_t len = 3;
  const auto* lp = reinterpret_cast<const std::byte*>(&len);
  payload.insert(payload.end(), lp, lp + 4);
  payload.push_back(std::byte{'a'});
  payload.push_back(std::byte{'b'});
  payload.push_back(std::byte{'c'});

  FieldMap fm;
  ASSERT_OK(dec.decode(payload.data(),
                       static_cast<uint64_t>(payload.size()), ctx, fm));

  EXPECT_EQ(std::get<std::string>(fm.fields.at("label")), "abc");
}

// ---------------------------------------------------------------------------
// Test 4: Nested struct (Vector3 with x, y, z)
// ---------------------------------------------------------------------------
TEST(Ros1Decoder, NestedStruct) {
  Ros1Decoder dec;
  std::string msg_def =
      "geometry_msgs/Vector3 position\n"
      "================================================================================\n"
      "MSG: geometry_msgs/Vector3\n"
      "float64 x\nfloat64 y\nfloat64 z\n";

  auto ctx = makeContext("test/Pose", msg_def);
  ASSERT_OK(dec.prepare(ctx));

  auto payload =
      Ros1PayloadBuilder().write(1.0).write(2.0).write(3.0).finish();

  FieldMap fm;
  ASSERT_OK(dec.decode(payload.data(),
                       static_cast<uint64_t>(payload.size()), ctx, fm));

  EXPECT_DOUBLE_EQ(std::get<double>(fm.fields.at("position.x")), 1.0);
  EXPECT_DOUBLE_EQ(std::get<double>(fm.fields.at("position.y")), 2.0);
  EXPECT_DOUBLE_EQ(std::get<double>(fm.fields.at("position.z")), 3.0);
}

// ---------------------------------------------------------------------------
// Test 5: Fixed array (float64[3]) -- no length prefix
// ---------------------------------------------------------------------------
TEST(Ros1Decoder, FixedArray) {
  Ros1Decoder dec;
  auto ctx = makeContext("test/Cov", "float64[3] values\n");
  ASSERT_OK(dec.prepare(ctx));

  auto payload =
      Ros1PayloadBuilder().write(10.0).write(20.0).write(30.0).finish();

  FieldMap fm;
  ASSERT_OK(dec.decode(payload.data(),
                       static_cast<uint64_t>(payload.size()), ctx, fm));

  const auto& vals = std::get<std::vector<double>>(fm.fields.at("values"));
  ASSERT_EQ(vals.size(), 3u);
  EXPECT_DOUBLE_EQ(vals[0], 10.0);
  EXPECT_DOUBLE_EQ(vals[1], 20.0);
  EXPECT_DOUBLE_EQ(vals[2], 30.0);
}

// ---------------------------------------------------------------------------
// Test 6: Dynamic array (uint8[] data)
// ---------------------------------------------------------------------------
TEST(Ros1Decoder, DynamicArray) {
  Ros1Decoder dec;
  auto ctx = makeContext("test/Bytes", "uint8[] data\n");
  ASSERT_OK(dec.prepare(ctx));

  auto payload = Ros1PayloadBuilder()
                     .write(static_cast<uint32_t>(3))
                     .write(static_cast<uint8_t>(0xAA))
                     .write(static_cast<uint8_t>(0xBB))
                     .write(static_cast<uint8_t>(0xCC))
                     .finish();

  FieldMap fm;
  ASSERT_OK(dec.decode(payload.data(),
                       static_cast<uint64_t>(payload.size()), ctx, fm));

  const auto& vals = std::get<std::vector<uint8_t>>(fm.fields.at("data"));
  ASSERT_EQ(vals.size(), 3u);
  EXPECT_EQ(vals[0], 0xAA);
  EXPECT_EQ(vals[1], 0xBB);
  EXPECT_EQ(vals[2], 0xCC);
}

// ---------------------------------------------------------------------------
// Test 7: Integer types (bool, int32, uint16) -- packed with no padding
// ---------------------------------------------------------------------------
TEST(Ros1Decoder, IntegerTypes) {
  Ros1Decoder dec;
  auto ctx =
      makeContext("test/Mixed", "bool flag\nint32 count\nuint16 id\n");
  ASSERT_OK(dec.prepare(ctx));

  auto payload = Ros1PayloadBuilder()
                     .write(static_cast<bool>(true))
                     .write(static_cast<int32_t>(-42))
                     .write(static_cast<uint16_t>(1000))
                     .finish();
  ASSERT_EQ(payload.size(), 7u);  // no padding between fields

  FieldMap fm;
  ASSERT_OK(dec.decode(payload.data(),
                       static_cast<uint64_t>(payload.size()), ctx, fm));

  EXPECT_TRUE(std::get<bool>(fm.fields.at("flag")));
  EXPECT_EQ(std::get<int32_t>(fm.fields.at("count")), -42);
  EXPECT_EQ(std::get<uint16_t>(fm.fields.at("id")), 1000);
}

// ---------------------------------------------------------------------------
// Test 8: No schema -- decode returns error
// ---------------------------------------------------------------------------
TEST(Ros1Decoder, NoSchema) {
  Ros1Decoder dec;
  // Do NOT call prepare first
  std::byte tiny[] = {std::byte{0x00}};
  DecoderContext ctx;
  FieldMap fm;
  auto status = dec.decode(tiny, 1, ctx, fm);
  EXPECT_FALSE(status.ok());
}

// ---------------------------------------------------------------------------
// Test 9: Mixed field with string between primitives
// ---------------------------------------------------------------------------
TEST(Ros1Decoder, StringBetweenPrimitives) {
  Ros1Decoder dec;
  auto ctx = makeContext("test/Tagged",
                         "int32 seq\nstring label\nfloat32 value\n");
  ASSERT_OK(dec.prepare(ctx));

  auto payload = Ros1PayloadBuilder()
                     .write(static_cast<int32_t>(7))
                     .writeString("sensor_A")
                     .write(static_cast<float>(3.14f))
                     .finish();

  FieldMap fm;
  ASSERT_OK(dec.decode(payload.data(),
                       static_cast<uint64_t>(payload.size()), ctx, fm));

  EXPECT_EQ(std::get<int32_t>(fm.fields.at("seq")), 7);
  EXPECT_EQ(std::get<std::string>(fm.fields.at("label")), "sensor_A");
  EXPECT_FLOAT_EQ(std::get<float>(fm.fields.at("value")), 3.14f);
}
