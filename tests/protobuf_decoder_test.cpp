// tests/tools/protobuf_decoder_test.cpp
#include "decoders/protobuf_decoder.hpp"
#include "ontology/field_map.hpp"

#include <google/protobuf/descriptor.h>
#include <google/protobuf/descriptor.pb.h>
#include <google/protobuf/dynamic_message.h>

#include <gtest/gtest.h>

#include <cstddef>
#include <cstring>
#include <string>
#include <vector>

#define ASSERT_OK(expr)                    \
  do {                                     \
    auto _s = (expr);                      \
    ASSERT_TRUE(_s.ok()) << _s.ToString(); \
  } while (0)

namespace gpb = google::protobuf;

namespace {

std::vector<std::byte> buildFileDescriptorSet(
    const gpb::FileDescriptorProto& file_proto) {
  gpb::FileDescriptorSet fds;
  *fds.add_file() = file_proto;

  std::string serialized;
  fds.SerializeToString(&serialized);

  std::vector<std::byte> result(serialized.size());
  std::memcpy(result.data(), serialized.data(), serialized.size());
  return result;
}

std::vector<std::byte> serializeMessage(const gpb::Message& msg) {
  std::string serialized;
  msg.SerializeToString(&serialized);
  std::vector<std::byte> result(serialized.size());
  std::memcpy(result.data(), serialized.data(), serialized.size());
  return result;
}

DecoderContext makeContext(const std::string& schema_name,
                          const std::vector<std::byte>& fds_bytes) {
  DecoderContext ctx;
  ctx.schema_name = schema_name;
  ctx.schema_encoding = "protobuf";
  ctx.schema_data = fds_bytes;
  return ctx;
}

}  // namespace

// ---------------------------------------------------------------------------
// Test 1: Simple message with double, int32, string fields
// ---------------------------------------------------------------------------
TEST(ProtobufDecoder, SimpleMessage) {
  gpb::FileDescriptorProto file;
  file.set_name("test.proto");
  file.set_package("test");
  file.set_syntax("proto3");

  auto* msg = file.add_message_type();
  msg->set_name("SimpleMsg");

  auto* f1 = msg->add_field();
  f1->set_name("value");
  f1->set_number(1);
  f1->set_type(gpb::FieldDescriptorProto::TYPE_DOUBLE);
  f1->set_label(gpb::FieldDescriptorProto::LABEL_OPTIONAL);

  auto* f2 = msg->add_field();
  f2->set_name("count");
  f2->set_number(2);
  f2->set_type(gpb::FieldDescriptorProto::TYPE_INT32);
  f2->set_label(gpb::FieldDescriptorProto::LABEL_OPTIONAL);

  auto* f3 = msg->add_field();
  f3->set_name("label");
  f3->set_number(3);
  f3->set_type(gpb::FieldDescriptorProto::TYPE_STRING);
  f3->set_label(gpb::FieldDescriptorProto::LABEL_OPTIONAL);

  auto fds_bytes = buildFileDescriptorSet(file);

  // Build a dynamic message to serialize
  gpb::DescriptorPool pool;
  pool.BuildFile(file);
  const auto* desc = pool.FindMessageTypeByName("test.SimpleMsg");
  ASSERT_NE(desc, nullptr);

  gpb::DynamicMessageFactory factory;
  std::unique_ptr<gpb::Message> proto_msg(factory.GetPrototype(desc)->New());
  const auto* refl = proto_msg->GetReflection();
  refl->SetDouble(proto_msg.get(), desc->FindFieldByName("value"), 3.14);
  refl->SetInt32(proto_msg.get(), desc->FindFieldByName("count"), -42);
  refl->SetString(proto_msg.get(), desc->FindFieldByName("label"), "hello");

  auto payload = serializeMessage(*proto_msg);

  // Test the decoder
  ProtobufDecoder dec;
  auto ctx = makeContext("test.SimpleMsg", fds_bytes);
  ASSERT_OK(dec.prepare(ctx));

  FieldMap fm;
  ASSERT_OK(dec.decode(payload.data(),
                       static_cast<uint64_t>(payload.size()), ctx, fm));

  EXPECT_DOUBLE_EQ(std::get<double>(fm.fields.at("value")), 3.14);
  EXPECT_EQ(std::get<int32_t>(fm.fields.at("count")), -42);
  EXPECT_EQ(std::get<std::string>(fm.fields.at("label")), "hello");
}

// ---------------------------------------------------------------------------
// Test 2: Nested message
// ---------------------------------------------------------------------------
TEST(ProtobufDecoder, NestedMessage) {
  gpb::FileDescriptorProto file;
  file.set_name("test.proto");
  file.set_package("test");
  file.set_syntax("proto3");

  auto* vec_msg = file.add_message_type();
  vec_msg->set_name("Vector3");
  for (int i = 0; i < 3; ++i) {
    auto* f = vec_msg->add_field();
    f->set_name(std::string(1, static_cast<char>('x' + i)));
    f->set_number(i + 1);
    f->set_type(gpb::FieldDescriptorProto::TYPE_DOUBLE);
    f->set_label(gpb::FieldDescriptorProto::LABEL_OPTIONAL);
  }

  auto* pose_msg = file.add_message_type();
  pose_msg->set_name("Pose");

  auto* pos_field = pose_msg->add_field();
  pos_field->set_name("position");
  pos_field->set_number(1);
  pos_field->set_type(gpb::FieldDescriptorProto::TYPE_MESSAGE);
  pos_field->set_type_name(".test.Vector3");
  pos_field->set_label(gpb::FieldDescriptorProto::LABEL_OPTIONAL);

  auto* heading_field = pose_msg->add_field();
  heading_field->set_name("heading");
  heading_field->set_number(2);
  heading_field->set_type(gpb::FieldDescriptorProto::TYPE_DOUBLE);
  heading_field->set_label(gpb::FieldDescriptorProto::LABEL_OPTIONAL);

  auto fds_bytes = buildFileDescriptorSet(file);

  gpb::DescriptorPool pool;
  pool.BuildFile(file);
  const auto* desc = pool.FindMessageTypeByName("test.Pose");
  ASSERT_NE(desc, nullptr);

  gpb::DynamicMessageFactory factory;
  std::unique_ptr<gpb::Message> proto_msg(factory.GetPrototype(desc)->New());
  const auto* refl = proto_msg->GetReflection();

  auto* pos_sub = refl->MutableMessage(
      proto_msg.get(), desc->FindFieldByName("position"));
  const auto* vec_desc = pos_sub->GetDescriptor();
  const auto* vec_refl = pos_sub->GetReflection();
  vec_refl->SetDouble(pos_sub, vec_desc->FindFieldByName("x"), 1.0);
  vec_refl->SetDouble(pos_sub, vec_desc->FindFieldByName("y"), 2.0);
  vec_refl->SetDouble(pos_sub, vec_desc->FindFieldByName("z"), 3.0);

  refl->SetDouble(proto_msg.get(), desc->FindFieldByName("heading"), 45.0);

  auto payload = serializeMessage(*proto_msg);

  // Test
  ProtobufDecoder dec;
  auto ctx = makeContext("test.Pose", fds_bytes);
  ASSERT_OK(dec.prepare(ctx));

  FieldMap fm;
  ASSERT_OK(dec.decode(payload.data(),
                       static_cast<uint64_t>(payload.size()), ctx, fm));

  // Nested message flattened into dot-path keys
  EXPECT_DOUBLE_EQ(std::get<double>(fm.fields.at("position.x")), 1.0);
  EXPECT_DOUBLE_EQ(std::get<double>(fm.fields.at("position.y")), 2.0);
  EXPECT_DOUBLE_EQ(std::get<double>(fm.fields.at("position.z")), 3.0);
  EXPECT_DOUBLE_EQ(std::get<double>(fm.fields.at("heading")), 45.0);
}

// ---------------------------------------------------------------------------
// Test 3: Repeated field
// ---------------------------------------------------------------------------
TEST(ProtobufDecoder, RepeatedField) {
  gpb::FileDescriptorProto file;
  file.set_name("test.proto");
  file.set_package("test");
  file.set_syntax("proto3");

  auto* msg = file.add_message_type();
  msg->set_name("Readings");

  auto* f1 = msg->add_field();
  f1->set_name("values");
  f1->set_number(1);
  f1->set_type(gpb::FieldDescriptorProto::TYPE_DOUBLE);
  f1->set_label(gpb::FieldDescriptorProto::LABEL_REPEATED);

  auto* f2 = msg->add_field();
  f2->set_name("name");
  f2->set_number(2);
  f2->set_type(gpb::FieldDescriptorProto::TYPE_STRING);
  f2->set_label(gpb::FieldDescriptorProto::LABEL_OPTIONAL);

  auto fds_bytes = buildFileDescriptorSet(file);

  gpb::DescriptorPool pool;
  pool.BuildFile(file);
  const auto* desc = pool.FindMessageTypeByName("test.Readings");
  ASSERT_NE(desc, nullptr);

  gpb::DynamicMessageFactory factory;
  std::unique_ptr<gpb::Message> proto_msg(factory.GetPrototype(desc)->New());
  const auto* refl = proto_msg->GetReflection();
  const auto* vals_field = desc->FindFieldByName("values");

  refl->AddDouble(proto_msg.get(), vals_field, 1.1);
  refl->AddDouble(proto_msg.get(), vals_field, 2.2);
  refl->AddDouble(proto_msg.get(), vals_field, 3.3);
  refl->SetString(proto_msg.get(), desc->FindFieldByName("name"), "sensor");

  auto payload = serializeMessage(*proto_msg);

  ProtobufDecoder dec;
  auto ctx = makeContext("test.Readings", fds_bytes);
  ASSERT_OK(dec.prepare(ctx));

  FieldMap fm;
  ASSERT_OK(dec.decode(payload.data(),
                       static_cast<uint64_t>(payload.size()), ctx, fm));

  const auto& vals = std::get<std::vector<double>>(fm.fields.at("values"));
  ASSERT_EQ(vals.size(), 3u);
  EXPECT_DOUBLE_EQ(vals[0], 1.1);
  EXPECT_DOUBLE_EQ(vals[1], 2.2);
  EXPECT_DOUBLE_EQ(vals[2], 3.3);
  EXPECT_EQ(std::get<std::string>(fm.fields.at("name")), "sensor");
}

// ---------------------------------------------------------------------------
// Test 4: All numeric types
// ---------------------------------------------------------------------------
TEST(ProtobufDecoder, AllNumericTypes) {
  gpb::FileDescriptorProto file;
  file.set_name("test.proto");
  file.set_package("test");
  file.set_syntax("proto3");

  auto* msg = file.add_message_type();
  msg->set_name("AllTypes");

  auto add_field = [&](const char* name, int number,
                       gpb::FieldDescriptorProto::Type type) {
    auto* f = msg->add_field();
    f->set_name(name);
    f->set_number(number);
    f->set_type(type);
    f->set_label(gpb::FieldDescriptorProto::LABEL_OPTIONAL);
  };

  add_field("d", 1, gpb::FieldDescriptorProto::TYPE_DOUBLE);
  add_field("f", 2, gpb::FieldDescriptorProto::TYPE_FLOAT);
  add_field("i32", 3, gpb::FieldDescriptorProto::TYPE_INT32);
  add_field("i64", 4, gpb::FieldDescriptorProto::TYPE_INT64);
  add_field("u32", 5, gpb::FieldDescriptorProto::TYPE_UINT32);
  add_field("u64", 6, gpb::FieldDescriptorProto::TYPE_UINT64);
  add_field("b", 7, gpb::FieldDescriptorProto::TYPE_BOOL);

  auto fds_bytes = buildFileDescriptorSet(file);

  gpb::DescriptorPool pool;
  pool.BuildFile(file);
  const auto* desc = pool.FindMessageTypeByName("test.AllTypes");
  ASSERT_NE(desc, nullptr);

  gpb::DynamicMessageFactory factory;
  std::unique_ptr<gpb::Message> proto_msg(factory.GetPrototype(desc)->New());
  const auto* refl = proto_msg->GetReflection();

  refl->SetDouble(proto_msg.get(), desc->FindFieldByName("d"), 1.5);
  refl->SetFloat(proto_msg.get(), desc->FindFieldByName("f"), 2.5f);
  refl->SetInt32(proto_msg.get(), desc->FindFieldByName("i32"), -10);
  refl->SetInt64(proto_msg.get(), desc->FindFieldByName("i64"), -20);
  refl->SetUInt32(proto_msg.get(), desc->FindFieldByName("u32"), 30);
  refl->SetUInt64(proto_msg.get(), desc->FindFieldByName("u64"), 40);
  refl->SetBool(proto_msg.get(), desc->FindFieldByName("b"), true);

  auto payload = serializeMessage(*proto_msg);

  ProtobufDecoder dec;
  auto ctx = makeContext("test.AllTypes", fds_bytes);
  ASSERT_OK(dec.prepare(ctx));

  FieldMap fm;
  ASSERT_OK(dec.decode(payload.data(),
                       static_cast<uint64_t>(payload.size()), ctx, fm));

  EXPECT_DOUBLE_EQ(std::get<double>(fm.fields.at("d")), 1.5);
  EXPECT_FLOAT_EQ(std::get<float>(fm.fields.at("f")), 2.5f);
  EXPECT_EQ(std::get<int32_t>(fm.fields.at("i32")), -10);
  EXPECT_EQ(std::get<int64_t>(fm.fields.at("i64")), -20);
  EXPECT_EQ(std::get<uint32_t>(fm.fields.at("u32")), 30u);
  EXPECT_EQ(std::get<uint64_t>(fm.fields.at("u64")), 40u);
  EXPECT_TRUE(std::get<bool>(fm.fields.at("b")));
}

// ---------------------------------------------------------------------------
// Test 5: Unknown message type
// ---------------------------------------------------------------------------
TEST(ProtobufDecoder, UnknownMessageType) {
  gpb::FileDescriptorProto file;
  file.set_name("test.proto");
  file.set_package("test");
  file.set_syntax("proto3");

  auto* msg = file.add_message_type();
  msg->set_name("Exists");
  auto* f = msg->add_field();
  f->set_name("x");
  f->set_number(1);
  f->set_type(gpb::FieldDescriptorProto::TYPE_DOUBLE);
  f->set_label(gpb::FieldDescriptorProto::LABEL_OPTIONAL);

  auto fds_bytes = buildFileDescriptorSet(file);

  ProtobufDecoder dec;
  auto ctx = makeContext("test.DoesNotExist", fds_bytes);

  auto result = dec.prepare(ctx);
  EXPECT_FALSE(result.ok());
  EXPECT_TRUE(result.IsInvalid());
}

// ---------------------------------------------------------------------------
// Test 6: Invalid schema data
// ---------------------------------------------------------------------------
TEST(ProtobufDecoder, InvalidSchemaData) {
  ProtobufDecoder dec;
  DecoderContext ctx;
  ctx.schema_name = "test.Foo";
  ctx.schema_encoding = "protobuf";
  // Garbage bytes
  ctx.schema_data = {std::byte{0xFF}, std::byte{0xFE}, std::byte{0xFD}};

  auto result = dec.prepare(ctx);
  EXPECT_FALSE(result.ok());
}

// ---------------------------------------------------------------------------
// Test 7: Enum field
// ---------------------------------------------------------------------------
TEST(ProtobufDecoder, EnumField) {
  gpb::FileDescriptorProto file;
  file.set_name("test.proto");
  file.set_package("test");
  file.set_syntax("proto3");

  auto* msg = file.add_message_type();
  msg->set_name("StatusMsg");

  auto* enum_type = msg->add_enum_type();
  enum_type->set_name("Status");
  auto* ev0 = enum_type->add_value();
  ev0->set_name("UNKNOWN");
  ev0->set_number(0);
  auto* ev1 = enum_type->add_value();
  ev1->set_name("ACTIVE");
  ev1->set_number(1);
  auto* ev2 = enum_type->add_value();
  ev2->set_name("INACTIVE");
  ev2->set_number(2);

  auto* f1 = msg->add_field();
  f1->set_name("status");
  f1->set_number(1);
  f1->set_type(gpb::FieldDescriptorProto::TYPE_ENUM);
  f1->set_type_name(".test.StatusMsg.Status");
  f1->set_label(gpb::FieldDescriptorProto::LABEL_OPTIONAL);

  auto* f2 = msg->add_field();
  f2->set_name("name");
  f2->set_number(2);
  f2->set_type(gpb::FieldDescriptorProto::TYPE_STRING);
  f2->set_label(gpb::FieldDescriptorProto::LABEL_OPTIONAL);

  auto fds_bytes = buildFileDescriptorSet(file);

  gpb::DescriptorPool pool;
  pool.BuildFile(file);
  const auto* desc = pool.FindMessageTypeByName("test.StatusMsg");
  ASSERT_NE(desc, nullptr);

  gpb::DynamicMessageFactory factory;
  std::unique_ptr<gpb::Message> proto_msg(factory.GetPrototype(desc)->New());
  const auto* refl = proto_msg->GetReflection();
  refl->SetEnumValue(proto_msg.get(), desc->FindFieldByName("status"), 2);
  refl->SetString(proto_msg.get(), desc->FindFieldByName("name"), "test");

  auto payload = serializeMessage(*proto_msg);

  ProtobufDecoder dec;
  auto ctx = makeContext("test.StatusMsg", fds_bytes);
  ASSERT_OK(dec.prepare(ctx));

  FieldMap fm;
  ASSERT_OK(dec.decode(payload.data(),
                       static_cast<uint64_t>(payload.size()), ctx, fm));

  // Enum stored as int32 (enum value number)
  EXPECT_EQ(std::get<int32_t>(fm.fields.at("status")), 2);
  EXPECT_EQ(std::get<std::string>(fm.fields.at("name")), "test");
}

// ---------------------------------------------------------------------------
// Test 8: Bytes field
// ---------------------------------------------------------------------------
TEST(ProtobufDecoder, BytesField) {
  gpb::FileDescriptorProto file;
  file.set_name("test.proto");
  file.set_package("test");
  file.set_syntax("proto3");

  auto* msg = file.add_message_type();
  msg->set_name("BlobMsg");

  auto* f1 = msg->add_field();
  f1->set_name("payload");
  f1->set_number(1);
  f1->set_type(gpb::FieldDescriptorProto::TYPE_BYTES);
  f1->set_label(gpb::FieldDescriptorProto::LABEL_OPTIONAL);

  auto fds_bytes = buildFileDescriptorSet(file);

  gpb::DescriptorPool pool;
  pool.BuildFile(file);
  const auto* desc = pool.FindMessageTypeByName("test.BlobMsg");
  ASSERT_NE(desc, nullptr);

  gpb::DynamicMessageFactory factory;
  std::unique_ptr<gpb::Message> proto_msg(factory.GetPrototype(desc)->New());
  proto_msg->GetReflection()->SetString(
      proto_msg.get(), desc->FindFieldByName("payload"),
      std::string("\x01\x02\x03\x04", 4));

  auto payload = serializeMessage(*proto_msg);

  ProtobufDecoder dec;
  auto ctx = makeContext("test.BlobMsg", fds_bytes);
  ASSERT_OK(dec.prepare(ctx));

  FieldMap fm;
  ASSERT_OK(dec.decode(payload.data(),
                       static_cast<uint64_t>(payload.size()), ctx, fm));

  const auto& blob = std::get<std::vector<uint8_t>>(fm.fields.at("payload"));
  ASSERT_EQ(blob.size(), 4u);
  EXPECT_EQ(blob[0], 0x01);
  EXPECT_EQ(blob[3], 0x04);
}

// ---------------------------------------------------------------------------
// Test 9: No prepare -- decode returns error
// ---------------------------------------------------------------------------
TEST(ProtobufDecoder, NoPrepare) {
  ProtobufDecoder dec;
  DecoderContext ctx;
  std::byte dummy[] = {std::byte{0x00}};
  FieldMap fm;
  auto status = dec.decode(dummy, 1, ctx, fm);
  EXPECT_FALSE(status.ok());
}
