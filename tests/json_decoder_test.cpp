// tests/tools/json_decoder_test.cpp
#include "decoders/json_decoder.hpp"
#include "ontology/field_map.hpp"

#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

using json = nlohmann::json;

TEST(JsonDecoder, NeedsSamples) {
  JsonDecoder dec;
  EXPECT_TRUE(dec.needsSamples());
}

TEST(JsonDecoder, DecodeSimpleObject) {
  JsonDecoder dec;
  DecoderContext ctx;

  std::string msg = json({{"x", 1.5}, {"y", 2.5}}).dump();
  FieldMap fm;
  auto status = dec.decode(reinterpret_cast<const std::byte*>(msg.data()),
                            msg.size(), ctx, fm);
  ASSERT_TRUE(status.ok()) << status.ToString();

  ASSERT_EQ(fm.fields.count("x"), 1u);
  ASSERT_EQ(fm.fields.count("y"), 1u);
  EXPECT_DOUBLE_EQ(std::get<double>(fm.fields.at("x")), 1.5);
  EXPECT_DOUBLE_EQ(std::get<double>(fm.fields.at("y")), 2.5);
}

TEST(JsonDecoder, DecodeNestedObject) {
  JsonDecoder dec;
  DecoderContext ctx;

  std::string msg = json({{"pos", {{"x", 1.0}, {"y", 2.0}}}}).dump();
  FieldMap fm;
  auto status = dec.decode(reinterpret_cast<const std::byte*>(msg.data()),
                            msg.size(), ctx, fm);
  ASSERT_TRUE(status.ok()) << status.ToString();

  // Nested objects become dot-path keys
  ASSERT_EQ(fm.fields.count("pos.x"), 1u);
  ASSERT_EQ(fm.fields.count("pos.y"), 1u);
  EXPECT_DOUBLE_EQ(std::get<double>(fm.fields.at("pos.x")), 1.0);
  EXPECT_DOUBLE_EQ(std::get<double>(fm.fields.at("pos.y")), 2.0);
}

TEST(JsonDecoder, DecodeIntegerField) {
  JsonDecoder dec;
  DecoderContext ctx;

  std::string msg = json({{"count", 42}}).dump();
  FieldMap fm;
  auto status = dec.decode(reinterpret_cast<const std::byte*>(msg.data()),
                            msg.size(), ctx, fm);
  ASSERT_TRUE(status.ok()) << status.ToString();

  ASSERT_EQ(fm.fields.count("count"), 1u);
  EXPECT_EQ(std::get<int64_t>(fm.fields.at("count")), 42);
}

TEST(JsonDecoder, DecodeBoolField) {
  JsonDecoder dec;
  DecoderContext ctx;

  std::string msg = json({{"flag", true}}).dump();
  FieldMap fm;
  auto status = dec.decode(reinterpret_cast<const std::byte*>(msg.data()),
                            msg.size(), ctx, fm);
  ASSERT_TRUE(status.ok()) << status.ToString();

  ASSERT_EQ(fm.fields.count("flag"), 1u);
  EXPECT_TRUE(std::get<bool>(fm.fields.at("flag")));
}

TEST(JsonDecoder, DecodeStringField) {
  JsonDecoder dec;
  DecoderContext ctx;

  std::string msg = json({{"name", "hello"}}).dump();
  FieldMap fm;
  auto status = dec.decode(reinterpret_cast<const std::byte*>(msg.data()),
                            msg.size(), ctx, fm);
  ASSERT_TRUE(status.ok()) << status.ToString();

  ASSERT_EQ(fm.fields.count("name"), 1u);
  EXPECT_EQ(std::get<std::string>(fm.fields.at("name")), "hello");
}

TEST(JsonDecoder, DecodeNumericArray) {
  JsonDecoder dec;
  DecoderContext ctx;

  std::string msg = json({{"values", {1.0, 2.0, 3.0}}}).dump();
  FieldMap fm;
  auto status = dec.decode(reinterpret_cast<const std::byte*>(msg.data()),
                            msg.size(), ctx, fm);
  ASSERT_TRUE(status.ok()) << status.ToString();

  ASSERT_EQ(fm.fields.count("values"), 1u);
  const auto& vals = std::get<std::vector<double>>(fm.fields.at("values"));
  ASSERT_EQ(vals.size(), 3u);
  EXPECT_DOUBLE_EQ(vals[0], 1.0);
  EXPECT_DOUBLE_EQ(vals[1], 2.0);
  EXPECT_DOUBLE_EQ(vals[2], 3.0);
}

TEST(JsonDecoder, DecodeStringArray) {
  JsonDecoder dec;
  DecoderContext ctx;

  std::string msg = json({{"tags", {"a", "b", "c"}}}).dump();
  FieldMap fm;
  auto status = dec.decode(reinterpret_cast<const std::byte*>(msg.data()),
                            msg.size(), ctx, fm);
  ASSERT_TRUE(status.ok()) << status.ToString();

  ASSERT_EQ(fm.fields.count("tags"), 1u);
  const auto& tags = std::get<std::vector<std::string>>(fm.fields.at("tags"));
  ASSERT_EQ(tags.size(), 3u);
  EXPECT_EQ(tags[0], "a");
  EXPECT_EQ(tags[1], "b");
  EXPECT_EQ(tags[2], "c");
}

TEST(JsonDecoder, DecodeInvalidJson) {
  JsonDecoder dec;
  DecoderContext ctx;

  std::string msg = "not json{";
  FieldMap fm;
  auto status = dec.decode(reinterpret_cast<const std::byte*>(msg.data()),
                            msg.size(), ctx, fm);
  EXPECT_FALSE(status.ok());
}

TEST(JsonDecoder, DecodeNonObject) {
  JsonDecoder dec;
  DecoderContext ctx;

  std::string msg = "[1,2,3]";
  FieldMap fm;
  auto status = dec.decode(reinterpret_cast<const std::byte*>(msg.data()),
                            msg.size(), ctx, fm);
  EXPECT_FALSE(status.ok());
}

TEST(JsonDecoder, DecodeMixedTypes) {
  JsonDecoder dec;
  DecoderContext ctx;

  std::string msg = json({{"val", 99.5}, {"name", "test"}, {"ok", false}}).dump();
  FieldMap fm;
  auto status = dec.decode(reinterpret_cast<const std::byte*>(msg.data()),
                            msg.size(), ctx, fm);
  ASSERT_TRUE(status.ok()) << status.ToString();

  EXPECT_DOUBLE_EQ(std::get<double>(fm.fields.at("val")), 99.5);
  EXPECT_EQ(std::get<std::string>(fm.fields.at("name")), "test");
  EXPECT_FALSE(std::get<bool>(fm.fields.at("ok")));
}
