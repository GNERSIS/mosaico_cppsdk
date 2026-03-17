// tests/tools/passthrough_decoder_test.cpp
#include "decoders/passthrough_decoder.hpp"
#include "ontology/field_map.hpp"

#include <gtest/gtest.h>

TEST(PassthroughDecoder, SupportedEncodings) {
  PassthroughDecoder dec;
  auto enc = dec.supportedEncodings();
  EXPECT_EQ(enc.size(), 2u);
  EXPECT_EQ(enc[0], "flatbuffer");
  EXPECT_EQ(enc[1], "cbor");
}

TEST(PassthroughDecoder, DecodePreservesBytes) {
  PassthroughDecoder dec;
  DecoderContext ctx;

  std::byte payload[] = {std::byte{0xDE}, std::byte{0xAD},
                          std::byte{0xBE}, std::byte{0xEF}};
  FieldMap fm;
  auto status = dec.decode(payload, 4, ctx, fm);
  ASSERT_TRUE(status.ok()) << status.ToString();

  ASSERT_EQ(fm.fields.count("data"), 1u);
  const auto& data = std::get<std::vector<uint8_t>>(fm.fields.at("data"));
  ASSERT_EQ(data.size(), 4u);
  EXPECT_EQ(data[0], 0xDE);
  EXPECT_EQ(data[1], 0xAD);
  EXPECT_EQ(data[2], 0xBE);
  EXPECT_EQ(data[3], 0xEF);
}

TEST(PassthroughDecoder, DecodeEmptyPayload) {
  PassthroughDecoder dec;
  DecoderContext ctx;

  FieldMap fm;
  auto status = dec.decode(nullptr, 0, ctx, fm);
  ASSERT_TRUE(status.ok());
  ASSERT_EQ(fm.fields.count("data"), 1u);
  const auto& data = std::get<std::vector<uint8_t>>(fm.fields.at("data"));
  EXPECT_TRUE(data.empty());
}
