// tests/tools/decoder_test.cpp
#include "decoders/decoder.hpp"
#include "ontology/field_map.hpp"

#include <gtest/gtest.h>

// Concrete subclass for testing the base class default implementation.
class TestDecoder : public MessageDecoder {
 public:
  std::vector<std::string> supportedEncodings() const override {
    return {"test"};
  }
  arrow::Status decode(const std::byte*, uint64_t,
                        const DecoderContext&,
                        FieldMap& out) override {
    out.fields["x"] = 1.0;
    return arrow::Status::OK();
  }
};

TEST(MessageDecoder, SupportedEncodings) {
  TestDecoder dec;
  auto encodings = dec.supportedEncodings();
  ASSERT_EQ(encodings.size(), 1u);
  EXPECT_EQ(encodings[0], "test");
}

TEST(MessageDecoder, NeedsSamplesDefaultFalse) {
  TestDecoder dec;
  EXPECT_FALSE(dec.needsSamples());
}

TEST(MessageDecoder, PrepareDefaultNoOp) {
  TestDecoder dec;
  DecoderContext ctx;
  auto status = dec.prepare(ctx);
  EXPECT_TRUE(status.ok());
}

TEST(MessageDecoder, DecodePopulatesFieldMap) {
  TestDecoder dec;
  DecoderContext ctx;
  FieldMap fm;
  auto status = dec.decode(nullptr, 0, ctx, fm);
  ASSERT_TRUE(status.ok());
  ASSERT_EQ(fm.fields.count("x"), 1u);
  EXPECT_DOUBLE_EQ(std::get<double>(fm.fields.at("x")), 1.0);
}
