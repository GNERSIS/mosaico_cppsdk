// tests/tools/mcap_roundtrip_test.cpp
#include "mcap_reader.hpp"
#include "mcap_writer.hpp"

#include <gtest/gtest.h>

#include <mcap/writer.hpp>
#include <nlohmann/json.hpp>

#include <arrow/api.h>

#include <filesystem>

// Helper macro — avoids linking arrow_testing which may not be available.
#define ASSERT_OK(expr)                                   \
  do {                                                    \
    auto _s = (expr);                                     \
    ASSERT_TRUE(_s.ok()) << _s.ToString();                \
  } while (0)

namespace fs = std::filesystem;
using json = nlohmann::json;

TEST(Roundtrip, McapToArrowToMcap) {
  // Step 1: Create a source MCAP with JSON messages
  auto src_path = fs::temp_directory_path() / "roundtrip_src.mcap";
  {
    mcap::McapWriter writer;
    auto opts = mcap::McapWriterOptions("test");
    opts.compression = mcap::Compression::None;
    writer.open(src_path.string(), opts);

    mcap::Schema schema("TestData", "jsonschema",
        R"({"type":"object","properties":{"value":{"type":"number"},"label":{"type":"string"}}})");
    writer.addSchema(schema);

    mcap::Channel channel("/test/data", "json", schema.id);
    writer.addChannel(channel);

    for (int i = 0; i < 10; ++i) {
      auto body = json({{"value", static_cast<double>(i) * 1.5},
                        {"label", "sample_" + std::to_string(i)}}).dump();
      mcap::Message msg;
      msg.channelId = channel.id;
      msg.sequence = i;
      msg.logTime = (i + 1) * 1'000'000'000ULL;
      msg.publishTime = msg.logTime;
      msg.data = reinterpret_cast<const std::byte*>(body.data());
      msg.dataSize = body.size();
      writer.write(msg);
    }
    writer.close();
  }

  // Step 2: Read the MCAP and convert to Arrow
  McapChannelIndex index;
  ASSERT_OK(indexMcapChannels(src_path.string(), index));
  ASSERT_EQ(index.channels.size(), 1u);

  std::shared_ptr<arrow::Schema> arrow_schema;
  std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
  ASSERT_OK(readMcapChannel(src_path.string(), index.channels[0],
                             arrow_schema, batches));

  int64_t total_rows = 0;
  for (const auto& b : batches) total_rows += b->num_rows();
  EXPECT_EQ(total_rows, 10);

  // Step 3: Write Arrow back to a new MCAP
  auto dst_path = fs::temp_directory_path() / "roundtrip_dst.mcap";
  TopicBatches topic{"/test/data", arrow_schema, batches};
  ASSERT_OK(writeMcap(dst_path.string(), {topic}));

  // Step 4: Read the output MCAP and verify
  McapChannelIndex out_index;
  ASSERT_OK(indexMcapChannels(dst_path.string(), out_index));
  ASSERT_EQ(out_index.channels.size(), 1u);
  EXPECT_EQ(out_index.channels[0].topic, "/test/data");
  EXPECT_EQ(out_index.channels[0].encoding, "json");
  EXPECT_EQ(out_index.channels[0].message_count, 10u);

  // Step 5: Verify data integrity by reading back to Arrow
  std::shared_ptr<arrow::Schema> out_schema;
  std::vector<std::shared_ptr<arrow::RecordBatch>> out_batches;
  ASSERT_OK(readMcapChannel(dst_path.string(), out_index.channels[0],
                             out_schema, out_batches));

  int64_t out_rows = 0;
  for (const auto& b : out_batches) out_rows += b->num_rows();
  EXPECT_EQ(out_rows, 10);

  // Verify the "value" and "label" columns are present
  EXPECT_NE(out_schema->GetFieldIndex("value"), -1);
  EXPECT_NE(out_schema->GetFieldIndex("label"), -1);

  fs::remove(src_path);
  fs::remove(dst_path);
}

TEST(Roundtrip, DumpLocalSmokeTest) {
  // Create a minimal MCAP and dump it (just verify no crash)
  auto path = fs::temp_directory_path() / "dump_smoke.mcap";
  {
    mcap::McapWriter writer;
    auto opts = mcap::McapWriterOptions("test");
    opts.compression = mcap::Compression::None;
    writer.open(path.string(), opts);

    mcap::Schema schema("X", "jsonschema", R"({"type":"object"})");
    writer.addSchema(schema);
    mcap::Channel channel("/x", "json", schema.id);
    writer.addChannel(channel);

    std::string body = "{}";
    mcap::Message msg;
    msg.channelId = channel.id;
    msg.logTime = 1000;
    msg.publishTime = 1000;
    msg.data = reinterpret_cast<const std::byte*>(body.data());
    msg.dataSize = body.size();
    writer.write(msg);
    writer.close();
  }

  McapChannelIndex index;
  ASSERT_OK(indexMcapChannels(path.string(), index));
  EXPECT_EQ(index.channels.size(), 1u);
  EXPECT_EQ(index.total_messages, 1u);

  fs::remove(path);
}
