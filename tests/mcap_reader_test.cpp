#include "mcap_reader.hpp"

#include <gtest/gtest.h>

#include <mcap/writer.hpp>
#include <nlohmann/json.hpp>

#include <arrow/api.h>

#include <cstdio>
#include <filesystem>
#include <string>

#define ASSERT_OK(expr)                                   \
  do {                                                    \
    auto _s = (expr);                                     \
    ASSERT_TRUE(_s.ok()) << _s.ToString();                \
  } while (0)

namespace fs = std::filesystem;
using json = nlohmann::json;

static fs::path createTestMcap(
    const std::string& topic, const std::string& schema_name,
    const std::string& json_schema,
    const std::vector<std::pair<uint64_t, json>>& messages) {
  auto path = fs::temp_directory_path() / "mcap_reader_test.mcap";
  mcap::McapWriter writer;
  auto opts = mcap::McapWriterOptions("test");
  opts.compression = mcap::Compression::None;
  auto status = writer.open(path.string(), opts);
  EXPECT_TRUE(status.ok()) << status.message;

  mcap::Schema schema(schema_name, "jsonschema", json_schema);
  writer.addSchema(schema);

  mcap::Channel channel(topic, "json", schema.id);
  writer.addChannel(channel);

  for (const auto& [ts, body] : messages) {
    std::string serialized = body.dump();
    mcap::Message msg;
    msg.channelId = channel.id;
    msg.sequence = 0;
    msg.logTime = ts;
    msg.publishTime = ts;
    msg.data = reinterpret_cast<const std::byte*>(serialized.data());
    msg.dataSize = serialized.size();
    writer.write(msg);
  }
  writer.close();
  return path;
}

TEST(McapReader, IndexChannels) {
  auto path = createTestMcap(
      "/sensor/temp", "Temperature",
      R"({"type":"object","properties":{"value":{"type":"number"}}})",
      {{1000, {{"value", 25.5}}}, {2000, {{"value", 26.0}}}});

  McapChannelIndex index;
  auto status = indexMcapChannels(path.string(), index);
  ASSERT_TRUE(status.ok()) << status.ToString();
  ASSERT_EQ(index.channels.size(), 1u);
  EXPECT_EQ(index.channels[0].topic, "/sensor/temp");
  EXPECT_EQ(index.channels[0].encoding, "json");
  EXPECT_EQ(index.channels[0].message_count, 2u);
  fs::remove(path);
}

TEST(McapReader, ReadJsonChannelToArrow) {
  std::vector<std::pair<uint64_t, json>> messages;
  for (int i = 0; i < 5; ++i) {
    messages.push_back({
        static_cast<uint64_t>((i + 1) * 1'000'000'000LL),
        {{"x", static_cast<double>(i) * 0.1}, {"y", static_cast<double>(i) * 0.2}}});
  }
  auto path = createTestMcap(
      "/point", "Point",
      R"({"type":"object","properties":{"x":{"type":"number"},"y":{"type":"number"}}})",
      messages);

  McapChannelIndex index;
  ASSERT_OK(indexMcapChannels(path.string(), index));

  std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
  std::shared_ptr<arrow::Schema> schema;
  auto status = readMcapChannel(path.string(), index.channels[0], schema, batches);
  ASSERT_TRUE(status.ok()) << status.ToString();
  ASSERT_FALSE(batches.empty());

  EXPECT_NE(schema->GetFieldIndex("timestamp_ns"), -1);
  EXPECT_NE(schema->GetFieldIndex("x"), -1);
  EXPECT_NE(schema->GetFieldIndex("y"), -1);

  int64_t total_rows = 0;
  for (const auto& batch : batches) total_rows += batch->num_rows();
  EXPECT_EQ(total_rows, 5);
  fs::remove(path);
}

TEST(McapReader, SkipsUnsupportedEncoding) {
  auto path = fs::temp_directory_path() / "mcap_reader_unknown_test.mcap";
  mcap::McapWriter writer;
  auto opts = mcap::McapWriterOptions("test");
  opts.compression = mcap::Compression::None;
  writer.open(path.string(), opts);

  mcap::Schema schema("some_type", "custom_encoding", "irrelevant");
  writer.addSchema(schema);
  mcap::Channel channel("/data", "custom_unknown", schema.id);
  writer.addChannel(channel);

  std::string dummy = "payload";
  mcap::Message msg;
  msg.channelId = channel.id;
  msg.logTime = 1000;
  msg.publishTime = 1000;
  msg.data = reinterpret_cast<const std::byte*>(dummy.data());
  msg.dataSize = dummy.size();
  writer.write(msg);
  writer.close();

  McapChannelIndex index;
  ASSERT_OK(indexMcapChannels(path.string(), index));
  ASSERT_EQ(index.channels.size(), 1u);
  EXPECT_EQ(index.channels[0].encoding, "custom_unknown");

  std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
  std::shared_ptr<arrow::Schema> schema_out;
  auto status = readMcapChannel(path.string(), index.channels[0], schema_out, batches);
  EXPECT_FALSE(status.ok());
  fs::remove(path);
}

TEST(McapReader, MultipleChannels) {
  auto path = fs::temp_directory_path() / "mcap_reader_multi_test.mcap";
  mcap::McapWriter writer;
  auto opts = mcap::McapWriterOptions("test");
  opts.compression = mcap::Compression::None;
  writer.open(path.string(), opts);

  mcap::Schema s1("Temp", "jsonschema", R"({"type":"object","properties":{"v":{"type":"number"}}})");
  writer.addSchema(s1);
  mcap::Channel c1("/temp", "json", s1.id);
  writer.addChannel(c1);

  mcap::Schema s2("Pres", "jsonschema", R"({"type":"object","properties":{"p":{"type":"number"}}})");
  writer.addSchema(s2);
  mcap::Channel c2("/pressure", "json", s2.id);
  writer.addChannel(c2);

  for (int i = 0; i < 3; ++i) {
    std::string body1 = json({{"v", 20.0 + i}}).dump();
    mcap::Message m1;
    m1.channelId = c1.id;
    m1.logTime = i * 1'000'000'000ULL;
    m1.publishTime = m1.logTime;
    m1.data = reinterpret_cast<const std::byte*>(body1.data());
    m1.dataSize = body1.size();
    writer.write(m1);

    std::string body2 = json({{"p", 101300.0 + i * 100}}).dump();
    mcap::Message m2;
    m2.channelId = c2.id;
    m2.logTime = i * 1'000'000'000ULL;
    m2.publishTime = m2.logTime;
    m2.data = reinterpret_cast<const std::byte*>(body2.data());
    m2.dataSize = body2.size();
    writer.write(m2);
  }
  writer.close();

  McapChannelIndex index;
  ASSERT_OK(indexMcapChannels(path.string(), index));
  EXPECT_EQ(index.channels.size(), 2u);
  fs::remove(path);
}

// ---------------------------------------------------------------------------
// Tests for decodeMcapChannel (callback-based API)
// ---------------------------------------------------------------------------

TEST(McapReader, DecodeMcapChannelCallsCallback) {
  std::vector<std::pair<uint64_t, json>> messages;
  for (int i = 0; i < 5; ++i) {
    messages.push_back({
        static_cast<uint64_t>((i + 1) * 1'000'000'000LL),
        {{"x", static_cast<double>(i) * 0.1},
         {"y", static_cast<double>(i) * 0.2}}});
  }
  auto path = createTestMcap(
      "/point", "Point",
      R"({"type":"object","properties":{"x":{"type":"number"},"y":{"type":"number"}}})",
      messages);

  McapChannelIndex index;
  ASSERT_OK(indexMcapChannels(path.string(), index));
  ASSERT_EQ(index.channels.size(), 1u);

  int callback_count = 0;
  auto status = decodeMcapChannel(
      path.string(), index.channels[0],
      [&](const FieldMap& fm) -> arrow::Status {
        ++callback_count;

        // Verify _log_time_ns is present and is uint64_t
        auto it = fm.fields.find("_log_time_ns");
        EXPECT_NE(it, fm.fields.end());
        if (it != fm.fields.end()) {
          EXPECT_TRUE(std::holds_alternative<uint64_t>(it->second));
          auto ts = std::get<uint64_t>(it->second);
          EXPECT_EQ(ts, static_cast<uint64_t>(callback_count) * 1'000'000'000ULL);
        }

        // Verify data fields are present
        EXPECT_NE(fm.fields.find("x"), fm.fields.end());
        EXPECT_NE(fm.fields.find("y"), fm.fields.end());

        return arrow::Status::OK();
      });
  ASSERT_TRUE(status.ok()) << status.ToString();
  EXPECT_EQ(callback_count, 5);
  fs::remove(path);
}

TEST(McapReader, DecodeMcapChannelLogTimeValues) {
  // Verify that _log_time_ns matches the original message log times
  std::vector<uint64_t> expected_times = {100, 200, 300};
  std::vector<std::pair<uint64_t, json>> messages;
  for (auto ts : expected_times) {
    messages.push_back({ts, {{"val", 1.0}}});
  }
  auto path = createTestMcap(
      "/ts_test", "TsTest",
      R"({"type":"object","properties":{"val":{"type":"number"}}})",
      messages);

  McapChannelIndex index;
  ASSERT_OK(indexMcapChannels(path.string(), index));

  std::vector<uint64_t> received_times;
  ASSERT_OK(decodeMcapChannel(
      path.string(), index.channels[0],
      [&](const FieldMap& fm) -> arrow::Status {
        auto it = fm.fields.find("_log_time_ns");
        EXPECT_NE(it, fm.fields.end());
        received_times.push_back(std::get<uint64_t>(it->second));
        return arrow::Status::OK();
      }));

  ASSERT_EQ(received_times.size(), expected_times.size());
  for (size_t i = 0; i < expected_times.size(); ++i) {
    EXPECT_EQ(received_times[i], expected_times[i]);
  }
  fs::remove(path);
}

TEST(McapReader, DecodeMcapChannelStopsOnError) {
  std::vector<std::pair<uint64_t, json>> messages;
  for (int i = 0; i < 10; ++i) {
    messages.push_back({
        static_cast<uint64_t>(i * 1000),
        {{"value", static_cast<double>(i)}}});
  }
  auto path = createTestMcap(
      "/stop_test", "StopTest",
      R"({"type":"object","properties":{"value":{"type":"number"}}})",
      messages);

  McapChannelIndex index;
  ASSERT_OK(indexMcapChannels(path.string(), index));

  int callback_count = 0;
  auto status = decodeMcapChannel(
      path.string(), index.channels[0],
      [&](const FieldMap& /*fm*/) -> arrow::Status {
        ++callback_count;
        if (callback_count == 3) {
          return arrow::Status::Cancelled("stop after 3");
        }
        return arrow::Status::OK();
      });

  EXPECT_FALSE(status.ok());
  EXPECT_TRUE(status.IsCancelled());
  EXPECT_EQ(callback_count, 3);
  fs::remove(path);
}

TEST(McapReader, DecodeMcapChannelUnsupportedEncoding) {
  auto path = fs::temp_directory_path() / "mcap_decode_unknown_test.mcap";
  mcap::McapWriter writer;
  auto opts = mcap::McapWriterOptions("test");
  opts.compression = mcap::Compression::None;
  writer.open(path.string(), opts);

  mcap::Schema schema("some_type", "custom_encoding", "irrelevant");
  writer.addSchema(schema);
  mcap::Channel channel("/data", "custom_unknown", schema.id);
  writer.addChannel(channel);

  std::string dummy = "payload";
  mcap::Message msg;
  msg.channelId = channel.id;
  msg.logTime = 1000;
  msg.publishTime = 1000;
  msg.data = reinterpret_cast<const std::byte*>(dummy.data());
  msg.dataSize = dummy.size();
  writer.write(msg);
  writer.close();

  McapChannelIndex index;
  ASSERT_OK(indexMcapChannels(path.string(), index));

  auto status = decodeMcapChannel(
      path.string(), index.channels[0],
      [](const FieldMap& /*fm*/) -> arrow::Status {
        return arrow::Status::OK();
      });
  EXPECT_FALSE(status.ok());
  fs::remove(path);
}

TEST(McapReader, DecodeMcapChannelProgressCallback) {
  std::vector<std::pair<uint64_t, json>> messages;
  for (int i = 0; i < 4; ++i) {
    messages.push_back({
        static_cast<uint64_t>(i * 1000),
        {{"v", static_cast<double>(i)}}});
  }
  auto path = createTestMcap(
      "/progress", "Prog",
      R"({"type":"object","properties":{"v":{"type":"number"}}})",
      messages);

  McapChannelIndex index;
  ASSERT_OK(indexMcapChannels(path.string(), index));

  int progress_calls = 0;
  int64_t last_rows = 0;
  int64_t last_bytes = 0;
  ASSERT_OK(decodeMcapChannel(
      path.string(), index.channels[0],
      [](const FieldMap& /*fm*/) -> arrow::Status {
        return arrow::Status::OK();
      },
      [&](int64_t rows, int64_t bytes) {
        ++progress_calls;
        last_rows = rows;
        last_bytes = bytes;
      }));

  EXPECT_EQ(progress_calls, 4);
  EXPECT_EQ(last_rows, 4);
  EXPECT_GT(last_bytes, 0);
  fs::remove(path);
}

TEST(McapReader, DecodeMcapChannelEmptyFile) {
  // MCAP with a channel but zero messages
  auto path = fs::temp_directory_path() / "mcap_decode_empty_test.mcap";
  mcap::McapWriter writer;
  auto opts = mcap::McapWriterOptions("test");
  opts.compression = mcap::Compression::None;
  writer.open(path.string(), opts);

  mcap::Schema schema("Empty", "jsonschema", R"({"type":"object"})");
  writer.addSchema(schema);
  mcap::Channel channel("/empty", "json", schema.id);
  writer.addChannel(channel);
  writer.close();

  McapChannelIndex index;
  ASSERT_OK(indexMcapChannels(path.string(), index));
  ASSERT_EQ(index.channels.size(), 1u);

  int callback_count = 0;
  ASSERT_OK(decodeMcapChannel(
      path.string(), index.channels[0],
      [&](const FieldMap& /*fm*/) -> arrow::Status {
        ++callback_count;
        return arrow::Status::OK();
      }));
  EXPECT_EQ(callback_count, 0);
  fs::remove(path);
}
