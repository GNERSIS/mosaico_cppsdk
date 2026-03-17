// tests/tools/mcap_writer_test.cpp
#include "mcap_writer.hpp"
#include "mcap_reader.hpp"

#include <gtest/gtest.h>

#include <arrow/api.h>
#include <arrow/builder.h>
#include <mcap/reader.hpp>
#include <nlohmann/json.hpp>

#include <filesystem>

namespace fs = std::filesystem;
using json = nlohmann::json;

// Helper macro — avoids linking arrow_testing which may not be available.
#define ASSERT_OK(expr)                                   \
  do {                                                    \
    auto _s = (expr);                                     \
    ASSERT_TRUE(_s.ok()) << _s.ToString();                \
  } while (0)

static std::shared_ptr<arrow::RecordBatch> makeTestBatch() {
  auto schema = arrow::schema({
      arrow::field("timestamp_ns", arrow::int64()),
      arrow::field("x", arrow::float64()),
      arrow::field("y", arrow::float64()),
  });
  arrow::Int64Builder ts_builder;
  EXPECT_TRUE(ts_builder.AppendValues({1000000000, 2000000000, 3000000000}).ok());
  arrow::DoubleBuilder x_builder, y_builder;
  EXPECT_TRUE(x_builder.AppendValues({1.0, 2.0, 3.0}).ok());
  EXPECT_TRUE(y_builder.AppendValues({4.0, 5.0, 6.0}).ok());
  std::shared_ptr<arrow::Array> ts_arr, x_arr, y_arr;
  EXPECT_TRUE(ts_builder.Finish(&ts_arr).ok());
  EXPECT_TRUE(x_builder.Finish(&x_arr).ok());
  EXPECT_TRUE(y_builder.Finish(&y_arr).ok());
  return arrow::RecordBatch::Make(schema, 3, {ts_arr, x_arr, y_arr});
}

TEST(McapWriter, WriteSingleTopic) {
  auto path = fs::temp_directory_path() / "mcap_writer_test.mcap";
  auto batch = makeTestBatch();

  TopicBatches topic;
  topic.topic_name = "/test/data";
  topic.schema = batch->schema();
  topic.batches.push_back(batch);

  auto status = writeMcap(path.string(), {topic});
  ASSERT_TRUE(status.ok()) << status.ToString();
  EXPECT_TRUE(fs::exists(path));

  // Verify by reading back with MCAP reader
  mcap::McapReader reader;
  ASSERT_TRUE(reader.open(path.string()).ok());
  reader.readSummary(mcap::ReadSummaryMethod::AllowFallbackScan);

  ASSERT_EQ(reader.channels().size(), 1u);
  auto& [id, ch] = *reader.channels().begin();
  EXPECT_EQ(ch->topic, "/test/data");
  EXPECT_EQ(ch->messageEncoding, "json");

  // Check message count
  int count = 0;
  for (const auto& msg : reader.readMessages()) {
    std::string_view payload(reinterpret_cast<const char*>(msg.message.data),
                             msg.message.dataSize);
    auto obj = json::parse(payload);
    EXPECT_TRUE(obj.contains("x"));
    EXPECT_TRUE(obj.contains("y"));
    ++count;
  }
  EXPECT_EQ(count, 3);

  reader.close();
  fs::remove(path);
}

TEST(McapWriter, WriteMultipleTopics) {
  auto path = fs::temp_directory_path() / "mcap_writer_multi_test.mcap";
  auto batch = makeTestBatch();

  TopicBatches t1{"/topic_a", batch->schema(), {batch}};
  TopicBatches t2{"/topic_b", batch->schema(), {batch}};

  auto status = writeMcap(path.string(), {t1, t2});
  ASSERT_TRUE(status.ok()) << status.ToString();

  mcap::McapReader reader;
  ASSERT_TRUE(reader.open(path.string()).ok());
  reader.readSummary(mcap::ReadSummaryMethod::AllowFallbackScan);
  EXPECT_EQ(reader.channels().size(), 2u);

  reader.close();
  fs::remove(path);
}

TEST(McapWriter, TimestampsFromColumn) {
  auto path = fs::temp_directory_path() / "mcap_writer_ts_test.mcap";
  auto batch = makeTestBatch();
  TopicBatches topic{"/ts_test", batch->schema(), {batch}};

  ASSERT_OK(writeMcap(path.string(), {topic}));

  mcap::McapReader reader;
  ASSERT_TRUE(reader.open(path.string()).ok());
  reader.readSummary(mcap::ReadSummaryMethod::AllowFallbackScan);

  std::vector<uint64_t> timestamps;
  for (const auto& msg : reader.readMessages()) {
    timestamps.push_back(msg.message.logTime);
  }
  ASSERT_EQ(timestamps.size(), 3u);
  EXPECT_EQ(timestamps[0], 1000000000u);
  EXPECT_EQ(timestamps[1], 2000000000u);
  EXPECT_EQ(timestamps[2], 3000000000u);

  reader.close();
  fs::remove(path);
}
