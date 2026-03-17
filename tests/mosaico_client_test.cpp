// tests/mosaico_client_test.cpp
//
// Integration tests for MosaicoClient — require a live Mosaico Flight server.
// Gracefully skip if the server is unreachable.

#include "flight/mosaico_client.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <thread>
#include <vector>

namespace {

const std::string kServerUri = "grpc://37.27.89.131:6726";
constexpr int kTimeout = 30;
const std::string kTestSequence = "nissan_zala_50_zeg_1_0";

// Helper: skip if the server is unreachable.
#define SKIP_IF_UNREACHABLE(client)                                        \
    do {                                                                   \
        auto _probe = (client).listSequences();                            \
        if (!_probe.ok()) {                                                \
            GTEST_SKIP() << "Flight server unreachable: "                  \
                         << _probe.status().ToString();                    \
        }                                                                  \
    } while (false)

TEST(MosaicoClientTest, ListSequences) {
    mosaico::MosaicoClient client(kServerUri, kTimeout, 2);
    SKIP_IF_UNREACHABLE(client);

    auto result = client.listSequences();
    ASSERT_TRUE(result.ok()) << result.status().ToString();
    EXPECT_GT(result->size(), 0u);
    EXPECT_FALSE((*result)[0].name.empty());
}

TEST(MosaicoClientTest, ListSequencesHasSize) {
    mosaico::MosaicoClient client(kServerUri, kTimeout, 2);
    SKIP_IF_UNREACHABLE(client);

    auto result = client.listSequences();
    ASSERT_TRUE(result.ok()) << result.status().ToString();

    // At least one sequence should have nonzero size.
    bool found_nonzero = false;
    for (const auto& seq : *result) {
        if (seq.total_size_bytes > 0) {
            found_nonzero = true;
            break;
        }
    }
    EXPECT_TRUE(found_nonzero)
        << "Expected at least one sequence with nonzero total_size_bytes";
}

TEST(MosaicoClientTest, ListTopics) {
    mosaico::MosaicoClient client(kServerUri, kTimeout, 2);
    SKIP_IF_UNREACHABLE(client);

    auto result = client.listTopics(kTestSequence);
    ASSERT_TRUE(result.ok()) << result.status().ToString();
    EXPECT_EQ(result->size(), 5u);

    // Each topic should have a name and ontology tag.
    for (const auto& topic : *result) {
        EXPECT_FALSE(topic.topic_name.empty());
        EXPECT_FALSE(topic.ontology_tag.empty())
            << "Topic " << topic.topic_name << " missing ontology tag";
    }
}

TEST(MosaicoClientTest, PullTopicNoTimeRange) {
    mosaico::MosaicoClient client(kServerUri, kTimeout, 2);
    SKIP_IF_UNREACHABLE(client);

    // First get the topics to find a valid topic name.
    auto topics_result = client.listTopics(kTestSequence);
    ASSERT_TRUE(topics_result.ok()) << topics_result.status().ToString();
    ASSERT_FALSE(topics_result->empty());

    const auto& first_topic = (*topics_result)[0];
    auto result = client.pullTopic(kTestSequence, first_topic.topic_name);
    ASSERT_TRUE(result.ok()) << result.status().ToString();
    EXPECT_NE(result->schema, nullptr);
    EXPECT_GT(result->batches.size(), 0u);
}

TEST(MosaicoClientTest, PullTopicWithTimeRange) {
    mosaico::MosaicoClient client(kServerUri, kTimeout, 2);
    SKIP_IF_UNREACHABLE(client);

    auto topics_result = client.listTopics(kTestSequence);
    ASSERT_TRUE(topics_result.ok()) << topics_result.status().ToString();
    ASSERT_FALSE(topics_result->empty());

    const auto& topic = (*topics_result)[0];

    // Pull full data first to get row count.
    auto full_result =
        client.pullTopic(kTestSequence, topic.topic_name);
    ASSERT_TRUE(full_result.ok()) << full_result.status().ToString();

    int64_t full_rows = 0;
    for (const auto& batch : full_result->batches) {
        full_rows += batch->num_rows();
    }
    ASSERT_GT(full_rows, 0);

    // Pull only the first second of data.
    mosaico::TimeRange range;
    range.start_ns = topic.min_ts_ns;
    range.end_ns = topic.min_ts_ns + 1'000'000'000;  // 1 second

    auto partial_result =
        client.pullTopic(kTestSequence, topic.topic_name, range);
    ASSERT_TRUE(partial_result.ok()) << partial_result.status().ToString();

    int64_t partial_rows = 0;
    for (const auto& batch : partial_result->batches) {
        partial_rows += batch->num_rows();
    }

    // Partial pull should have fewer rows than full pull (unless the
    // sequence is shorter than 1 second, which is unlikely).
    EXPECT_LT(partial_rows, full_rows)
        << "Expected time-ranged pull to return fewer rows than full pull";
}

TEST(MosaicoClientTest, ParallelPull) {
    mosaico::MosaicoClient client(kServerUri, kTimeout, 4);
    SKIP_IF_UNREACHABLE(client);

    auto topics_result = client.listTopics(kTestSequence);
    ASSERT_TRUE(topics_result.ok()) << topics_result.status().ToString();
    ASSERT_FALSE(topics_result->empty());

    const auto& topics = *topics_result;
    std::atomic<int> successes{0};
    std::vector<std::thread> threads;

    for (const auto& topic : topics) {
        threads.emplace_back([&client, &topic, &successes] {
            auto result =
                client.pullTopic(kTestSequence, topic.topic_name);
            if (result.ok() && !result->batches.empty()) {
                successes++;
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    EXPECT_EQ(successes.load(), static_cast<int>(topics.size()));
}

TEST(MosaicoClientTest, BeginUploadAndCleanup) {
    mosaico::MosaicoClient client(kServerUri, kTimeout, 2);
    SKIP_IF_UNREACHABLE(client);

    auto result = client.beginUpload("cpp_sdk_test_client");
    ASSERT_TRUE(result.ok()) << result.status().ToString();

    auto& session = *result;
    EXPECT_EQ(session->sequenceName(), "cpp_sdk_test_client");

    // Cleanup without finalize — should delete the sequence.
    auto status = session->cleanup();
    ASSERT_TRUE(status.ok()) << status.ToString();
}

} // namespace
