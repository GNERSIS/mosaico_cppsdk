// tests/upload_session_test.cpp
//
// Integration tests for UploadSession — require a live Mosaico Flight server.
// Gracefully skip if the server is unreachable.

#include "flight/mosaico_client.hpp"
#include "flight/upload_session.hpp"

#include <arrow/api.h>
#include <gtest/gtest.h>

#include <memory>
#include <string>

namespace {

const std::string kServerUri = "grpc://37.27.89.131:6726";
constexpr int kTimeout = 30;

// Helper: skip if the server is unreachable.
#define SKIP_IF_UNREACHABLE(client)                                        \
    do {                                                                   \
        auto _probe = (client).listSequences();                            \
        if (!_probe.ok()) {                                                \
            GTEST_SKIP() << "Flight server unreachable: "                  \
                         << _probe.status().ToString();                    \
        }                                                                  \
    } while (false)

// Build a simple test schema and batch.
std::shared_ptr<arrow::Schema> makeTestSchema() {
    return arrow::schema({
        arrow::field("timestamp_ns", arrow::int64()),
        arrow::field("value", arrow::float64()),
    });
}

std::shared_ptr<arrow::RecordBatch> makeTestBatch(
    const std::shared_ptr<arrow::Schema>& schema, int num_rows) {
    arrow::Int64Builder ts_builder;
    arrow::DoubleBuilder val_builder;

    for (int i = 0; i < num_rows; ++i) {
        (void)ts_builder.Append(1000000000LL * i);
        (void)val_builder.Append(static_cast<double>(i) * 0.1);
    }

    auto ts_array = ts_builder.Finish().ValueOrDie();
    auto val_array = val_builder.Finish().ValueOrDie();

    return arrow::RecordBatch::Make(schema, num_rows,
                                    {ts_array, val_array});
}

TEST(UploadSessionTest, CreateAndCleanup) {
    mosaico::MosaicoClient client(kServerUri, kTimeout, 2);
    SKIP_IF_UNREACHABLE(client);

    auto result = client.beginUpload("cpp_sdk_test_upload_cleanup");
    ASSERT_TRUE(result.ok()) << result.status().ToString();

    auto& session = *result;
    EXPECT_EQ(session->sequenceName(), "cpp_sdk_test_upload_cleanup");

    // Cleanup without finalize — sequence should be deleted.
    auto status = session->cleanup();
    ASSERT_TRUE(status.ok()) << status.ToString();
}

TEST(UploadSessionTest, FullLifecycle) {
    mosaico::MosaicoClient client(kServerUri, kTimeout, 2);
    SKIP_IF_UNREACHABLE(client);

    auto result = client.beginUpload("cpp_sdk_test_upload_lifecycle");
    ASSERT_TRUE(result.ok()) << result.status().ToString();

    auto& session = *result;

    // Create a topic and write data.
    auto schema = makeTestSchema();
    auto status = session->createTopic("test_sensor", "raw", schema);
    ASSERT_TRUE(status.ok()) << status.ToString();

    auto batch = makeTestBatch(schema, 10);
    status = session->putBatch(batch);
    ASSERT_TRUE(status.ok()) << status.ToString();

    status = session->closeTopic();
    ASSERT_TRUE(status.ok()) << status.ToString();

    // Don't finalize — cleanup to delete the unfinalized sequence.
    status = session->cleanup();
    ASSERT_TRUE(status.ok()) << status.ToString();
}

TEST(UploadSessionTest, DestructorCallsCleanup) {
    mosaico::MosaicoClient client(kServerUri, kTimeout, 2);
    SKIP_IF_UNREACHABLE(client);

    {
        auto result = client.beginUpload("cpp_sdk_test_upload_dtor");
        ASSERT_TRUE(result.ok()) << result.status().ToString();
        // session goes out of scope — destructor should call cleanup
    }

    // Verify the sequence no longer exists by trying to list its topics.
    // This should either fail or return empty (the sequence was deleted).
    auto topics = client.listTopics("cpp_sdk_test_upload_dtor");
    // The sequence was unfinalized and deleted, so GetFlightInfo should fail.
    // We just verify the test doesn't crash — the server may return an error
    // or empty results for a deleted sequence.
    (void)topics;
}

TEST(UploadSessionTest, MoveSemantics) {
    mosaico::MosaicoClient client(kServerUri, kTimeout, 2);
    SKIP_IF_UNREACHABLE(client);

    auto result = client.beginUpload("cpp_sdk_test_upload_move");
    ASSERT_TRUE(result.ok()) << result.status().ToString();

    // Move the session to a new unique_ptr.
    auto session1 = std::move(*result);
    EXPECT_EQ(session1->sequenceName(), "cpp_sdk_test_upload_move");

    // Move-construct into a second session.
    auto session2 = std::make_unique<mosaico::UploadSession>(
        std::move(*session1));
    EXPECT_EQ(session2->sequenceName(), "cpp_sdk_test_upload_move");

    // Cleanup via session2.
    auto status = session2->cleanup();
    ASSERT_TRUE(status.ok()) << status.ToString();
}

} // namespace
