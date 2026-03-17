// tests/connection_pool_test.cpp
//
// Integration tests for ConnectionPool — require a live Mosaico Flight server.
// Gracefully skip if the server is unreachable.

#include "flight/connection_pool.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <thread>
#include <vector>

namespace {

const std::string kServerUri = "grpc://37.27.89.131:6726";
constexpr int kTimeout = 30;

// Helper: attempt a checkout; skip the test if the server is unreachable.
#define SKIP_IF_UNREACHABLE(pool)                                          \
    do {                                                                   \
        auto _h = (pool).checkout();                                       \
        if (!_h.ok()) {                                                    \
            GTEST_SKIP() << "Flight server unreachable: "                  \
                         << _h.status().ToString();                        \
        }                                                                  \
        arrow::flight::FlightCallOptions _opts;                            \
        _opts.timeout = arrow::flight::TimeoutDuration{10.0};              \
        auto _listing = (*_h)->ListFlights(_opts, {});                     \
        if (!_listing.ok()) {                                              \
            GTEST_SKIP() << "Flight server unreachable: "                  \
                         << _listing.status().ToString();                  \
        }                                                                  \
    } while (false)

TEST(ConnectionPoolTest, CheckoutReturnsValidHandle) {
    mosaico::ConnectionPool pool(kServerUri, kTimeout, 2);
    SKIP_IF_UNREACHABLE(pool);

    auto handle = pool.checkout();
    ASSERT_TRUE(handle.ok()) << handle.status().ToString();
    EXPECT_TRUE(handle->valid());
}

TEST(ConnectionPoolTest, CheckoutAndReturn) {
    mosaico::ConnectionPool pool(kServerUri, kTimeout, 2);
    SKIP_IF_UNREACHABLE(pool);

    {
        auto h1 = pool.checkout();
        ASSERT_TRUE(h1.ok()) << h1.status().ToString();
        auto h2 = pool.checkout();
        ASSERT_TRUE(h2.ok()) << h2.status().ToString();
        EXPECT_TRUE(h1->valid());
        EXPECT_TRUE(h2->valid());
    }
    // Both returned — can checkout again.
    auto h3 = pool.checkout();
    ASSERT_TRUE(h3.ok()) << h3.status().ToString();
    EXPECT_TRUE(h3->valid());
}

TEST(ConnectionPoolTest, OverflowWhenExhausted) {
    mosaico::ConnectionPool pool(kServerUri, kTimeout, 1);
    SKIP_IF_UNREACHABLE(pool);

    auto h1 = pool.checkout();
    ASSERT_TRUE(h1.ok()) << h1.status().ToString();

    // Second checkout overflows since pool_size == 1.
    auto h2 = pool.checkout();
    ASSERT_TRUE(h2.ok()) << h2.status().ToString();
    EXPECT_TRUE(h1->valid());
    EXPECT_TRUE(h2->valid());
}

TEST(ConnectionPoolTest, HandleMoveSemantics) {
    mosaico::ConnectionPool pool(kServerUri, kTimeout, 1);
    SKIP_IF_UNREACHABLE(pool);

    auto h1 = pool.checkout();
    ASSERT_TRUE(h1.ok());

    auto h2 = std::move(*h1);
    EXPECT_TRUE(h2.valid());
    // After move, source handle should be invalid.
    EXPECT_FALSE(h1->valid());
}

TEST(ConnectionPoolTest, MoveAssignment) {
    mosaico::ConnectionPool pool(kServerUri, kTimeout, 2);
    SKIP_IF_UNREACHABLE(pool);

    auto h1_result = pool.checkout();
    ASSERT_TRUE(h1_result.ok());
    auto h1 = std::move(*h1_result);

    auto h2_result = pool.checkout();
    ASSERT_TRUE(h2_result.ok());

    // Move-assign h2 into h1; h1's original connection should be returned.
    h1 = std::move(*h2_result);
    EXPECT_TRUE(h1.valid());
}

TEST(ConnectionPoolTest, ConcurrentCheckout) {
    mosaico::ConnectionPool pool(kServerUri, kTimeout, 4);
    SKIP_IF_UNREACHABLE(pool);

    std::vector<std::thread> threads;
    std::atomic<int> successes{0};

    for (int i = 0; i < 8; ++i) {
        threads.emplace_back([&] {
            auto handle = pool.checkout();
            if (handle.ok() && handle->valid()) {
                successes++;
            }
        });
    }
    for (auto& t : threads) {
        t.join();
    }
    EXPECT_EQ(successes.load(), 8);
}

TEST(ConnectionPoolTest, CanUseCheckedOutConnection) {
    mosaico::ConnectionPool pool(kServerUri, kTimeout, 1);
    SKIP_IF_UNREACHABLE(pool);

    auto handle = pool.checkout();
    ASSERT_TRUE(handle.ok()) << handle.status().ToString();

    // Use the connection to list flights (basic smoke test).
    arrow::flight::FlightCallOptions opts;
    opts.timeout = arrow::flight::TimeoutDuration{30.0};
    auto listing = (*handle)->ListFlights(opts, {});
    ASSERT_TRUE(listing.ok()) << listing.status().ToString();
}

TEST(ConnectionPoolTest, PoolReusesConnections) {
    mosaico::ConnectionPool pool(kServerUri, kTimeout, 1);
    SKIP_IF_UNREACHABLE(pool);

    arrow::flight::FlightClient* first_ptr = nullptr;
    {
        auto h = pool.checkout();
        ASSERT_TRUE(h.ok());
        first_ptr = &(**h);
    }
    // Checkout again — should get the same pooled client.
    {
        auto h = pool.checkout();
        ASSERT_TRUE(h.ok());
        EXPECT_EQ(&(**h), first_ptr);
    }
}

} // namespace
