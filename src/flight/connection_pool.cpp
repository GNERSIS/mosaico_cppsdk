// src/flight/connection_pool.cpp
#include "flight/connection_pool.hpp"

#include <arrow/flight/api.h>

#include <cassert>
#include <utility>

namespace fl = arrow::flight;

namespace mosaico {

// ---------------------------------------------------------------------------
// ConnectionPool
// ---------------------------------------------------------------------------

ConnectionPool::ConnectionPool(const std::string& server_uri,
                               int timeout_seconds, size_t pool_size)
    : server_uri_(server_uri),
      timeout_(timeout_seconds),
      pool_size_(pool_size),
      clients_(pool_size),
      in_use_(pool_size, false) {}

ConnectionPool::~ConnectionPool() = default;

arrow::Result<std::unique_ptr<fl::FlightClient>>
ConnectionPool::createConnection() {
    ARROW_ASSIGN_OR_RAISE(auto location, fl::Location::Parse(server_uri_));
    fl::FlightClientOptions opts;
    return fl::FlightClient::Connect(location, opts);
}

arrow::Result<ConnectionPool::Handle> ConnectionPool::checkout() {
    std::lock_guard<std::mutex> lock(mutex_);

    // Find a free pooled slot.
    for (size_t i = 0; i < pool_size_; ++i) {
        if (!in_use_[i]) {
            // Lazy creation: connect on first use of this slot.
            if (!clients_[i]) {
                ARROW_ASSIGN_OR_RAISE(clients_[i], createConnection());
            }
            in_use_[i] = true;
            return Handle(this, i, clients_[i].get());
        }
    }

    // All pooled slots in use — create an overflow connection.
    ARROW_ASSIGN_OR_RAISE(auto overflow, createConnection());
    return Handle(std::move(overflow));
}

void ConnectionPool::returnConnection(size_t index) {
    std::lock_guard<std::mutex> lock(mutex_);
    assert(index < pool_size_);
    in_use_[index] = false;
}

// ---------------------------------------------------------------------------
// Handle
// ---------------------------------------------------------------------------

ConnectionPool::Handle::Handle(ConnectionPool* pool, size_t index,
                               fl::FlightClient* client)
    : pool_(pool), index_(index), client_(client), overflow_(false) {}

ConnectionPool::Handle::Handle(
    std::unique_ptr<fl::FlightClient> overflow_client)
    : overflow_client_(std::move(overflow_client)), overflow_(true) {
    client_ = overflow_client_.get();
}

ConnectionPool::Handle::Handle(Handle&& other) noexcept
    : pool_(other.pool_),
      index_(other.index_),
      client_(other.client_),
      overflow_client_(std::move(other.overflow_client_)),
      overflow_(other.overflow_) {
    other.reset();
}

ConnectionPool::Handle& ConnectionPool::Handle::operator=(
    Handle&& other) noexcept {
    if (this != &other) {
        // Return our current connection first (if any).
        if (valid() && !overflow_ && pool_) {
            pool_->returnConnection(index_);
        }
        // overflow_client_ unique_ptr will be destroyed by assignment.

        pool_ = other.pool_;
        index_ = other.index_;
        client_ = other.client_;
        overflow_client_ = std::move(other.overflow_client_);
        overflow_ = other.overflow_;
        other.reset();
    }
    return *this;
}

ConnectionPool::Handle::~Handle() {
    if (!valid()) return;
    if (!overflow_ && pool_) {
        pool_->returnConnection(index_);
    }
    // Overflow: overflow_client_ unique_ptr destroys the client.
}

fl::FlightClient* ConnectionPool::Handle::operator->() const {
    return client_;
}

fl::FlightClient& ConnectionPool::Handle::operator*() const {
    return *client_;
}

bool ConnectionPool::Handle::valid() const { return client_ != nullptr; }

void ConnectionPool::Handle::reset() noexcept {
    pool_ = nullptr;
    index_ = 0;
    client_ = nullptr;
    overflow_ = false;
    // Don't reset overflow_client_ — it was already moved.
}

} // namespace mosaico
