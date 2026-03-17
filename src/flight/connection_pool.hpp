// src/flight/connection_pool.hpp
#pragma once

#include <arrow/flight/api.h>
#include <arrow/result.h>

#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace mosaico {

/// Thread-safe connection pool for Arrow Flight clients.
///
/// Manages a fixed set of reusable connections plus overflow connections
/// created on demand when all pooled slots are in use. Connections are
/// created lazily on first checkout of each slot.
class ConnectionPool {
 public:
    ConnectionPool(const std::string& server_uri, int timeout_seconds,
                   size_t pool_size);
    ~ConnectionPool();

    ConnectionPool(const ConnectionPool&) = delete;
    ConnectionPool& operator=(const ConnectionPool&) = delete;

    /// RAII handle returned by checkout(). Returns the connection to the
    /// pool on destruction (or destroys it if overflow).
    class Handle {
     public:
        Handle(Handle&& other) noexcept;
        Handle& operator=(Handle&& other) noexcept;
        ~Handle();

        Handle(const Handle&) = delete;
        Handle& operator=(const Handle&) = delete;

        arrow::flight::FlightClient* operator->() const;
        arrow::flight::FlightClient& operator*() const;
        bool valid() const;

     private:
        friend class ConnectionPool;
        // Pooled connection constructor.
        Handle(ConnectionPool* pool, size_t index,
               arrow::flight::FlightClient* client);
        // Overflow connection constructor.
        explicit Handle(
            std::unique_ptr<arrow::flight::FlightClient> overflow_client);

        void reset() noexcept;

        ConnectionPool* pool_ = nullptr;
        size_t index_ = 0;
        arrow::flight::FlightClient* client_ = nullptr;
        std::unique_ptr<arrow::flight::FlightClient> overflow_client_;
        bool overflow_ = false;
    };

    /// Check out a Flight client connection.
    ///
    /// If a pooled slot is available, returns a handle to it (creating the
    /// underlying connection lazily if needed). If all slots are in use,
    /// creates a temporary overflow connection that is destroyed when the
    /// handle goes out of scope.
    arrow::Result<Handle> checkout();

    /// Current pool capacity (fixed at construction).
    size_t poolSize() const { return pool_size_; }

 private:
    friend class Handle;
    void returnConnection(size_t index);
    arrow::Result<std::unique_ptr<arrow::flight::FlightClient>>
    createConnection();

    std::string server_uri_;
    int timeout_;
    size_t pool_size_;
    std::mutex mutex_;
    std::vector<std::unique_ptr<arrow::flight::FlightClient>> clients_;
    std::vector<bool> in_use_;
};

} // namespace mosaico
