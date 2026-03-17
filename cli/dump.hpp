// tools/dump.hpp
#pragma once

#include <cstdint>
#include <string>

// Format a duration in nanoseconds as human-readable (e.g., "2h 14m 32s").
std::string formatDuration(uint64_t duration_ns);

// Format a file size in bytes (e.g., "148.3 MB").
std::string formatFileSize(uint64_t bytes);

// Format a nanosecond timestamp as ISO 8601 UTC (e.g., "2026-03-10T08:12:00Z").
std::string formatTimestampUtc(uint64_t timestamp_ns);

// Dump local MCAP file info to stdout.
int dumpLocal(const std::string& file_path);

// Dump remote sequence info to stdout.
int dumpRemote(const std::string& sequence_name, const std::string& server_uri,
               int timeout_seconds);
