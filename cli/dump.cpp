// cli/dump.cpp
#include "dump.hpp"
#include "flight/mosaico_client.hpp"
#include "flight/types.hpp"
#include "mcap_reader.hpp"

#include <ctime>
#include <iomanip>
#include <iostream>
#include <sstream>

std::string formatDuration(uint64_t duration_ns) {
  uint64_t total_seconds = duration_ns / 1'000'000'000ULL;
  uint64_t hours = total_seconds / 3600;
  uint64_t minutes = (total_seconds % 3600) / 60;
  uint64_t seconds = total_seconds % 60;

  std::ostringstream ss;
  if (hours > 0) ss << hours << "h " << minutes << "m " << seconds << "s";
  else if (minutes > 0) ss << minutes << "m " << seconds << "s";
  else ss << seconds << "s";
  return ss.str();
}

std::string formatFileSize(uint64_t bytes) {
  if (bytes >= 1'000'000'000) {
    std::ostringstream ss;
    ss << std::fixed << std::setprecision(1) << (static_cast<double>(bytes) / 1e9) << " GB";
    return ss.str();
  }
  if (bytes >= 1'000'000) {
    std::ostringstream ss;
    ss << std::fixed << std::setprecision(1) << (static_cast<double>(bytes) / 1e6) << " MB";
    return ss.str();
  }
  if (bytes >= 1'000) {
    std::ostringstream ss;
    ss << std::fixed << std::setprecision(1) << (static_cast<double>(bytes) / 1e3) << " KB";
    return ss.str();
  }
  return std::to_string(bytes) + " B";
}

std::string formatTimestampUtc(uint64_t timestamp_ns) {
  time_t seconds = static_cast<time_t>(timestamp_ns / 1'000'000'000ULL);
  struct tm tm_buf{};
  gmtime_r(&seconds, &tm_buf);
  char buf[32];
  strftime(buf, sizeof(buf), "%Y-%m-%dT%H:%M:%SZ", &tm_buf);
  return buf;
}

int dumpLocal(const std::string& file_path) {
  McapChannelIndex index;
  auto status = indexMcapChannels(file_path, index);
  if (!status.ok()) {
    std::cerr << "Error: " << status.ToString() << "\n";
    return 1;
  }

  std::cout << "Source: local file " << file_path
            << " (" << formatFileSize(index.file_size_bytes) << ")\n";
  std::cout << "Channels: " << index.channels.size() << "\n";

  if (index.start_time_ns > 0 && index.end_time_ns > 0) {
    uint64_t duration = index.end_time_ns - index.start_time_ns;
    std::cout << "Duration: " << formatDuration(duration)
              << " (" << formatTimestampUtc(index.start_time_ns)
              << " \xe2\x86\x92 " << formatTimestampUtc(index.end_time_ns) << ")\n";
  }

  std::cout << "\n";
  std::cout << std::right << std::setw(2) << "#" << "  "
            << std::left << std::setw(22) << "Channel"
            << std::left << std::setw(10) << "Encoding"
            << std::right << std::setw(12) << "Messages" << "   "
            << "Schema\n";

  int num = 1;
  for (const auto& ch : index.channels) {
    std::cout << std::right << std::setw(2) << num++ << "  "
              << std::left << std::setw(22) << ch.topic
              << std::left << std::setw(10) << ch.encoding
              << std::right << std::setw(12) << ch.message_count << "   "
              << ch.schema_name << "\n";
  }

  return 0;
}

int dumpRemote(const std::string& sequence_name, const std::string& server_uri,
               int timeout_seconds) {
  mosaico::MosaicoClient client(server_uri, timeout_seconds, 1);
  auto topics_result = client.listTopics(sequence_name);
  if (!topics_result.ok()) {
    std::cerr << "Error: " << topics_result.status().ToString() << "\n";
    return 1;
  }
  auto& topics = *topics_result;

  std::cout << "Source: remote sequence '" << sequence_name
            << "' at " << server_uri << "\n";
  std::cout << "Topics: " << topics.size() << "\n";

  // Find global time range
  int64_t global_min = 0, global_max = 0;
  for (const auto& t : topics) {
    if (t.min_ts_ns > 0 && (global_min == 0 || t.min_ts_ns < global_min))
      global_min = t.min_ts_ns;
    if (t.max_ts_ns > global_max) global_max = t.max_ts_ns;
  }
  if (global_min > 0 && global_max > 0) {
    std::cout << "Time range: " << formatTimestampUtc(global_min)
              << " \xe2\x86\x92 " << formatTimestampUtc(global_max) << "\n";
  }

  std::cout << "\n";
  std::cout << std::right << std::setw(2) << "#" << "  "
            << std::left << std::setw(22) << "Topic"
            << std::right << std::setw(7) << "Fields"
            << std::right << std::setw(16) << "Timestamp range" << "\n";

  int num = 1;
  for (const auto& t : topics) {
    std::string duration_str = "\xe2\x80\x94";  // em dash
    if (t.min_ts_ns > 0 && t.max_ts_ns > 0) {
      duration_str = formatDuration(t.max_ts_ns - t.min_ts_ns);
    }
    int num_fields = t.schema ? t.schema->num_fields() : 0;
    std::string fields_str = num_fields > 0 ? std::to_string(num_fields) : "\xe2\x80\x94";
    std::cout << std::right << std::setw(2) << num++ << "  "
              << std::left << std::setw(22) << t.topic_name
              << std::right << std::setw(7) << fields_str
              << std::right << std::setw(16) << duration_str << "\n";
  }

  return 0;
}
