// tests/tools/dump_test.cpp
#include "dump.hpp"

#include <gtest/gtest.h>
#include <sstream>

TEST(Dump, FormatDuration) {
  // 2h 14m 32s in nanoseconds
  uint64_t start = 0;
  uint64_t end = (2 * 3600 + 14 * 60 + 32) * 1'000'000'000ULL;
  EXPECT_EQ(formatDuration(end - start), "2h 14m 32s");
}

TEST(Dump, FormatDurationMinutesOnly) {
  uint64_t ns = 5 * 60 * 1'000'000'000ULL;
  EXPECT_EQ(formatDuration(ns), "5m 0s");
}

TEST(Dump, FormatDurationSecondsOnly) {
  uint64_t ns = 45 * 1'000'000'000ULL;
  EXPECT_EQ(formatDuration(ns), "45s");
}

TEST(Dump, FormatFileSize) {
  EXPECT_EQ(formatFileSize(0), "0 B");
  EXPECT_EQ(formatFileSize(999), "999 B");
  EXPECT_EQ(formatFileSize(1000), "1.0 KB");
  EXPECT_EQ(formatFileSize(1500), "1.5 KB");
  EXPECT_EQ(formatFileSize(1'000'000), "1.0 MB");
  EXPECT_EQ(formatFileSize(148'300'000), "148.3 MB");
}

TEST(Dump, FormatTimestampUtc) {
  // 2026-03-10T08:12:00Z in nanoseconds from epoch
  uint64_t ts_ns = 1773216720ULL * 1'000'000'000ULL;
  auto result = formatTimestampUtc(ts_ns);
  EXPECT_NE(result.find("2026"), std::string::npos);
  EXPECT_NE(result.find("Z"), std::string::npos);
}
