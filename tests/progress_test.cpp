// tests/tools/progress_test.cpp
#include "progress.hpp"

#include <gtest/gtest.h>
#include <sstream>

TEST(ProgressBar, FormatPercentageZero) {
  ProgressBar bar(std::cerr);
  // setw(3) on 0 → "  0", preceded by "] " → "]   0%"
  EXPECT_EQ(bar.formatBar("test", 0, 100, 0),
            "test                 [\xe2\x96\x91\xe2\x96\x91\xe2\x96\x91\xe2\x96\x91\xe2\x96\x91\xe2\x96\x91\xe2\x96\x91\xe2\x96\x91\xe2\x96\x91\xe2\x96\x91\xe2\x96\x91\xe2\x96\x91\xe2\x96\x91\xe2\x96\x91\xe2\x96\x91\xe2\x96\x91\xe2\x96\x91\xe2\x96\x91\xe2\x96\x91\xe2\x96\x91]   0%  0/100 rows");
}

TEST(ProgressBar, FormatPercentageFull) {
  ProgressBar bar(std::cerr);
  EXPECT_EQ(bar.formatBar("test", 100, 100, 0),
            "test                 [\xe2\x96\x88\xe2\x96\x88\xe2\x96\x88\xe2\x96\x88\xe2\x96\x88\xe2\x96\x88\xe2\x96\x88\xe2\x96\x88\xe2\x96\x88\xe2\x96\x88\xe2\x96\x88\xe2\x96\x88\xe2\x96\x88\xe2\x96\x88\xe2\x96\x88\xe2\x96\x88\xe2\x96\x88\xe2\x96\x88\xe2\x96\x88\xe2\x96\x88] 100%  100/100 rows");
}

TEST(ProgressBar, FormatPercentagePartial) {
  ProgressBar bar(std::cerr);
  auto line = bar.formatBar("test", 50, 100, 0);
  EXPECT_NE(line.find("50%"), std::string::npos);
  EXPECT_NE(line.find("50/100"), std::string::npos);
}

TEST(ProgressBar, FormatLargeNumbers) {
  ProgressBar bar(std::cerr);
  auto line = bar.formatBar("test", 500'000, 1'000'000, 1024 * 1024 * 10);
  EXPECT_NE(line.find("500.0K"), std::string::npos);
  EXPECT_NE(line.find("1.0M"), std::string::npos);
}

TEST(ProgressBar, TruncateLongLabel) {
  ProgressBar bar(std::cerr);
  auto line = bar.formatBar("/very/long/topic/name/here", 10, 100, 0);
  EXPECT_NE(line.find("\xe2\x80\xa6"), std::string::npos);  // …
}

TEST(ProgressBar, FormatFinishedSuccess) {
  ProgressBar bar(std::cerr);
  auto line = bar.formatFinished("/topic", true, 45120, 8200000, "");
  EXPECT_NE(line.find("done"), std::string::npos);
  EXPECT_NE(line.find("45,120"), std::string::npos);
}

TEST(ProgressBar, FormatFinishedFailure) {
  ProgressBar bar(std::cerr);
  auto line = bar.formatFinished("/topic", false, 1000, 0, "connection reset");
  EXPECT_NE(line.find("FAILED"), std::string::npos);
  EXPECT_NE(line.find("connection reset"), std::string::npos);
}
