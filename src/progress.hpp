// tools/progress.hpp
#pragma once

#include <chrono>
#include <cmath>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <unistd.h>

class ProgressBar {
 public:
  explicit ProgressBar(std::ostream& out) : out_(out) {}

  std::string formatBar(const std::string& label, int64_t rows_done,
                        int64_t total_rows, int64_t bytes_per_sec) const {
    std::ostringstream ss;
    ss << padLabel(label);

    double frac = total_rows > 0
                      ? static_cast<double>(rows_done) / static_cast<double>(total_rows)
                      : 0.0;
    frac = std::clamp(frac, 0.0, 1.0);
    int filled = static_cast<int>(std::round(frac * kBarWidth));

    ss << " [";
    for (int i = 0; i < kBarWidth; ++i) {
      ss << (i < filled ? "\xe2\x96\x88" : "\xe2\x96\x91");
    }
    ss << "] ";

    int pct = static_cast<int>(std::round(frac * 100.0));
    ss << std::setw(3) << pct << "%  ";
    ss << formatCount(rows_done) << "/" << formatCount(total_rows) << " rows";

    if (bytes_per_sec > 0) {
      ss << "   " << formatBytes(bytes_per_sec) << "/s";
    }
    return ss.str();
  }

  std::string formatFinished(const std::string& label, bool success,
                             int64_t total_rows, int64_t total_bytes,
                             const std::string& error_msg) const {
    std::ostringstream ss;
    ss << padLabel(label);
    if (success) {
      ss << " done   " << formatCountWithCommas(total_rows) << " rows";
      if (total_bytes > 0) {
        ss << "   " << formatBytes(total_bytes);
      }
    } else {
      ss << " FAILED";
      if (!error_msg.empty()) {
        ss << " \xe2\x80\x94 " << error_msg;
      }
    }
    return ss.str();
  }

  void begin(const std::string& label, int64_t total_rows) {
    label_ = label;
    total_rows_ = total_rows;
    rows_done_ = 0;
    bytes_done_ = 0;
    start_time_ = std::chrono::steady_clock::now();
    is_tty_ = isatty(fileno(stderr));
    if (is_tty_) {
      out_ << formatBar(label_, 0, total_rows_, 0) << std::flush;
    }
  }

  void update(int64_t rows_done, int64_t bytes_done) {
    rows_done_ = rows_done;
    bytes_done_ = bytes_done;
    if (!is_tty_) return;

    auto now = std::chrono::steady_clock::now();
    double elapsed_s = std::chrono::duration<double>(now - start_time_).count();
    int64_t bps = elapsed_s > 0.1
                      ? static_cast<int64_t>(static_cast<double>(bytes_done_) / elapsed_s)
                      : 0;
    out_ << "\r" << formatBar(label_, rows_done_, total_rows_, bps) << std::flush;
  }

  void finish(bool success, const std::string& error_msg = "") {
    if (is_tty_) out_ << "\r";
    out_ << formatFinished(label_, success, rows_done_, bytes_done_, error_msg) << "\n";
  }

 private:
  static constexpr int kBarWidth = 20;
  static constexpr int kLabelWidth = 20;

  std::string padLabel(const std::string& label) const {
    if (static_cast<int>(label.size()) > kLabelWidth) {
      return label.substr(0, kLabelWidth - 1) + "\xe2\x80\xa6";
    }
    std::string padded = label;
    padded.resize(kLabelWidth, ' ');
    return padded;
  }

  static std::string formatCount(int64_t n) {
    if (n >= 1'000'000) {
      std::ostringstream ss;
      ss << std::fixed << std::setprecision(1) << (static_cast<double>(n) / 1'000'000.0) << "M";
      return ss.str();
    }
    if (n >= 10'000) {
      std::ostringstream ss;
      ss << std::fixed << std::setprecision(1) << (static_cast<double>(n) / 1'000.0) << "K";
      return ss.str();
    }
    return std::to_string(n);
  }

  static std::string formatCountWithCommas(int64_t n) {
    auto s = std::to_string(n);
    int insert_pos = static_cast<int>(s.size()) - 3;
    while (insert_pos > 0) {
      s.insert(insert_pos, ",");
      insert_pos -= 3;
    }
    return s;
  }

  static std::string formatBytes(int64_t bytes) {
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

  std::ostream& out_;
  std::string label_;
  int64_t total_rows_ = 0;
  int64_t rows_done_ = 0;
  int64_t bytes_done_ = 0;
  bool is_tty_ = false;
  std::chrono::steady_clock::time_point start_time_;
};
