// cli/cli_main.cpp
#include "dump.hpp"
#include "flight/mosaico_client.hpp"
#include "flight/types.hpp"
#include "mcap_reader.hpp"
#include "mcap_writer.hpp"
#include "ontology/ontology_registry.hpp"
#include "ontology/tag_resolver.hpp"
#include "progress.hpp"

#include <arrow/ipc/api.h>
#include <arrow/io/api.h>
#include <arrow/record_batch.h>
#include <arrow/table.h>
#include <parquet/arrow/writer.h>

#include <atomic>
#include <csignal>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <string>

namespace fs = std::filesystem;

std::atomic<bool> g_interrupted{false};

// ── helpers ────────────────────────────────────────────────────────

static void printUsage(const char* prog) {
  std::cerr << "Usage: " << prog << " <command> [options]\n"
            << "\n"
            << "Commands:\n"
            << "  push   <file.mcap> --server URI [--name NAME] [--timeout SEC]\n"
            << "  pull   <sequence>  --server URI [--output PATH] [--format FORMAT] [--timeout SEC]\n"
            << "  dump   <target>    [--server URI] [--timeout SEC]\n"
            << "  list               --server URI [--timeout SEC]\n"
            << "  delete <sequence>  --server URI [--timeout SEC]\n"
            << "\n"
            << "Options:\n"
            << "  --server, -s URI     Flight server URI (grpc://host:port)\n"
            << "  --name     NAME      Sequence name for push (default: filename)\n"
            << "  --output, -o PATH    Output directory for pull (default: <sequence>/)\n"
            << "  --format, -f FORMAT  Output format: arrow (default), parquet, mcap\n"
            << "  --timeout, -t SEC    RPC timeout in seconds (default: 30)\n"
            << "  --help, -h           Show this help\n";
}

// Return the value following a flag, or empty string if not found.
// Supports both "--flag value" and "--flag=value" forms.
static std::string getArg(int argc, char** argv, const char* long_flag,
                           char short_flag = '\0') {
  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];

    // --flag=value
    std::string prefix = std::string(long_flag) + "=";
    if (arg.rfind(prefix, 0) == 0) {
      return arg.substr(prefix.size());
    }

    // --flag value  or  -X value
    bool match = (arg == long_flag);
    if (!match && short_flag != '\0') {
      match = (arg.size() == 2 && arg[0] == '-' && arg[1] == short_flag);
    }
    if (match && i + 1 < argc) {
      return argv[i + 1];
    }
  }
  return {};
}

static bool hasFlag(int argc, char** argv, const char* long_flag,
                    char short_flag = '\0') {
  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    if (arg == long_flag) return true;
    if (short_flag != '\0' && arg.size() == 2 && arg[0] == '-' &&
        arg[1] == short_flag)
      return true;
  }
  return false;
}

// Find the first positional arg (not a flag or flag value).
// skip_count: how many positionals to skip (0 = first positional after command).
static std::string getPositional(int argc, char** argv, int skip_count = 0) {
  // Flags that consume the next argument.
  auto eatsNext = [](const std::string& a) {
    return a == "--server" || a == "-s" || a == "--name" || a == "--output" ||
           a == "-o" || a == "--format" || a == "-f" || a == "--timeout" || a == "-t";
  };

  int found = 0;
  for (int i = 2; i < argc; ++i) {  // skip argv[0] (program) and argv[1] (command)
    std::string arg = argv[i];
    if (arg[0] == '-') {
      // Skip flag value if this flag eats the next arg.
      if (eatsNext(arg) && i + 1 < argc) ++i;
      continue;
    }
    if (found == skip_count) return arg;
    ++found;
  }
  return {};
}

// ── commands ───────────────────────────────────────────────────────

static int runPush(const std::string& mcap_path, const std::string& server_uri,
                   const std::string& name_override, int timeout) {
  if (!fs::exists(mcap_path)) {
    std::cerr << "Error: file not found: " << mcap_path << "\n";
    return 1;
  }

  // 1. Index MCAP channels
  McapChannelIndex index;
  auto status = indexMcapChannels(mcap_path, index);
  if (!status.ok()) {
    std::cerr << "Error reading MCAP: " << status.ToString() << "\n";
    return 1;
  }

  std::string seq_name = name_override.empty()
                             ? fs::path(mcap_path).stem().string()
                             : name_override;

  std::cerr << "Pushing '" << mcap_path << "' as sequence '" << seq_name
            << "' (" << index.channels.size() << " channels)\n";

  // 2. Set up ontology pipeline
  auto resolver = createRosTagResolver();
  auto registry = createDefaultRegistry();

  // 3. Open server session
  mosaico::MosaicoClient client(server_uri, timeout, 1);
  auto upload_result = client.beginUpload(seq_name);
  if (!upload_result.ok()) {
    std::cerr << "Error creating sequence: " << upload_result.status().ToString() << "\n";
    return 1;
  }
  auto& upload = *upload_result;

  int topics_uploaded = 0, topics_skipped = 0;

  // 4. Per-channel loop
  for (const auto& channel : index.channels) {
    if (g_interrupted.load(std::memory_order_relaxed)) break;

    // Resolve ontology tag
    auto ontology_tag = resolver.resolve(channel.schema_name);
    OntologyBuilder* builder = nullptr;
    if (ontology_tag) {
      builder = registry.find(*ontology_tag);
    }

    // Skip channels with no ontology adapter
    if (!builder) {
      std::cerr << "  Skipping " << channel.topic << " (no adapter for "
                << channel.schema_name << ")\n";
      topics_skipped++;
      continue;
    }

    status = upload->createTopic(channel.topic, builder->ontologyTag(),
                                  builder->schema());
    if (!status.ok()) {
      std::cerr << "  FAILED to create topic " << channel.topic << ": "
                << status.ToString() << "\n";
      continue;
    }

    ProgressBar bar(std::cerr);
    bar.begin(channel.topic, channel.message_count);

    status = decodeMcapChannel(
        mcap_path, channel,
        [&](const FieldMap& fields) -> arrow::Status {
          if (g_interrupted.load(std::memory_order_relaxed))
            return arrow::Status::Cancelled("interrupted");
          ARROW_RETURN_NOT_OK(builder->append(fields));
          if (builder->shouldFlush()) {
            ARROW_ASSIGN_OR_RAISE(auto batch, builder->flush());
            if (batch) ARROW_RETURN_NOT_OK(upload->putBatch(batch));
          }
          return arrow::Status::OK();
        },
        [&](int64_t rows, int64_t bytes) { bar.update(rows, bytes); });

    if (!status.ok()) {
      bar.finish(false, status.ToString());
      (void)upload->closeTopic();
      continue;
    }

    // Flush remaining
    auto batch_result = builder->flush();
    if (batch_result.ok() && *batch_result) {
      auto put_status = upload->putBatch(*batch_result);
      if (!put_status.ok()) {
        bar.finish(false, put_status.ToString());
        (void)upload->closeTopic();
        continue;
      }
    }

    bar.finish(true);
    topics_uploaded++;
    (void)upload->closeTopic();
  }

  // 5. Finalize or cleanup
  if (g_interrupted.load(std::memory_order_relaxed)) {
    (void)upload->cleanup();
    std::cerr << "\nInterrupted.\n";
    return 1;
  }

  status = upload->finalize();

  std::cerr << "\nPush complete: " << topics_uploaded << " topics uploaded";
  if (topics_skipped > 0) std::cerr << ", " << topics_skipped << " skipped";
  std::cerr << "\n";

  if (status.ok()) {
    std::cerr << "Sequence '" << seq_name << "' finalized.\n";
  } else {
    std::cerr << "Error finalizing: " << status.ToString() << "\n";
  }

  return status.ok() ? 0 : 1;
}

// Sanitize a topic name for use as a filename: replace '/' with '_'.
static std::string sanitizeName(const std::string& name) {
  std::string result = name;
  for (char& c : result) {
    if (c == '/') c = '_';
  }
  // Strip leading underscore if the topic started with '/'.
  if (!result.empty() && result[0] == '_') result = result.substr(1);
  return result;
}

static int runPull(const std::string& sequence_name,
                   const std::string& server_uri,
                   const std::string& output_path,
                   const std::string& format,
                   int timeout) {
  if (format == "mcap") {
    std::cerr << "Error: MCAP output not yet supported — reverse adapters pending\n";
    return 1;
  }

  if (format != "arrow" && format != "parquet") {
    std::cerr << "Error: unknown format '" << format
              << "'. Valid formats: arrow, parquet, mcap\n";
    return 1;
  }

  // Output directory: user-specified or default to <sequence_name>/
  std::string out_dir = output_path.empty() ? sequence_name : output_path;

  mosaico::MosaicoClient client(server_uri, timeout, 2);
  auto topics_result = client.listTopics(sequence_name);
  if (!topics_result.ok()) {
    std::cerr << "Error: " << topics_result.status().ToString() << "\n";
    return 1;
  }
  auto& topics = *topics_result;

  std::cerr << "Pulling sequence '" << sequence_name << "' ("
            << topics.size() << " topics) -> " << out_dir
            << " [" << format << "]\n";

  // Create output directory
  std::error_code ec;
  fs::create_directories(out_dir, ec);
  if (ec) {
    std::cerr << "Error: cannot create output directory '" << out_dir
              << "': " << ec.message() << "\n";
    return 1;
  }

  ProgressBar bar(std::cerr);
  int topics_written = 0;

  for (const auto& topic : topics) {
    if (g_interrupted.load(std::memory_order_relaxed)) break;

    bar.begin(topic.topic_name, 0);

    auto pull_result = client.pullTopic(
        sequence_name, topic.topic_name, {},
        [&](int64_t rows, int64_t bytes) { bar.update(rows, bytes); });

    if (!pull_result.ok()) {
      bar.finish(false, pull_result.status().ToString());
      std::cerr << "Warning: failed to pull " << topic.topic_name << "\n";
      continue;
    }

    auto& pulled = *pull_result;
    auto& schema = pulled.schema;
    auto& batches = pulled.batches;

    int64_t total_rows = 0;
    for (const auto& b : batches) total_rows += b->num_rows();
    bar.update(total_rows, 0);
    bar.finish(true);

    if (batches.empty()) {
      std::cerr << "  Skipping " << topic.topic_name << " (no data)\n";
      continue;
    }

    std::string safe_name = sanitizeName(topic.topic_name);
    std::string ext = (format == "arrow") ? ".arrow" : ".parquet";
    std::string file_path = out_dir + "/" + safe_name + ext;

    if (format == "arrow") {
      // Write Arrow IPC file
      auto open_result = arrow::io::FileOutputStream::Open(file_path);
      if (!open_result.ok()) {
        std::cerr << "Error: cannot open " << file_path << ": "
                  << open_result.status().ToString() << "\n";
        continue;
      }
      auto out_file = *open_result;

      auto writer_result = arrow::ipc::MakeFileWriter(out_file, schema);
      if (!writer_result.ok()) {
        std::cerr << "Error: cannot create IPC writer for " << file_path
                  << ": " << writer_result.status().ToString() << "\n";
        continue;
      }
      auto writer = *writer_result;

      bool write_ok = true;
      for (const auto& batch : batches) {
        auto ws = writer->WriteRecordBatch(*batch);
        if (!ws.ok()) {
          std::cerr << "Error writing batch: " << ws.ToString() << "\n";
          write_ok = false;
          break;
        }
      }

      auto close_status = writer->Close();
      if (!close_status.ok()) {
        std::cerr << "Error closing writer: " << close_status.ToString() << "\n";
        write_ok = false;
      }

      if (write_ok) {
        std::cerr << "  Wrote " << file_path << " ("
                  << formatFileSize(fs::file_size(file_path)) << ")\n";
        topics_written++;
      }

    } else {
      // format == "parquet"
      // Combine batches into a Table
      auto table_result = arrow::Table::FromRecordBatches(schema, batches);
      if (!table_result.ok()) {
        std::cerr << "Error: cannot build Table for " << topic.topic_name
                  << ": " << table_result.status().ToString() << "\n";
        continue;
      }
      auto table = *table_result;

      auto open_result = arrow::io::FileOutputStream::Open(file_path);
      if (!open_result.ok()) {
        std::cerr << "Error: cannot open " << file_path << ": "
                  << open_result.status().ToString() << "\n";
        continue;
      }
      auto out_file = *open_result;

      auto write_status = parquet::arrow::WriteTable(
          *table, arrow::default_memory_pool(), out_file);
      if (!write_status.ok()) {
        std::cerr << "Error writing Parquet for " << topic.topic_name << ": "
                  << write_status.ToString() << "\n";
        continue;
      }

      std::cerr << "  Wrote " << file_path << " ("
                << formatFileSize(fs::file_size(file_path)) << ")\n";
      topics_written++;
    }
  }

  if (g_interrupted.load(std::memory_order_relaxed)) {
    std::cerr << "\nInterrupted.\n";
    return 1;
  }

  if (topics_written == 0) {
    std::cerr << "Error: no topics were written\n";
    return 1;
  }

  std::cerr << "\nPull complete: " << topics_written << " topic(s) written to "
            << out_dir << "/\n";
  return 0;
}

static int runDump(const std::string& target, const std::string& server_uri,
                   int timeout) {
  if (fs::exists(target)) {
    return dumpLocal(target);
  }
  if (server_uri.empty()) {
    std::cerr << "Error: '" << target
              << "' is not a local file. Use --server to dump a remote "
                 "sequence.\n";
    return 2;
  }
  return dumpRemote(target, server_uri, timeout);
}

static int runList(const std::string& server_uri, int timeout) {
  mosaico::MosaicoClient client(server_uri, timeout, 1);
  auto result = client.listSequences();
  if (!result.ok()) {
    std::cerr << "Error: " << result.status().ToString() << "\n";
    return 1;
  }

  for (const auto& seq : *result) {
    std::cout << seq.name << "\n";
  }
  std::cerr << result->size() << " sequence(s)\n";
  return 0;
}

static int runDelete(const std::string& sequence_name,
                     const std::string& server_uri, int timeout) {
  mosaico::MosaicoClient client(server_uri, timeout, 1);
  auto status = client.deleteSequence(sequence_name);
  if (!status.ok()) {
    std::cerr << "Error: " << status.ToString() << "\n";
    return 1;
  }

  std::cerr << "Deleted sequence '" << sequence_name << "'\n";
  return 0;
}

// ── main ───────────────────────────────────────────────────────────

static void sigintHandler(int /*sig*/) {
  g_interrupted.store(true, std::memory_order_relaxed);
}

int main(int argc, char** argv) {
  std::signal(SIGINT, sigintHandler);

  if (argc < 2 || hasFlag(argc, argv, "--help", 'h')) {
    printUsage(argv[0]);
    return (argc < 2) ? 2 : 0;
  }

  std::string command = argv[1];
  std::string server = getArg(argc, argv, "--server", 's');
  std::string timeout_str = getArg(argc, argv, "--timeout", 't');
  int timeout = timeout_str.empty() ? 30 : std::atoi(timeout_str.c_str());

  if (command == "push") {
    std::string file = getPositional(argc, argv);
    if (file.empty()) {
      std::cerr << "Error: push requires a file argument\n";
      return 2;
    }
    if (server.empty()) {
      std::cerr << "Error: push requires --server\n";
      return 2;
    }
    std::string name = getArg(argc, argv, "--name");
    return runPush(file, server, name, timeout);
  }

  if (command == "pull") {
    std::string seq = getPositional(argc, argv);
    if (seq.empty()) {
      std::cerr << "Error: pull requires a sequence name\n";
      return 2;
    }
    if (server.empty()) {
      std::cerr << "Error: pull requires --server\n";
      return 2;
    }
    std::string output = getArg(argc, argv, "--output", 'o');
    std::string format = getArg(argc, argv, "--format", 'f');
    if (format.empty()) format = "arrow";
    return runPull(seq, server, output, format, timeout);
  }

  if (command == "dump") {
    std::string target = getPositional(argc, argv);
    if (target.empty()) {
      std::cerr << "Error: dump requires a target (file path or sequence name)\n";
      return 2;
    }
    return runDump(target, server, timeout);
  }

  if (command == "list") {
    if (server.empty()) {
      std::cerr << "Error: list requires --server\n";
      return 2;
    }
    return runList(server, timeout);
  }

  if (command == "delete") {
    std::string seq = getPositional(argc, argv);
    if (seq.empty()) {
      std::cerr << "Error: delete requires a sequence name\n";
      return 2;
    }
    if (server.empty()) {
      std::cerr << "Error: delete requires --server\n";
      return 2;
    }
    return runDelete(seq, server, timeout);
  }

  std::cerr << "Error: unknown command '" << command << "'\n";
  printUsage(argv[0]);
  return 2;
}
