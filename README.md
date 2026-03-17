# Mosaico C++ SDK

C++ SDK for transferring timeseries data between local MCAP files and a Mosaico Arrow Flight server.

## Features

- Read MCAP files with multiple encoding support (CDR, ROS1, Protobuf, JSON, FlatBuffer, CBOR)
- Decode messages into Apache Arrow RecordBatches
- Push data to a Mosaico Arrow Flight server
- Pull data from the server back to MCAP files
- CLI tool for push, pull, dump, list, and delete operations

## Dependencies

```bash
# Conan 2.x (C++ package manager)
pip install conan
conan profile detect
```

C++ libraries (Arrow, nlohmann_json, Fast-CDR, GTest) are managed by Conan.

## Build

```bash
conan install . --build=missing -s build_type=Debug
cmake --preset conan-debug -DWITH_ALL_DECODERS=ON
cmake --build build/Debug -j$(nproc)
```

### Decoder Flags

| Flag | Encoding | Dependency | Description |
|------|----------|------------|-------------|
| (always on) | `json` | — | JSON-encoded MCAP messages |
| (always on) | `flatbuffer`, `cbor` | — | Passthrough (raw bytes preserved) |
| `-DWITH_CDR=ON` | `cdr` | `fast-cdr` | ROS2 CDR serialization |
| `-DWITH_ROS1=ON` | `ros1` | — | ROS1 serialization |
| `-DWITH_PROTOBUF=ON` | `protobuf` | `libprotobuf` (via Arrow) | Protobuf serialization |
| `-DWITH_ALL_DECODERS=ON` | all above | all above | Convenience flag |

## CLI Tool

```bash
SERVER="grpc://37.27.89.131:6726"

# List sequences on the server
mosaico_cli list --server $SERVER

# Push an MCAP file to the server
mosaico_cli push recording.mcap --server $SERVER
mosaico_cli push recording.mcap --server $SERVER --name my_sequence

# Pull a sequence from the server to an MCAP file
mosaico_cli pull my_sequence --server $SERVER
mosaico_cli pull my_sequence --server $SERVER --output /path/to/output.mcap

# Inspect a local MCAP file or remote sequence
mosaico_cli dump recording.mcap
mosaico_cli dump my_sequence --server $SERVER

# Delete a sequence (unfinalized sequences only)
mosaico_cli delete my_sequence --server $SERVER
```

Ctrl+C during upload cleanly interrupts and removes the partial sequence from the server.

## Tests

```bash
cmake --preset conan-debug -DWITH_ALL_DECODERS=ON -DBUILD_TESTING=ON
cmake --build build/Debug -j$(nproc)
ctest --test-dir build/Debug --output-on-failure
```

## Project Structure

```
src/            Core SDK library (mosaico_sdk) — decoders, MCAP I/O, Flight RPC
cli/            CLI executable (mosaico_cli) — push, pull, dump, list, delete
tests/          Unit tests
third_party/    Vendored MCAP header-only library
datasets/       Test MCAP files (gitignored)
reference/      Python SDK for reference
```
