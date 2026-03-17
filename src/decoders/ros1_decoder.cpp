// tools/decoders/ros1_decoder.cpp
#include "decoders/ros1_decoder.hpp"

#include <cstdint>
#include <cstring>
#include <string>

namespace {

// ---------------------------------------------------------------------------
// Simple sequential buffer reader -- no alignment, little-endian.
// ROS1 serialization packs bytes with no padding and no encapsulation header.
// ---------------------------------------------------------------------------
class RosReader {
 public:
  RosReader(const std::byte* data, uint64_t size)
      : data_(data), size_(size), pos_(0) {}

  template <typename T>
  T read() {
    T val{};
    std::memcpy(&val, data_ + pos_, sizeof(T));
    pos_ += sizeof(T);
    return val;
  }

  // ROS1 string: uint32 length, then `length` UTF-8 bytes (no null terminator)
  std::string readString() {
    uint32_t len = read<uint32_t>();
    std::string s(reinterpret_cast<const char*>(data_ + pos_), len);
    pos_ += len;
    return s;
  }

  uint64_t remaining() const { return size_ - pos_; }

 private:
  const std::byte* data_;
  uint64_t size_;
  uint64_t pos_;
};

// ---------------------------------------------------------------------------
// Read a single primitive from ROS1 and return it as a FieldValue.
// ---------------------------------------------------------------------------
FieldValue readPrimitive(RosReader& reader, msg::PrimitiveType prim) {
  switch (prim) {
    case msg::PrimitiveType::kBool:    return reader.read<bool>();
    case msg::PrimitiveType::kInt8:    return reader.read<int8_t>();
    case msg::PrimitiveType::kUint8:   return reader.read<uint8_t>();
    case msg::PrimitiveType::kInt16:   return reader.read<int16_t>();
    case msg::PrimitiveType::kUint16:  return reader.read<uint16_t>();
    case msg::PrimitiveType::kInt32:   return reader.read<int32_t>();
    case msg::PrimitiveType::kUint32:  return reader.read<uint32_t>();
    case msg::PrimitiveType::kInt64:   return reader.read<int64_t>();
    case msg::PrimitiveType::kUint64:  return reader.read<uint64_t>();
    case msg::PrimitiveType::kFloat32: return reader.read<float>();
    case msg::PrimitiveType::kFloat64: return reader.read<double>();
    case msg::PrimitiveType::kString:
    case msg::PrimitiveType::kWstring: return reader.readString();
  }
  return std::string("<unknown>");
}

// Forward declaration.
arrow::Status decodeField(RosReader& reader,
                          const msg::MsgField& field,
                          const msg::MsgSchema& parent_schema,
                          const std::string& prefix,
                          FieldMap& out);

// ---------------------------------------------------------------------------
// Decode all fields of a nested struct into the FieldMap with dot-path prefix.
// ---------------------------------------------------------------------------
arrow::Status decodeStructFields(RosReader& reader,
                                 const msg::MsgSchema& schema,
                                 const std::string& prefix,
                                 FieldMap& out) {
  for (const auto& field : schema.fields) {
    ARROW_RETURN_NOT_OK(decodeField(reader, field, schema, prefix, out));
  }
  return arrow::Status::OK();
}

// ---------------------------------------------------------------------------
// Decode a single element (for use in arrays).
// ---------------------------------------------------------------------------
arrow::Status decodeArrayElement(RosReader& reader,
                                 const msg::MsgField& field,
                                 const msg::MsgSchema& parent_schema,
                                 FieldMap& element_out) {
  if (field.primitive.has_value()) {
    element_out.fields["value"] = readPrimitive(reader, *field.primitive);
    return arrow::Status::OK();
  }
  auto it = parent_schema.nested.find(field.nested_type);
  if (it == parent_schema.nested.end()) {
    return arrow::Status::Invalid("Unresolved nested type: " +
                                  field.nested_type);
  }
  return decodeStructFields(reader, *it->second, "", element_out);
}

// ---------------------------------------------------------------------------
// Decode a single field (scalar, fixed-array, or dynamic-array).
// ---------------------------------------------------------------------------
arrow::Status decodeField(RosReader& reader,
                          const msg::MsgField& field,
                          const msg::MsgSchema& parent_schema,
                          const std::string& prefix,
                          FieldMap& out) {
  std::string key = prefix.empty() ? field.name : prefix + "." + field.name;

  if (field.array_size == 0) {
    // Scalar
    if (field.primitive.has_value()) {
      out.fields[key] = readPrimitive(reader, *field.primitive);
      return arrow::Status::OK();
    }
    // Nested struct -- flatten into dot-path keys
    auto it = parent_schema.nested.find(field.nested_type);
    if (it == parent_schema.nested.end()) {
      return arrow::Status::Invalid("Unresolved nested type: " +
                                    field.nested_type);
    }
    return decodeStructFields(reader, *it->second, key, out);
  }

  // Array (fixed or dynamic)
  int32_t count = field.array_size;
  if (count < 0) {
    // Dynamic array -- read uint32 length prefix
    count = static_cast<int32_t>(reader.read<uint32_t>());
  }

  // For primitive arrays, produce appropriate vector types
  if (field.primitive.has_value()) {
    // uint8 arrays -> vector<uint8_t>
    if (*field.primitive == msg::PrimitiveType::kUint8) {
      std::vector<uint8_t> vals;
      vals.reserve(count);
      for (int32_t i = 0; i < count; ++i) {
        vals.push_back(reader.read<uint8_t>());
      }
      out.fields[key] = std::move(vals);
      return arrow::Status::OK();
    }
    // All other numeric primitives -> vector<double>
    std::vector<double> vals;
    vals.reserve(count);
    for (int32_t i = 0; i < count; ++i) {
      auto fv = readPrimitive(reader, *field.primitive);
      std::visit([&](auto&& v) {
        using T = std::decay_t<decltype(v)>;
        if constexpr (std::is_arithmetic_v<T>) {
          vals.push_back(static_cast<double>(v));
        }
      }, fv);
    }
    out.fields[key] = std::move(vals);
    return arrow::Status::OK();
  }

  // Array of nested structs -> vector<FieldMap>
  msg::MsgField elem_field;
  elem_field.name = field.name;
  elem_field.primitive = field.primitive;
  elem_field.nested_type = field.nested_type;
  elem_field.array_size = 0;

  std::vector<FieldMap> elements;
  elements.reserve(count);
  for (int32_t i = 0; i < count; ++i) {
    FieldMap elem;
    ARROW_RETURN_NOT_OK(
        decodeArrayElement(reader, elem_field, parent_schema, elem));
    elements.push_back(std::move(elem));
  }
  out.fields[key] = std::move(elements);
  return arrow::Status::OK();
}

}  // namespace

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

std::vector<std::string> Ros1Decoder::supportedEncodings() const {
  return {"ros1"};
}

arrow::Status Ros1Decoder::prepare(const DecoderContext& ctx) {
  std::string text(reinterpret_cast<const char*>(ctx.schema_data.data()),
                   ctx.schema_data.size());

  msg_schema_ = msg::parseMsgDef(ctx.schema_name, text);
  if (!msg_schema_) {
    return arrow::Status::Invalid(
        "Failed to parse .msg definition for " + ctx.schema_name);
  }
  return arrow::Status::OK();
}

arrow::Status Ros1Decoder::decode(
    const std::byte* data, uint64_t size,
    const DecoderContext& /*ctx*/,
    FieldMap& out) {
  if (!msg_schema_) {
    return arrow::Status::Invalid("prepare() must be called first");
  }

  RosReader reader(data, size);

  for (const auto& field : msg_schema_->fields) {
    ARROW_RETURN_NOT_OK(
        decodeField(reader, field, *msg_schema_, "", out));
  }

  return arrow::Status::OK();
}
