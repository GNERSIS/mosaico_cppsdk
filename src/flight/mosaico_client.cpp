// src/flight/mosaico_client.cpp
#include "flight/mosaico_client.hpp"

#include "flight/metadata.hpp"

#include <arrow/api.h>
#include <arrow/flight/api.h>
#include <nlohmann/json.hpp>

#include <utility>

namespace fl = arrow::flight;
using json = nlohmann::json;

namespace mosaico {

namespace {

// Strip leading slashes from a string.
std::string stripLeadingSlashes(const std::string& s) {
    size_t pos = s.find_first_not_of('/');
    return pos == std::string::npos ? "" : s.substr(pos);
}

// Build "seq_name/topic_name" resource string.
std::string packResource(const std::string& seq, const std::string& topic) {
    return stripLeadingSlashes(seq) + "/" + stripLeadingSlashes(topic);
}

// Decode a protobuf-encoded ticket to extract the resource locator string.
// The Mosaico server packs the resource locator inside the Flight ticket bytes
// as a length-prefixed string, optionally preceded by a protobuf field tag.
// Observed formats:
//   A) <varint length> <resource UTF-8> [trailing bytes]
//   B) 0x0a <varint length> <resource UTF-8> [trailing bytes]
std::string extractTicketResource(const std::string& ticket_bytes) {
    if (ticket_bytes.size() < 2) return ticket_bytes;

    for (std::size_t start : {std::size_t(1), std::size_t(0)}) {
        if (start >= ticket_bytes.size()) continue;

        std::size_t pos = start;
        uint64_t length = 0;
        unsigned shift = 0;
        while (pos < ticket_bytes.size()) {
            auto b = static_cast<uint8_t>(ticket_bytes[pos]);
            ++pos;
            length |= static_cast<uint64_t>(b & 0x7F) << shift;
            if ((b & 0x80) == 0) break;
            shift += 7;
        }

        if (length == 0 || length > 4096 ||
            pos + length > ticket_bytes.size())
            continue;

        std::string candidate =
            ticket_bytes.substr(pos, static_cast<std::size_t>(length));

        bool valid = true;
        for (unsigned char c : candidate) {
            if (c < 0x20 || c > 0x7E) {
                valid = false;
                break;
            }
        }
        if (valid) return candidate;
    }

    return ticket_bytes;
}

// Parse "seq/topic" -> topic_name (with leading slash restored).
std::string extractTopicFromResource(const std::string& resource,
                                     const std::string& sequence_name) {
    std::string prefix = sequence_name + "/";
    if (resource.rfind(prefix, 0) == 0) {
        return "/" + resource.substr(prefix.size());
    }
    if (resource.rfind(sequence_name, 0) == 0 &&
        resource.size() > sequence_name.size()) {
        return "/" + resource.substr(sequence_name.size() + 1);
    }
    return resource;
}

// Parse endpoint app_metadata JSON for timestamp min/max.
bool parseEndpointMetadata(const std::string& metadata,
                           int64_t& ts_min, int64_t& ts_max) {
    if (metadata.empty()) return false;
    try {
        auto j = json::parse(metadata);
        if (!j.contains("timestamp")) return false;
        const auto& ts = j["timestamp"];
        ts_min = ts.at("min").get<int64_t>();
        ts_max = ts.at("max").get<int64_t>();
        return true;
    } catch (const json::exception&) {
        return false;
    }
}

} // namespace

// ---------------------------------------------------------------------------
// Construction
// ---------------------------------------------------------------------------

MosaicoClient::MosaicoClient(const std::string& server_uri,
                              int timeout_seconds, size_t pool_size)
    : pool_(server_uri, timeout_seconds, pool_size),
      timeout_(timeout_seconds) {}

// ---------------------------------------------------------------------------
// Private helpers
// ---------------------------------------------------------------------------

fl::FlightCallOptions MosaicoClient::callOpts() const {
    fl::FlightCallOptions opts;
    opts.timeout = fl::TimeoutDuration{static_cast<double>(timeout_)};
    return opts;
}

arrow::Status MosaicoClient::doAction(fl::FlightClient* client,
                                      const std::string& action_type,
                                      const std::string& json_body,
                                      std::string* response) {
    fl::Action action;
    action.type = action_type;
    action.body = arrow::Buffer::FromString(json_body);

    auto opts = callOpts();
    ARROW_ASSIGN_OR_RAISE(auto results, client->DoAction(opts, action));

    std::string response_data;
    while (true) {
        ARROW_ASSIGN_OR_RAISE(auto result, results->Next());
        if (!result) break;
        response_data.append(
            reinterpret_cast<const char*>(result->body->data()),
            result->body->size());
    }

    if (response_data.empty()) {
        return arrow::Status::IOError("DoAction '", action_type,
                                      "' returned empty response");
    }

    if (response) {
        *response = std::move(response_data);
    }

    return arrow::Status::OK();
}

// ---------------------------------------------------------------------------
// Discovery
// ---------------------------------------------------------------------------

arrow::Result<std::vector<SequenceInfo>> MosaicoClient::listSequences() {
    ARROW_ASSIGN_OR_RAISE(auto conn, pool_.checkout());
    auto opts = callOpts();

    ARROW_ASSIGN_OR_RAISE(auto listing, conn->ListFlights(opts, {}));

    std::vector<SequenceInfo> sequences;
    while (true) {
        ARROW_ASSIGN_OR_RAISE(auto info, listing->Next());
        if (!info) break;

        for (const auto& path_component : info->descriptor().path) {
            SequenceInfo seq;
            seq.name = path_component;

            // Extract timestamps from endpoints.
            for (const auto& ep : info->endpoints()) {
                int64_t ts_min = 0, ts_max = 0;
                if (parseEndpointMetadata(ep.app_metadata, ts_min, ts_max)) {
                    if (seq.min_ts_ns == 0 || ts_min < seq.min_ts_ns) {
                        seq.min_ts_ns = ts_min;
                    }
                    if (ts_max > seq.max_ts_ns) {
                        seq.max_ts_ns = ts_max;
                    }
                }
            }

            // Extract user metadata from schema metadata.
            auto schema_result = info->GetSchema(nullptr);
            if (schema_result.ok() && *schema_result) {
                auto meta = std::const_pointer_cast<arrow::KeyValueMetadata>(
                    (*schema_result)->metadata());
                seq.user_metadata = extractUserMetadata(meta);
            }

            // Get total_size_bytes via sequence_system_info action.
            std::string sys_response;
            json sys_payload = {{"name", seq.name}};
            auto sys_status = doAction(
                &*conn, "sequence_system_info", sys_payload.dump(),
                &sys_response);
            if (sys_status.ok()) {
                try {
                    auto sys_json = json::parse(sys_response);
                    if (sys_json.contains("response")) {
                        sys_json = sys_json["response"];
                    }
                    if (sys_json.contains("total_size_bytes")) {
                        seq.total_size_bytes =
                            sys_json["total_size_bytes"].get<int64_t>();
                    }
                } catch (const json::exception&) {
                    // Ignore parse errors — size remains 0.
                }
            }

            sequences.push_back(std::move(seq));
        }
    }

    return sequences;
}

arrow::Result<std::vector<TopicInfo>> MosaicoClient::listTopics(
    const std::string& sequence_name) {
    ARROW_ASSIGN_OR_RAISE(auto conn, pool_.checkout());
    auto opts = callOpts();

    // Get sequence-level FlightInfo to discover endpoints (one per topic).
    auto descriptor = fl::FlightDescriptor::Command(
        json{{"resource_locator", sequence_name}}.dump());

    ARROW_ASSIGN_OR_RAISE(auto info, conn->GetFlightInfo(opts, descriptor));

    std::vector<TopicInfo> topics;
    for (const auto& ep : info->endpoints()) {
        TopicInfo topic;
        topic.ticket_bytes = ep.ticket.ticket;

        // Parse topic name from ticket resource locator.
        std::string resource = extractTicketResource(ep.ticket.ticket);
        topic.topic_name =
            extractTopicFromResource(resource, sequence_name);

        // Extract timestamps from endpoint metadata.
        int64_t ts_min = 0, ts_max = 0;
        if (parseEndpointMetadata(ep.app_metadata, ts_min, ts_max)) {
            topic.min_ts_ns = ts_min;
            topic.max_ts_ns = ts_max;
        }

        // Get per-topic schema and metadata via GetFlightInfo.
        auto topic_descriptor = fl::FlightDescriptor::Command(
            json{{"resource_locator",
                  packResource(sequence_name, topic.topic_name)}}
                .dump());

        auto topic_info_result =
            conn->GetFlightInfo(opts, topic_descriptor);
        if (topic_info_result.ok()) {
            auto& topic_info = *topic_info_result;
            auto schema_result = topic_info->GetSchema(nullptr);
            if (schema_result.ok() && *schema_result) {
                topic.schema = *schema_result;
                auto metadata =
                    std::const_pointer_cast<arrow::KeyValueMetadata>(
                        topic.schema->metadata());
                auto tag = extractOntologyTag(metadata);
                if (tag.has_value()) {
                    topic.ontology_tag = *tag;
                }
                topic.user_metadata = extractUserMetadata(metadata);
            }
        }

        topics.push_back(std::move(topic));
    }

    return topics;
}

// ---------------------------------------------------------------------------
// Pull
// ---------------------------------------------------------------------------

arrow::Result<PullResult> MosaicoClient::pullTopic(
    const std::string& sequence_name,
    const std::string& topic_name,
    const TimeRange& range,
    ProgressCallback progress_cb) {
    ARROW_ASSIGN_OR_RAISE(auto conn, pool_.checkout());
    auto opts = callOpts();

    // Build resource locator for the topic.
    std::string resource = packResource(sequence_name, topic_name);

    // Build GetFlightInfo command, with optional time range filter.
    json cmd = {{"resource_locator", resource}};
    if (range.start_ns.has_value()) {
        cmd["timestamp_ns_start"] = *range.start_ns;
    }
    if (range.end_ns.has_value()) {
        cmd["timestamp_ns_end"] = *range.end_ns;
    }

    auto descriptor = fl::FlightDescriptor::Command(cmd.dump());
    ARROW_ASSIGN_OR_RAISE(auto info, conn->GetFlightInfo(opts, descriptor));

    // Need at least one endpoint to get the ticket.
    if (info->endpoints().empty()) {
        return arrow::Status::IOError(
            "GetFlightInfo returned no endpoints for topic: ", resource);
    }

    fl::Ticket ticket;
    ticket.ticket = info->endpoints()[0].ticket.ticket;

    ARROW_ASSIGN_OR_RAISE(auto reader, conn->DoGet(opts, ticket));

    PullResult result;
    ARROW_ASSIGN_OR_RAISE(result.schema, reader->GetSchema());

    int64_t total_rows = 0;
    int64_t total_bytes = 0;

    while (true) {
        ARROW_ASSIGN_OR_RAISE(auto chunk, reader->Next());
        if (!chunk.data) break;
        if (chunk.data->num_rows() == 0) continue;

        total_rows += chunk.data->num_rows();
        for (int i = 0; i < chunk.data->num_columns(); ++i) {
            for (const auto& buf : chunk.data->column(i)->data()->buffers) {
                if (buf) total_bytes += buf->size();
            }
        }

        result.batches.push_back(chunk.data);

        if (progress_cb) progress_cb(total_rows, total_bytes);
    }

    return result;
}

// ---------------------------------------------------------------------------
// Management
// ---------------------------------------------------------------------------

arrow::Status MosaicoClient::deleteSequence(const std::string& name) {
    ARROW_ASSIGN_OR_RAISE(auto conn, pool_.checkout());
    json payload = {{"name", name}};
    return doAction(&*conn, "sequence_delete", payload.dump());
}

// ---------------------------------------------------------------------------
// Upload
// ---------------------------------------------------------------------------

arrow::Result<std::unique_ptr<UploadSession>> MosaicoClient::beginUpload(
    const std::string& sequence_name) {
    ARROW_ASSIGN_OR_RAISE(auto conn, pool_.checkout());

    json payload = {{"name", sequence_name},
                    {"user_metadata", {{"source", "mosaico_sdk"}}}};

    std::string response;
    auto status =
        doAction(&*conn, "sequence_create", payload.dump(), &response);
    if (!status.ok()) return status;

    auto resp = json::parse(response);
    if (resp.contains("response")) resp = resp["response"];
    std::string sequence_key = resp["key"].get<std::string>();

    auto session = std::unique_ptr<UploadSession>(new UploadSession(
        std::move(conn), timeout_, sequence_name, std::move(sequence_key)));

    return session;
}

} // namespace mosaico
