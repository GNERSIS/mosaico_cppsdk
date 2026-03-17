// src/flight/mosaico_client.hpp
#pragma once

#include "flight/connection_pool.hpp"
#include "flight/types.hpp"
#include "flight/upload_session.hpp"

#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/type_fwd.h>

#include <memory>
#include <string>
#include <vector>

namespace mosaico {

class MosaicoClient {
 public:
    MosaicoClient(const std::string& server_uri, int timeout_seconds,
                  size_t pool_size = 4);

    // Discovery
    arrow::Result<std::vector<SequenceInfo>> listSequences();
    arrow::Result<std::vector<TopicInfo>> listTopics(
        const std::string& sequence_name);

    // Pull
    arrow::Result<PullResult> pullTopic(
        const std::string& sequence_name,
        const std::string& topic_name,
        const TimeRange& range = {},
        ProgressCallback progress_cb = nullptr);

    // Management
    arrow::Status deleteSequence(const std::string& name);

    // Upload
    arrow::Result<std::unique_ptr<UploadSession>> beginUpload(
        const std::string& sequence_name);

 private:
    arrow::Status doAction(arrow::flight::FlightClient* client,
                           const std::string& action_type,
                           const std::string& json_body,
                           std::string* response = nullptr);
    arrow::flight::FlightCallOptions callOpts() const;

    ConnectionPool pool_;
    int timeout_;
};

} // namespace mosaico
