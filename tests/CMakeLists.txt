add_executable(mosaico_sdk_tests
    decoder_test.cpp
    json_decoder_test.cpp
    passthrough_decoder_test.cpp
    msg_parser_test.cpp
    mcap_reader_test.cpp
    mcap_writer_test.cpp
    mcap_roundtrip_test.cpp
    dump_test.cpp
    progress_test.cpp
    ${CMAKE_SOURCE_DIR}/cli/dump.cpp
)

target_sources(mosaico_sdk_tests PRIVATE field_map_test.cpp)
target_sources(mosaico_sdk_tests PRIVATE ontology_registry_test.cpp)
target_sources(mosaico_sdk_tests PRIVATE builder_utils_test.cpp raw_builder_test.cpp)
target_sources(mosaico_sdk_tests PRIVATE geometry_builders_test.cpp)
target_sources(mosaico_sdk_tests PRIVATE imu_builder_test.cpp)
target_sources(mosaico_sdk_tests PRIVATE gps_builder_test.cpp)
target_sources(mosaico_sdk_tests PRIVATE nav_builders_test.cpp)
target_sources(mosaico_sdk_tests PRIVATE sensor_builders_test.cpp)
target_sources(mosaico_sdk_tests PRIVATE std_type_builder_test.cpp)
target_sources(mosaico_sdk_tests PRIVATE metadata_test.cpp)
target_sources(mosaico_sdk_tests PRIVATE connection_pool_test.cpp)
target_sources(mosaico_sdk_tests PRIVATE mosaico_client_test.cpp)
target_sources(mosaico_sdk_tests PRIVATE upload_session_test.cpp)

if(WITH_CDR)
    target_sources(mosaico_sdk_tests PRIVATE cdr_decoder_test.cpp)
endif()

if(WITH_ROS1)
    target_sources(mosaico_sdk_tests PRIVATE ros1_decoder_test.cpp)
endif()

if(WITH_PROTOBUF)
    target_sources(mosaico_sdk_tests PRIVATE protobuf_decoder_test.cpp)
endif()

target_include_directories(mosaico_sdk_tests PRIVATE
    ${CMAKE_SOURCE_DIR}/cli
)

target_link_libraries(mosaico_sdk_tests PRIVATE
    mosaico_sdk
    GTest::gtest_main
)

include(GoogleTest)
gtest_discover_tests(mosaico_sdk_tests)
