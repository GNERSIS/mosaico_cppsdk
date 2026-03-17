add_executable(mosaico_cli
    cli_main.cpp
    dump.cpp
)

target_include_directories(mosaico_cli PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(mosaico_cli PRIVATE
    mosaico_sdk
    parquet
)
