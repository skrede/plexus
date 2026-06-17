# The host-side flat-stream to pcapng transcode: a reusable mapping object linked by both
# the CLI and the gated round-trip test, plus the standalone CLI binary. Unlike the mcap
# tool there is no third-party dependency target — the pcapng container is hand-rolled here,
# so there is nothing to resolve (no find_package, no FetchContent, no cmake fragment).

add_library(pcap_transcode_lib STATIC src/flat_to_pcap.cpp)

target_include_directories(pcap_transcode_lib PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include)

target_link_libraries(pcap_transcode_lib PUBLIC plexus::core)

set_target_properties(pcap_transcode_lib PROPERTIES
    CXX_STANDARD 20
    CXX_STANDARD_REQUIRED ON
    CXX_EXTENSIONS OFF)

add_executable(pcap_transcode src/main.cpp)
target_link_libraries(pcap_transcode PRIVATE pcap_transcode_lib)
