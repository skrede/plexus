# A library, so the gated round-trip test links the same mapping as the CLI. The pcapng container is hand-rolled, so
# there is no third-party dependency to resolve.

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
