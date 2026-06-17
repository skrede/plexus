# The host-side flat-stream to MCAP transcode: a reusable mapping object linked by
# both the CLI and the gated round-trip test, plus the standalone CLI binary. mcap
# is resolved here on the dedicated INTERFACE target so it never reaches the core.

include(${PROJECT_SOURCE_DIR}/cmake/plexus-mcap.cmake)

add_library(mcap_transcode_lib STATIC src/flat_to_mcap.cpp)

target_include_directories(mcap_transcode_lib PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include)

target_link_libraries(mcap_transcode_lib
    PUBLIC plexus::core
    PRIVATE plexus_mcap_dep)

# The TU compiles at the project C++20 (it includes the C++20 header-only core);
# mcap requires only C++17, so C++20 satisfies both.
set_target_properties(mcap_transcode_lib PROPERTIES
    CXX_STANDARD 20
    CXX_STANDARD_REQUIRED ON
    CXX_EXTENSIONS OFF)

add_executable(mcap_transcode src/main.cpp)
target_link_libraries(mcap_transcode PRIVATE mcap_transcode_lib)
