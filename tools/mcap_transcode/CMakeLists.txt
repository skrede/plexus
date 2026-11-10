# A library, so the gated round-trip test links the same mapping as the CLI.

include(${PROJECT_SOURCE_DIR}/cmake/plexus-mcap.cmake)

add_library(mcap_transcode_lib STATIC
    src/flat_to_mcap.cpp
    src/foxglove_backend.cpp)

target_include_directories(mcap_transcode_lib PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include)

# plexus-hints-robotics is PRIVATE: the vendor backend TU reads the concept vocabulary to translate a hint, but the
# enum never leaks to a consumer of this library.
target_link_libraries(mcap_transcode_lib
    PUBLIC plexus::core
    PRIVATE plexus_mcap_dep plexus::hints-robotics)

# mcap needs only C++17, but the TU includes the C++20 header-only core.
set_target_properties(mcap_transcode_lib PROPERTIES
    CXX_STANDARD 20
    CXX_STANDARD_REQUIRED ON
    CXX_EXTENSIONS OFF)

add_executable(mcap_transcode src/main.cpp)
target_link_libraries(mcap_transcode PRIVATE mcap_transcode_lib)
