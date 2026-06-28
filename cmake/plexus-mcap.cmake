# Host-only MCAP container resolution for the flat-stream transcode tool.
#
# mcap is the single-header reference C++ implementation of the MCAP log
# container (the robotics analysis format Foxglove reads). It is resolved only
# when the transcode tool is enabled and is carried on a dedicated INTERFACE
# target so it never reaches the header-only core or any MCU-bound target.
#
# Resolving order mirrors the asio fragment so an installed copy wins:
#   1. find_package(mcap CONFIG)  — an installed/vcpkg mcap target.
#   2. FetchContent at a pinned releases/cpp tag — the header-only source tree
#      has no CMake package of its own, so the include dir is added directly.
#
# Defines the INTERFACE target plexus_mcap_dep carrying the mcap include path
# and MCAP_COMPRESSION_NO_LZ4 / MCAP_COMPRESSION_NO_ZSTD (compression disabled =>
# zero transitive deps; no lz4/zstd find_package).

if(TARGET plexus_mcap_dep)
    return()
endif()

add_library(plexus_mcap_dep INTERFACE)

find_package(mcap CONFIG QUIET)
if(mcap_FOUND AND TARGET mcap::mcap)
    target_link_libraries(plexus_mcap_dep INTERFACE mcap::mcap)
    message(STATUS "plexus: mcap resolved via find_package (mcap::mcap)")
else()
    include(FetchContent)
    FetchContent_Declare(mcap
        GIT_REPOSITORY https://github.com/foxglove/mcap.git
        GIT_TAG releases/cpp/v2.1.3)
    FetchContent_MakeAvailable(mcap)
    target_include_directories(plexus_mcap_dep SYSTEM INTERFACE
        $<BUILD_INTERFACE:${mcap_SOURCE_DIR}/cpp/mcap/include>)
    message(STATUS "plexus: mcap fetched at releases/cpp/v2.1.3")
endif()

# Compression off: the transcode lays raw payload bytes into the container and
# names the encoding in the Schema, so the lz4/zstd codecs are never linked.
target_compile_definitions(plexus_mcap_dep INTERFACE
    MCAP_COMPRESSION_NO_LZ4
    MCAP_COMPRESSION_NO_ZSTD)
