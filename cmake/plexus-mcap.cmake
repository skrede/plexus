# MCAP is the log container Foxglove reads. It sits on its own INTERFACE target so it never reaches the header-only
# core or an MCU-bound target. An installed package wins; the fetched header-only source has no CMake package of its
# own, so its include dir is added directly.

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

# The transcode lays raw payload bytes into the container and names the encoding in the Schema, so the lz4 and zstd
# codecs are never linked.
target_compile_definitions(plexus_mcap_dep INTERFACE
    MCAP_COMPRESSION_NO_LZ4
    MCAP_COMPRESSION_NO_ZSTD)
