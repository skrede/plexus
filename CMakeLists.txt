cmake_minimum_required(VERSION 3.25)
project(plexus VERSION 0.2.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Backends and extras are opt-in, gated targets — never required by the core.
option(PLEXUS_ENABLE_ASIO_BACKEND "Build the asio executor/socket/timer backend" OFF)
# Declared at root (ahead of the fragment globs below) so every fragment sees it:
# the asio backend's gated shm fragment must read the correct value on a fresh
# configure regardless of the order the lib/ and backends/ fragments are processed.
option(PLEXUS_ENABLE_SHM_BACKEND "Build the gated POSIX shared-memory backend (linux-first)" OFF)
option(PLEXUS_BUILD_TESTS "Build the test suite" OFF)
option(PLEXUS_BUILD_FUZZ_TESTS "Build the libFuzzer harnesses" OFF)
option(PLEXUS_BUILD_EXAMPLES "Build the examples" OFF)
option(PLEXUS_BUILD_MCAP_TRANSCODE "Build the host-side flat-stream to MCAP transcode tool" OFF)
option(PLEXUS_BUILD_PCAP_TRANSCODE "Build the host-side flat-stream to pcapng transcode tool" OFF)
option(PLEXUS_BUILD_BENCHMARKS "Build the standalone scalability benchmarks" OFF)
option(PLEXUS_WARNINGS_AS_ERRORS "Promote warnings to errors (the enforced gate)" ON)

# Warn broadly on every compiled translation unit the build owns (the gated backends
# and the test suite; the INTERFACE cores carry no TUs of their own). Applied before
# the fragment glob so each discovered target inherits it. The warning-flag syntax is
# per compiler: MSVC takes /W4, GCC/Clang take -Wall -Wextra.
if(MSVC)
    add_compile_options(/W4 /permissive- /Zc:__cplusplus)
    # Enable warnings-as-errors for MSVC once the build is warning-clean under /W4.
    # add_compile_options(/WX)
else()
    add_compile_options(-Wall -Wextra)
    # -Werror promotes those warnings to the enforced gate: the build is clean under it
    # today, so any newly introduced warning fails the build going forward. -Werror is
    # opt-OUTable (default ON, so every ordinary tree enforces it) because a
    # ThreadSanitizer tree emits an inherent, uninstrumentable -Wtsan diagnostic on
    # asio's atomic_thread_fence (a property of running asio under tsan, not a plexus
    # defect — see the tsan tree notes); that one tree configures with the gate OFF so
    # the race-detection run is not blocked by a diagnostic the authoritative GCC tree
    # never sees.
    if(PLEXUS_WARNINGS_AS_ERRORS)
        add_compile_options(-Werror)
    endif()
endif()

# Project-wide library registration contract: every lib/<target>/ (Tier-1 core +
# contracts and Tier-2 mechanism-free family logic) and every backends/<target>/
# (Tier-3 OS/library mechanisms) owns a CMakeLists.txt fragment and is
# auto-discovered here. The dependency arrow is one-way: backends/ depends on
# lib/, never the reverse. The glob is target-kind-agnostic — INTERFACE cores
# (plexus, plexus-wire, plexus-inproc) and gated compiled backends (plexus-asio,
# plexus-crypto) are discovered the same way; each fragment self-gates (an
# INTERFACE target is unconditional, a compiled backend defines its target only
# when its option is ON). New targets register by adding their fragment, never by
# editing this file.
file(GLOB PLEXUS_LIB_FRAGMENTS CONFIGURE_DEPENDS
    ${CMAKE_CURRENT_SOURCE_DIR}/lib/*/CMakeLists.txt)
file(GLOB PLEXUS_BACKEND_FRAGMENTS CONFIGURE_DEPENDS
    ${CMAKE_CURRENT_SOURCE_DIR}/backends/*/CMakeLists.txt)
list(APPEND PLEXUS_LIB_FRAGMENTS ${PLEXUS_BACKEND_FRAGMENTS})
foreach(fragment ${PLEXUS_LIB_FRAGMENTS})
    get_filename_component(fragment_dir ${fragment} DIRECTORY)
    add_subdirectory(${fragment_dir})
endforeach()

# Install/export the dependency-free INTERFACE set (core, api, wire, inproc) as the
# plexusTargets export set, namespaced plexus::. The gated backends reach link-interface
# targets outside every export set (the asio dep, the mDNS FetchContent, OpenSSL imported
# targets), so install(EXPORT) cannot carry them; their per-component targets files are a
# separate follow-up the config package's COMPONENTS loop already tolerates the absence of.
include(GNUInstallDirs)
include(CMakePackageConfigHelpers)
install(EXPORT plexusTargets
    NAMESPACE plexus::
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/plexus)
configure_package_config_file(cmake/plexusConfig.cmake.in
    ${CMAKE_CURRENT_BINARY_DIR}/plexusConfig.cmake
    INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/plexus)
write_basic_package_version_file(
    ${CMAKE_CURRENT_BINARY_DIR}/plexusConfigVersion.cmake
    COMPATIBILITY SameMajorVersion)
install(FILES
    ${CMAKE_CURRENT_BINARY_DIR}/plexusConfig.cmake
    ${CMAKE_CURRENT_BINARY_DIR}/plexusConfigVersion.cmake
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/plexus)

if(PLEXUS_BUILD_TESTS OR PLEXUS_BUILD_FUZZ_TESTS)
    enable_testing()
    # The function-size gate (tests/CMakeLists.txt) drives run-clang-tidy against
    # this tree's compile DB, so the DB must be emitted whenever the test suite
    # configures it; the gate fails closed if it is missing.
    set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
    add_subdirectory(tests)
endif()

# The host-side transcode tools (flat-stream to MCAP, flat-stream to pcapng) are host-only
# and never seen by the core or the MCU build. Each self-gates inside tools/; this directory
# is added when at least one tool is enabled. Added before the examples so the recording
# example can wire its optional MCAP step against mcap_transcode_lib when that tool is built.
if(PLEXUS_BUILD_MCAP_TRANSCODE OR PLEXUS_BUILD_PCAP_TRANSCODE)
    add_subdirectory(tools)
endif()

if(PLEXUS_BUILD_EXAMPLES)
    add_subdirectory(examples)
endif()

# Standalone scalability benchmarks: host-only probe binaries that link only the public
# core + the header-only in-process backend. Self-gated here; the default tree stays inert.
if(PLEXUS_BUILD_BENCHMARKS)
    add_subdirectory(benchmarks)
endif()
