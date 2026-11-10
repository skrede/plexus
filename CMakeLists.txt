cmake_minimum_required(VERSION 3.25)
project(plexus VERSION 0.3.2 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

option(PLEXUS_ENABLE_ASIO_BACKEND "Build the asio executor/socket/timer backend" OFF)
# Declared ahead of the fragment globs so the asio backend's shm fragment reads the right value on a fresh configure,
# whatever order the fragments are processed in.
option(PLEXUS_ENABLE_SHM_BACKEND "Build the shared-memory backend" OFF)
option(PLEXUS_BUILD_TESTS "Build the test suite" OFF)
option(PLEXUS_BUILD_FUZZ_TESTS "Build the libFuzzer harnesses" OFF)
option(PLEXUS_BUILD_EXAMPLES "Build the examples" OFF)
option(PLEXUS_BUILD_MCAP_TRANSCODE "Build the host-side flat-stream to MCAP transcode tool" OFF)
option(PLEXUS_BUILD_PCAP_TRANSCODE "Build the host-side flat-stream to pcapng transcode tool" OFF)
option(PLEXUS_BUILD_BENCHMARKS "Build the standalone scalability benchmarks" OFF)
option(PLEXUS_WARNINGS_AS_ERRORS "Promote warnings to errors (the enforced gate)" ON)

# Debug configurations always get the single-executor check; this option forces it into every configuration. Every
# translation unit of one program must see the same setting: MSVC refuses to link a mismatch, other toolchains cannot
# detect it.
option(PLEXUS_AFFINITY_CHECKS "Force the single-executor check on in every configuration" OFF)

# Set before the fragment glob so every discovered target inherits the warnings.
if(MSVC)
    # The conformant preprocessor, because the deliberate empty-argument trait-macro expansions trip the legacy one's
    # C4003.
    add_compile_options(/W4 /permissive- /Zc:__cplusplus /Zc:preprocessor)
    # An enumerator missing from a default-less switch is an error, as -Wswitch is under -Werror on GCC and Clang.
    add_compile_options(/we4062)
    # No /WX yet: the MSVC build is not warning-clean under /W4.
else()
    add_compile_options(-Wall -Wextra)
    # -Werror can be turned off only because a ThreadSanitizer tree emits an inherent -Wtsan diagnostic on asio's
    # atomic_thread_fence; that tree configures with the gate OFF.
    if(PLEXUS_WARNINGS_AS_ERRORS)
        add_compile_options(-Werror)
    endif()
endif()

# Each fragment self-gates: an INTERFACE target is unconditional, a compiled backend defines its target only when its
# option is ON. backends/ depends on lib/, never the reverse.
file(GLOB PLEXUS_LIB_FRAGMENTS CONFIGURE_DEPENDS
    ${CMAKE_CURRENT_SOURCE_DIR}/lib/*/CMakeLists.txt)
file(GLOB PLEXUS_BACKEND_FRAGMENTS CONFIGURE_DEPENDS
    ${CMAKE_CURRENT_SOURCE_DIR}/backends/*/CMakeLists.txt)
list(APPEND PLEXUS_LIB_FRAGMENTS ${PLEXUS_BACKEND_FRAGMENTS})
foreach(fragment ${PLEXUS_LIB_FRAGMENTS})
    get_filename_component(fragment_dir ${fragment} DIRECTORY)
    add_subdirectory(${fragment_dir})
endforeach()

# plexusTargets carries only the dependency-free INTERFACE set; each gated backend installs its own component targets
# file, which the config package loads per requested component and tolerates the absence of.
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
    # The function-size gate runs clang-tidy against this tree's compile database and fails closed without it.
    set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
    add_subdirectory(tests)
endif()

# Added before the examples so the recording example can wire its optional MCAP step against mcap_transcode_lib.
if(PLEXUS_BUILD_MCAP_TRANSCODE OR PLEXUS_BUILD_PCAP_TRANSCODE)
    add_subdirectory(tools)
endif()

if(PLEXUS_BUILD_EXAMPLES)
    add_subdirectory(examples)
endif()

if(PLEXUS_BUILD_BENCHMARKS)
    add_subdirectory(benchmarks)
endif()
