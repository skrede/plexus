cmake_minimum_required(VERSION 3.25)
project(plexus VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Backends and extras are opt-in, gated targets — never required by the core.
option(PLEXUS_ENABLE_ASIO_BACKEND "Build the asio executor/socket/timer backend" OFF)
option(PLEXUS_BUILD_TESTS "Build the test suite" OFF)
option(PLEXUS_BUILD_FUZZ_TESTS "Build the libFuzzer harnesses" OFF)
option(PLEXUS_BUILD_EXAMPLES "Build the examples" OFF)
option(PLEXUS_WARNINGS_AS_ERRORS "Promote warnings to errors (the enforced gate)" ON)

# Warn broadly on every compiled translation unit the build owns (the gated backends
# and the test suite; the INTERFACE cores carry no TUs of their own). Applied before
# the fragment glob so each discovered target inherits it. -Werror promotes those
# warnings to the enforced gate: the build is clean under it today, so any newly
# introduced warning fails the build going forward. -Werror is opt-OUTable (default
# ON, so every ordinary tree enforces it) because a ThreadSanitizer tree emits an
# inherent, uninstrumentable -Wtsan diagnostic on asio's atomic_thread_fence (a
# property of running asio under tsan, not a plexus defect — see the tsan tree notes);
# that one tree configures with the gate OFF so the race-detection run is not blocked
# by a diagnostic the authoritative GCC tree never sees.
add_compile_options(-Wall -Wextra)
if(PLEXUS_WARNINGS_AS_ERRORS)
    add_compile_options(-Werror)
endif()

# Project-wide library registration contract: every lib/<target>/ owns a
# CMakeLists.txt fragment and is auto-discovered here. The glob is
# target-kind-agnostic — INTERFACE cores (plexus, plexus-wire, plexus-inproc)
# and gated compiled backends (plexus-asio, plexus-mdnspp) are discovered the
# same way; each fragment self-gates (an INTERFACE target is unconditional, a
# compiled backend defines its target only when its option is ON). New targets
# register by adding their fragment, never by editing this file.
file(GLOB PLEXUS_LIB_FRAGMENTS CONFIGURE_DEPENDS
    ${CMAKE_CURRENT_SOURCE_DIR}/lib/*/CMakeLists.txt)
foreach(fragment ${PLEXUS_LIB_FRAGMENTS})
    get_filename_component(fragment_dir ${fragment} DIRECTORY)
    add_subdirectory(${fragment_dir})
endforeach()

if(PLEXUS_BUILD_TESTS OR PLEXUS_BUILD_FUZZ_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()
