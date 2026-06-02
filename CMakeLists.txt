cmake_minimum_required(VERSION 3.25)
project(plexus VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Backends and extras are opt-in, gated targets — never required by the core.
option(PLEXUS_ENABLE_ASIO_BACKEND "Build the asio executor/socket/timer backend" OFF)
option(PLEXUS_BUILD_TESTS "Build the test suite" OFF)
option(PLEXUS_BUILD_EXAMPLES "Build the examples" OFF)

add_subdirectory(lib/plexus)

if(PLEXUS_BUILD_TESTS)
    enable_testing()
    # add_subdirectory(tests)  # enabled with the first slice
endif()
