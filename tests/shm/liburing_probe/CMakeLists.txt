# Configured only by run_probe.cmake; the tests fragment glob is one level deep, so this never joins the outer build.
cmake_minimum_required(VERSION 3.25)
project(plexus_liburing_probe LANGUAGES CXX)

add_subdirectory(${PLEXUS_SOURCE_DIR} plexus EXCLUDE_FROM_ALL)

add_library(probe_consumer OBJECT probe_consumer.cpp)

target_link_libraries(probe_consumer PRIVATE
    plexus::asio::shm
    plexus::asio)
