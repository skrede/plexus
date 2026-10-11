# Configured only by run_shm_off.cmake; the tests fragment glob is one level deep, so this never joins the outer build.
cmake_minimum_required(VERSION 3.25)
project(plexus_shm_off_consumer LANGUAGES CXX)

add_subdirectory(${PLEXUS_SOURCE_DIR} plexus EXCLUDE_FROM_ALL)

add_library(shm_off_consumer OBJECT consumer.cpp)

target_link_libraries(shm_off_consumer PRIVATE plexus::asio)
