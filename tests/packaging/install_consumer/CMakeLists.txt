# Configured only by run_install_consume.cmake against an installed prefix; never joins the outer build.
cmake_minimum_required(VERSION 3.25)
project(plexus_install_consumer LANGUAGES CXX)

find_package(plexus REQUIRED COMPONENTS asio asio-shm native)

add_library(install_consumer OBJECT consumer.cpp)

target_link_libraries(install_consumer PRIVATE plexus::asio-shm)
