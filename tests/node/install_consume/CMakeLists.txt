cmake_minimum_required(VERSION 3.25)
project(plexus_install_consume LANGUAGES CXX)

find_package(plexus CONFIG REQUIRED)

add_executable(consume main.cpp)
target_link_libraries(consume PRIVATE plexus::plexus)
