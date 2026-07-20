# Top-level ESP-IDF project for the topic-propagation example: a node over the real UART link that
# produces a typed topic, consumes the host's, and enumerates what its peer declared. Cross-builds a
# compile-and-link proof of the enumeration surface against real FreeRTOS for esp32 (xtensa):
#   . /opt/esp-idf/export.sh && idf.py set-target esp32 && idf.py build
cmake_minimum_required(VERSION 3.16)

# Resolve the repo root so the component can add the plexus include trees.
get_filename_component(PLEXUS_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/../../.." ABSOLUTE)
set(PLEXUS_ROOT "${PLEXUS_ROOT}" CACHE PATH "plexus repository root")

include($ENV{IDF_PATH}/tools/cmake/project.cmake)
project(plexus_mcu_topic_graph)
