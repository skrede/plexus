# The onboard self-delivery example: one node whose only transport is the intra-node self-route, cross-built as a
# compile-and-link proof against real FreeRTOS for esp32 (xtensa) and esp32c3 (RISC-V):
#   . /opt/esp-idf/export.sh && idf.py set-target esp32   && idf.py build
#   . /opt/esp-idf/export.sh && idf.py set-target esp32c3 && idf.py build
cmake_minimum_required(VERSION 3.16)

# Resolve the repo root so the component can add the plexus include trees.
get_filename_component(PLEXUS_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/../../.." ABSOLUTE)
set(PLEXUS_ROOT "${PLEXUS_ROOT}" CACHE PATH "plexus repository root")

include($ENV{IDF_PATH}/tools/cmake/project.cmake)
project(plexus_mcu_onboard)
