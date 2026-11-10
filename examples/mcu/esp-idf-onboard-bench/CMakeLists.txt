# The onboard zero-copy benchmark: one node whose only transport is the intra-node self-route times a typed request to
# its own subscriber and the echoed reply, with no link at all. The typed lane delivers by address, so the codec's
# encode never runs on the path; the firmware reports an encode count next to the latency samples as the witness.
#   . /opt/esp-idf/export.sh && idf.py set-target esp32 && idf.py build
cmake_minimum_required(VERSION 3.16)

get_filename_component(PLEXUS_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/../../.." ABSOLUTE)
set(PLEXUS_ROOT "${PLEXUS_ROOT}" CACHE PATH "plexus repository root")

include($ENV{IDF_PATH}/tools/cmake/project.cmake)
project(plexus_onboard_bench)
