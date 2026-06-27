# Top-level ESP-IDF project for the onboard intra-node zero-copy benchmark: a single node whose
# sole transport is the intra-node self-route — it publishes a typed request to its OWN subscriber
# on-device, that subscriber echoes a reply, and the round trip is timed, all with zero link (no
# UART, no Wi-Fi, no lwIP). The typed lane delivers by address, so the codec's encode is never
# invoked on the path; the firmware emits an encode-count witness alongside the latency samples.
#   . /opt/esp-idf/export.sh && idf.py set-target esp32 && idf.py build
cmake_minimum_required(VERSION 3.16)

get_filename_component(PLEXUS_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/../../.." ABSOLUTE)
set(PLEXUS_ROOT "${PLEXUS_ROOT}" CACHE PATH "plexus repository root")

include($ENV{IDF_PATH}/tools/cmake/project.cmake)
project(plexus_onboard_bench)
