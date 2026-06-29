# The host half of the on-device serial example: it reuses the asio serial transport on the
# real /dev/ttyUSB0 link, so it is guarded on asio alone (static discovery, no mDNS). It is a
# board-gated runnable the gate driver flashes + drives — deliberately NOT a registered ctest.
#
# The esp-idf/ subdirectory is the on-device firmware. It is a standalone ESP-IDF project built
# with idf.py (see mcu/README.md), not part of this host CMake build, so it is not added here.
if(TARGET plexus::asio)
    add_executable(serial_gate_host serial_gate_host.cpp)
    target_link_libraries(serial_gate_host PRIVATE plexus::plexus plexus::asio)

    add_executable(lwip_gate_host lwip_gate_host.cpp)
    target_link_libraries(lwip_gate_host PRIVATE plexus::plexus plexus::asio)

    add_executable(lwip_bench_host lwip_bench_host.cpp)
    target_link_libraries(lwip_bench_host PRIVATE plexus::plexus plexus::asio)

    add_executable(multicast_gate_host multicast_gate_host.cpp)
    target_link_libraries(multicast_gate_host PRIVATE plexus::plexus plexus::asio)

    add_executable(lwip_discovery_dial_host lwip_discovery_dial_host.cpp)
    target_link_libraries(lwip_discovery_dial_host PRIVATE plexus::plexus plexus::asio)
endif()
