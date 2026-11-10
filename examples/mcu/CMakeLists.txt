# The host ends of the board gates and benches. They need a board, so the gate scripts run them; none is a registered
# ctest. The esp-idf*/ firmware projects are standalone ESP-IDF projects built with idf.py, not part of this build.
if(TARGET plexus::asio)
    # The two serial gate hosts drive the line through raw POSIX termios/ioctl (sys/ioctl.h).
    if(NOT WIN32)
        add_executable(serial_gate_host serial_gate_host.cpp)
        target_link_libraries(serial_gate_host PRIVATE plexus::plexus plexus::asio)

        add_executable(topic_propagation_gate_host topic_propagation_gate_host.cpp)
        target_link_libraries(topic_propagation_gate_host PRIVATE plexus::plexus plexus::asio)
    endif()

    add_executable(lwip_gate_host lwip_gate_host.cpp)
    target_link_libraries(lwip_gate_host PRIVATE plexus::plexus plexus::asio)

    add_executable(lwip_bench_host lwip_bench_host.cpp)
    target_link_libraries(lwip_bench_host PRIVATE plexus::plexus plexus::asio)

    add_executable(serial_bench_host serial_bench_host.cpp)
    target_link_libraries(serial_bench_host PRIVATE plexus::plexus plexus::asio)

    add_executable(multicast_gate_host multicast_gate_host.cpp)
    target_link_libraries(multicast_gate_host PRIVATE plexus::plexus plexus::asio)

    add_executable(lwip_discovery_dial_host lwip_discovery_dial_host.cpp)
    target_link_libraries(lwip_discovery_dial_host PRIVATE plexus::plexus plexus::asio)

    # These read their config through std::getenv, which MSVC's CRT deprecates (C4996); silence it here, never globally.
    if(MSVC)
        set_property(TARGET lwip_gate_host lwip_bench_host serial_bench_host multicast_gate_host
            APPEND PROPERTY COMPILE_DEFINITIONS _CRT_SECURE_NO_WARNINGS)
    endif()
endif()
