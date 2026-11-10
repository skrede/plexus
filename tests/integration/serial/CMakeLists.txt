plexus_add_catch_test(test_serial_atomic_admission
    SOURCES test_serial_atomic_admission.cpp
    LINKS plexus::core plexus::wire plexus::stream plexus::asio)

# openpty(3) lives in libutil and is POSIX-only, hence the util link and the Windows exclusion.
if(NOT WIN32)
plexus_add_catch_test(test_serial_pty_roundtrip
    SOURCES test_serial_pty_roundtrip.cpp
    LINKS plexus::core plexus::wire plexus::asio plexus::inproc util)
endif()

# openpty(3) lives in libutil and is POSIX-only, hence the util link and the Windows exclusion.
if(NOT WIN32)
plexus_add_catch_test(test_serial_resync
    SOURCES test_serial_resync.cpp
    LINKS plexus::core plexus::wire plexus::asio plexus::inproc util)
endif()

# openpty(3) lives in libutil and is POSIX-only, hence the util link and the Windows exclusion.
if(NOT WIN32)
plexus_add_catch_test(test_serial_retire_drain
    SOURCES test_serial_retire_drain.cpp
    LINKS plexus::core plexus::wire plexus::asio plexus::inproc util)
endif()

# openpty(3) lives in libutil and is POSIX-only, hence the util link and the Windows exclusion.
if(NOT WIN32)
plexus_add_catch_test(test_serial_egress_kat
    SOURCES test_serial_egress_kat.cpp
    LINKS plexus::core plexus::wire plexus::asio plexus::inproc util)
endif()

# openpty(3) lives in libutil and is POSIX-only, hence the util link and the Windows exclusion.
if(NOT WIN32)
plexus_add_catch_test(test_serial_header_drop
    SOURCES test_serial_header_drop.cpp
    LINKS plexus::core plexus::wire plexus::asio plexus::inproc util)
endif()

# openpty(3) lives in libutil and is POSIX-only, hence the util link and the Windows exclusion.
if(NOT WIN32)
plexus_add_catch_test(test_serial_oversize
    SOURCES test_serial_oversize.cpp
    LINKS plexus::core plexus::wire plexus::asio plexus::inproc util)
endif()

# openpty(3) lives in libutil and is POSIX-only, hence the util link and the Windows exclusion.
if(NOT WIN32)
plexus_add_catch_test(test_serial_cap_refusal
    SOURCES test_serial_cap_refusal.cpp
    LINKS plexus::core plexus::wire plexus::asio plexus::inproc util)
endif()

# openpty(3) lives in libutil and is POSIX-only, hence the util link and the Windows exclusion.
if(NOT WIN32)
plexus_add_catch_test(test_serial_send_refusal
    SOURCES test_serial_send_refusal.cpp
    LINKS plexus::core plexus::wire plexus::asio plexus::inproc util)
endif()

# openpty(3) lives in libutil and is POSIX-only, hence the util link and the Windows exclusion.
if(NOT WIN32)
plexus_add_catch_test(test_serial_qos_ceiling
    SOURCES test_serial_qos_ceiling.cpp
    LINKS plexus::core plexus::wire plexus::asio plexus::inproc util)
endif()

# openpty(3) lives in libutil and is POSIX-only, hence the util link and the Windows exclusion.
if(NOT WIN32)
plexus_add_catch_test(test_serial_options
    SOURCES test_serial_options.cpp
    LINKS plexus::core plexus::wire plexus::asio plexus::inproc util)
endif()

# Paced openpty measurement rigs (the serial payload-cap sweep and the hold after a dropped header byte), run by
# hand and POSIX-only: they report numbers and assert nothing, so no ctest is registered.
if(NOT WIN32)
add_executable(test_serial_cap_sweep test_serial_cap_sweep.cpp)
target_link_libraries(test_serial_cap_sweep PRIVATE
    plexus::core
    plexus::wire
    plexus::asio
    plexus::inproc
    Catch2::Catch2WithMain
    util)
target_include_directories(test_serial_cap_sweep PRIVATE
    ${PLEXUS_TEST_INCLUDES}
    ${PROJECT_SOURCE_DIR}/tests)
endif()

# openpty(3) lives in libutil and is POSIX-only, hence the util link and the Windows exclusion.
if(NOT WIN32)
plexus_add_catch_test(test_relay_serial_e2e
    SOURCES test_relay_serial_e2e.cpp
    LINKS plexus::plexus plexus::core plexus::wire plexus::asio plexus::inproc util)

plexus_add_catch_test(test_forward_serial_e2e
    SOURCES test_forward_serial_e2e.cpp
    LINKS plexus::plexus plexus::core plexus::wire plexus::asio plexus::inproc util)

plexus_add_catch_test(test_forward_serial_kill_origin
    SOURCES test_forward_serial_kill_origin.cpp
    LINKS plexus::plexus plexus::core plexus::wire plexus::asio plexus::inproc util)

plexus_add_catch_test(test_forward_reqres_robustness
    SOURCES test_forward_reqres_robustness.cpp
    LINKS plexus::plexus plexus::core plexus::wire plexus::asio plexus::inproc util)
endif()

# openpty(3) lives in libutil and is POSIX-only, hence the util link and the Windows exclusion.
if(NOT WIN32)
plexus_add_catch_test(test_forward_envelope_gate
    SOURCES test_forward_envelope_gate.cpp
    LINKS plexus::core plexus::wire plexus::asio plexus::inproc util)
endif()

plexus_add_catch_test(test_serial_mux_member
    SOURCES test_serial_mux_member.cpp
    LINKS plexus::plexus plexus::asio)
