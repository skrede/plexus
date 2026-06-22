# Wire-layer unit tests: the codec/reassembler/length-prefix parity oracle.
# Each links the header-only plexus::wire target plus Catch2's main; ctest -R
# wire selects them.
set(PLEXUS_WIRE_TESTS
    test_varint
    test_length_prefixed_helper
    test_subscribe_qos_codec
    test_fetch_latched_codec
    test_heartbeat_codec
    test_crc32c
    test_serial_crc_kat)

foreach(test ${PLEXUS_WIRE_TESTS})
    add_executable(${test} ${test}.cpp)
    target_link_libraries(${test} PRIVATE plexus::wire Catch2::Catch2WithMain)
    # Prefix the ctest registration so `ctest -R wire` selects the whole wire
    # suite by name (the [wire] Catch2 tag is not a ctest-visible selector).
    catch_discover_tests(${test} TEST_PREFIX "wire.")
endforeach()

add_executable(test_frame_reassembler
    test_frame_reassembler.cpp
    test_frame_reassembler_overflow.cpp)
target_link_libraries(test_frame_reassembler PRIVATE plexus::wire Catch2::Catch2WithMain)
catch_discover_tests(test_frame_reassembler TEST_PREFIX "wire.")

# The serial CRC32C trailer + magic-resync decorator oracle. crc_serial.h pulls
# close_cause / move_only_function from stream_inbound.h (which lives in plexus::wire
# but includes the core compat shim), so this TU additionally links plexus::core for
# that include path — the rest of the wire oracle needs only plexus::wire.
add_executable(test_crc_serial_decorator
    test_crc_serial_decorator.cpp
    test_crc_serial_resync.cpp)
target_link_libraries(test_crc_serial_decorator PRIVATE
    plexus::core
    plexus::wire
    Catch2::Catch2WithMain)
catch_discover_tests(test_crc_serial_decorator TEST_PREFIX "wire.")

# The codec parity oracle's scenario clusters (header/data/subscribe/rpc/handshake)
# compile into the one test_wire_codec binary so catch_discover_tests enumerates the
# identical set across the split layout.
add_executable(test_wire_codec
    test_wire_codec.cpp
    test_wire_codec_data.cpp
    test_wire_codec_subscribe.cpp
    test_wire_codec_rpc.cpp
    test_wire_codec_handshake.cpp)
target_link_libraries(test_wire_codec PRIVATE plexus::wire Catch2::Catch2WithMain)
catch_discover_tests(test_wire_codec TEST_PREFIX "wire.")

# The UDP wire oracle: the envelope codec round-trip + the uint16 dedup window's
# RFC-1982 wrap-safety. Both are pure header-only wire units (no backend), so each
# links only plexus::wire + Catch2's main; the "udp." ctest prefix selects them.
set(PLEXUS_UDP_WIRE_TESTS
    test_udp_envelope
    test_udp_dedup_window
    test_udp_ack)

foreach(test ${PLEXUS_UDP_WIRE_TESTS})
    add_executable(${test} ${test}.cpp)
    target_link_libraries(${test} PRIVATE plexus::wire Catch2::Catch2WithMain)
    catch_discover_tests(${test} TEST_PREFIX "udp.")
endforeach()

# The locality QoS unit oracle proves the delivery-tier bitflag truth table and the
# scheme->tier classifier. It includes the core io/locality.h + topic_qos.h headers,
# so it additionally links plexus::core (the header-only core); the rest of the wire
# oracle needs only plexus::wire.
add_executable(test_locality_qos test_locality_qos.cpp)
target_link_libraries(test_locality_qos PRIVATE
    plexus::core
    plexus::wire
    Catch2::Catch2WithMain)
catch_discover_tests(test_locality_qos TEST_PREFIX "wire.")

# The stream_inbound detection oracle drives a virtual-clock inproc timer, so this
# TU additionally links plexus::inproc (the other wire targets do not) — mirroring
# how the integration inproc targets pull in the manual-clock executor/timer.
add_executable(test_stream_inbound
    test_stream_inbound.cpp
    test_stream_inbound_feed_error.cpp)
target_link_libraries(test_stream_inbound PRIVATE
    plexus::wire
    plexus::inproc
    Catch2::Catch2WithMain)
catch_discover_tests(test_stream_inbound TEST_PREFIX "wire.")

# The handshake encode-into NOALLOC gate is isolated in its own executable:
# support/alloc_counter.h overrides global operator new/delete once per program,
# so it must not share a binary with the rest of the wire oracle. tests/ is on the
# include path so it can pull in support/alloc_counter.h.
add_executable(test_handshake_codec_noalloc test_handshake_codec_noalloc.cpp)
target_link_libraries(test_handshake_codec_noalloc PRIVATE plexus::wire Catch2::Catch2WithMain)
target_include_directories(test_handshake_codec_noalloc PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/..)
# RUN_SERIAL: the noalloc gate snapshots the single-TU global operator-new counter across a
# steady-state loop and asserts a zero delta; serializing keeps the count immune to the CPU
# contention of concurrent test processes.
catch_discover_tests(test_handshake_codec_noalloc TEST_PREFIX "wire."
    PROPERTIES RUN_SERIAL TRUE)

# The wire::reader/writer cursor oracle: the field-list round-trip, the fail-closed
# per-read bound, the over-long-varint delegation, and the writer-into-a-reserved-
# buffer no-realloc property. The no-realloc case includes support/alloc_counter.h
# (one operator-new override per program), so it lives in its own executable like the
# handshake noalloc gate; tests/ is on the include path for it.
add_executable(test_wire_cursor test_wire_cursor.cpp)
target_link_libraries(test_wire_cursor PRIVATE plexus::wire Catch2::Catch2WithMain)
target_include_directories(test_wire_cursor PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/..)
# RUN_SERIAL: no-realloc gate over the single-TU global operator-new counter (see above).
catch_discover_tests(test_wire_cursor TEST_PREFIX "wire_cursor."
    PROPERTIES RUN_SERIAL TRUE)
