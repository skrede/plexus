# Wire-layer unit tests: the codec/reassembler/length-prefix parity oracle.
# Each links the header-only plexus::wire target plus Catch2's main; ctest -R
# wire selects them.
set(PLEXUS_WIRE_TESTS
    test_wire_codec
    test_frame_reassembler
    test_length_prefixed_helper)

foreach(test ${PLEXUS_WIRE_TESTS})
    add_executable(${test} ${test}.cpp)
    target_link_libraries(${test} PRIVATE plexus::wire Catch2::Catch2WithMain)
    # Prefix the ctest registration so `ctest -R wire` selects the whole wire
    # suite by name (the [wire] Catch2 tag is not a ctest-visible selector).
    catch_discover_tests(${test} TEST_PREFIX "wire.")
endforeach()

# The UDP wire oracle: the envelope codec round-trip + the uint16 dedup window's
# RFC-1982 wrap-safety. Both are pure header-only wire units (no backend), so each
# links only plexus::wire + Catch2's main; the "udp." ctest prefix selects them.
set(PLEXUS_UDP_WIRE_TESTS
    test_udp_envelope
    test_udp_dedup_window)

foreach(test ${PLEXUS_UDP_WIRE_TESTS})
    add_executable(${test} ${test}.cpp)
    target_link_libraries(${test} PRIVATE plexus::wire Catch2::Catch2WithMain)
    catch_discover_tests(${test} TEST_PREFIX "udp.")
endforeach()

# The locality QoS unit oracle proves the delivery-tier bitflag truth table and the
# scheme->tier classifier. It includes the core io/locality.h + topic_qos.h headers,
# so it additionally links plexus::plexus (the header-only core); the rest of the wire
# oracle needs only plexus::wire.
add_executable(test_locality_qos test_locality_qos.cpp)
target_link_libraries(test_locality_qos PRIVATE
    plexus::plexus
    plexus::wire
    Catch2::Catch2WithMain)
catch_discover_tests(test_locality_qos TEST_PREFIX "wire.")

# The stream_inbound detection oracle drives a virtual-clock inproc timer, so this
# TU additionally links plexus::inproc (the other wire targets do not) — mirroring
# how the integration inproc targets pull in the manual-clock executor/timer.
add_executable(test_stream_inbound test_stream_inbound.cpp)
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
catch_discover_tests(test_handshake_codec_noalloc TEST_PREFIX "wire.")
