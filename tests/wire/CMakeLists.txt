set(PLEXUS_TEST_PREFIX "wire.")

set(PLEXUS_WIRE_TESTS
    test_varint
    test_length_prefixed_helper
    test_subscribe_qos_codec
    test_subscribe_status_codec
    test_companion_codec
    test_fetch_latched_codec
    test_heartbeat_codec
    test_topic_declaration_codec
    test_crc32c
    test_fnv1a_kat)

foreach(test ${PLEXUS_WIRE_TESTS})
    plexus_add_catch_test(${test}
        SOURCES ${test}.cpp
        LINKS plexus::wire)
endforeach()

plexus_add_catch_test(test_frame_reassembler
    SOURCES test_frame_reassembler.cpp test_frame_reassembler_overflow.cpp
    LINKS plexus::wire)

# The decorator is stream-family discipline over the wire codec, so it links plexus::stream.
plexus_add_catch_test(test_crc_serial_decorator
    SOURCES test_crc_serial_decorator.cpp test_crc_serial_resync.cpp test_crc_serial_cap.cpp
    LINKS plexus::wire plexus::stream)

plexus_add_catch_test(test_serial_frame_kat
    SOURCES test_serial_frame_kat.cpp
    LINKS plexus::wire plexus::stream)

# Owns the single replaceable operator new (support/alloc_counter.h), so it is its own executable; RUN_SERIAL keeps
# concurrent tests from skewing the count.
plexus_add_catch_test(test_crc_serial_noalloc
    SOURCES test_crc_serial_noalloc.cpp
    LINKS plexus::wire plexus::stream
    PROPERTIES RUN_SERIAL TRUE)

plexus_add_catch_test(test_wire_codec
    SOURCES test_wire_codec.cpp test_wire_codec_data.cpp test_wire_codec_subscribe.cpp
        test_wire_codec_subscribe_type.cpp test_wire_codec_rpc.cpp test_wire_codec_handshake.cpp
    LINKS plexus::wire)

set(PLEXUS_UDP_WIRE_TESTS
    test_udp_envelope
    test_udp_dedup_window
    test_udp_ack)

foreach(test ${PLEXUS_UDP_WIRE_TESTS})
    plexus_add_catch_test(${test}
        PREFIX "udp."
        SOURCES ${test}.cpp
        LINKS plexus::wire)
endforeach()

plexus_add_catch_test(test_locality_qos
    SOURCES test_locality_qos.cpp
    LINKS plexus::core plexus::wire)

plexus_add_catch_test(test_announcement
    SOURCES test_announcement.cpp
    LINKS plexus::core plexus::wire)

plexus_add_catch_test(test_peer_report_codec
    SOURCES test_peer_report_codec.cpp
    LINKS plexus::core plexus::wire)

plexus_add_catch_test(test_forwarded_frame_codec
    SOURCES test_forwarded_frame_codec.cpp
    LINKS plexus::core plexus::wire)

# The detection oracle drives a virtual-clock inproc timer, so unlike the other wire tests it links plexus::inproc.
plexus_add_catch_test(test_stream_inbound
    SOURCES test_stream_inbound.cpp test_stream_inbound_feed_error.cpp test_stream_inbound_timer_race.cpp
    LINKS plexus::wire plexus::stream plexus::inproc)

# Owns the single replaceable operator new (support/alloc_counter.h), so it is its own executable; RUN_SERIAL keeps
# its zero-delta count immune to the contention of concurrent test processes.
plexus_add_catch_test(test_handshake_codec_noalloc
    SOURCES test_handshake_codec_noalloc.cpp
    LINKS plexus::wire
    PROPERTIES RUN_SERIAL TRUE)

# Its no-realloc case owns the single replaceable operator new (support/alloc_counter.h), so it is its own executable;
# RUN_SERIAL keeps the count clean.
plexus_add_catch_test(test_wire_cursor
    PREFIX "wire_cursor."
    SOURCES test_wire_cursor.cpp
    LINKS plexus::wire
    PROPERTIES RUN_SERIAL TRUE)
