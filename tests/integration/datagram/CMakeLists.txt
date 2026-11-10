plexus_add_catch_test(test_reassembler_dos
    SOURCES test_reassembler_dos.cpp test_reassembler_dos_eviction.cpp
    LINKS plexus::core plexus::wire plexus::datagram plexus::testing)

if(PLEXUS_ENABLE_ASIO_BACKEND)
    plexus_add_catch_test(test_udp_reproducibility
        PREFIX "udp."
        SOURCES test_udp_reproducibility.cpp test_udp_reproducibility_reliable.cpp
        LINKS plexus::asio)

    plexus_add_catch_test(test_udp_send_burst
        PREFIX "udp."
        SOURCES test_udp_send_burst.cpp test_udp_send_burst_relay.cpp
        LINKS plexus::asio)

    plexus_add_catch_test(test_udp_large_payload
        PREFIX "udp."
        SOURCES test_udp_large_payload.cpp test_udp_large_payload_roundtrip.cpp test_udp_large_payload_probe.cpp
            test_udp_large_payload_fragment.cpp
        LINKS plexus::asio)

    # Its shared fixture reaches raw POSIX (unistd.h), so it is excluded from Windows. A real-loopback leg with a
    # fixed wall-clock pump deadline, so it runs alone and holds the loopback lock.
    if(NOT WIN32)
        plexus_add_catch_test(test_outofbox_message_ceiling
            SOURCES test_outofbox_message_ceiling.cpp test_outofbox_message_ceiling_udp.cpp
                test_outofbox_message_ceiling_tls.cpp
            LINKS plexus::core plexus::wire plexus::asio
            PROPERTIES RUN_SERIAL TRUE RESOURCE_LOCK tcp-loopback)
    endif()

    if(PLEXUS_ENABLE_CRYPTO_BACKEND)
        plexus_add_catch_test(test_drop_coverage
            SOURCES test_drop_coverage.cpp $<TARGET_OBJECTS:plexus-crypto>
            LINKS plexus::core plexus::wire plexus::asio plexus::crypto plexus::inproc)

        plexus_add_catch_test(test_aead_fragment
            SOURCES test_aead_fragment.cpp $<TARGET_OBJECTS:plexus-crypto>
            LINKS plexus::core plexus::wire plexus::crypto plexus::testing)

        if(NOT WIN32)
            target_sources(test_outofbox_message_ceiling PRIVATE $<TARGET_OBJECTS:plexus-crypto>)
            target_link_libraries(test_outofbox_message_ceiling PRIVATE plexus::crypto)
            target_compile_definitions(test_outofbox_message_ceiling PRIVATE PLEXUS_HAVE_TLS_OUTOFBOX)
        endif()
    endif()
endif()
