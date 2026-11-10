if(NOT PLEXUS_ENABLE_CRYPTO_BACKEND)
    return()
endif()

set(PLEXUS_TEST_PREFIX "dtls.")

plexus_add_catch_test(test_dtls_handshake
    SOURCES test_dtls_handshake.cpp test_dtls_handshake_dial.cpp test_dtls_handshake_relay.cpp
        $<TARGET_OBJECTS:plexus-crypto>
    LINKS plexus::crypto)

plexus_add_catch_test(test_dtls_cookie
    SOURCES test_dtls_cookie.cpp test_dtls_cookie_verify.cpp $<TARGET_OBJECTS:plexus-crypto>
    LINKS plexus::crypto)

plexus_add_catch_test(test_dtls_mtu
    SOURCES test_dtls_mtu.cpp test_dtls_mtu_record.cpp $<TARGET_OBJECTS:plexus-crypto>
    LINKS plexus::crypto)

plexus_add_catch_test(test_dtls_pending_abort
    SOURCES test_dtls_pending_abort.cpp $<TARGET_OBJECTS:plexus-crypto>
    LINKS plexus::crypto)

plexus_add_catch_test(test_dtls_teardown
    SOURCES test_dtls_teardown.cpp $<TARGET_OBJECTS:plexus-crypto>
    LINKS plexus::crypto)

plexus_add_catch_test(test_dtls_fragmentation
    SOURCES test_dtls_fragmentation.cpp test_dtls_fragmentation_burst.cpp test_dtls_fragmentation_envelope.cpp
        $<TARGET_OBJECTS:plexus-crypto>
    LINKS plexus::crypto)

plexus_add_catch_test(test_dtls_mux
    SOURCES test_dtls_mux.cpp test_dtls_mux_route.cpp $<TARGET_OBJECTS:plexus-crypto>
    LINKS plexus::asio plexus::crypto)

plexus_add_catch_test(test_dtls_transport_ceiling
    SOURCES test_dtls_transport_ceiling.cpp $<TARGET_OBJECTS:plexus-crypto>
    LINKS plexus::crypto)

plexus_add_catch_test(test_dtls_retire_drain
    SOURCES test_dtls_retire_drain.cpp $<TARGET_OBJECTS:plexus-crypto>
    LINKS plexus::crypto)
