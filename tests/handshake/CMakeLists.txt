set(PLEXUS_TEST_PREFIX "handshake.")

# RUN_SERIAL keeps the alloc-count contract over the global operator-new counter immune to the contention of
# concurrent test processes.
plexus_add_catch_test(test_handshake_fsm
    SOURCES test_handshake_fsm.cpp
    LINKS plexus::core plexus::wire
    PROPERTIES RUN_SERIAL TRUE)

plexus_add_catch_test(test_handshake_codec
    SOURCES test_handshake_codec.cpp
    LINKS plexus::wire)

plexus_add_catch_test(test_version_interop
    SOURCES test_version_interop.cpp
    LINKS plexus::core plexus::wire)

plexus_add_catch_test(test_handshake_identity
    SOURCES test_handshake_identity.cpp test_handshake_refusal.cpp
    LINKS plexus::core plexus::wire)
