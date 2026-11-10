if(NOT PLEXUS_ENABLE_CRYPTO_BACKEND)
    return()
endif()

set(PLEXUS_TEST_PREFIX "crypto.")

# Each crypto test, here and in the tls and dtls areas, compiles the plexus-crypto objects into its own executable
# and links plexus::crypto only for its usage requirements, so no second asio-bearing archive enters the link
# (the COMDAT-safe wiring).
plexus_add_catch_test(test_aead_cipher
    SOURCES test_aead_cipher.cpp $<TARGET_OBJECTS:plexus-crypto>
    LINKS plexus::crypto)

plexus_add_catch_test(test_rekey_threshold_sweep
    SOURCES test_rekey_threshold_sweep.cpp $<TARGET_OBJECTS:plexus-crypto>
    LINKS plexus::crypto)

plexus_add_catch_test(test_authenticated_channel
    SOURCES test_authenticated_channel.cpp test_authenticated_channel_epoch.cpp $<TARGET_OBJECTS:plexus-crypto>
    LINKS plexus::crypto)

plexus_add_catch_test(test_nonce_uniqueness
    SOURCES test_nonce_uniqueness.cpp test_nonce_uniqueness_counter.cpp $<TARGET_OBJECTS:plexus-crypto>
    LINKS plexus::crypto)

plexus_add_catch_test(test_datagram_replay
    SOURCES test_datagram_replay.cpp test_datagram_replay_drop.cpp test_datagram_replay_epoch.cpp
        $<TARGET_OBJECTS:plexus-crypto>
    LINKS plexus::crypto)

plexus_add_catch_test(test_wire_crypto_position
    SOURCES test_wire_crypto_position.cpp $<TARGET_OBJECTS:plexus-crypto>
    LINKS plexus::core plexus::crypto)
