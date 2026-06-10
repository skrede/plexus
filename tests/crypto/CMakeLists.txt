# Crypto-primitive tests: the AEAD seal/open round-trip + tamper-reject proof, the
# HKDF per-direction transcript-bound key schedule, and the rekey-before-wrap
# threshold sweep. Only built when PLEXUS_ENABLE_CRYPTO_BACKEND is ON (plexus::crypto
# exists only under the gate); the `crypto.` ctest prefix lets `ctest -R crypto`
# select it. Auto-discovered by the tests glob.
if(NOT PLEXUS_ENABLE_CRYPTO_BACKEND)
    return()
endif()

# The OBJECT library's objects fold DIRECTLY into each executable's link unit via
# $<TARGET_OBJECTS:plexus-crypto>; linking plexus::crypto pulls only the INTERFACE
# usage requirements (include dirs + OpenSSL/asio deps), never a second
# asio-bearing archive (the COMDAT-safe wiring shared with the tls/dtls areas).
add_executable(test_aead_cipher test_aead_cipher.cpp $<TARGET_OBJECTS:plexus-crypto>)

target_link_libraries(test_aead_cipher PRIVATE
    plexus::crypto
    Catch2::Catch2WithMain)

catch_discover_tests(test_aead_cipher TEST_PREFIX "crypto.")

add_executable(test_rekey_threshold_sweep test_rekey_threshold_sweep.cpp $<TARGET_OBJECTS:plexus-crypto>)

target_link_libraries(test_rekey_threshold_sweep PRIVATE
    plexus::crypto
    Catch2::Catch2WithMain)

catch_discover_tests(test_rekey_threshold_sweep TEST_PREFIX "crypto.")

add_executable(test_authenticated_channel test_authenticated_channel.cpp $<TARGET_OBJECTS:plexus-crypto>)

target_link_libraries(test_authenticated_channel PRIVATE
    plexus::crypto
    Catch2::Catch2WithMain)

catch_discover_tests(test_authenticated_channel TEST_PREFIX "crypto.")

add_executable(test_nonce_uniqueness test_nonce_uniqueness.cpp $<TARGET_OBJECTS:plexus-crypto>)

target_link_libraries(test_nonce_uniqueness PRIVATE
    plexus::crypto
    Catch2::Catch2WithMain)

catch_discover_tests(test_nonce_uniqueness TEST_PREFIX "crypto.")

add_executable(test_datagram_replay test_datagram_replay.cpp $<TARGET_OBJECTS:plexus-crypto>)

target_link_libraries(test_datagram_replay PRIVATE
    plexus::crypto
    Catch2::Catch2WithMain)

catch_discover_tests(test_datagram_replay TEST_PREFIX "crypto.")

add_executable(test_anti_replay_window_sweep test_anti_replay_window_sweep.cpp $<TARGET_OBJECTS:plexus-crypto>)

target_link_libraries(test_anti_replay_window_sweep PRIVATE
    plexus::crypto
    Catch2::Catch2WithMain)

catch_discover_tests(test_anti_replay_window_sweep TEST_PREFIX "crypto.")
