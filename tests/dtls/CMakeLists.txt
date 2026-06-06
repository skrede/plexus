# DTLS backend tests: the live loopback mutual-DTLS handshake proof, the
# external_complete / SPKI-pin / fail-closed / cookie-gate / handshake-over-loss
# proofs, and the static_assert concept gates (exercised by building the gated
# engine into each executable). Only built when PLEXUS_ENABLE_CRYPTO_BACKEND is ON
# (plexus::crypto exists only under the gate); the `dtls.` ctest prefix lets
# `ctest -R dtls` select it. Auto-discovered by the tests glob.
if(NOT PLEXUS_ENABLE_CRYPTO_BACKEND)
    return()
endif()

# The OBJECT library's objects fold DIRECTLY into each executable's link unit via
# $<TARGET_OBJECTS:plexus-crypto>; linking plexus::crypto pulls only the INTERFACE
# usage requirements (include dirs + OpenSSL/asio deps), never a second
# asio-bearing archive (the COMDAT-safe wiring, proven by link+run).
add_executable(test_dtls_handshake test_dtls_handshake.cpp $<TARGET_OBJECTS:plexus-crypto>)

target_link_libraries(test_dtls_handshake PRIVATE
    plexus::crypto
    Catch2::Catch2WithMain)

catch_discover_tests(test_dtls_handshake TEST_PREFIX "dtls.")

# The cookie / anti-DoS proof: a ClientHello with no/invalid cookie allocates no
# full handshake state (HelloVerifyRequest), a valid cookie proceeds to completion.
add_executable(test_dtls_cookie test_dtls_cookie.cpp $<TARGET_OBJECTS:plexus-crypto>)

target_link_libraries(test_dtls_cookie PRIVATE
    plexus::crypto
    Catch2::Catch2WithMain)

catch_discover_tests(test_dtls_cookie TEST_PREFIX "dtls.")

# The mux integration proof: the selector classifies "dtls" remote (so locality
# confinement excludes it); a multiplexing transport over a (unix, tcp, tls, udp,
# dtls) quintet routes a "dtls" dial to the secure-datagram member (completing a
# mutual handshake + flowing an app frame over the wrapped channel), while "tcp"/
# "tls" dials still reach their own members and a same-host "unix" dial routes
# local — never the dtls member. Links plexus::asio (the mux + unix/tcp/udp members)
# alongside plexus::crypto (the secure/secure-datagram members). Live legs loop
# N>=100; re-run >=3.
add_executable(test_dtls_mux test_dtls_mux.cpp $<TARGET_OBJECTS:plexus-crypto>)

target_link_libraries(test_dtls_mux PRIVATE
    plexus::asio
    plexus::crypto
    Catch2::Catch2WithMain)

catch_discover_tests(test_dtls_mux TEST_PREFIX "dtls.")
