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
add_executable(test_dtls_handshake
    test_dtls_handshake.cpp
    test_dtls_handshake_dial.cpp
    test_dtls_handshake_relay.cpp
    $<TARGET_OBJECTS:plexus-crypto>)

target_link_libraries(test_dtls_handshake PRIVATE
    plexus::crypto
    Catch2::Catch2WithMain)

catch_discover_tests(test_dtls_handshake TEST_PREFIX "dtls.")

# The cookie / anti-DoS proof: a ClientHello with no/invalid cookie allocates no
# full handshake state (HelloVerifyRequest), a valid cookie proceeds to completion.
add_executable(test_dtls_cookie test_dtls_cookie.cpp test_dtls_cookie_verify.cpp $<TARGET_OBJECTS:plexus-crypto>)

target_link_libraries(test_dtls_cookie PRIVATE
    plexus::crypto
    Catch2::Catch2WithMain)

catch_discover_tests(test_dtls_cookie TEST_PREFIX "dtls.")

# The R-1 MTU proof: over a live completed DTLS session, a frame at the encrypted
# data MTU (DTLS_get_data_mtu) is delivered AS ONE datagram and one byte over it is
# rejected at publish via on_error(message_too_large) — never fragmented. A high
# configured cap lets DTLS_get_data_mtu bind the min(configured_cap, DTLS_get_data_mtu)
# reject; a low cap proves the configured term binds. Live legs loop N>=100.
add_executable(test_dtls_mtu test_dtls_mtu.cpp test_dtls_mtu_record.cpp $<TARGET_OBJECTS:plexus-crypto>)

target_link_libraries(test_dtls_mtu PRIVATE
    plexus::crypto
    Catch2::Catch2WithMain)

catch_discover_tests(test_dtls_mtu TEST_PREFIX "dtls.")

# The asan dial-abort leak proof: a dtls dial whose handshake never completes (a
# dead port; a black-holing relay) is torn down WITH THE HANDSHAKE PENDING by
# destroying the io_context + the transports. The transport-owned m_pending frees
# every in-flight channel (and its armed retransmit timer) cleanly — no leak, no
# use-after-free (the structural fix vs the TLS self-owning-channel cycle). Run under
# asan/ubsan (the build-asan tree) as the boundary proof. Live legs loop N>=100.
add_executable(test_dtls_pending_abort test_dtls_pending_abort.cpp $<TARGET_OBJECTS:plexus-crypto>)

target_link_libraries(test_dtls_pending_abort PRIVATE
    plexus::crypto
    Catch2::Catch2WithMain)

catch_discover_tests(test_dtls_pending_abort TEST_PREFIX "dtls.")

# The teardown-discipline proof: the close_notify branch posts on_closed off its
# own drain_inbound stack (a consumer that destroys the channel in on_closed must
# not free `this` mid-drain — a UAF under asan), and the inbound demux overwrites a
# stale entry on a re-insert (a re-dial routes to the new channel, not the old).
add_executable(test_dtls_teardown test_dtls_teardown.cpp $<TARGET_OBJECTS:plexus-crypto>)

target_link_libraries(test_dtls_teardown PRIVATE
    plexus::crypto
    Catch2::Catch2WithMain)

catch_discover_tests(test_dtls_teardown TEST_PREFIX "dtls.")

# The large-payload fragmentation proof: over a live completed DTLS session a payload
# larger than the encrypted record budget (DTLS_get_data_mtu) is SPLIT across numbered
# records (each one DTLS record, never one oversize record) and reassembled into ONE
# on_data byte-equal at the peer, while a frame at the record budget still rides exactly
# one record (parity, no fragmentation). Live legs loop N>=50; re-run >=3.
add_executable(test_dtls_fragmentation
    test_dtls_fragmentation.cpp
    test_dtls_fragmentation_burst.cpp
    test_dtls_fragmentation_envelope.cpp
    $<TARGET_OBJECTS:plexus-crypto>)

target_link_libraries(test_dtls_fragmentation PRIVATE
    plexus::crypto
    Catch2::Catch2WithMain)

catch_discover_tests(test_dtls_fragmentation TEST_PREFIX "dtls.")

# The mux integration proof: the selector classifies "dtls" remote (so locality
# confinement excludes it); a multiplexing transport over a (unix, tcp, tls, udp,
# dtls) quintet routes a "dtls" dial to the secure-datagram member (completing a
# mutual handshake + flowing an app frame over the wrapped channel), while "tcp"/
# "tls" dials still reach their own members and a same-host "unix" dial routes
# local — never the dtls member. Links plexus::asio (the mux + unix/tcp/udp members)
# alongside plexus::crypto (the secure/secure-datagram members). Live legs loop
# N>=100; re-run >=3.
add_executable(test_dtls_mux test_dtls_mux.cpp test_dtls_mux_route.cpp $<TARGET_OBJECTS:plexus-crypto>)

target_link_libraries(test_dtls_mux PRIVATE
    plexus::asio
    plexus::crypto
    Catch2::Catch2WithMain)

catch_discover_tests(test_dtls_mux TEST_PREFIX "dtls.")

# The transport-threading proof: dtls_transport threads its global_default per-MESSAGE
# ceiling (and the reassembly budget) to every dtls_channel it mints at dial + accept.
# Constructed with a LOW 4096-byte ceiling, a completed dial then rejects a 5000-byte send
# with message_too_large — which is only possible if the low ceiling reached the minted
# channel (at the old 8 MiB hardcoded default that message would deliver). Live; loops N>=50.
add_executable(test_dtls_transport_ceiling test_dtls_transport_ceiling.cpp $<TARGET_OBJECTS:plexus-crypto>)

target_link_libraries(test_dtls_transport_ceiling PRIVATE
    plexus::crypto
    Catch2::Catch2WithMain)

catch_discover_tests(test_dtls_transport_ceiling TEST_PREFIX "dtls.")
