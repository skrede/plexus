# TLS backend tests: the static_assert concept gates plus the live loopback
# mutual-TLS handshake proof and the COMDAT link+run proof. Only built when
# PLEXUS_ENABLE_TLS_BACKEND is ON (plexus::tls exists only under the gate); the
# `tls.` ctest prefix lets `ctest -R tls` select it. Auto-discovered by the
# tests glob.
if(NOT PLEXUS_ENABLE_TLS_BACKEND)
    return()
endif()

# The OBJECT library's objects are folded DIRECTLY into this executable's link
# unit via $<TARGET_OBJECTS:plexus-tls>; linking plexus::tls pulls only the
# INTERFACE usage requirements (include dirs + OpenSSL/asio deps), never a
# second asio-bearing archive. This is the COMDAT-safe wiring the run proof
# below validates at runtime.
add_executable(test_tls_seam test_tls_seam.cpp $<TARGET_OBJECTS:plexus-tls>)

target_link_libraries(test_tls_seam PRIVATE
    plexus::tls
    Catch2::Catch2WithMain)

catch_discover_tests(test_tls_seam TEST_PREFIX "tls.")

# The live trust-model proof: mutual cross-pinning accepts and delivers; an
# unpinned dialer (accepter-side reject) and an unpinned accepter (dialer-side
# reject) BOTH fail closed; a wrong-digest pin rejects; an empty (no-pin) policy
# rejects every peer. Each case loops N>=100 in-body; re-run >=3 process runs.
add_executable(test_tls_trust test_tls_trust.cpp $<TARGET_OBJECTS:plexus-tls>)

target_link_libraries(test_tls_trust PRIVATE
    plexus::tls
    Catch2::Catch2WithMain)

catch_discover_tests(test_tls_trust TEST_PREFIX "tls.")

# The mux integration proof: the selector classifies "tls"/"tcp" remote and
# "unix"/"inproc" local (so locality confinement excludes "tls"); a multiplexing
# transport over a (unix, tcp, tls) triple routes a "tls" dial to the secure
# member and a "tcp" dial to the plain-TCP member on the same node, with the
# scheme surviving the erasure. Links plexus::asio (the mux + unix/tcp members)
# alongside plexus::tls (the secure member). Live legs loop N>=100; re-run >=3.
add_executable(test_tls_mux test_tls_mux.cpp $<TARGET_OBJECTS:plexus-tls>)

target_link_libraries(test_tls_mux PRIVATE
    plexus::asio
    plexus::tls
    Catch2::Catch2WithMain)

catch_discover_tests(test_tls_mux TEST_PREFIX "tls.")
