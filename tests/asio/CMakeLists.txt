# Asio-Policy compile/construct smoke test: the static_assert(Policy<asio_policy>)
# compile gate plus runtime construct + post-runs-callback assertions. Only built
# when PLEXUS_ENABLE_ASIO_BACKEND is ON (plexus::asio exists only under the gate);
# the `asio.` ctest prefix lets `ctest -R asio` select it. Auto-discovered by the
# tests glob.
if(NOT PLEXUS_ENABLE_ASIO_BACKEND)
    return()
endif()

add_executable(test_asio_policy test_asio_policy.cpp)

target_link_libraries(test_asio_policy PRIVATE
    plexus::asio
    Catch2::Catch2WithMain)

catch_discover_tests(test_asio_policy TEST_PREFIX "asio.")

# Live connectionless-UDP legs: the best_effort transport (end-to-end + dedup +
# oversize) and the handshake ARQ over injected loss. Header-only inside plexus::asio
# (no new compiled backend target); the `udp.` ctest prefix selects them. When the
# crypto backend is on, the SAME TU also compiles the mux-route legs (the multiplexing
# transport header pulls in plexus::crypto for its secure member), gated by
# PLEXUS_HAVE_TLS_MUX so the base UDP legs build with crypto off.
add_executable(test_udp_transport test_udp_transport.cpp test_udp_transport_mux.cpp)
target_link_libraries(test_udp_transport PRIVATE
    plexus::asio
    Catch2::Catch2WithMain)
if(PLEXUS_ENABLE_CRYPTO_BACKEND)
    target_sources(test_udp_transport PRIVATE $<TARGET_OBJECTS:plexus-crypto>)
    target_link_libraries(test_udp_transport PRIVATE plexus::crypto)
    target_compile_definitions(test_udp_transport PRIVATE PLEXUS_HAVE_TLS_MUX)
endif()
catch_discover_tests(test_udp_transport TEST_PREFIX "udp.")

add_executable(test_udp_handshake_arq test_udp_handshake_arq.cpp
    test_udp_handshake_arq_component.cpp)
target_link_libraries(test_udp_handshake_arq PRIVATE
    plexus::asio
    Catch2::Catch2WithMain)
catch_discover_tests(test_udp_handshake_arq TEST_PREFIX "udp.")

# The teardown-discipline proof: an inbound datagram delivered AFTER the engine
# destroys a completed accepted/dialed channel mid-session must resolve to a clean
# demux MISS, never a freed-pointer deref (a heap-use-after-free under asan on the
# pre-fix code). Run under the build-asan tree as the boundary proof.
add_executable(test_udp_teardown test_udp_teardown.cpp)
target_link_libraries(test_udp_teardown PRIVATE
    plexus::asio
    Catch2::Catch2WithMain)
catch_discover_tests(test_udp_teardown TEST_PREFIX "udp.")

# The reliable-data ARQ legs (header-only inside plexus::asio): the sans-IO reorder
# buffer's head-of-line in-order release, and the selective-repeat engine proven
# end-to-end over deterministic loss/reorder/duplication via the lossy_relay fixture.
# The `udp.` ctest prefix selects them.
add_executable(test_udp_reorder_buffer test_udp_reorder_buffer.cpp)
target_link_libraries(test_udp_reorder_buffer PRIVATE
    plexus::asio
    Catch2::Catch2WithMain)
catch_discover_tests(test_udp_reorder_buffer TEST_PREFIX "udp.")

add_executable(test_udp_rto_estimator test_udp_rto_estimator.cpp)
target_link_libraries(test_udp_rto_estimator PRIVATE
    plexus::asio
    Catch2::Catch2WithMain)
catch_discover_tests(test_udp_rto_estimator TEST_PREFIX "udp.")

add_executable(test_udp_reliable_arq test_udp_reliable_arq.cpp)
target_link_libraries(test_udp_reliable_arq PRIVATE
    plexus::asio
    Catch2::Catch2WithMain)
catch_discover_tests(test_udp_reliable_arq TEST_PREFIX "udp.")

# The numeric sweep: an empirical substantiation of the RTO/window defaults over a
# loss x window x RTO grid. Hidden behind the [.sweep] Catch2 tag so the default ctest
# run skips it; `ctest -R udp.sweep` selects it.
add_executable(test_udp_arq_sweep test_udp_arq_sweep.cpp)
target_link_libraries(test_udp_arq_sweep PRIVATE
    plexus::asio
    Catch2::Catch2WithMain)
catch_discover_tests(test_udp_arq_sweep TEST_PREFIX "udp.")

# The two-axis selector unit test: a PURE value-object check of the tier x
# scheme->reliability composition (no socket, no io_context). Header-only inside
# plexus::asio; the `udp.` ctest prefix selects it.
add_executable(test_udp_selector test_udp_selector.cpp test_udp_selector_dispatch.cpp)
target_link_libraries(test_udp_selector PRIVATE
    plexus::asio
    Catch2::Catch2WithMain)
catch_discover_tests(test_udp_selector TEST_PREFIX "udp.")

# The congestion knobs over the reliable-datagram ARQ: congestion=block back-
# pressures a full window through a bounded ack-drained queue WITHOUT stalling the
# io_context (a concurrent flow keeps flowing); congestion=drop sheds at the publisher.
# Header-only inside plexus::asio; the `udp.` ctest prefix selects it.
add_executable(test_udp_congestion test_udp_congestion.cpp)
target_link_libraries(test_udp_congestion PRIVATE
    plexus::asio
    Catch2::Catch2WithMain)
catch_discover_tests(test_udp_congestion TEST_PREFIX "udp.")

# The reliable-datagram opt-in engaged through the "udpr" scheme: a "udpr" dial mints a
# reliable-mode channel (symmetric on both ends) whose send() drives the ARQ in-order
# over loss; a "udp" dial stays best_effort; oversize rejects at publish on the reliable
# class too. Header-only inside plexus::asio; the `udp.` ctest prefix selects it.
add_executable(test_udp_reliable_datagram test_udp_reliable_datagram.cpp
    test_udp_reliable_datagram_loss.cpp)
target_link_libraries(test_udp_reliable_datagram PRIVATE
    plexus::asio
    Catch2::Catch2WithMain)
catch_discover_tests(test_udp_reliable_datagram TEST_PREFIX "udp.")

# The randomized per-session ISN (RFC 6528 lineage): the handshake frame carries the ISN
# append-only (legacy frames decode to 0), and a spoofed seq=0 reliable-data segment on a
# non-zero-ISN plaintext udpr session is rejected as out of window. Header-only inside
# plexus::asio; the `udp.` ctest prefix selects it.
add_executable(test_udp_isn test_udp_isn.cpp test_udp_isn_spoof.cpp test_udp_isn_frag.cpp)
target_link_libraries(test_udp_isn PRIVATE
    plexus::asio
    Catch2::Catch2WithMain)
catch_discover_tests(test_udp_isn TEST_PREFIX "udp.")

# Live large-payload fragmentation legs: an oversize payload is split across numbered
# datagrams and reassembled into ONE message at the peer — best_effort (drop-whole on a
# lost fragment via the reassembly timeout) and reliable (each fragment ARQ'd above the
# engine, a lost fragment retransmitted, the message completes byte-equal). Header-only
# inside plexus::asio; the `udp.` ctest prefix selects it.
add_executable(test_udp_fragmentation test_udp_fragmentation.cpp
    test_udp_fragmentation_reliable.cpp)
target_link_libraries(test_udp_fragmentation PRIVATE
    plexus::asio
    Catch2::Catch2WithMain)
catch_discover_tests(test_udp_fragmentation TEST_PREFIX "udp.")

# The opt-in spin-then-park run-loop driver over a user-owned io_context: a live udp
# round-trip carried end-to-end by spin_run_loop::poll_spin(), default-equivalence at
# budget 0, and a breakable park. Header-only inside plexus::asio; the `asio.` ctest
# prefix selects it.
add_executable(test_spin_run_loop test_spin_run_loop.cpp)
target_link_libraries(test_spin_run_loop PRIVATE
    plexus::asio
    Catch2::Catch2WithMain)
catch_discover_tests(test_spin_run_loop TEST_PREFIX "asio.")
