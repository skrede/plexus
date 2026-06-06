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
# (no new compiled backend target); the `udp.` ctest prefix selects them. When the TLS
# backend is on, the SAME TU also compiles the mux-route legs (the multiplexing
# transport header pulls in plexus::tls for its secure member), gated by
# PLEXUS_HAVE_TLS_MUX so the base UDP legs build with TLS off.
add_executable(test_udp_transport test_udp_transport.cpp)
target_link_libraries(test_udp_transport PRIVATE
    plexus::asio
    Catch2::Catch2WithMain)
if(PLEXUS_ENABLE_TLS_BACKEND)
    target_sources(test_udp_transport PRIVATE $<TARGET_OBJECTS:plexus-tls>)
    target_link_libraries(test_udp_transport PRIVATE plexus::tls)
    target_compile_definitions(test_udp_transport PRIVATE PLEXUS_HAVE_TLS_MUX)
endif()
catch_discover_tests(test_udp_transport TEST_PREFIX "udp.")

add_executable(test_udp_handshake_arq test_udp_handshake_arq.cpp)
target_link_libraries(test_udp_handshake_arq PRIVATE
    plexus::asio
    Catch2::Catch2WithMain)
catch_discover_tests(test_udp_handshake_arq TEST_PREFIX "udp.")

# The reliable-data ARQ legs (header-only inside plexus::asio): the sans-IO reorder
# buffer's head-of-line in-order release (D-04), and the selective-repeat engine proven
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
add_executable(test_udp_selector test_udp_selector.cpp)
target_link_libraries(test_udp_selector PRIVATE
    plexus::asio
    Catch2::Catch2WithMain)
catch_discover_tests(test_udp_selector TEST_PREFIX "udp.")

# The congestion knobs (D-05) over the reliable-datagram ARQ: congestion=block back-
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
add_executable(test_udp_reliable_datagram test_udp_reliable_datagram.cpp)
target_link_libraries(test_udp_reliable_datagram PRIVATE
    plexus::asio
    Catch2::Catch2WithMain)
catch_discover_tests(test_udp_reliable_datagram TEST_PREFIX "udp.")
