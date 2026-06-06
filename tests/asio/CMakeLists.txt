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
# (no new compiled backend target); the `udp.` ctest prefix selects them.
add_executable(test_udp_transport test_udp_transport.cpp)
target_link_libraries(test_udp_transport PRIVATE
    plexus::asio
    Catch2::Catch2WithMain)
catch_discover_tests(test_udp_transport TEST_PREFIX "udp.")

add_executable(test_udp_handshake_arq test_udp_handshake_arq.cpp)
target_link_libraries(test_udp_handshake_arq PRIVATE
    plexus::asio
    Catch2::Catch2WithMain)
catch_discover_tests(test_udp_handshake_arq TEST_PREFIX "udp.")
