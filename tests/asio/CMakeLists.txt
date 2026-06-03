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
