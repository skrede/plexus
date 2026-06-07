# Core io building-block unit tests: the mtu_budget value-object and the generic
# send_queue serial-drain block. Both are header-only sans-IO core units (no asio,
# no socket), so each links plexus::plexus + Catch2's main — NOT a backend target.
# The "io." ctest prefix lets `ctest -R io\.` select the area; each Catch2 TEST_CASE
# name carries its block substring so `-R send_queue` / `-R mtu_budget` also resolve.
# Auto-discovered by the tests glob (gated on PLEXUS_BUILD_TESTS by the parent).
if(NOT PLEXUS_BUILD_TESTS)
    return()
endif()

set(PLEXUS_IO_TESTS
    test_mtu_budget
    test_send_queue)

foreach(test ${PLEXUS_IO_TESTS})
    add_executable(${test} ${test}.cpp)
    target_link_libraries(${test} PRIVATE plexus::plexus Catch2::Catch2WithMain)
    catch_discover_tests(${test} TEST_PREFIX "io.")
endforeach()

# The mtu_budget oracle pins the oversize-boundary arithmetic against the real
# wire::udp_envelope_overhead constant, so it additionally links the header-only
# plexus::wire interface target (still sans-IO — no backend, no socket).
target_link_libraries(test_mtu_budget PRIVATE plexus::wire)
