# Core io building-block unit tests: the mtu_budget value-object, the generic
# send_queue serial-drain block, the pending_dial_registry half-open dial table, and
# the handshake_gate open-before-data buffer. All are header-only sans-IO core units
# (no asio, no socket), so each links plexus::plexus + Catch2's main — NOT a backend
# target. The "io." ctest prefix lets `ctest -R io\.` select the area; each Catch2
# TEST_CASE name carries its block substring so `-R send_queue` / `-R mtu_budget` /
# `-R pending_dial` / `-R handshake_gate` also resolve.
# Auto-discovered by the tests glob (gated on PLEXUS_BUILD_TESTS by the parent).
if(NOT PLEXUS_BUILD_TESTS)
    return()
endif()

set(PLEXUS_IO_TESTS
    test_mtu_budget
    test_send_queue
    test_udp_backpressure_queue
    test_pending_dial_registry
    test_handshake_gate
    test_assembly_pattern
    test_cert_facts
    test_verify_policy
    test_cookie_secret
    test_fragmentation)

foreach(test ${PLEXUS_IO_TESTS})
    add_executable(${test} ${test}.cpp)
    target_link_libraries(${test} PRIVATE plexus::plexus Catch2::Catch2WithMain)
    catch_discover_tests(${test} TEST_PREFIX "io.")
endforeach()

# The mtu_budget oracle pins the oversize-boundary arithmetic against the real
# wire::udp_envelope_overhead constant, so it additionally links the header-only
# plexus::wire interface target (still sans-IO — no backend, no socket).
target_link_libraries(test_mtu_budget PRIVATE plexus::wire)

# The fragment/reassemble unit drives the wire fragment codec and proves the bounded
# reassembler deterministically on the virtual-clock harness, so it additionally links
# the wire codec and the plexus::testing substrate (still sans-IO — no backend, no socket).
target_link_libraries(test_fragmentation PRIVATE plexus::wire plexus::testing)
