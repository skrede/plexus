# Core io building-block unit tests: the pending_dial_registry half-open dial table
# and the handshake_gate open-before-data buffer (core), plus the datagram-family
# value-objects/blocks (mtu_budget, send_queue, the reassembler, the assembly pattern)
# that link the header-only plexus::datagram family. All are header-only sans-IO units
# (no asio, no socket), so each links plexus::core (+ plexus::datagram for the family
# units) + Catch2's main — NOT a backend target. The "io." ctest prefix lets
# `ctest -R io\.` select the area; each Catch2
# TEST_CASE name carries its block substring so `-R send_queue` / `-R mtu_budget` /
# `-R pending_dial` / `-R handshake_gate` also resolve.
# Auto-discovered by the tests glob (gated on PLEXUS_BUILD_TESTS by the parent).
if(NOT PLEXUS_BUILD_TESTS)
    return()
endif()

set(PLEXUS_IO_TESTS
    test_mtu_budget
    test_udp_backpressure_queue
    test_handshake_gate
    test_assembly_pattern
    test_cert_facts
    test_verify_policy
    test_cookie_secret
    test_reassembler_flat_bound
    test_identity_types
    test_epoch_source
    test_known_peers_storage)

foreach(test ${PLEXUS_IO_TESTS})
    add_executable(${test} ${test}.cpp)
    target_link_libraries(${test} PRIVATE plexus::core Catch2::Catch2WithMain)
    catch_discover_tests(${test} TEST_PREFIX "io.")
endforeach()

# The scenario-split block oracles: each split file's siblings compile into the one
# existing target so catch_discover_tests enumerates the identical set.
add_executable(test_send_queue
    test_send_queue.cpp
    test_send_queue_capacity.cpp)
target_link_libraries(test_send_queue PRIVATE plexus::core Catch2::Catch2WithMain)
catch_discover_tests(test_send_queue TEST_PREFIX "io.")

add_executable(test_stream_send_queue
    test_stream_send_queue.cpp
    test_stream_send_queue_capacity.cpp
    test_stream_send_queue_gather.cpp)
target_link_libraries(test_stream_send_queue PRIVATE plexus::core plexus::stream Catch2::Catch2WithMain)
catch_discover_tests(test_stream_send_queue TEST_PREFIX "io.")

add_executable(test_pending_dial_registry
    test_pending_dial_registry.cpp
    test_pending_dial_registry_accepted.cpp)
target_link_libraries(test_pending_dial_registry PRIVATE plexus::core Catch2::Catch2WithMain)
catch_discover_tests(test_pending_dial_registry TEST_PREFIX "io.")

add_executable(test_attach_policy
    test_attach_policy.cpp
    test_attach_policy_edges.cpp)
target_link_libraries(test_attach_policy PRIVATE plexus::core Catch2::Catch2WithMain)
catch_discover_tests(test_attach_policy TEST_PREFIX "io.")

# The mtu_budget oracle pins the oversize-boundary arithmetic against the real
# wire::udp_envelope_overhead constant, so it additionally links the header-only
# plexus::wire interface target alongside the datagram family that homes mtu_budget
# (still sans-IO — no backend, no socket).
target_link_libraries(test_mtu_budget PRIVATE plexus::wire plexus::datagram)

# The datagram-family building-block units link the header-only plexus::datagram
# family that homes their types (send_queue, the udp backpressure queue, the
# assembly pattern, the reassembler flat-map bound) — still sans-IO.
target_link_libraries(test_udp_backpressure_queue PRIVATE plexus::datagram)
target_link_libraries(test_assembly_pattern PRIVATE plexus::datagram)
target_link_libraries(test_send_queue PRIVATE plexus::datagram)

# The fragment/reassemble unit drives the wire fragment codec and proves the bounded
# reassembler deterministically on the virtual-clock harness, so it additionally links
# the wire codec and the plexus::testing substrate (still sans-IO — no backend, no socket).
# Its wire/split/reassemble scenario clusters compile into the one target.
add_executable(test_fragmentation
    test_fragmentation.cpp
    test_fragmentation_split.cpp
    test_fragmentation_reassemble.cpp)
target_link_libraries(test_fragmentation PRIVATE
    plexus::core
    plexus::wire
    plexus::datagram
    plexus::testing
    Catch2::Catch2WithMain)
catch_discover_tests(test_fragmentation TEST_PREFIX "io.")

# The reassembler structural-cost bound oracle drives feed() directly against a crafted
# frag_cnt on the virtual-clock harness, so it additionally links the plexus::testing
# substrate and the datagram family that homes the reassembler (still sans-IO).
target_link_libraries(test_reassembler_flat_bound PRIVATE plexus::testing plexus::datagram)

# The move_only_function fallback oracle asserts the SBO inline storage allocates zero
# heap for a small callable, so it includes support/alloc_counter.h (one operator-new
# override per program) and lives in its own executable, like the wire noalloc gates;
# tests/ is on the include path for it.
add_executable(test_move_only_function test_move_only_function.cpp)
target_link_libraries(test_move_only_function PRIVATE plexus::core Catch2::Catch2WithMain)
target_include_directories(test_move_only_function PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/..)
# RUN_SERIAL: the zero-heap assertion snapshots the single-TU global operator-new counter
# around the wrap; serializing keeps the count immune to concurrent-process contention.
catch_discover_tests(test_move_only_function TEST_PREFIX "io."
    PROPERTIES RUN_SERIAL TRUE)
