if(NOT PLEXUS_BUILD_TESTS)
    return()
endif()

set(PLEXUS_TEST_PREFIX "io.")

set(PLEXUS_IO_TESTS
    test_mtu_budget
    test_udp_backpressure_queue
    test_seq_ring
    test_handshake_gate
    test_assembly_pattern
    test_cert_facts
    test_verify_policy
    test_cookie_secret
    test_reassembler_flat_bound
    test_identity_types
    test_epoch_source
    test_known_peers_storage
    test_known_peers_change
    test_known_peers_binding
    test_liveness_storage
    test_peer_liveliness_types
    test_pending_ops
    test_close_notification
    test_channel_graveyard
    test_slot_rebuild_sequencing
    test_registry_retire
    test_executor_affinity
    test_driver_record
    test_session_gate_classify
    test_warn_line)

plexus_add_catch_test(test_peer_liveliness
    SOURCES test_peer_liveliness.cpp test_peer_liveliness_arbitration.cpp
    LINKS plexus::core)

foreach(test ${PLEXUS_IO_TESTS})
    plexus_add_catch_test(${test}
        SOURCES ${test}.cpp
        LINKS plexus::core)
endforeach()

plexus_add_catch_test(test_send_queue
    SOURCES test_send_queue.cpp test_send_queue_capacity.cpp
    LINKS plexus::core)

plexus_add_catch_test(test_stream_send_queue
    SOURCES test_stream_send_queue.cpp test_stream_send_queue_capacity.cpp test_stream_send_queue_gather.cpp
    LINKS plexus::core plexus::stream)

plexus_add_catch_test(test_pending_dial_registry
    SOURCES test_pending_dial_registry.cpp test_pending_dial_registry_accepted.cpp
    LINKS plexus::core)

plexus_add_catch_test(test_attach_policy
    SOURCES test_attach_policy.cpp test_attach_policy_edges.cpp
    LINKS plexus::core)

# Drives two routing engines over one inproc bus, so unlike its sans-IO siblings it links a backend.
plexus_add_catch_test(test_routing_engine_dial
    SOURCES test_routing_engine_dial.cpp
    LINKS plexus::core plexus::wire plexus::inproc)

# Drives the routing engine over the inproc backend on the plexus::testing virtual clock.
plexus_add_catch_test(test_known_peers_aging
    SOURCES test_known_peers_aging.cpp test_known_peers_aging_session.cpp
    LINKS plexus::core plexus::wire plexus::inproc plexus::testing)

# The oversize-boundary arithmetic is pinned against the real wire::udp_envelope_overhead constant.
target_link_libraries(test_mtu_budget PRIVATE plexus::wire plexus::datagram)

target_link_libraries(test_udp_backpressure_queue PRIVATE plexus::datagram)
target_link_libraries(test_seq_ring PRIVATE plexus::datagram)
target_link_libraries(test_assembly_pattern PRIVATE plexus::datagram)
target_link_libraries(test_send_queue PRIVATE plexus::datagram)

plexus_add_catch_test(test_fragmentation
    SOURCES test_fragmentation.cpp test_fragmentation_split.cpp test_fragmentation_reassemble.cpp
    LINKS plexus::core plexus::wire plexus::datagram plexus::testing)

target_link_libraries(test_reassembler_flat_bound PRIVATE plexus::testing plexus::datagram)

# Owns the single replaceable operator new (support/alloc_counter.h), so it is its own executable; RUN_SERIAL keeps
# its zero-heap count immune to the contention of concurrent test processes.
plexus_add_catch_test(test_move_only_function
    SOURCES test_move_only_function.cpp
    LINKS plexus::core
    PROPERTIES RUN_SERIAL TRUE)

# The interface selector is a core value type, but its resolver lives in the asio backend.
if(PLEXUS_ENABLE_ASIO_BACKEND)
    plexus_add_catch_test(test_network_interface
        SOURCES test_network_interface.cpp
        LINKS plexus::asio plexus::core)
endif()
