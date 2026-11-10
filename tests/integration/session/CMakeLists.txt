plexus_add_catch_test(test_peer_session_inproc
    SOURCES test_peer_session_inproc.cpp test_peer_session_inproc_flow.cpp test_peer_session_inproc_staleness.cpp
        test_peer_session_inproc_lifecycle.cpp
    LINKS plexus::core plexus::wire plexus::inproc)

plexus_add_catch_test(test_transport_dial_inproc
    SOURCES test_transport_dial_inproc.cpp
    LINKS plexus::core plexus::wire plexus::inproc)

plexus_add_catch_test(test_security_attach_inproc
    SOURCES test_security_attach_inproc.cpp
    LINKS plexus::core plexus::wire plexus::inproc)

plexus_add_catch_test(test_downgrade_inproc
    SOURCES test_downgrade_inproc.cpp
    LINKS plexus::core plexus::wire plexus::inproc)

plexus_add_catch_test(test_host_identity_inproc
    SOURCES test_host_identity_inproc.cpp test_host_identity_inproc_plaintext.cpp
    LINKS plexus::core plexus::wire plexus::inproc)

plexus_add_catch_test(test_heartbeat_decline_gate
    SOURCES test_heartbeat_decline_gate.cpp
    LINKS plexus::core plexus::wire plexus::inproc)

plexus_add_catch_test(test_session_gate
    SOURCES test_session_gate_matrix.cpp test_session_gate_object_lane.cpp test_session_gate_latch.cpp
    LINKS plexus::core plexus::wire plexus::inproc)

plexus_add_catch_test(test_session_identity
    SOURCES test_session_identity.cpp
    LINKS plexus::core plexus::wire plexus::inproc)

plexus_add_catch_test(test_session_teardown_in_callback
    SOURCES test_session_teardown_in_callback.cpp
    LINKS plexus::core plexus::wire plexus::inproc)

plexus_add_catch_test(test_subscribe_refusal_set
    SOURCES test_subscribe_refusal_set.cpp
    LINKS plexus::core plexus::wire plexus::inproc)

# An alloc gate over the single-TU operator-new counter; concurrent test processes perturb it, so it runs alone.
plexus_add_catch_test(test_session_malformed_logging
    SOURCES test_session_malformed_logging.cpp test_session_malformed_clock.cpp test_session_malformed_sites.cpp
    LINKS plexus::core plexus::wire plexus::inproc
    PROPERTIES RUN_SERIAL TRUE)

plexus_add_catch_test(test_dialed_identity
    SOURCES test_dialed_identity_inproc.cpp test_dialed_identity_redial_inproc.cpp
        test_dialed_identity_binding_inproc.cpp
    LINKS plexus::core plexus::wire plexus::inproc)

plexus_add_catch_test(test_slot_reclaim_inproc
    SOURCES test_slot_reclaim_inproc.cpp
    LINKS plexus::core plexus::wire plexus::inproc)

plexus_add_catch_test(test_session_soak_inproc
    SOURCES test_session_soak_inproc.cpp
    LINKS plexus::core plexus::wire plexus::inproc)
# std::getenv is portable, but MSVC's CRT deprecates it (C4996).
if(MSVC)
    target_compile_definitions(test_session_soak_inproc PRIVATE _CRT_SECURE_NO_WARNINGS)
endif()

plexus_add_catch_test(test_subscribe_retry_inproc
    SOURCES test_subscribe_retry_inproc.cpp
    LINKS plexus::core plexus::wire plexus::inproc)

plexus_add_catch_test(test_crossed_dial_inproc
    SOURCES test_crossed_dial_common.cpp test_crossed_dial_inproc.cpp test_crossed_dial_quiet_inproc.cpp
        test_crossed_dial_order_inproc.cpp test_crossed_dial_remote_inproc.cpp
    LINKS plexus::core plexus::wire plexus::inproc)

plexus_add_catch_test(test_proven_identity_inproc
    SOURCES test_proven_identity_common.cpp test_proven_identity_inproc.cpp
    LINKS plexus::core plexus::wire plexus::inproc)
target_sources(test_proven_identity_inproc PRIVATE
    test_crossed_dial_common.cpp
    test_proven_identity_crossed_inproc.cpp)

plexus_add_catch_test(test_same_kind_duplicate_inproc
    SOURCES test_crossed_dial_common.cpp test_same_kind_duplicate_inproc.cpp
    LINKS plexus::core plexus::wire plexus::inproc)

plexus_add_catch_test(test_identity_edges_inproc
    SOURCES test_crossed_dial_common.cpp test_identity_edges_inproc.cpp
    LINKS plexus::core plexus::wire plexus::inproc)

plexus_add_catch_test(test_session_stale_inproc
    SOURCES test_session_stale_inproc.cpp test_session_stale_edge_inproc.cpp test_session_stale_timeout_inproc.cpp
    LINKS plexus::core plexus::wire plexus::inproc)

plexus_add_catch_test(test_reach_guard_inproc
    SOURCES test_reach_guard_inproc.cpp test_reach_guard_backoff_inproc.cpp
    LINKS plexus::core plexus::wire plexus::inproc)

plexus_add_catch_test(test_idle_dialed_slot_inproc
    SOURCES test_idle_dialed_slot_inproc.cpp test_idle_dialed_slot_accept_inproc.cpp
    LINKS plexus::core plexus::wire plexus::inproc)
