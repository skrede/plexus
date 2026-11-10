set(PLEXUS_TEST_PREFIX "forwarder.")

plexus_add_catch_test(test_message_forwarder
    SOURCES test_message_forwarder.cpp test_message_forwarder_receive.cpp test_message_forwarder_alloc.cpp
        test_message_forwarder_directions.cpp
    LINKS plexus::core plexus::wire plexus::inproc)

plexus_add_catch_test(test_subscribe_duplicate
    SOURCES test_subscribe_duplicate.cpp
    LINKS plexus::core plexus::wire plexus::inproc)

plexus_add_catch_test(test_forward_splice_alloc
    SOURCES test_forward_splice_alloc.cpp
    LINKS plexus::core plexus::wire)

plexus_add_catch_test(test_forward_gate
    SOURCES test_forward_gate.cpp
    LINKS plexus::core plexus::wire)

plexus_add_catch_test(test_frame_router
    SOURCES test_frame_router.cpp test_frame_router_drop.cpp
    LINKS plexus::core plexus::wire)

plexus_add_catch_test(test_procedure_forwarder
    SOURCES test_procedure_forwarder.cpp test_procedure_forwarder_alloc.cpp test_procedure_forwarder_timeout.cpp
    LINKS plexus::core plexus::wire plexus::inproc)

plexus_add_catch_test(test_procedure_forwarder_detach_reentry
    SOURCES test_procedure_forwarder_detach_reentry.cpp
    LINKS plexus::core plexus::wire plexus::inproc)

plexus_add_catch_test(test_procedure_forwarder_deferred_reply
    SOURCES test_procedure_forwarder_deferred_reply.cpp
    LINKS plexus::core plexus::wire plexus::inproc)

plexus_add_catch_test(test_latch_forwarder
    SOURCES test_latch_forwarder.cpp test_latch_forwarder_churn.cpp test_latch_forwarder_alloc.cpp
    LINKS plexus::core plexus::wire plexus::inproc)

plexus_add_catch_test(test_message_info
    SOURCES test_message_info.cpp test_message_info_gid.cpp
    LINKS plexus::core plexus::wire plexus::inproc)

plexus_add_catch_test(test_type_id_match
    SOURCES test_type_id_match.cpp test_type_id_match_strict.cpp
    LINKS plexus::core plexus::wire plexus::inproc)

plexus_add_catch_test(test_durability_replay
    SOURCES test_durability_replay.cpp
    LINKS plexus::core plexus::wire plexus::inproc)

plexus_add_catch_test(test_history_ring
    SOURCES test_history_ring.cpp test_history_ring_depth.cpp
    LINKS plexus::core plexus::wire plexus::inproc)

plexus_add_catch_test(test_priority_band_queue
    SOURCES test_priority_band_queue.cpp test_priority_band_queue_alloc.cpp
    LINKS plexus::core)

plexus_add_catch_test(test_egress_priority
    SOURCES test_egress_priority.cpp
    LINKS plexus::core plexus::wire plexus::inproc)

plexus_add_catch_test(test_fetch_latched
    SOURCES test_fetch_latched.cpp
    LINKS plexus::core plexus::wire plexus::inproc)

plexus_add_catch_test(test_liveliness_monitor
    SOURCES test_liveliness_monitor.cpp test_liveliness_monitor_lifetime.cpp
    LINKS plexus::core plexus::wire plexus::inproc plexus::testing)

plexus_add_catch_test(test_subscriber_qos_thread
    SOURCES test_subscriber_qos_thread.cpp
    LINKS plexus::core plexus::wire plexus::inproc)

plexus_add_catch_test(test_qos_rxo
    SOURCES test_qos_rxo.cpp test_qos_rxo_size.cpp
    LINKS plexus::core plexus::wire)

plexus_add_catch_test(test_publish_object
    SOURCES test_publish_object.cpp test_publish_object_latch.cpp
    LINKS plexus::core plexus::wire plexus::inproc)

