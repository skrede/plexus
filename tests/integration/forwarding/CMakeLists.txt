# An alloc gate over the single-TU operator-new counter; concurrent test processes perturb it, so it runs alone.
plexus_add_catch_test(test_route_note_noalloc
    SOURCES test_route_note_noalloc.cpp
    LINKS plexus::core
    PROPERTIES RUN_SERIAL TRUE)

plexus_add_catch_test(test_peer_report_oracle
    SOURCES test_peer_report_oracle.cpp
    LINKS plexus::core plexus::wire)

plexus_add_catch_test(test_forwarded_frame_oracle
    SOURCES test_forwarded_frame_oracle.cpp
    LINKS plexus::core plexus::wire)

plexus_add_catch_test(test_forward_splice_pubsub
    SOURCES test_forward_splice_pubsub.cpp
    LINKS plexus::core plexus::wire plexus::inproc)

plexus_add_catch_test(test_forward_splice_hardening
    SOURCES test_forward_splice_hardening.cpp
    LINKS plexus::core plexus::wire plexus::inproc)

plexus_add_catch_test(test_forward_splice_no_hol
    SOURCES test_forward_splice_no_hol.cpp
    LINKS plexus::core plexus::wire plexus::inproc)

plexus_add_catch_test(test_forward_reqres
    SOURCES test_forward_reqres.cpp
    LINKS plexus::core plexus::wire plexus::inproc)

plexus_add_catch_test(test_forward_pubsub
    SOURCES test_forward_pubsub.cpp
    LINKS plexus::core plexus::wire plexus::inproc)

plexus_add_catch_test(test_forward_dedup_pruning_inproc
    SOURCES test_forward_dedup_pruning_inproc.cpp
    LINKS plexus::core plexus::wire plexus::inproc)

plexus_add_catch_test(test_forward_reply_reresolve
    SOURCES test_forward_reply_reresolve.cpp
    LINKS plexus::core plexus::wire plexus::inproc)

plexus_add_catch_test(test_forwarded_rpc_tables
    SOURCES test_forwarded_rpc_tables.cpp
    LINKS plexus::core plexus::wire plexus::inproc)

plexus_add_catch_test(test_forward_demand_propagation
    SOURCES test_forward_demand_propagation.cpp
    LINKS plexus::core plexus::wire plexus::inproc)
