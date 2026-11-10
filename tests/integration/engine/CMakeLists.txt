# The alloc gate snapshots the single-TU global operator-new counter across a steady-state loop and asserts a zero
# delta; concurrent test processes perturb that window, so it runs alone.
plexus_add_catch_test(test_hot_path_alloc
    SOURCES test_hot_path_alloc.cpp
    LINKS plexus::core plexus::wire plexus::inproc
    PROPERTIES RUN_SERIAL TRUE)

# An alloc gate over the single-TU operator-new counter; concurrent test processes perturb it, so it runs alone.
plexus_add_catch_test(test_typed_inproc_alloc
    SOURCES test_typed_inproc_alloc.cpp
    LINKS plexus::plexus plexus::inproc plexus::discovery
    PROPERTIES RUN_SERIAL TRUE)

plexus_add_catch_test(test_reqres_roundtrip_inproc
    SOURCES test_reqres_roundtrip_inproc.cpp test_reqres_roundtrip_inproc_noalloc.cpp
    LINKS plexus::core plexus::wire plexus::inproc)

plexus_add_catch_test(test_peer_session_same_host_inproc
    SOURCES test_peer_session_same_host_inproc.cpp
    LINKS plexus::core plexus::shm plexus::wire plexus::inproc)

# An alloc gate over the single-TU operator-new counter; concurrent test processes perturb it, so it runs alone.
plexus_add_catch_test(test_reconnect_inproc
    SOURCES test_reconnect_inproc.cpp
    LINKS plexus::core plexus::wire plexus::inproc
    PROPERTIES RUN_SERIAL TRUE)

plexus_add_catch_test(test_reconnect_observer_inproc
    SOURCES test_reconnect_observer_inproc.cpp
    LINKS plexus::core plexus::wire plexus::inproc)

plexus_add_catch_test(test_routing_engine_inproc
    SOURCES test_routing_engine_inproc.cpp test_routing_engine_inproc_isolation.cpp
    LINKS plexus::core plexus::wire plexus::inproc)

plexus_add_catch_test(test_shared_receive_route
    SOURCES test_shared_receive_route.cpp
    LINKS plexus::core plexus::wire plexus::inproc)

plexus_add_catch_test(test_object_route_inproc
    SOURCES test_object_route_inproc.cpp
    LINKS plexus::core plexus::wire plexus::inproc)

plexus_add_catch_test(test_drop_seam_inproc
    SOURCES test_drop_seam_inproc.cpp
    LINKS plexus::core plexus::shm plexus::wire plexus::inproc)

plexus_add_catch_test(test_multipeer_reconnect_inproc
    SOURCES test_multipeer_reconnect_inproc.cpp
    LINKS plexus::core plexus::wire plexus::inproc)

plexus_add_catch_test(test_peer_observer_inproc
    SOURCES test_peer_observer_inproc.cpp
    LINKS plexus::core plexus::wire plexus::inproc)

plexus_add_catch_test(test_graph_change_reentrancy_inproc
    SOURCES test_graph_change_reentrancy_inproc.cpp
    LINKS plexus::core plexus::wire plexus::inproc)

# An alloc gate over the single-TU operator-new counter; concurrent test processes perturb it, so it runs alone.
plexus_add_catch_test(test_graph_change_alloc
    SOURCES test_graph_change_alloc.cpp
    LINKS plexus::core plexus::wire plexus::inproc
    PROPERTIES RUN_SERIAL TRUE)

plexus_add_catch_test(test_obs_tap_points
    SOURCES test_obs_tap_points.cpp test_obs_tap_points_rpc.cpp
    LINKS plexus::core plexus::wire plexus::inproc)

plexus_add_catch_test(test_spine_lifecycle_inproc
    SOURCES test_spine_lifecycle_inproc.cpp test_spine_lifecycle_inproc_edges.cpp
    LINKS plexus::plexus plexus::wire plexus::inproc plexus::discovery)

plexus_add_catch_test(test_unsubscribe_e2e_inproc
    SOURCES test_unsubscribe_e2e_inproc.cpp
    LINKS plexus::plexus plexus::wire plexus::inproc plexus::discovery)

plexus_add_catch_test(test_dispatch_hint_retention_inproc
    SOURCES test_dispatch_hint_retention_inproc.cpp
    LINKS plexus::plexus plexus::wire plexus::inproc plexus::discovery)

plexus_add_catch_test(test_unsolicited_data_inproc
    SOURCES test_unsolicited_data_inproc.cpp
    LINKS plexus::core plexus::wire plexus::inproc)

if(PLEXUS_ENABLE_ASIO_BACKEND)
    target_link_libraries(test_hot_path_alloc PRIVATE plexus::asio)
    target_compile_definitions(test_hot_path_alloc PRIVATE PLEXUS_HAVE_ASIO_MUX)

    if(PLEXUS_ENABLE_CRYPTO_BACKEND)
        target_sources(test_hot_path_alloc PRIVATE $<TARGET_OBJECTS:plexus-crypto>)
        target_link_libraries(test_hot_path_alloc PRIVATE plexus::crypto)
        target_compile_definitions(test_hot_path_alloc PRIVATE PLEXUS_HAVE_CRYPTO_DATAGRAM)
    endif()
endif()

plexus_add_catch_test(test_forwarded_dedup_delivery
    SOURCES test_forwarded_dedup_delivery.cpp
    LINKS plexus::core plexus::wire plexus::inproc)
