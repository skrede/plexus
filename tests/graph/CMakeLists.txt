if(NOT PLEXUS_BUILD_TESTS)
    return()
endif()

set(PLEXUS_TEST_PREFIX "graph.")

plexus_add_catch_test(test_graph_records
    SOURCES test_graph_records.cpp
    LINKS plexus::core)

plexus_add_catch_test(test_route_select
    SOURCES test_route_select.cpp
    LINKS plexus::core)

plexus_add_catch_test(test_fixed_peer_storage_candidates
    SOURCES test_fixed_peer_storage_candidates.cpp
    LINKS plexus::core)

# Owns the single replaceable operator new (support/alloc_counter.h) for its zero-alloc sweep proof, so it is
# its own executable.
plexus_add_catch_test(test_graph_topic_table
    SOURCES test_graph_topic_table.cpp
    LINKS plexus::core)

plexus_add_catch_test(test_graph_change_signal
    SOURCES test_graph_change_signal.cpp
    LINKS plexus::core)

# A compile-time gate run as a plain program. It reaches the query sweeps as well as the records, so it links the
# facade's header tree and no backend: it constructs nothing that needs one.
add_executable(platform_neutral_graph_tu platform_neutral_graph_tu.cpp)
target_link_libraries(platform_neutral_graph_tu PRIVATE plexus::plexus)
add_test(NAME graph.platform_neutral_graph_tu COMMAND platform_neutral_graph_tu)

# A compile-time profile proof run as a plain program; it reuses the inproc policy as a real mechanism policy.
add_executable(target_profile_tu target_profile_tu.cpp)
target_link_libraries(target_profile_tu PRIVATE plexus::plexus plexus::inproc)
add_test(NAME graph.target_profile_tu COMMAND target_profile_tu)

# Reuses the inproc policy and transport as a real mechanism pair.
add_executable(facade_bounded_reachable_tu facade_bounded_reachable_tu.cpp)
target_link_libraries(facade_bounded_reachable_tu PRIVATE plexus::plexus plexus::inproc)
add_test(NAME graph.facade_bounded_reachable_tu COMMAND facade_bounded_reachable_tu)

add_executable(muxify_bounded_storage_tu muxify_bounded_storage_tu.cpp)
target_link_libraries(muxify_bounded_storage_tu PRIVATE plexus::plexus plexus::inproc)
add_test(NAME graph.muxify_bounded_storage_tu COMMAND muxify_bounded_storage_tu)

# Reuses the inproc policy and transport as a real mechanism pair.
add_executable(relay_profile_tu relay_profile_tu.cpp)
target_link_libraries(relay_profile_tu PRIVATE plexus::plexus plexus::inproc)
add_test(NAME graph.relay_profile_tu COMMAND relay_profile_tu)

plexus_add_catch_test(test_graph_snapshot
    SOURCES test_graph_snapshot.cpp
    LINKS plexus::plexus plexus::inproc plexus::discovery)

plexus_add_catch_test(test_graph_extended_enumeration
    SOURCES test_graph_extended_enumeration.cpp
    LINKS plexus::plexus plexus::inproc plexus::discovery)

# Owns the single replaceable operator new (support/alloc_counter.h), so it is its own executable.
plexus_add_catch_test(test_graph_truncate
    SOURCES test_graph_truncate.cpp
    LINKS plexus::plexus plexus::inproc plexus::discovery)

if(PLEXUS_ENABLE_ASIO_BACKEND)
    plexus_add_catch_test(test_graph_foreign_thread
        SOURCES test_graph_foreign_thread.cpp test_graph_blocking_helper_rejected.cpp
        LINKS plexus::plexus plexus::asio plexus::discovery
        PROPERTIES TIMEOUT 60)

    # Proven over real loopback multicast, the path the universe drop lives on. RUN_SERIAL and the loopback lock keep
    # it off the other socket legs, and the label lets runners without multicast loopback exclude it.
    plexus_add_catch_test(test_graph_two_universe
        SOURCES test_graph_two_universe.cpp
        LINKS plexus::plexus plexus::asio plexus::discovery
        PROPERTIES RUN_SERIAL TRUE RESOURCE_LOCK tcp-loopback LABELS "live-multicast")
endif()

plexus_add_catch_test(test_graph_topics
    SOURCES test_graph_topics.cpp
    LINKS plexus::plexus plexus::inproc plexus::discovery)

plexus_add_catch_test(test_graph_change_coalescing
    SOURCES test_graph_change_coalescing.cpp
    LINKS plexus::plexus plexus::inproc plexus::discovery)

plexus_add_catch_test(test_graph_change_teardown
    SOURCES test_graph_change_teardown.cpp
    LINKS plexus::plexus plexus::inproc plexus::discovery)

plexus_add_catch_test(test_graph_change_payload
    SOURCES test_graph_change_payload.cpp
    LINKS plexus::plexus plexus::inproc plexus::discovery)

plexus_add_catch_test(test_bounded_card_refusal
    SOURCES test_bounded_card_refusal.cpp
    LINKS plexus::plexus plexus::inproc)

# Its own executable: the three-node topology (two rival publishers both remote to the enumerating node) would
# widen the two-node propagation oracle.
plexus_add_catch_test(test_graph_topic_query
    SOURCES test_graph_topic_query.cpp
    LINKS plexus::plexus plexus::inproc plexus::discovery)

plexus_add_catch_test(test_graph_self_edges
    SOURCES test_graph_self_edges.cpp
    LINKS plexus::plexus plexus::inproc plexus::discovery)

plexus_add_catch_test(test_peer_report_admission
    SOURCES test_peer_report_admission.cpp
    LINKS plexus::plexus plexus::inproc)

plexus_add_catch_test(test_peer_report_withdrawal
    SOURCES test_peer_report_withdrawal.cpp
    LINKS plexus::plexus plexus::inproc)

plexus_add_catch_test(test_peer_report_emitter
    SOURCES test_peer_report_emitter.cpp
    LINKS plexus::plexus plexus::inproc)

plexus_add_catch_test(test_peer_report_emitter_withdrawal
    SOURCES test_peer_report_emitter_withdrawal.cpp
    LINKS plexus::plexus plexus::inproc)

plexus_add_catch_test(test_peer_report_flood_bound
    SOURCES test_peer_report_flood_bound.cpp
    LINKS plexus::plexus plexus::inproc)

plexus_add_catch_test(test_peer_report_relay_refresh
    SOURCES test_peer_report_relay_refresh.cpp
    LINKS plexus::plexus plexus::inproc)
