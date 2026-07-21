if(NOT PLEXUS_BUILD_TESTS)
    return()
endif()

add_executable(test_graph_records test_graph_records.cpp)
target_link_libraries(test_graph_records PRIVATE plexus::core Catch2::Catch2WithMain)
catch_discover_tests(test_graph_records TEST_PREFIX "graph.")

# The pure stateless route-selection ranking over a candidate span: direct before relayed, fewer hops
# before more, first-in-span tie-break. Header-only core, no backend, no discovery.
add_executable(test_route_select test_route_select.cpp)
target_link_libraries(test_route_select PRIVATE plexus::core Catch2::Catch2WithMain)
catch_discover_tests(test_route_select TEST_PREFIX "graph.")

# The owning-table semantics over the fixed-capacity backend. It owns the single replaceable
# operator new (support/alloc_counter.h) for the zero-alloc sweep proof, so it must be its own
# executable and needs tests/ on the include path.
add_executable(test_graph_topic_table test_graph_topic_table.cpp)
target_link_libraries(test_graph_topic_table PRIVATE plexus::core Catch2::Catch2WithMain)
target_include_directories(test_graph_topic_table PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/..)
catch_discover_tests(test_graph_topic_table TEST_PREFIX "graph.")

# The change-detection signal on the topic-table mutators (upsert changed flag + did-erase removals),
# proven symmetric across both storage twins. Header-only core, no backend.
add_executable(test_graph_change_signal test_graph_change_signal.cpp)
target_link_libraries(test_graph_change_signal PRIVATE plexus::core Catch2::Catch2WithMain)
catch_discover_tests(test_graph_change_signal TEST_PREFIX "graph.")

# The platform-independence gate reaches the query sweeps as well as the records, so it links the
# facade's header tree — no backend, no discovery: it constructs nothing that needs one.
add_executable(inv1_graph_tu inv1_graph_tu.cpp)
target_link_libraries(inv1_graph_tu PRIVATE plexus::plexus)
add_test(NAME graph.inv1_graph_tu COMMAND inv1_graph_tu)

# The target_profile vocabulary gate: a compile-time proof that a bare policy resolves to the heap
# twins and bounded<> resolves the graph tables to the fixed twins. It reuses the inproc policy as a
# real mechanism policy, so it links the facade plus the inproc backend.
add_executable(target_profile_tu target_profile_tu.cpp)
target_link_libraries(target_profile_tu PRIVATE plexus::plexus plexus::inproc)
add_test(NAME graph.target_profile_tu COMMAND target_profile_tu)

# Through-the-facade reachability: a single-transport bounded<> node's engine resolves peer and topic
# storage to the fixed twins. It reuses the inproc policy + inproc transport as a real mechanism pair.
add_executable(facade_bounded_reachable_tu facade_bounded_reachable_tu.cpp)
target_link_libraries(facade_bounded_reachable_tu PRIVATE plexus::plexus plexus::inproc)
add_test(NAME graph.facade_bounded_reachable_tu COMMAND facade_bounded_reachable_tu)

# The muxify heap-revert guard: a two-transport bounded<> node resolves to the SAME fixed twins, so
# storage rides the profile, not muxify. The two members are minimal in-TU mux_member mocks.
add_executable(muxify_bounded_storage_tu muxify_bounded_storage_tu.cpp)
target_link_libraries(muxify_bounded_storage_tu PRIVATE plexus::plexus plexus::inproc)
add_test(NAME graph.muxify_bounded_storage_tu COMMAND muxify_bounded_storage_tu)

# The participant-snapshot fill + node_id keying oracle over the deterministic inproc backend:
# a basic_node test, so it links the facade + inproc + discovery.
add_executable(test_graph_snapshot test_graph_snapshot.cpp)
target_link_libraries(test_graph_snapshot PRIVATE
    plexus::plexus
    plexus::inproc
    plexus::discovery
    Catch2::Catch2WithMain)
catch_discover_tests(test_graph_snapshot TEST_PREFIX "graph.")

# The reject-and-count + zero-alloc proof owns the single replaceable operator new
# (support/alloc_counter.h), so it must be its own executable and needs tests/ on the include path.
add_executable(test_graph_truncate test_graph_truncate.cpp)
target_link_libraries(test_graph_truncate PRIVATE
    plexus::plexus
    plexus::inproc
    plexus::discovery
    Catch2::Catch2WithMain)
target_include_directories(test_graph_truncate PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/..)
catch_discover_tests(test_graph_truncate TEST_PREFIX "graph.")

# The foreign-thread post-and-wait race gate needs a thread-safe executor post — asio's post() is
# thread-safe, the cooperative inproc step-executor's is not — so the gate runs over the asio node
# and links plexus::asio (available only under the backend gate).
#
# The universe-scope-by-construction exclusion proof rides the multicast_discovery on_inbound
# universe drop, which lives on the asio multicast path — it links plexus::asio + default_discovery
# for real loopback multicast, not the inproc static-discovery path (which has no universe compare).
if(PLEXUS_ENABLE_ASIO_BACKEND)
    add_executable(test_graph_foreign_thread test_graph_foreign_thread.cpp)
    target_link_libraries(test_graph_foreign_thread PRIVATE
        plexus::plexus
        plexus::asio
        plexus::discovery
        Catch2::Catch2WithMain)
    catch_discover_tests(test_graph_foreign_thread TEST_PREFIX "graph.")

    add_executable(test_graph_two_universe test_graph_two_universe.cpp)
    target_link_libraries(test_graph_two_universe PRIVATE
        plexus::plexus
        plexus::asio
        plexus::discovery
        Catch2::Catch2WithMain)
    catch_discover_tests(test_graph_two_universe TEST_PREFIX "graph.")
endif()

# The cross-participant propagation oracle: a two-node inproc pair driving real sessions, so it
# links the facade + inproc + discovery like the snapshot oracle.
add_executable(test_graph_topics test_graph_topics.cpp)
target_link_libraries(test_graph_topics PRIVATE
    plexus::plexus
    plexus::inproc
    plexus::discovery
    Catch2::Catch2WithMain)
catch_discover_tests(test_graph_topics TEST_PREFIX "graph.")

# The graph-change observer engine-level unit oracles: a recording graph observer on a real inproc
# engine pins the coalescing/idempotency, the posted-not-synchronous teardown, and the host {who, kind}
# payload. Each drives the shared support/graph_change_inproc.h harness, so tests/ is on the include path.
add_executable(test_graph_change_coalescing test_graph_change_coalescing.cpp)
target_link_libraries(test_graph_change_coalescing PRIVATE
    plexus::plexus
    plexus::inproc
    plexus::discovery
    Catch2::Catch2WithMain)
target_include_directories(test_graph_change_coalescing PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/..)
catch_discover_tests(test_graph_change_coalescing TEST_PREFIX "graph.")

add_executable(test_graph_change_teardown test_graph_change_teardown.cpp)
target_link_libraries(test_graph_change_teardown PRIVATE
    plexus::plexus
    plexus::inproc
    plexus::discovery
    Catch2::Catch2WithMain)
target_include_directories(test_graph_change_teardown PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/..)
catch_discover_tests(test_graph_change_teardown TEST_PREFIX "graph.")

add_executable(test_graph_change_payload test_graph_change_payload.cpp)
target_link_libraries(test_graph_change_payload PRIVATE
    plexus::plexus
    plexus::inproc
    plexus::discovery
    Catch2::Catch2WithMain)
target_include_directories(test_graph_change_payload PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/..)
catch_discover_tests(test_graph_change_payload TEST_PREFIX "graph.")

# The public query surface the propagation feeds. It drives a three-node topology (two rival
# publishers must both be remote to the node enumerating them), so it is its own executable rather
# than a widening of the two-node propagation oracle.
add_executable(test_graph_topic_query test_graph_topic_query.cpp)
target_link_libraries(test_graph_topic_query PRIVATE
    plexus::plexus
    plexus::inproc
    plexus::discovery
    Catch2::Catch2WithMain)
catch_discover_tests(test_graph_topic_query TEST_PREFIX "graph.")

# The same query surface asked at a node that is itself party to the answer: its own endpoints in
# its own table, and the retire path they leave by. Shares the query oracle's three-node fixture.
add_executable(test_graph_self_edges test_graph_self_edges.cpp)
target_link_libraries(test_graph_self_edges PRIVATE
    plexus::plexus
    plexus::inproc
    plexus::discovery
    Catch2::Catch2WithMain)
catch_discover_tests(test_graph_self_edges TEST_PREFIX "graph.")
