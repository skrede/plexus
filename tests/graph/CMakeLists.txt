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

# The fixed twin's direct-first candidate admission: a direct row survives a transitive flood under
# both protection schemes, transitive overflow is rejected-and-counted, and lookup surfaces the direct
# endpoint only. Header-only core, no backend, no discovery.
add_executable(test_fixed_peer_storage_candidates test_fixed_peer_storage_candidates.cpp)
target_link_libraries(test_fixed_peer_storage_candidates PRIVATE plexus::core Catch2::Catch2WithMain)
catch_discover_tests(test_fixed_peer_storage_candidates TEST_PREFIX "graph.")

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

# The relay-decorator vocabulary gate: a compile-time proof that a non-relay profile resolves the
# peer_report emitter to the structurally-absent null twin and composing relay<> selects the real one
# while preserving every storage alias. Reuses the inproc policy + transport as a real mechanism pair.
add_executable(relay_profile_tu relay_profile_tu.cpp)
target_link_libraries(relay_profile_tu PRIVATE plexus::plexus plexus::inproc)
add_test(NAME graph.relay_profile_tu COMMAND relay_profile_tu)

# The participant-snapshot fill + node_id keying oracle over the deterministic inproc backend:
# a basic_node test, so it links the facade + inproc + discovery.
add_executable(test_graph_snapshot test_graph_snapshot.cpp)
target_link_libraries(test_graph_snapshot PRIVATE
    plexus::plexus
    plexus::inproc
    plexus::discovery
    Catch2::Catch2WithMain)
catch_discover_tests(test_graph_snapshot TEST_PREFIX "graph.")

# The extended-world enumeration facade: a reported peer surfaces in participants()/topics() shown as
# relayed (reported-by and via-whom visible) beside an unblended direct peer in the same snapshot. A
# basic_node test over the inproc backend, so it links the facade + inproc + discovery.
add_executable(test_graph_extended_enumeration test_graph_extended_enumeration.cpp)
target_link_libraries(test_graph_extended_enumeration PRIVATE
    plexus::plexus
    plexus::inproc
    plexus::discovery
    Catch2::Catch2WithMain)
catch_discover_tests(test_graph_extended_enumeration TEST_PREFIX "graph.")

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

# The peer_report receive gate chain at the engine seam: origin-universe fail-closed, self-guard, hop
# budget, and per-origin dedup admit a via-only reported candidate without dialing or perturbing the
# direct-peer table. Drives the shared support/graph_change_inproc.h harness, so tests/ is on the path.
add_executable(test_peer_report_admission test_peer_report_admission.cpp)
target_link_libraries(test_peer_report_admission PRIVATE
    plexus::plexus
    plexus::inproc
    Catch2::Catch2WithMain)
target_include_directories(test_peer_report_admission PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/..)
catch_discover_tests(test_peer_report_admission TEST_PREFIX "graph.")

# Withdrawal and TTL retirement of a reported entry: a goodbye flag or an aged-out row retires the
# candidate AND the origin's topic edges without churning the identity; only a live reporter refreshes.
add_executable(test_peer_report_withdrawal test_peer_report_withdrawal.cpp)
target_link_libraries(test_peer_report_withdrawal PRIVATE
    plexus::plexus
    plexus::inproc
    Catch2::Catch2WithMain)
target_include_directories(test_peer_report_withdrawal PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/..)
catch_discover_tests(test_peer_report_withdrawal TEST_PREFIX "graph.")

# The relay emitter's lift + host-side universe stamp + refuse-to-bridge, exercised directly over a
# topic table so the identity, topics-with-types, and universe of the produced report are asserted and
# a foreign-universe origin is refused with no report.
add_executable(test_peer_report_emitter test_peer_report_emitter.cpp)
target_link_libraries(test_peer_report_emitter PRIVATE
    plexus::plexus
    plexus::inproc
    Catch2::Catch2WithMain)
target_include_directories(test_peer_report_emitter PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/..)
catch_discover_tests(test_peer_report_emitter TEST_PREFIX "graph.")

# The relay emit + withdrawal wiring end to end: a relay engine asserts an attached origin to a
# downstream session, and a lost live path (origin goodbye, session death, or TTL age-out) emits a
# withdrawal-flagged report to connected sessions and drops the origin from the replay set.
add_executable(test_peer_report_emitter_withdrawal test_peer_report_emitter_withdrawal.cpp)
target_link_libraries(test_peer_report_emitter_withdrawal PRIVATE
    plexus::plexus
    plexus::inproc
    Catch2::Catch2WithMain)
target_include_directories(test_peer_report_emitter_withdrawal PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/..)
catch_discover_tests(test_peer_report_emitter_withdrawal TEST_PREFIX "graph.")

# The receiver's flood bound against a hostile authenticated relay: a per-reporter origin ceiling and
# a per-origin topic ceiling reject-and-count without evicting a held row or a direct peer, plus the
# reporter-is-origin self-report guard. Drives the shared support/graph_change_inproc.h harness.
add_executable(test_peer_report_flood_bound test_peer_report_flood_bound.cpp)
target_link_libraries(test_peer_report_flood_bound PRIVATE
    plexus::plexus
    plexus::inproc
    Catch2::Catch2WithMain)
target_include_directories(test_peer_report_flood_bound PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/..)
catch_discover_tests(test_peer_report_flood_bound TEST_PREFIX "graph.")

# The refresh/lifetime story of a relayed origin end to end: the relay's periodic re-assert keeps a
# live-but-idle origin alive past the downstream's awareness_ttl, and a downstream retires origins
# reachable only via a relay whose session dies. A relay engine + two plain nodes on one virtual clock.
add_executable(test_peer_report_relay_refresh test_peer_report_relay_refresh.cpp)
target_link_libraries(test_peer_report_relay_refresh PRIVATE
    plexus::plexus
    plexus::inproc
    Catch2::Catch2WithMain)
target_include_directories(test_peer_report_relay_refresh PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/..)
catch_discover_tests(test_peer_report_relay_refresh TEST_PREFIX "graph.")
