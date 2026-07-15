if(NOT PLEXUS_BUILD_TESTS)
    return()
endif()

add_executable(test_graph_records test_graph_records.cpp)
target_link_libraries(test_graph_records PRIVATE plexus::core Catch2::Catch2WithMain)
catch_discover_tests(test_graph_records TEST_PREFIX "graph.")

# The owning-table semantics over the fixed-capacity backend. It owns the single replaceable
# operator new (support/alloc_counter.h) for the zero-alloc sweep proof, so it must be its own
# executable and needs tests/ on the include path.
add_executable(test_graph_topic_table test_graph_topic_table.cpp)
target_link_libraries(test_graph_topic_table PRIVATE plexus::core Catch2::Catch2WithMain)
target_include_directories(test_graph_topic_table PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/..)
catch_discover_tests(test_graph_topic_table TEST_PREFIX "graph.")

add_executable(inv1_graph_tu inv1_graph_tu.cpp)
target_link_libraries(inv1_graph_tu PRIVATE plexus::core)
add_test(NAME graph.inv1_graph_tu COMMAND inv1_graph_tu)

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
