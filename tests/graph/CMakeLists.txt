if(NOT PLEXUS_BUILD_TESTS)
    return()
endif()

add_executable(test_graph_records test_graph_records.cpp)
target_link_libraries(test_graph_records PRIVATE plexus::core Catch2::Catch2WithMain)
catch_discover_tests(test_graph_records TEST_PREFIX "graph.")

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
