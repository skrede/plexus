if(NOT PLEXUS_BUILD_TESTS)
    return()
endif()

add_executable(test_graph_records test_graph_records.cpp)
target_link_libraries(test_graph_records PRIVATE plexus::core Catch2::Catch2WithMain)
catch_discover_tests(test_graph_records TEST_PREFIX "graph.")

add_executable(inv1_graph_tu inv1_graph_tu.cpp)
target_link_libraries(inv1_graph_tu PRIVATE plexus::core)
add_test(NAME graph.inv1_graph_tu COMMAND inv1_graph_tu)
