# The structural build-discipline checks (no Catch2): a zero-facade standalone link
# check and a real install + out-of-tree find_package(plexus) consume check. Both run
# as ordinary ctest entries under the `node.` prefix. Auto-discovered by the tests glob;
# the nested install_consume/ consumer project is NOT globbed (the glob is one level deep).

# The zero-facade / MCU-minimal link check: links ONLY plexus::core + plexus::inproc, so
# its include path is exactly those two trees. Instantiating the routing_engine compiles
# the core's templates; a core header reaching for a facade or backend header would fail
# header-not-found. plexus::wire arrives transitively through core — linking it explicitly
# would mask a broken core->wire link.
add_executable(core_standalone core_standalone.cpp)
target_link_libraries(core_standalone PRIVATE plexus::core plexus::inproc)
add_test(NAME node.core_standalone COMMAND core_standalone)

# The install + out-of-tree consume check: a real install to a throwaway prefix, then a
# fresh find_package(plexus CONFIG REQUIRED) + compile + link of a consumer linking
# plexus::plexus, all driven by the cmake -P script.
add_test(NAME node.install_consume
    COMMAND ${CMAKE_COMMAND}
        -DPLEXUS_BUILD_DIR=${CMAKE_BINARY_DIR}
        -DPLEXUS_CONSUME_DIR=${CMAKE_CURRENT_SOURCE_DIR}/install_consume
        -P ${CMAKE_CURRENT_SOURCE_DIR}/install_consume.cmake)

# The facade vocabulary unit test — the one place that links plexus::plexus (the facade
# library), exercising the public expected alias and the call_errc error category.
add_executable(test_vocabulary test_vocabulary.cpp)
target_link_libraries(test_vocabulary PRIVATE
    plexus::plexus
    Catch2::Catch2WithMain)
catch_discover_tests(test_vocabulary TEST_PREFIX "node.")
