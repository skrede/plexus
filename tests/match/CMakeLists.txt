# The wildcard-matcher vocabulary tests: the refusal enum's distinct values and the tunable
# compile-time bounds, header-only core only, so this links plexus::core + Catch2's main. The
# "match." ctest prefix selects the area; the TEST_CASE names carry "key_pattern" so
# `-R key_pattern` resolves. Auto-discovered by the tests glob (gated on PLEXUS_BUILD_TESTS).
if(NOT PLEXUS_BUILD_TESTS)
    return()
endif()

add_executable(test_key_pattern_bounds test_key_pattern_bounds.cpp)
target_link_libraries(test_key_pattern_bounds PRIVATE plexus::core plexus::wire Catch2::Catch2WithMain)
catch_discover_tests(test_key_pattern_bounds TEST_PREFIX "match.")
