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

# The validate-once value type: the canonical refusal + success truth table plus
# the constexpr make() static_asserts. Header-only core, so it links plexus::core
# and Catch2's main only.
add_executable(test_key_pattern_canonical test_key_pattern_canonical.cpp)
target_link_libraries(test_key_pattern_canonical PRIVATE plexus::core Catch2::Catch2WithMain)
catch_discover_tests(test_key_pattern_canonical TEST_PREFIX "match.")

# The intersects set-relation: the MATCH + INTERSECTS truth table (incl. the
# empty-** rows) as constexpr static_asserts AND runtime cases, plus symmetry
# and reflexivity. Header-only core, so it links plexus::core and Catch2's main.
add_executable(test_key_pattern_relations test_key_pattern_relations.cpp)
target_link_libraries(test_key_pattern_relations PRIVATE plexus::core Catch2::Catch2WithMain)
catch_discover_tests(test_key_pattern_relations TEST_PREFIX "match.")

# The zero-alloc construction proof owns the single replaceable operator new
# (support/alloc_counter.h), so it must be its own executable and needs tests/ on
# the include path.
add_executable(test_key_pattern_alloc test_key_pattern_alloc.cpp)
target_link_libraries(test_key_pattern_alloc PRIVATE plexus::core Catch2::Catch2WithMain)
target_include_directories(test_key_pattern_alloc PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/..)
catch_discover_tests(test_key_pattern_alloc TEST_PREFIX "match.")
