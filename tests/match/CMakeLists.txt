if(NOT PLEXUS_BUILD_TESTS)
    return()
endif()

set(PLEXUS_TEST_PREFIX "match.")

plexus_add_catch_test(test_key_pattern_bounds
    SOURCES test_key_pattern_bounds.cpp
    LINKS plexus::core plexus::wire)

plexus_add_catch_test(test_key_pattern_canonical
    SOURCES test_key_pattern_canonical.cpp
    LINKS plexus::core)

plexus_add_catch_test(test_key_pattern_relations
    SOURCES test_key_pattern_relations.cpp
    LINKS plexus::core)

# Owns the single replaceable operator new (support/alloc_counter.h), so it is its own executable.
plexus_add_catch_test(test_key_pattern_alloc
    SOURCES test_key_pattern_alloc.cpp
    LINKS plexus::core)

# A compile-time gate run as a plain program: the ESP-IDF xtensa cross-compile builds the same source, so its
# static_asserts prove the matcher carries no platform #ifdef.
add_executable(platform_neutral_matcher_tu platform_neutral_matcher_tu.cpp)
target_link_libraries(platform_neutral_matcher_tu PRIVATE plexus::core)
add_test(NAME match.platform_neutral_matcher_tu COMMAND platform_neutral_matcher_tu)
