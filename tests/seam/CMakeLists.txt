# Seam-concept smoke test: compile-time proof the byte_channel / Policy concepts
# are satisfiable (maintainability evidence) plus the wire_bytes
# owner-keeps-bytes-alive guarantee. Links the header-only plexus core and the
# wire layer; the `seam.` ctest prefix lets `ctest -R seam` select it by name.
add_executable(test_seam_concepts test_seam_concepts.cpp)
target_link_libraries(test_seam_concepts PRIVATE
    plexus::core
    plexus::wire
    Catch2::Catch2WithMain)
catch_discover_tests(test_seam_concepts TEST_PREFIX "seam.")

# Constrained-target seam proof: fires static_assert(Policy<freertos_policy>) at
# compile time and instantiates the generic core (peer_context) against the
# substrate to prove it binds unchanged. Links the constrained-target substrate
# alongside the core and wire layers; registered under the same `seam.` prefix.
add_executable(test_freertos_seam test_freertos_seam.cpp)
target_link_libraries(test_freertos_seam PRIVATE
    plexus::core
    plexus::wire
    plexus::mcu
    Catch2::Catch2WithMain)
catch_discover_tests(test_freertos_seam TEST_PREFIX "seam.")

# The permissive/strict reliability enforcement gate on the routing_engine subscribe
# demand path — a deterministic inproc seam test (no backend socket). Links the inproc
# Policy substrate for the engine harness. Registered under the `udp.` prefix so
# `ctest -R udp` selects it with the rest of the phase's reliability legs.
add_executable(test_reliability_enforcement
    test_reliability_enforcement.cpp
    test_reliability_enforcement_classifier.cpp)
target_link_libraries(test_reliability_enforcement PRIVATE
    plexus::core
    plexus::wire
    plexus::inproc
    Catch2::Catch2WithMain)
catch_discover_tests(test_reliability_enforcement TEST_PREFIX "udp.")
