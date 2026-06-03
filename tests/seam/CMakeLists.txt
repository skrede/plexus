# Seam-concept smoke test: compile-time proof the byte_channel / Policy concepts
# are satisfiable (SLICE-4 maintainability evidence) plus the wire_bytes
# owner-keeps-bytes-alive guarantee. Links the header-only plexus core and the
# wire layer; the `seam.` ctest prefix lets `ctest -R seam` select it by name.
add_executable(test_seam_concepts test_seam_concepts.cpp)
target_link_libraries(test_seam_concepts PRIVATE
    plexus::plexus
    plexus::wire
    Catch2::Catch2WithMain)
catch_discover_tests(test_seam_concepts TEST_PREFIX "seam.")
