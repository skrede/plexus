# Conformance tests for the reusable test substrate (plexus::testing): the
# recording mock byte_channel's fidelity against the real-channel contract. The
# substrate is header-only, so the executable links plexus::plexus + plexus::testing
# + Catch2's main — no backend target. The "testing." ctest prefix lets
# `ctest -R testing\.` select the area. Auto-discovered by the tests glob; gated on
# PLEXUS_BUILD_TESTS by the parent.
if(NOT PLEXUS_BUILD_TESTS)
    return()
endif()

add_executable(test_mock_fidelity test_mock_fidelity.cpp)
target_link_libraries(test_mock_fidelity PRIVATE plexus::plexus plexus::testing Catch2::Catch2WithMain)
catch_discover_tests(test_mock_fidelity TEST_PREFIX "testing.")
