# Conformance tests for the reusable test substrate (plexus::testing): the recording
# mock byte_channel against the real-channel contract, and the bounded pump helpers.
# The asio pump cases build only when the asio backend exists.
if(NOT PLEXUS_BUILD_TESTS)
    return()
endif()

add_executable(test_mock_fidelity test_mock_fidelity.cpp)
target_link_libraries(test_mock_fidelity PRIVATE plexus::core plexus::testing Catch2::Catch2WithMain)
catch_discover_tests(test_mock_fidelity TEST_PREFIX "testing.")

add_executable(test_pump test_pump.cpp)
target_link_libraries(test_pump PRIVATE plexus::testing plexus::inproc Catch2::Catch2WithMain)
if(TARGET plexus::asio)
    target_link_libraries(test_pump PRIVATE plexus::asio)
    target_compile_definitions(test_pump PRIVATE PLEXUS_HAVE_ASIO)
endif()
catch_discover_tests(test_pump TEST_PREFIX "testing.")

add_executable(test_mock_retire_quiescence test_mock_retire_quiescence.cpp)
target_link_libraries(test_mock_retire_quiescence PRIVATE plexus::core plexus::testing Catch2::Catch2WithMain)
catch_discover_tests(test_mock_retire_quiescence TEST_PREFIX "testing.")
