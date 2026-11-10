if(NOT PLEXUS_BUILD_TESTS)
    return()
endif()

set(PLEXUS_TEST_PREFIX "testing.")

plexus_add_catch_test(test_mock_fidelity
    SOURCES test_mock_fidelity.cpp
    LINKS plexus::core plexus::testing)

plexus_add_catch_test(test_pump
    SOURCES test_pump.cpp
    LINKS plexus::testing plexus::inproc)
if(TARGET plexus::asio)
    target_link_libraries(test_pump PRIVATE plexus::asio)
    target_compile_definitions(test_pump PRIVATE PLEXUS_HAVE_ASIO)
endif()

plexus_add_catch_test(test_mock_retire_quiescence
    SOURCES test_mock_retire_quiescence.cpp
    LINKS plexus::core plexus::testing)

# support/alloc_watch.h replaces the global operator new/delete, so its self-test is its own executable.
plexus_add_catch_test(test_alloc_watch
    SOURCES test_alloc_watch.cpp)
