if(NOT PLEXUS_BUILD_TESTS)
    return()
endif()

add_executable(test_anti_replay_window_sweep test_anti_replay_window_sweep.cpp)
target_link_libraries(test_anti_replay_window_sweep PRIVATE plexus::datagram Catch2::Catch2WithMain)
catch_discover_tests(test_anti_replay_window_sweep TEST_PREFIX "datagram.")
