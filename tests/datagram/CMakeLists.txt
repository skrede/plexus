if(NOT PLEXUS_BUILD_TESTS)
    return()
endif()

set(PLEXUS_TEST_PREFIX "datagram.")

plexus_add_catch_test(test_anti_replay_window_sweep
    SOURCES test_anti_replay_window_sweep.cpp
    LINKS plexus::datagram)
