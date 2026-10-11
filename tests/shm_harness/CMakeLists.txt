# Multi-process node harness for same-host shared memory: each case re-execs its own binary as separate node
# processes coordinated through a named control block.
if(NOT PLEXUS_ENABLE_SHM_BACKEND OR NOT TARGET plexus::asio::shm)
    return()
endif()

function(plexus_add_shm_harness_test name)
    cmake_parse_arguments(PARSE_ARGV 1 harness "" "" "PROPERTIES")
    add_executable(${name} xproc_main.cpp node_role.cpp ${harness_UNPARSED_ARGUMENTS})

    target_include_directories(${name} PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}
        ${PROJECT_SOURCE_DIR}/tests/shm)

    target_link_libraries(${name} PRIVATE
        plexus::plexus
        plexus::core
        plexus::wire
        plexus::asio
        plexus::asio::shm
        plexus::native
        plexus::testing
        Catch2::Catch2)

    catch_discover_tests(${name} TEST_PREFIX "shm." PROPERTIES ${harness_PROPERTIES})
endfunction()

if(LINUX)
    plexus_add_shm_harness_test(test_shm_harness test_shm_harness_baseline.cpp)
endif()

# Every case in this executable is an expected failure that pins a same-host delivery defect; the label keeps the set
# listable with ctest -L shm-red. The ring's creator unlinking a live region is a POSIX behavior (Windows mappings
# are reference counted), so the set is Linux-only. A stalled burst spins for tens of seconds.
if(LINUX)
    plexus_add_shm_harness_test(test_shm_lane_probes
        registry_role.cpp
        test_shm_lane_probes_fan.cpp
        test_shm_lane_probes_burst.cpp
        test_shm_lane_probes_restart.cpp
        PROPERTIES LABELS "shm-red" TIMEOUT 300)
endif()

# On the threaded notifier floor a denied subscriber is still woken, so the case would pass and its expected-failure
# tag would fail the suite.
if(LINUX AND PLEXUS_LIBURING_HAS_FUTEX)
    plexus_add_shm_harness_test(test_shm_harness_denial test_shm_harness_denial.cpp)
endif()
