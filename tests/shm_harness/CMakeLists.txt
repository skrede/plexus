# Multi-process node harness for same-host shared memory: each case re-execs its own binary as separate node
# processes coordinated through a named control block.
if(NOT PLEXUS_ENABLE_SHM_BACKEND OR NOT TARGET plexus::asio::shm)
    return()
endif()

function(plexus_add_shm_harness_test name)
    add_executable(${name} xproc_main.cpp node_role.cpp ${ARGN})

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

    catch_discover_tests(${name} TEST_PREFIX "shm.")
endfunction()

if(LINUX)
    plexus_add_shm_harness_test(test_shm_harness test_shm_harness_baseline.cpp)
endif()

# On the threaded notifier floor a denied subscriber is still woken, so the case would pass and its expected-failure
# tag would fail the suite.
if(LINUX AND PLEXUS_LIBURING_HAS_FUTEX)
    plexus_add_shm_harness_test(test_shm_harness_denial test_shm_harness_denial.cpp)
endif()
