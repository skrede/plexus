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
    plexus_add_shm_harness_test(test_shm_harness test_shm_harness_baseline.cpp test_shm_harness_wire.cpp)
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

# The subscriber denies itself io_uring with a seccomp filter before it builds its node.
if(LINUX)
    plexus_add_shm_harness_test(test_shm_harness_denial test_shm_harness_denial.cpp)
endif()

# Burst-then-idle wake stress over one ring with one producer process and one or two consumer processes.
if(LINUX)
    plexus_add_shm_harness_test(test_shm_wake_stress wake_roles.cpp test_shm_wake_stress.cpp PROPERTIES TIMEOUT 300)
endif()

function(plexus_add_shm_xproc_test name)
    add_executable(${name} xproc_main.cpp ${ARGN})

    target_include_directories(${name} PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}
        ${PROJECT_SOURCE_DIR}/tests/shm)

    target_link_libraries(${name} PRIVATE
        plexus::shm
        plexus::asio
        plexus::asio::shm
        plexus::native
        plexus::wire
        plexus::testing
        Catch2::Catch2)

    catch_discover_tests(${name} TEST_PREFIX "shm.")
endfunction()

# One publisher process and two subscriber processes, each with its own registry and notifier on one ring; it runs on
# every platform.
plexus_add_shm_xproc_test(test_shm_three_process three_process_roles.cpp test_shm_three_process.cpp)

# Every case is an expected failure that runs the POSIX-semaphore notifier on Linux: a disarm whose stop wake another
# process's waiter takes, a wake pipe filled while the reactor is stalled, and a stale semaphore adopted by name. A
# case that can hang runs its hanging side in a child the parent kills after a bound.
if(LINUX)
    plexus_add_shm_xproc_test(test_shm_sem_notifier
        three_process_roles.cpp
        sem_roles.cpp
        test_shm_sem_teardown.cpp
        test_shm_sem_pipe.cpp
        test_shm_sem_adoption.cpp)
endif()
