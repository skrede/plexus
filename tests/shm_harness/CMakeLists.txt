# Multi-process node harness for same-host shared memory: each case re-execs its own binary as separate node
# processes coordinated through a named control block.
if(NOT PLEXUS_ENABLE_SHM_BACKEND OR NOT TARGET plexus::asio::shm)
    return()
endif()

set(PLEXUS_TEST_PREFIX "shm.")

function(plexus_add_shm_harness_test name)
    cmake_parse_arguments(PARSE_ARGV 1 harness "" "" "PROPERTIES")
    plexus_add_catch_test(${name}
        NO_MAIN
        SOURCES xproc_main.cpp node_role.cpp ${harness_UNPARSED_ARGUMENTS}
        LINKS plexus::plexus plexus::core plexus::wire plexus::asio plexus::asio::shm plexus::native plexus::testing
        INCLUDES ${PROJECT_SOURCE_DIR}/tests/shm ${PROJECT_SOURCE_DIR}/tests/shm/common
        PROPERTIES ${harness_PROPERTIES})
endfunction()

if(LINUX)
    plexus_add_shm_harness_test(test_shm_harness
        test_shm_harness_baseline.cpp
        test_shm_harness_wire.cpp
        test_shm_harness_fan_in.cpp
        test_shm_harness_engagement.cpp
        test_shm_harness_write_once.cpp)
endif()

# The probes count a topic's regions under /dev/shm and their registry-level roles use Linux-only process control, so
# the set is Linux-only. A stalled burst spins for tens of seconds.
if(LINUX)
    plexus_add_shm_harness_test(test_shm_lane_probes
        registry_role.cpp
        test_shm_lane_probes_burst.cpp
        test_shm_lane_probes_restart.cpp
        PROPERTIES TIMEOUT 300)
endif()

# The subscriber denies itself io_uring with a seccomp filter before it builds its node.
if(LINUX)
    plexus_add_shm_harness_test(test_shm_harness_denial test_shm_harness_denial.cpp)
endif()

if(LINUX)
    plexus_add_shm_harness_test(test_shm_wake_stress wake_roles.cpp test_shm_wake_stress.cpp PROPERTIES TIMEOUT 300)
endif()

function(plexus_add_shm_xproc_test name)
    plexus_add_catch_test(${name}
        NO_MAIN
        SOURCES xproc_main.cpp ${ARGN}
        LINKS plexus::shm plexus::asio plexus::asio::shm plexus::native plexus::wire plexus::testing
        INCLUDES ${PROJECT_SOURCE_DIR}/tests/shm)
endfunction()

plexus_add_shm_xproc_test(test_shm_three_process three_process_roles.cpp test_shm_three_process.cpp)
plexus_add_shm_xproc_test(test_shm_slot_liveness liveness_roles.cpp test_shm_slot_liveness.cpp)
plexus_add_shm_xproc_test(test_shm_ring_adoption liveness_roles.cpp test_shm_ring_adoption.cpp)

# The POSIX-semaphore notifier is macOS's own and a second notifier on Linux. Its burst case exercises the wake pipe,
# which the Windows event notifier does not have.
if(NOT WIN32)
    plexus_add_shm_xproc_test(test_shm_sem_notifier
        three_process_roles.cpp
        sem_roles.cpp
        test_shm_sem_teardown.cpp
        test_shm_sem_pipe.cpp
        test_shm_sem_adoption.cpp
        test_shm_sem_pending.cpp
        test_shm_sem_reclaim.cpp)
endif()

if(WIN32)
    plexus_add_shm_xproc_test(test_shm_event_notifier
        three_process_roles.cpp
        event_roles.cpp
        test_shm_event_teardown.cpp
        test_shm_event_adoption.cpp)
endif()

# The allocation counter replaces the global operator new for the whole binary, so the signal-path gate has its own
# executable and runs alone.
if(LINUX)
    plexus_add_catch_test(test_shm_sem_alloc
        SOURCES test_shm_sem_alloc.cpp
        LINKS plexus::shm plexus::asio plexus::asio::shm plexus::native plexus::wire plexus::testing
        INCLUDES ${PROJECT_SOURCE_DIR}/tests/shm
        PROPERTIES RUN_SERIAL TRUE)
endif()
