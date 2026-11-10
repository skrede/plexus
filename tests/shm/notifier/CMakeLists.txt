# The test links no liburing of its own, so a green link proves plexus::asio::shm carries it transitively. It
# drives io_uring, so it is Linux-only: Windows wakes through the reactor bridge and macOS links no liburing.
if(TARGET plexus::asio::shm AND NOT WIN32 AND NOT APPLE)
    plexus_add_catch_test(test_shm_liburing_link
        SOURCES test_shm_liburing_link.cpp
        LINKS plexus::asio::shm plexus::asio)

    include(FetchContent)
    FetchContent_GetProperties(asio)
    set(_liburing_probe_asio "")
    if(asio_POPULATED)
        set(_liburing_probe_asio -DASIO_SOURCE_DIR=${asio_SOURCE_DIR})
    endif()

    # Configures the real tree against a liburing without the futex-wait helper and asserts the
    # shm notifier resolves to the threaded floor.
    add_test(NAME shm.liburing_probe_falls_back_without_futex_wait
        COMMAND ${CMAKE_COMMAND}
            -DPLEXUS_SOURCE_DIR=${PROJECT_SOURCE_DIR}
            -DPROBE_DIR=${CMAKE_CURRENT_SOURCE_DIR}/liburing_probe
            -DWORK_DIR=${CMAKE_CURRENT_BINARY_DIR}/liburing_probe
            "-DPROBE_GENERATOR=${CMAKE_GENERATOR}"
            -DPROBE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
            "-DPROBE_CXX_COMPILER_LAUNCHER=${CMAKE_CXX_COMPILER_LAUNCHER}"
            ${_liburing_probe_asio}
            -P ${CMAKE_CURRENT_SOURCE_DIR}/liburing_probe/run_probe.cmake)
    set_tests_properties(shm.liburing_probe_falls_back_without_futex_wait PROPERTIES TIMEOUT 600)
endif()

plexus_add_catch_test(test_shm_seams
    SOURCES test_shm_seams.cpp
    LINKS plexus::shm plexus::core)

# Drives the Linux futex primitive directly, so it is excluded from Windows and macOS.
if(NOT WIN32 AND NOT APPLE)
    plexus_add_catch_test(test_shm_notifier_lostwakeup
        SOURCES test_shm_notifier_lostwakeup.cpp
        LINKS plexus::native)
endif()

# Drives the Linux futex primitive directly, so it is excluded from Windows and macOS.
if(NOT WIN32 AND NOT APPLE)
    plexus_add_catch_test(test_shm_wake_gating
        SOURCES test_shm_wake_gating.cpp
        LINKS plexus::native plexus::shm)
endif()

# Drives the Linux futex primitive directly, so it is excluded from Windows and macOS.
if(NOT WIN32 AND NOT APPLE)
    plexus_add_catch_test(test_shm_wake_per_waiter
        SOURCES test_shm_wake_per_waiter.cpp
        LINKS plexus::native plexus::shm)
endif()

if(NOT WIN32 AND NOT APPLE)
    plexus_add_catch_test(test_shm_park_protocol
        SOURCES test_shm_park_protocol.cpp
        LINKS plexus::shm)
endif()

if(TARGET plexus::asio::shm AND LINUX)
    plexus_add_catch_test(test_shm_fallback_notifier
        SOURCES test_shm_fallback_notifier.cpp
        LINKS plexus::shm plexus::asio plexus::asio::shm plexus::native plexus::testing)
endif()

if(TARGET plexus::asio::shm AND LINUX AND PLEXUS_LIBURING_HAS_FUTEX)
    plexus_add_catch_test(test_shm_uring_cqe
        SOURCES test_shm_uring_cqe.cpp
        LINKS plexus::shm plexus::asio plexus::asio::shm plexus::native)
endif()

if(LIBURING_FOUND)
    if(PLEXUS_SHM_TESTS_HAVE_FUTEX)
        # plexus::asio is linked only for its include path: the spike reaps the CQE through an asio reactor, and
        # plexus::native itself stays asio-free.
        plexus_add_catch_test(spike_iouring_futex
            SOURCES spike_iouring_futex.cpp
            LINKS plexus::native plexus::asio PkgConfig::LIBURING)
    endif()

    plexus_add_catch_test(test_shm_notifier_bridge
        SOURCES test_shm_notifier_bridge.cpp
        LINKS plexus::plexus plexus::shm plexus::asio plexus::asio::shm plexus::native plexus::wire)

    plexus_add_catch_test(test_shm_teardown_race
        SOURCES test_shm_teardown_race.cpp
        LINKS plexus::plexus plexus::shm plexus::asio plexus::asio::shm plexus::native plexus::wire)

    plexus_add_catch_test(posix_test_shm_dual_delivery
        SOURCES posix_test_shm_dual_delivery.cpp
        LINKS plexus::plexus plexus::shm plexus::asio plexus::asio::shm plexus::native plexus::wire)
endif()

# It runs its own main(), which dispatches a re-exec'd child's role before the Catch2 session.
plexus_add_catch_test(test_shm_xproc_process
    NO_MAIN
    SOURCES test_shm_xproc_process.cpp
    LINKS plexus::shm plexus::native plexus::testing)
