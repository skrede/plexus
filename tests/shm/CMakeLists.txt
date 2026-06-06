# Shared-memory backend tests: the cross-process (fork) xproc harness, the
# MAP_SHARED round-trip behavioral-oracle placeholder, and the io_uring
# FUTEX_WAIT ⇄ reactor de-risking spike. Only built when PLEXUS_ENABLE_SHM_BACKEND
# is ON (plexus::shm exists only under the gate); the `shm.` ctest prefix lets
# `ctest -R shm` select it. Auto-discovered by the tests glob.
if(NOT PLEXUS_ENABLE_SHM_BACKEND)
    return()
endif()

# The scaffold oracle: forks a child, round-trips a value through an anonymous
# MAP_SHARED page, and asserts equality looped N>=100 — proving fork +
# MAP_SHARED actually crosses the process boundary on this host. The later waves
# fill it with the real ring/notifier xproc cases.
add_executable(test_shm_scaffold test_shm_scaffold.cpp)

target_include_directories(test_shm_scaffold PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(test_shm_scaffold PRIVATE
    plexus::shm
    Catch2::Catch2WithMain)

catch_discover_tests(test_shm_scaffold TEST_PREFIX "shm.")

# The D-10 thread-free notifier spike (NON-GATING): probes whether an
# IORING_OP_FUTEX_WAIT CQE on a MAP_SHARED word can be reaped alongside an asio
# reactor without a plexus-spawned thread. Built only when the host distro's
# liburing is found (the io_uring path's optional dependency, the OpenSSL trust
# class — a system lib, NOT a language-registry package). When liburing is
# absent the spike is silently skipped; it never gates the suite.
find_package(PkgConfig QUIET)
if(PkgConfig_FOUND)
    pkg_check_modules(LIBURING QUIET IMPORTED_TARGET liburing)
endif()

if(LIBURING_FOUND)
    add_executable(spike_iouring_futex spike_iouring_futex.cpp)

    # The spike links plexus::asio for the asio include path ONLY — it probes
    # whether an io_uring CQE can be reaped THROUGH the user's asio reactor (the
    # D-10 question). The backend target plexus-shm itself stays asio-free (D-09);
    # this asio dependency lives in the spike test executable, never in the lib.
    target_link_libraries(spike_iouring_futex PRIVATE
        plexus::shm
        plexus::asio
        PkgConfig::LIBURING
        Catch2::Catch2WithMain)

    catch_discover_tests(spike_iouring_futex TEST_PREFIX "shm.")

    message(STATUS "plexus shm tests: liburing ${LIBURING_VERSION} found — io_uring futex spike enabled")
else()
    message(STATUS "plexus shm tests: liburing absent — io_uring futex spike skipped (non-gating)")
endif()
