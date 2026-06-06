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

# The header-only broadcast ring + loan/take core. These TUs include ONLY the
# core headers under plexus/io/shm/ — they need no plexus::shm backend link
# (the ring drives a caller-supplied anonymous mapped span), proving the core is
# POSIX/asio-free. They link plexus::plexus for the core include path and the
# xproc harness header path.
add_executable(test_shm_ring_core test_shm_ring_core.cpp)

target_include_directories(test_shm_ring_core PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(test_shm_ring_core PRIVATE
    plexus::plexus
    Catch2::Catch2WithMain)

catch_discover_tests(test_shm_ring_core TEST_PREFIX "shm.")

# Ring backpressure: best_effort overwrite-skips a pinned slot, the full-lap
# congested fallback, and the reliable cursor gate (lossless).
add_executable(test_shm_ring_backpressure test_shm_ring_backpressure.cpp)

target_link_libraries(test_shm_ring_backpressure PRIVATE
    plexus::plexus
    Catch2::Catch2WithMain)

catch_discover_tests(test_shm_ring_backpressure TEST_PREFIX "shm.")

# The overwrite-vs-pin Dekker race, driven across a producer + consumer thread
# (the looped-logic proof; tsan is the phase-boundary gate). The two driver
# threads are a test-local concern (the ring itself spawns none) -- the SHM test
# tree is the sole consumer of pthreads, so the dependency is resolved here in the
# fragment, never in the shared root.
find_package(Threads REQUIRED)

add_executable(test_shm_ring_dekker test_shm_ring_dekker.cpp)

target_link_libraries(test_shm_ring_dekker PRIVATE
    plexus::plexus
    Threads::Threads
    Catch2::Catch2WithMain)

catch_discover_tests(test_shm_ring_dekker TEST_PREFIX "shm.")

# The D-06 loan reshape: the move-only intrusive shm_slot_owner, the zero-copy
# take() returning wire_bytes<shm_slot_owner> (no shared_ptr), and the move-only
# loaned_buffer / taken_message lifetime (inert moved-from, unpin exactly once).
add_executable(test_shm_zerocopy_take test_shm_zerocopy_take.cpp)

target_include_directories(test_shm_zerocopy_take PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(test_shm_zerocopy_take PRIVATE
    plexus::plexus
    plexus::wire
    Catch2::Catch2WithMain)

catch_discover_tests(test_shm_zerocopy_take TEST_PREFIX "shm.")

add_executable(test_shm_loan_lifetime test_shm_loan_lifetime.cpp)

target_include_directories(test_shm_loan_lifetime PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(test_shm_loan_lifetime PRIVATE
    plexus::plexus
    plexus::wire
    Catch2::Catch2WithMain)

catch_discover_tests(test_shm_loan_lifetime TEST_PREFIX "shm.")

# The generic selection + identity layer (header-only, POSIX/asio-free): the
# dispatch_hint FLAGS type wired into topic_qos + the selector's locality x hint
# SHM-eligibility decision. The TU includes only core headers, proving the
# selection layer carries no backend dependency.
add_executable(test_shm_dispatch_hint test_shm_dispatch_hint.cpp)

target_link_libraries(test_shm_dispatch_hint PRIVATE
    plexus::plexus
    Catch2::Catch2WithMain)

catch_discover_tests(test_shm_dispatch_hint TEST_PREFIX "shm.")

# The same-host identity layer: the null-guarded fingerprint compare + the
# deterministic, direction-discriminated region naming (the generic half; the
# machine-id read is deferred to the backend).
add_executable(test_shm_same_host test_shm_same_host.cpp)

target_link_libraries(test_shm_same_host PRIVATE
    plexus::plexus
    plexus::wire
    Catch2::Catch2WithMain)

catch_discover_tests(test_shm_same_host TEST_PREFIX "shm.")

# The shared-memory selection decision (locality x hint, bilateral) + the
# per-forwarder acquired-ring refcount set (no singleton).
add_executable(test_shm_selection test_shm_selection.cpp)

target_link_libraries(test_shm_selection PRIVATE
    plexus::plexus
    plexus::wire
    Catch2::Catch2WithMain)

catch_discover_tests(test_shm_selection TEST_PREFIX "shm.")

# The two core seams (region_broker + notifier): stub models prove the concepts are
# satisfiable and the notifier's drain is a move-only callback, with no POSIX/asio in
# the core translation unit.
add_executable(test_shm_seams test_shm_seams.cpp)

target_link_libraries(test_shm_seams PRIVATE
    plexus::plexus
    Catch2::Catch2WithMain)

catch_discover_tests(test_shm_seams TEST_PREFIX "shm.")

# The thread-free notifier spike (NON-GATING): probes whether an
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
