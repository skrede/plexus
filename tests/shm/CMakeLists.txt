# Shared-memory backend tests: the cross-process (re-exec, open-by-name) xproc
# harness, the MAP_SHARED round-trip behavioral-oracle placeholder, and the
# io_uring FUTEX_WAIT ⇄ reactor de-risking spike. Only built when PLEXUS_ENABLE_SHM_BACKEND
# is ON (plexus::native exists only under the gate); the `shm.` ctest prefix lets
# `ctest -R shm` select it. Auto-discovered by the tests glob.
if(NOT PLEXUS_ENABLE_SHM_BACKEND)
    return()
endif()

# The liburing usage-requirement proof: links ONLY plexus::asio::shm (+ plexus::asio for the
# policy header path) and NO manual liburing, then drives ring_notifier's io_uring calls. A
# green link proves plexus::asio::shm carries liburing transitively — the symptom a real
# consumer hit (undefined reference to io_uring_*). Built only when the shm backend exposes
# its bridge target. The proof drives io_uring (ring_notifier), so it is Linux-only; Windows
# carries its same-host wake in the reactor bridge, not liburing. macOS likewise carries no
# liburing (the shm target links it only if(LINUX)), so the proof is excluded there too.
if(TARGET plexus::asio::shm AND NOT WIN32 AND NOT APPLE)
    add_executable(test_shm_liburing_link test_shm_liburing_link.cpp)

    target_link_libraries(test_shm_liburing_link PRIVATE
        plexus::asio::shm
        plexus::asio
        Catch2::Catch2WithMain)

    catch_discover_tests(test_shm_liburing_link TEST_PREFIX "shm.")

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

# The scaffold oracle: forks a child, round-trips a value through an anonymous
# MAP_SHARED page, and asserts equality looped N>=100 — proving fork +
# MAP_SHARED actually crosses the process boundary on this host. The later waves
# fill it with the real ring/notifier xproc cases.
add_executable(test_shm_scaffold test_shm_scaffold.cpp)

target_include_directories(test_shm_scaffold PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(test_shm_scaffold PRIVATE
    plexus::native
    Catch2::Catch2WithMain)

target_compile_definitions(test_shm_scaffold PRIVATE PLEXUS_TEST_PROJECT_VERSION="${PROJECT_VERSION}")

catch_discover_tests(test_shm_scaffold TEST_PREFIX "shm.")

# The header-only broadcast ring + loan/take family. These TUs include ONLY the
# family headers under plexus/shm/ — they need no plexus::native backend link
# (the ring drives a caller-supplied anonymous mapped span), proving the family is
# POSIX/asio-free. They link plexus::shm for the family include path, plexus::core
# for the engine, and the xproc harness header path.
add_executable(test_shm_ring_core test_shm_ring_core.cpp test_shm_ring_core_capacity.cpp test_shm_ring_magic.cpp test_shm_ring_attach_bounds.cpp test_shm_ring_tampered.cpp)

target_include_directories(test_shm_ring_core PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(test_shm_ring_core PRIVATE
    plexus::shm
    plexus::core
    Catch2::Catch2WithMain)

catch_discover_tests(test_shm_ring_core TEST_PREFIX "shm.")

# Ring backpressure: best_effort overwrite-skips a pinned slot, the full-lap
# congested fallback, and the reliable cursor gate (lossless).
add_executable(test_shm_ring_backpressure test_shm_ring_backpressure.cpp
    test_shm_ring_backpressure_recovery.cpp)

target_link_libraries(test_shm_ring_backpressure PRIVATE
    plexus::shm
    plexus::core
    Catch2::Catch2WithMain)

catch_discover_tests(test_shm_ring_backpressure TEST_PREFIX "shm.")

# The overwrite-vs-pin Dekker race, driven across a producer + consumer thread
# (a looped-logic proof here; the tsan tree runs it to catch the race). The two driver
# threads are a test-local concern (the ring itself spawns none) -- the SHM test
# tree is the sole consumer of pthreads, so the dependency is resolved here in the
# fragment, never in the shared root.
find_package(Threads REQUIRED)

add_executable(test_shm_ring_dekker test_shm_ring_dekker.cpp)

target_link_libraries(test_shm_ring_dekker PRIVATE
    plexus::shm
    plexus::core
    Threads::Threads
    Catch2::Catch2WithMain)

catch_discover_tests(test_shm_ring_dekker TEST_PREFIX "shm.")

# A reliable claim lapping a claim still in flight on a ring with no cursor, by its own producer and by a second one.
add_executable(test_shm_ring_overtake test_shm_ring_overtake.cpp)

target_include_directories(test_shm_ring_overtake PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(test_shm_ring_overtake PRIVATE
    plexus::shm
    plexus::core
    Catch2::Catch2WithMain)

catch_discover_tests(test_shm_ring_overtake TEST_PREFIX "shm.")

# A loan dropped unpublished becomes a skip every consumer passes, and a best-effort claim stops waiting on a cell whose
# claim one lap earlier is still in flight.
add_executable(test_shm_loan_abandon test_shm_loan_abandon.cpp)

target_include_directories(test_shm_loan_abandon PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(test_shm_loan_abandon PRIVATE
    plexus::shm
    plexus::core
    Catch2::Catch2WithMain)

catch_discover_tests(test_shm_loan_abandon TEST_PREFIX "shm.")

# The loan reshape: the move-only intrusive shm_slot_owner, the zero-copy
# take() returning wire_bytes<shm_slot_owner> (no shared_ptr), and the move-only
# loaned_buffer / taken_message lifetime (inert moved-from, unpin exactly once).
add_executable(test_shm_zerocopy_take test_shm_zerocopy_take.cpp)

target_include_directories(test_shm_zerocopy_take PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(test_shm_zerocopy_take PRIVATE
    plexus::shm
    plexus::core
    plexus::wire
    Catch2::Catch2WithMain)

catch_discover_tests(test_shm_zerocopy_take TEST_PREFIX "shm.")

add_executable(test_shm_loan_lifetime test_shm_loan_lifetime.cpp)

target_include_directories(test_shm_loan_lifetime PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(test_shm_loan_lifetime PRIVATE
    plexus::shm
    plexus::core
    plexus::wire
    Catch2::Catch2WithMain)

catch_discover_tests(test_shm_loan_lifetime TEST_PREFIX "shm.")

# The generic selection + identity layer (header-only, POSIX/asio-free): the
# dispatch_hint FLAGS type wired into topic_qos + the selector's locality x hint
# SHM-eligibility decision. The TU includes only core headers, proving the
# selection layer carries no backend dependency.
add_executable(test_shm_dispatch_hint test_shm_dispatch_hint.cpp)

target_link_libraries(test_shm_dispatch_hint PRIVATE
    plexus::core
    Catch2::Catch2WithMain)

catch_discover_tests(test_shm_dispatch_hint TEST_PREFIX "shm.")

# The same-host identity layer: the null-guarded fingerprint compare + the
# deterministic, direction-discriminated region naming (the generic half; the
# machine-id read is deferred to the backend).
add_executable(test_shm_same_host test_shm_same_host.cpp)

target_link_libraries(test_shm_same_host PRIVATE
    plexus::shm
    plexus::core
    plexus::wire
    Catch2::Catch2WithMain)

catch_discover_tests(test_shm_same_host TEST_PREFIX "shm.")

# The naming-budget invariant: the region name is a fixed 16 hex chars for any fqn/direction/
# namespace, the composed macOS canonical/slab names fit the Darwin 31-char limit and the doorbell
# names the semaphore budget. Platform-neutral naming math (no Darwin symbol needed); links
# plexus::wire for the fqn hash the naming routes through.
add_executable(test_region_naming_length test_region_naming_length.cpp)

target_link_libraries(test_region_naming_length PRIVATE
    plexus::shm
    plexus::wire
    Catch2::Catch2WithMain)

catch_discover_tests(test_region_naming_length TEST_PREFIX "shm.")

# The per-waiter doorbell name: one fixed-array format of region, cursor slot and tag within the macOS semaphore budget.
add_executable(test_shm_doorbell_naming test_shm_doorbell_naming.cpp)

target_link_libraries(test_shm_doorbell_naming PRIVATE
    plexus::shm
    Catch2::Catch2WithMain)

catch_discover_tests(test_shm_doorbell_naming TEST_PREFIX "shm.")

# The shared-memory selection decision (locality x hint, bilateral) + the
# per-forwarder acquired-ring refcount set (no singleton).
add_executable(test_shm_selection test_shm_selection.cpp)

target_link_libraries(test_shm_selection PRIVATE
    plexus::shm
    plexus::core
    plexus::wire
    Catch2::Catch2WithMain)

catch_discover_tests(test_shm_selection TEST_PREFIX "shm.")

# The two core seams (region_broker + notifier): stub models prove the concepts are
# satisfiable and the notifier's drain is a move-only callback, with no POSIX/asio in
# the core translation unit.
add_executable(test_shm_seams test_shm_seams.cpp)

target_link_libraries(test_shm_seams PRIVATE
    plexus::shm
    plexus::core
    Catch2::Catch2WithMain)

catch_discover_tests(test_shm_seams TEST_PREFIX "shm.")

# The POSIX region broker, portable negative-path behaviors: an oversize create fast-fails
# (too_large) with no region minted, an attach of a never-created name returns not_found, and a
# sliced/empty name is rejected. These read only the broker's status and handle validity (no
# /dev/shm path inspection), so they run on every POSIX host. Links plexus::native for the
# compiled broker bodies (shm_open/mmap/ftruncate/shm_unlink). Drives the POSIX broker directly
# (sys/mman.h), so it is excluded from the Windows build.
if(NOT WIN32)
    add_executable(test_shm_region_broker test_shm_region_broker.cpp)

    target_include_directories(test_shm_region_broker PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR})

    target_link_libraries(test_shm_region_broker PRIVATE
        plexus::shm
        plexus::native
        Catch2::Catch2WithMain)

    catch_discover_tests(test_shm_region_broker TEST_PREFIX "shm.")
endif()

# The POSIX region broker, Linux-only /dev/shm-view behaviors: the 0600 owner-only mode, the
# name contract (only unlink_name removes it), and unlink_stale_on_create reclaiming an orphan -- each verified
# by stat()/access()-ing the object under /dev/shm, the tmpfs view of POSIX shm that exists only
# on Linux. macOS shm_open objects are not path-visible, so those broker guarantees are proven
# there by the cross-process shm proofs instead; the target is excluded on macOS and Windows.
if(NOT WIN32 AND NOT APPLE)
    add_executable(linux_test_shm_region_broker linux_test_shm_region_broker.cpp)

    target_include_directories(linux_test_shm_region_broker PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR})

    target_link_libraries(linux_test_shm_region_broker PRIVATE
        plexus::shm
        plexus::native
        Catch2::Catch2WithMain)

    catch_discover_tests(linux_test_shm_region_broker TEST_PREFIX "shm.")

    # The slot locks on a region's /dev/shm descriptor within one process: two handles are two holders, a closed holder
    # frees its slot, and every descriptor is closed on exec.
    add_executable(linux_test_shm_slot_lock linux_test_shm_slot_lock.cpp)

    target_link_libraries(linux_test_shm_slot_lock PRIVATE
        plexus::shm
        plexus::native
        plexus::testing
        Catch2::Catch2WithMain)

    catch_discover_tests(linux_test_shm_slot_lock TEST_PREFIX "shm.")
endif()

# The cross-process futex notifier: the wake crosses address spaces (the
# no-PRIVATE-flag proof, a forked producer waking a forked consumer on a shared
# MAP_SHARED word) and the drain-before-wait protocol delivers an interleaved
# publish (lost-wakeup-safe). Links plexus::native for the raw SYS_futex bodies.
# Drives the Linux futex notifier primitive directly (SYS_futex, <linux/futex.h>), so it is
# Linux-only and excluded on Windows AND macOS (neither carries the futex notifier body).
if(NOT WIN32 AND NOT APPLE)
    add_executable(test_shm_notifier_lostwakeup test_shm_notifier_lostwakeup.cpp)

    target_include_directories(test_shm_notifier_lostwakeup PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR})

    target_link_libraries(test_shm_notifier_lostwakeup PRIVATE
        plexus::native
        Catch2::Catch2WithMain)

    catch_discover_tests(test_shm_notifier_lostwakeup TEST_PREFIX "shm.")
endif()

# The per-slot wake gate: only a registered slot whose waiter is PARKED asks for a wake, the
# scan stays inside the cursor array whatever the header claims, and a forked parked waiter
# is still woken across address spaces. Drives the Linux futex notifier primitive directly
# (SYS_futex, <linux/futex.h>), so it is Linux-only.
if(NOT WIN32 AND NOT APPLE)
    add_executable(test_shm_wake_gating test_shm_wake_gating.cpp)

    target_include_directories(test_shm_wake_gating PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR})

    target_link_libraries(test_shm_wake_gating PRIVATE
        plexus::native
        plexus::shm
        Catch2::Catch2WithMain)

    catch_discover_tests(test_shm_wake_gating TEST_PREFIX "shm.")
endif()

# Two waiters in their own park slots: a waiter that unparks leaves the park of a waiter still blocked intact. Drives
# the Linux futex primitive directly, so it is Linux-only.
if(NOT WIN32 AND NOT APPLE)
    add_executable(test_shm_wake_per_waiter test_shm_wake_per_waiter.cpp)

    target_include_directories(test_shm_wake_per_waiter PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR})

    target_link_libraries(test_shm_wake_per_waiter PRIVATE
        plexus::native
        plexus::shm
        Catch2::Catch2WithMain)

    catch_discover_tests(test_shm_wake_per_waiter TEST_PREFIX "shm.")
endif()

# Exhaustive interleavings of the park/notify steps: the per-slot parker over the real ring header, and the rejected
# orderings over a model of the Linux kernel futex.
if(NOT WIN32 AND NOT APPLE)
    add_executable(test_shm_park_protocol test_shm_park_protocol.cpp)

    target_include_directories(test_shm_park_protocol PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR})

    target_link_libraries(test_shm_park_protocol PRIVATE
        plexus::shm
        Catch2::Catch2WithMain)

    catch_discover_tests(test_shm_park_protocol TEST_PREFIX "shm.")
endif()

# The first whole-stack cross-process proof: two processes converge on the same
# deterministically-named regions with no exchange (the machine fingerprint + the
# direction-discriminated region naming), one creates + publishes, the other
# attaches + consumes, and a real value round-trips byte-equal over the MAP_SHARED
# broadcast ring. Links plexus::native (broker + fingerprint) + plexus::wire (the fqn
# hash the region naming routes through).
# Drives fork() + the POSIX broker directly (unistd.h), so it is excluded from the Windows build.
if(NOT WIN32)
    add_executable(test_shm_same_host_roundtrip test_shm_same_host_roundtrip.cpp
        test_shm_same_host_fingerprint.cpp)

    target_include_directories(test_shm_same_host_roundtrip PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR})

    target_link_libraries(test_shm_same_host_roundtrip PRIVATE
        plexus::plexus
        plexus::shm
        plexus::native
        plexus::wire
        Catch2::Catch2WithMain)

    catch_discover_tests(test_shm_same_host_roundtrip TEST_PREFIX "shm.")
endif()

# The shm-region namespace cross-process proof: a producer mints regions under one
# namespace; a consumer with the SAME namespace receives the value (convergence), a
# consumer with a DIFFERENT namespace cannot attach the producer's region and sees
# nothing (isolation — two unrelated co-host apps never collide on a shared region).
# Drives fork() + the POSIX broker directly (unistd.h), so it is excluded from the Windows build.
if(NOT WIN32)
    add_executable(test_shm_region_namespace test_shm_region_namespace.cpp)

    target_include_directories(test_shm_region_namespace PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR})

    target_link_libraries(test_shm_region_namespace PRIVATE
        plexus::plexus
        plexus::shm
        plexus::native
        plexus::wire
        Catch2::Catch2WithMain)

    catch_discover_tests(test_shm_region_namespace TEST_PREFIX "shm.")
endif()

# The core pub/sub endpoints (header-only, asio/POSIX-free): the shm_channel
# send -> drain round-trip over an anonymous mapped span (loan -> one memcpy ->
# publish -> signal), the notifier signaling exactly once per successful send via
# a recording stub, and the oversize fallback (a payload over the slot
# capacity returns rejected, no publish, no signal). The TU includes only core
# headers, proving the endpoints carry no backend dependency.
add_executable(test_shm_channel test_shm_channel.cpp)

target_include_directories(test_shm_channel PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(test_shm_channel PRIVATE
    plexus::shm
    plexus::core
    plexus::wire
    Catch2::Catch2WithMain)

catch_discover_tests(test_shm_channel TEST_PREFIX "shm.")

# Adaptive spin-then-park consumer policy at the slot_subscriber take seam (header-only
# core); drives a real publisher/subscriber over a heap-backed ring and a producer thread.
add_executable(test_shm_spin_park test_shm_spin_park.cpp)
target_include_directories(test_shm_spin_park PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(test_shm_spin_park PRIVATE
    plexus::shm
    plexus::core
    plexus::wire
    Threads::Threads
    Catch2::Catch2WithMain)
catch_discover_tests(test_shm_spin_park TEST_PREFIX "shm.")

# Backpressure + observable congestion (header-only): a best_effort ring with every
# slot pinned returns congested off send() (OBSERVABLE, never a silent drop); a
# reliable producer blocks losslessly on a lagging consumer (every value arrives in
# order, none skipped, across a reader + writer thread); and the reliable blocking
# spin/yield path allocates nothing across N sends (the alloc-counter idiom from
# tests/support). The two driver threads are a test-local concern (the channel
# spawns none).
add_executable(test_shm_congested test_shm_congested.cpp test_shm_congested_alloc.cpp)

target_include_directories(test_shm_congested PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR}/tests)

target_link_libraries(test_shm_congested PRIVATE
    plexus::shm
    plexus::core
    plexus::wire
    Threads::Threads
    Catch2::Catch2WithMain)

catch_discover_tests(test_shm_congested TEST_PREFIX "shm.")

# The demand-driven topic registry (header-only, asio/strand/std::function-free): a
# stub broker over heap-backed regions + a recording stub notifier prove the
# acquire/release refcount lifecycle (created -> attached -> teardown+unlink), the
# max_payload ring sizing with a subscriber-only default fallback, and the teardown
# ordering (disarm BEFORE the subscriber the drain touches is destroyed -- a
# recording notifier brackets the order). The registry borrows the broker + notifier
# seams by reference; the TU includes only core headers.
add_executable(test_shm_registry test_shm_registry.cpp)

target_include_directories(test_shm_registry PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(test_shm_registry PRIVATE
    plexus::shm
    plexus::core
    plexus::wire
    Catch2::Catch2WithMain)

catch_discover_tests(test_shm_registry TEST_PREFIX "shm.")

# A join over a co-host ring its creator has not finished minting, over an in-memory broker that withholds regions.
add_executable(test_shm_registry_join test_shm_registry_join.cpp)

target_include_directories(test_shm_registry_join PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(test_shm_registry_join PRIVATE
    plexus::plexus
    plexus::shm
    plexus::core
    plexus::testing
    Catch2::Catch2WithMain)

catch_discover_tests(test_shm_registry_join TEST_PREFIX "shm.")

# A ring outlives its creator and goes with its last holder, over the platform broker: last-out removal, adoption by a
# restarted publisher, a second live publisher refused, a recreate on another geometry, a dead creator's claim in flight
# and a foreign layout.
add_executable(test_shm_ring_lifetime test_shm_ring_lifetime.cpp test_shm_ring_recreate.cpp)

target_include_directories(test_shm_ring_lifetime PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(test_shm_ring_lifetime PRIVATE
    plexus::plexus
    plexus::shm
    plexus::native
    plexus::core
    plexus::testing
    Catch2::Catch2WithMain)

catch_discover_tests(test_shm_ring_lifetime TEST_PREFIX "shm.")

# A newcomer that maps a ring between its retirer's sidecar lock file removal and control-name removal, over the POSIX
# broker; only a host whose slot locks live on a sidecar can leave one behind.
if(NOT WIN32)
    add_executable(posix_test_shm_sidecar_leak posix_test_shm_sidecar_leak.cpp)

    target_include_directories(posix_test_shm_sidecar_leak PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR})

    target_link_libraries(posix_test_shm_sidecar_leak PRIVATE
        plexus::plexus
        plexus::shm
        plexus::native
        plexus::core
        plexus::testing
        Catch2::Catch2WithMain)

    catch_discover_tests(posix_test_shm_sidecar_leak TEST_PREFIX "shm.")
endif()

# A taken message or a wire view made from it, held past its ring entry's teardown, keeps that ring's mapping, over a
# counting in-memory broker and over the platform broker.
add_executable(test_shm_take_pin_mapping test_shm_take_pin_mapping.cpp)

target_include_directories(test_shm_take_pin_mapping PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(test_shm_take_pin_mapping PRIVATE
    plexus::plexus
    plexus::shm
    plexus::native
    plexus::core
    plexus::testing
    Catch2::Catch2WithMain)

catch_discover_tests(test_shm_take_pin_mapping TEST_PREFIX "shm.")

# Which halves of a ring each holder's role engages: producer-only entries register no cursor and arm nothing, each
# consumer adds one cursor, and a consumer never creates the ring.
add_executable(test_shm_registry_roles test_shm_registry_roles.cpp)

target_include_directories(test_shm_registry_roles PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(test_shm_registry_roles PRIVATE
    plexus::plexus
    plexus::shm
    plexus::core
    plexus::testing
    Catch2::Catch2WithMain)

catch_discover_tests(test_shm_registry_roles TEST_PREFIX "shm.")

# A congested reliable send reclaims the cursor of a consumer whose slot lock died with it, over the platform broker;
# a live consumer and the producer's own cursor keep theirs.
add_executable(test_shm_cursor_reclaim test_shm_cursor_reclaim.cpp)

target_include_directories(test_shm_cursor_reclaim PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(test_shm_cursor_reclaim PRIVATE
    plexus::plexus
    plexus::shm
    plexus::native
    plexus::core
    plexus::testing
    Catch2::Catch2WithMain)

catch_discover_tests(test_shm_cursor_reclaim TEST_PREFIX "shm.")

# A co-host acquire past a ring's consumer capacity, over an in-memory broker two registries share.
add_executable(test_shm_registry_capacity test_shm_registry_capacity.cpp)

target_include_directories(test_shm_registry_capacity PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(test_shm_registry_capacity PRIVATE
    plexus::plexus
    plexus::shm
    plexus::core
    plexus::testing
    Catch2::Catch2WithMain)

catch_discover_tests(test_shm_registry_capacity TEST_PREFIX "shm.")

# Rings are named per publisher and universe: two universes or two publishers never share a ring, over an in-memory broker.
add_executable(test_shm_region_universe test_shm_region_universe.cpp)

target_include_directories(test_shm_region_universe PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(test_shm_region_universe PRIVATE
    plexus::shm
    plexus::plexus
    plexus::testing
    Catch2::Catch2WithMain)

catch_discover_tests(test_shm_region_universe TEST_PREFIX "shm.")

# A notifier that cannot arm must fail the acquire, over the platform broker.
add_executable(test_shm_registry_arm test_shm_registry_arm.cpp)

target_link_libraries(test_shm_registry_arm PRIVATE
    plexus::plexus
    plexus::shm
    plexus::native
    plexus::core
    plexus::testing
    Catch2::Catch2WithMain)

catch_discover_tests(test_shm_registry_arm TEST_PREFIX "shm.")

# A waiter lost after its acquire armed it, over the platform broker and the fallback composite's injected legs.
if(TARGET plexus::asio)
    add_executable(test_shm_registry_loss test_shm_registry_loss.cpp)

    target_include_directories(test_shm_registry_loss PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR})

    target_link_libraries(test_shm_registry_loss PRIVATE
        plexus::plexus
        plexus::shm
        plexus::asio
        plexus::native
        plexus::core
        plexus::testing
        Catch2::Catch2WithMain)

    catch_discover_tests(test_shm_registry_loss TEST_PREFIX "shm.")
endif()

# The fallback composite over injected legs and the threaded leg.
if(TARGET plexus::asio::shm AND LINUX)
    add_executable(test_shm_fallback_notifier test_shm_fallback_notifier.cpp)

    target_include_directories(test_shm_fallback_notifier PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR})

    target_link_libraries(test_shm_fallback_notifier PRIVATE
        plexus::shm
        plexus::asio
        plexus::asio::shm
        plexus::native
        plexus::testing
        Catch2::Catch2WithMain)

    catch_discover_tests(test_shm_fallback_notifier TEST_PREFIX "shm.")
endif()

# The io_uring futex-wait completion classifier and the production composite's engaged leg.
if(TARGET plexus::asio::shm AND LINUX AND PLEXUS_LIBURING_HAS_FUTEX)
    add_executable(test_shm_uring_cqe test_shm_uring_cqe.cpp)

    target_include_directories(test_shm_uring_cqe PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR})

    target_link_libraries(test_shm_uring_cqe PRIVATE
        plexus::shm
        plexus::asio
        plexus::asio::shm
        plexus::native
        Catch2::Catch2WithMain)

    catch_discover_tests(test_shm_uring_cqe TEST_PREFIX "shm.")
endif()

# The ring memory cost query (header-only core): ring_memory_for equals the layout
# helpers AND the bytes the registry actually requests from the broker for a
# (payload, capacity) table, pinning the depth-32 16 MiB figure (512 MiB) so a
# depth-17 / deep-tier regression is caught. A recording stub broker captures the
# requested slab sizes; the TU includes only core headers.
add_executable(test_shm_ring_memory_query test_shm_ring_memory_query.cpp)

target_include_directories(test_shm_ring_memory_query PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(test_shm_ring_memory_query PRIVATE
    plexus::shm
    plexus::core
    plexus::wire
    Catch2::Catch2WithMain)

catch_discover_tests(test_shm_ring_memory_query TEST_PREFIX "shm.")

# The safe-defaults regression + the unified fail-closed path (header-only core): the
# small-payload default geometry is byte-identical to the shipped values, and a
# registration over the tunable ceiling AND one the broker/OS cannot map BOTH fail
# closed naming which bound was hit (ceiling vs OS allocator) with the ask vs available
# — never a silent downgrade. A heap-backed stub broker drives both legs.
add_executable(test_shm_geometry_defaults test_shm_geometry_defaults.cpp)

target_include_directories(test_shm_geometry_defaults PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(test_shm_geometry_defaults PRIVATE
    plexus::shm
    plexus::core
    plexus::wire
    Catch2::Catch2WithMain)

catch_discover_tests(test_shm_geometry_defaults TEST_PREFIX "shm.")

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

# io_uring_prep_futex_wait is liburing >= 2.6; the spike below calls it directly, so it needs the
# capability, not merely liburing's presence. The reactor-bridge tests route through ring_notifier,
# which falls back to its threaded floor when the helper is absent, so they stay under LIBURING_FOUND.
if(LIBURING_FOUND)
    include(CheckCXXSourceCompiles)
    set(CMAKE_REQUIRED_INCLUDES ${LIBURING_INCLUDE_DIRS})
    check_cxx_source_compiles([[
        #include <liburing.h>
        int main() { io_uring_prep_futex_wait(static_cast<io_uring_sqe *>(nullptr), static_cast<unsigned *>(nullptr), 0, 0, 0, 0); }
    ]] PLEXUS_SHM_TESTS_HAVE_FUTEX)
    unset(CMAKE_REQUIRED_INCLUDES)
endif()

if(LIBURING_FOUND)
    if(PLEXUS_SHM_TESTS_HAVE_FUTEX)
        add_executable(spike_iouring_futex spike_iouring_futex.cpp)

        # The spike links plexus::asio for the asio include path ONLY — it probes
        # whether an io_uring CQE can be reaped THROUGH the user's asio reactor (the
        # spike question). The backend target plexus-native itself stays asio-free;
        # this asio dependency lives in the spike test executable, never in the lib.
        target_link_libraries(spike_iouring_futex PRIVATE
            plexus::native
            plexus::asio
            PkgConfig::LIBURING
            Catch2::Catch2WithMain)

        catch_discover_tests(spike_iouring_futex TEST_PREFIX "shm.")
    endif()

    # The wakeup -> reactor bridge functional proof: a forked producer publishes +
    # signals the in-region notify word; the parent's io_uring-futex bridge, armed on
    # a user-owned io_context, reaps the CQE on a reactor turn and posts the drain.
    # Links plexus::asio (the bridge + its asio include path), plexus::native (the broker
    # + the futex primitive the bridge's signal() delegates to), plexus::wire (the fqn
    # hash the region naming routes through), and liburing (the io_uring the bridge
    # drives). The bridge is the only asio-coupled shared-memory piece (the core +
    # the compiled backend stay asio-free).
    add_executable(test_shm_notifier_bridge test_shm_notifier_bridge.cpp)

    target_include_directories(test_shm_notifier_bridge PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR})

    target_link_libraries(test_shm_notifier_bridge PRIVATE
    plexus::plexus
    plexus::shm
        plexus::asio
        plexus::asio::shm
        plexus::native
        plexus::wire
        Catch2::Catch2WithMain)

    catch_discover_tests(test_shm_notifier_bridge TEST_PREFIX "shm.")

    # The teardown-race proof (asan-targeted): a producer process signals at the
    # moment the consumer tears the channel + bridge down, repeatedly, to drive the
    # wake-vs-teardown race. The PASS criterion is asan/ubsan-clean -- no
    # heap-use-after-free on the posted-drain path during teardown (the
    # stop -> wake -> close ordering). Same link set as the bridge proof.
    add_executable(test_shm_teardown_race test_shm_teardown_race.cpp)

    target_include_directories(test_shm_teardown_race PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR})

    target_link_libraries(test_shm_teardown_race PRIVATE
    plexus::plexus
    plexus::shm
        plexus::asio
        plexus::asio::shm
        plexus::native
        plexus::wire
        Catch2::Catch2WithMain)

    catch_discover_tests(test_shm_teardown_race TEST_PREFIX "shm.")

    # The dual-delivery xproc proof: for ONE topic, a same-host SHM ring round-trips a
    # value over the io_uring-futex reactor bridge AND a cross-host loopback wire path
    # delivers the same bytes — the SHM upgrade ADDS the fast path, it does not suppress
    # the wire attach. Same link set as the bridge proof (the bridge + the posix broker +
    # the futex primitive + the fqn-hash region naming + the io_uring the bridge drives).
    add_executable(posix_test_shm_dual_delivery posix_test_shm_dual_delivery.cpp)

    target_include_directories(posix_test_shm_dual_delivery PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR})

    target_link_libraries(posix_test_shm_dual_delivery PRIVATE
    plexus::plexus
    plexus::shm
        plexus::asio
        plexus::asio::shm
        plexus::native
        plexus::wire
        Catch2::Catch2WithMain)

    catch_discover_tests(posix_test_shm_dual_delivery TEST_PREFIX "shm.")

    # The composition compile-gate: the shm-bearing all-backends multiplexer (shm as the
    # second local member alongside AF_UNIX) satisfies transport_backend AND the existing
    # no-shm composition stays intact. The full member set pulls in the secure (TLS/DTLS)
    # members, so it links plexus::crypto via the COMDAT-fold object pattern and builds only
    # when the secure backend is ON.
    if(PLEXUS_ENABLE_CRYPTO_BACKEND)
        add_executable(test_shm_mux_composition test_shm_mux_composition.cpp
            $<TARGET_OBJECTS:plexus-crypto>)

        target_link_libraries(test_shm_mux_composition PRIVATE
    plexus::shm
            plexus::core
            plexus::wire
            plexus::asio
            plexus::asio::shm
            plexus::native
            plexus::crypto
            Catch2::Catch2WithMain)

        catch_discover_tests(test_shm_mux_composition TEST_PREFIX "shm.")
    endif()

    # The variadic-leaf shm node composition: a live node<asio_policy, shm_member,
    # unix_transport, asio_transport> constructs + brings up a same-host listener, and the
    # lean crypto-free local_shm_mux alias satisfies transport_backend. This is the consumer's
    # composition path (the node builds its own mux from the borrowed leaves). It needs the
    # node/api surface (plexus::plexus) + the shm bridge (liburing) but NOT crypto — the lean
    # alias deliberately omits the secure members, so it builds with the crypto backend off.
    add_executable(test_shm_node_composition test_shm_node_composition.cpp)

    target_include_directories(test_shm_node_composition PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR})

    target_link_libraries(test_shm_node_composition PRIVATE
    plexus::shm
        plexus::plexus
        plexus::core
        plexus::wire
        plexus::asio
        plexus::asio::shm
        plexus::native
        Catch2::Catch2WithMain)

    catch_discover_tests(test_shm_node_composition TEST_PREFIX "shm.")

    # The owning composition bundle: a transport_set names the leaves once and mints a node
    # that borrows them — covering both the shm-bearing {io, broker} form and the lean no-shm
    # {io} form in one TU. Same dependency surface as the node-composition proof.
    add_executable(test_transport_set test_transport_set.cpp)

    target_include_directories(test_transport_set PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR})

    target_link_libraries(test_transport_set PRIVATE
    plexus::shm
        plexus::plexus
        plexus::core
        plexus::wire
        plexus::asio
        plexus::asio::shm
        plexus::native
        Catch2::Catch2WithMain)

    catch_discover_tests(test_transport_set TEST_PREFIX "shm.")

    # The portable same-host composition: one consumer surface on every platform that resolves
    # to shm + AF_UNIX + TCP on Linux. Proves the portable type stands up a live node AND that
    # two independent per-instance brokers (the model the type owns) share an shm ring by name.
    add_executable(test_same_host_transports test_same_host_transports.cpp)

    target_include_directories(test_same_host_transports PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR})

    target_link_libraries(test_same_host_transports PRIVATE
    plexus::shm
        plexus::plexus
        plexus::core
        plexus::wire
        plexus::asio
        plexus::asio::shm
        plexus::native
        Catch2::Catch2WithMain)

    catch_discover_tests(test_same_host_transports TEST_PREFIX "shm.")

    # Two same-host nodes in one process, each on its own io_context, stepped by hand across the switch of a subscriber
    # from the wire to the publisher's ring.
    add_executable(test_shm_switch_marker test_shm_switch_marker.cpp)

    target_include_directories(test_shm_switch_marker PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR})

    target_link_libraries(test_shm_switch_marker PRIVATE
        plexus::plexus
        plexus::core
        plexus::wire
        plexus::asio
        plexus::asio::shm
        plexus::native
        Catch2::Catch2WithMain)

    catch_discover_tests(test_shm_switch_marker TEST_PREFIX "shm.")

    # A reader engaged on a publisher's reliable ring beside a newcomer whose wire is backed up, and its control. The
    # cases time a loaded producer and reader, so they run alone.
    add_executable(test_shm_switch_hold test_shm_switch_hold.cpp)

    target_include_directories(test_shm_switch_hold PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR})

    target_link_libraries(test_shm_switch_hold PRIVATE
        plexus::plexus
        plexus::core
        plexus::wire
        plexus::asio
        plexus::asio::shm
        plexus::native
        Catch2::Catch2WithMain)

    catch_discover_tests(test_shm_switch_hold TEST_PREFIX "shm."
        PROPERTIES RUN_SERIAL TRUE)

    # Two same-host nodes in one process: only a node with a local publisher for a topic offers its ring, and a publisher
    # declared after a co-host subscriber subscribed offers it late.
    add_executable(test_shm_offer_scope test_shm_offer_scope.cpp)

    target_include_directories(test_shm_offer_scope PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR})

    target_link_libraries(test_shm_offer_scope PRIVATE
        plexus::plexus
        plexus::core
        plexus::wire
        plexus::asio
        plexus::asio::shm
        plexus::native
        Catch2::Catch2WithMain)

    catch_discover_tests(test_shm_offer_scope TEST_PREFIX "shm.")

    # The node-level same-host fingerprint fix: make_node default-fills the node's local
    # fingerprint from the real machine read when the caller left it null, and respects a
    # caller-supplied value. Reads the effective fingerprint off the minted node's engine.
    # Same dependency surface as the portable-composition proof.
    add_executable(test_same_host_fingerprint_node test_same_host_fingerprint_node.cpp)

    target_include_directories(test_same_host_fingerprint_node PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR})

    target_link_libraries(test_same_host_fingerprint_node PRIVATE
    plexus::shm
        plexus::plexus
        plexus::core
        plexus::wire
        plexus::asio
        plexus::asio::shm
        plexus::native
        Catch2::Catch2WithMain)

    catch_discover_tests(test_same_host_fingerprint_node TEST_PREFIX "shm.")

    if(PLEXUS_SHM_TESTS_HAVE_FUTEX)
        message(STATUS "plexus shm tests: liburing ${LIBURING_VERSION} found with futex-wait — io_uring spike + reactor bridge enabled")
    else()
        message(STATUS "plexus shm tests: liburing ${LIBURING_VERSION} found without futex-wait (< 2.6) — reactor bridge on the threaded floor, io_uring spike skipped")
    endif()
else()
    message(STATUS "plexus shm tests: liburing absent — io_uring futex spike + reactor bridge skipped (non-gating)")
endif()

# The mux-join selection oracle (header-only, asio/POSIX-free): the shm member composed
# over the core multiplexer alongside a stream member proves the member satisfies
# mux_member + byte_channel, the preference hook routes a same-host dial to the shm member
# when the ring acquires, and the SAME dial falls back to the stream member on a forced
# broker failure. A heap-backed stub broker + a no-op notifier keep it single-process and
# deterministic; the TU includes only core headers (the real cross-process data path is the
# dual_delivery xproc proof).
add_executable(test_shm_mux test_shm_mux.cpp)

target_include_directories(test_shm_mux PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(test_shm_mux PRIVATE
    plexus::shm
    plexus::core
    plexus::wire
    Catch2::Catch2WithMain)

catch_discover_tests(test_shm_mux TEST_PREFIX "shm.")

# The medium-coordinator oracle (header-only core + inproc): the engine-owned coordinator
# consumes the forwarder's demand transition, reads same_host, runs the upgrade policy, and
# acquires/releases the per-topic ring THROUGH the injected gate (the can_acquire/prefer_upgradeable_hook
# path), never a direct registry acquire — with refcount 0->1/1->0 gating, last-unsubscribe and
# peer-dead teardown, the node's accumulated hint, and dual delivery (the wire never suppressed). It also
# drives the forwarder's real on_demand_transition emit seam (boundary-only fire, no-op when unset).
add_executable(test_medium_coordinator test_medium_coordinator.cpp)

target_link_libraries(test_medium_coordinator PRIVATE
    plexus::shm
    plexus::core
    plexus::wire
    plexus::inproc
    Catch2::Catch2WithMain)

catch_discover_tests(test_medium_coordinator TEST_PREFIX "shm.")

# A medium that loses its ring waiter drops every lane the coordinator holds for the topic.
add_executable(test_coordinator_medium_lost test_coordinator_medium_lost.cpp)

target_link_libraries(test_coordinator_medium_lost PRIVATE
    plexus::core
    Catch2::Catch2WithMain)

catch_discover_tests(test_coordinator_medium_lost TEST_PREFIX "shm.")

# The coordinator's posted re-attempt of a transient decline: backoff, cap, cancellation, through a manual scheduler.
add_executable(test_medium_coordinator_retry test_medium_coordinator_retry.cpp)

target_link_libraries(test_medium_coordinator_retry PRIVATE
    plexus::core
    Catch2::Catch2WithMain)

catch_discover_tests(test_medium_coordinator_retry TEST_PREFIX "shm.")

# The coordinator counts every decline by its reason, each transient attempt, an exhausted pair once, and a demotion.
add_executable(test_medium_coordinator_reasons test_medium_coordinator_reasons.cpp)

target_link_libraries(test_medium_coordinator_reasons PRIVATE
    plexus::core
    Catch2::Catch2WithMain)

catch_discover_tests(test_medium_coordinator_reasons TEST_PREFIX "shm.")

# Shared memory engages only bilaterally: the publisher offers its ring, the subscriber attaches it when its own policy
# agrees and answers ready, the publisher routes only after that answer and returns to the wire on a withdrawal.
add_executable(test_medium_coordinator_engagement test_medium_coordinator_engagement.cpp)

target_link_libraries(test_medium_coordinator_engagement PRIVATE
    plexus::core
    Catch2::Catch2WithMain)

catch_discover_tests(test_medium_coordinator_engagement TEST_PREFIX "shm.")

# A ring that appears after a subscriber was answered is offered late, and the subscribe response says whether the lane
# was offered, withheld or has no publisher.
add_executable(test_medium_coordinator_offers test_medium_coordinator_offers.cpp)

target_link_libraries(test_medium_coordinator_offers PRIVATE
    plexus::core
    Catch2::Catch2WithMain)

catch_discover_tests(test_medium_coordinator_offers TEST_PREFIX "shm.")

# A member's declines, demotions and lost waiters reach the coordinator's counters by reason, over in-memory brokers.
add_executable(test_medium_coordinator_member_reasons test_medium_coordinator_member_reasons.cpp)

target_include_directories(test_medium_coordinator_member_reasons PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(test_medium_coordinator_member_reasons PRIVATE
    plexus::plexus
    plexus::shm
    plexus::core
    plexus::testing
    Catch2::Catch2WithMain)

catch_discover_tests(test_medium_coordinator_member_reasons TEST_PREFIX "shm.")

# The forwarder's delivery order across a late wire-to-companion switch and across a subscriber attach.
add_executable(test_shm_switch_order test_shm_switch_order.cpp)

target_include_directories(test_shm_switch_order PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(test_shm_switch_order PRIVATE
    plexus::core
    plexus::wire
    Catch2::Catch2WithMain)

catch_discover_tests(test_shm_switch_order TEST_PREFIX "shm.")

# A publisher holds one lane per topic for every co-host peer it offered it to and for its own subscriber, destroying it
# only through a release its forwarder hears first.
add_executable(test_medium_coordinator_lanes test_medium_coordinator_lanes.cpp)

target_link_libraries(test_medium_coordinator_lanes PRIVATE
    plexus::core
    Catch2::Catch2WithMain)

catch_discover_tests(test_medium_coordinator_lanes TEST_PREFIX "shm.")

# Each publish is written to the topic's lane at most once, whichever readers route to it, the node itself included.
add_executable(test_shm_write_once test_shm_write_once.cpp)

target_include_directories(test_shm_write_once PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(test_shm_write_once PRIVATE
    plexus::core
    plexus::wire
    Catch2::Catch2WithMain)

catch_discover_tests(test_shm_write_once TEST_PREFIX "shm.")

# The bilateral, consumer-sovereign upgrade via demand-driven convergence (xproc):
# two real processes each decide INDEPENDENTLY whether to attempt the shared-memory
# acquire from their OWN dispatch hint (no hint on the wire), converge on the same
# deterministically-named ring, and round-trip a value over it; the publisher sizes
# the ring via max_payload, a no-max_payload upgrade falls back to the default
# geometry. Links plexus::native (the POSIX broker + the ring) + plexus::wire (the fqn
# hash the region naming routes through).
# Drives fork() + the POSIX broker directly (unistd.h), so it is excluded from the Windows build.
if(NOT WIN32)
    add_executable(test_shm_bilateral test_shm_bilateral.cpp)

    target_include_directories(test_shm_bilateral PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR})

    target_link_libraries(test_shm_bilateral PRIVATE
        plexus::plexus
        plexus::shm
        plexus::native
        plexus::wire
        Catch2::Catch2WithMain)

    catch_discover_tests(test_shm_bilateral TEST_PREFIX "shm.")
endif()

# The large-message live-mux round-trip (xproc): a publisher declares a sizing
# same_host_geometry and mints the ring through shm_mux_member's ACQUIRE path (not the unit
# registry); a >1 MiB then a 16 MiB payload crosses byte-exact to a converged
# subscriber, per notifier variant (futex-wake shmn + reactor-reaped/spin shmr;
# busy-poll shm excluded). The large cells run >= 2 times per ctest invocation. The
# probe-higher cell proves reliable_preserving fails closed above the ceiling while
# explicit best_effort_large round-trips. Links plexus::native (the POSIX broker + the
# futex primitive) + plexus::wire (the fqn-hash region naming).
# Drives the Linux futex notifier primitive directly (SYS_futex, <linux/futex.h>), so it is
# Linux-only and excluded on Windows AND macOS (neither carries the futex notifier body).
if(NOT WIN32 AND NOT APPLE)
    add_executable(test_shm_large_message test_shm_large_message.cpp)

    target_include_directories(test_shm_large_message PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR})

    target_link_libraries(test_shm_large_message PRIVATE
        plexus::plexus
        plexus::shm
        plexus::native
        plexus::wire
        Catch2::Catch2WithMain)

    catch_discover_tests(test_shm_large_message TEST_PREFIX "shm.")
endif()

# The crashed-creator orphan-reclaim proof (xproc): a child creates a region that
# is not a ring, stamps a sentinel, and _exits WITHOUT unlinking, leaving an orphan
# in /dev/shm; the parent's create with unlink_stale_on_create reclaims the orphan
# and mints a FRESH (zero-filled) region, proving the orphan's stale bytes do not
# leak through. Looped N>=100. Links
# plexus::native for the compiled broker (shm_open/ftruncate/shm_unlink).
# A custom main() runs the re-exec child dispatcher BEFORE the Catch2 session, so
# it links the main-less Catch2 target and pulls plexus::testing for the portable
# process-id / shared-object-name helper the re-exec harness routes through.
add_executable(test_shm_stale_reclaim test_shm_stale_reclaim.cpp)

target_include_directories(test_shm_stale_reclaim PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(test_shm_stale_reclaim PRIVATE
    plexus::shm
    plexus::native
    plexus::testing
    Catch2::Catch2)

catch_discover_tests(test_shm_stale_reclaim TEST_PREFIX "shm.")

# A custom main() dispatches the re-exec'd child role before the Catch2 session, so it links the main-less Catch2.
add_executable(test_shm_xproc_process test_shm_xproc_process.cpp)

target_include_directories(test_shm_xproc_process PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(test_shm_xproc_process PRIVATE
    plexus::shm
    plexus::native
    plexus::testing
    Catch2::Catch2)

catch_discover_tests(test_shm_xproc_process TEST_PREFIX "shm.")

# The wire_fallback per-message routing proof (xproc): a topic in wire_fallback mode
# mints a CAPPED ring through shm_mux_member's acquire path AND routes per message at the
# message_forwarder publish fan-out — a message that fits the cap rides the SHM ring
# (verified byte-exact cross-process), a message over the cap rides the same-host wire.
# The carrying medium is ASSERTED per message via a per-channel tap (proven, not assumed),
# the over-cap cell repeats in one invocation, and a ring-less subscriber still receives
# both over the wire (the dual-delivery rescue). Links plexus::native (the POSIX broker + the
# futex primitive the live ring acquires) + plexus::wire (the fqn-hash region naming + the
# forwarder framing) + plexus::inproc (the test Policy's executor/timer substrate).
# Drives fork() + the POSIX broker directly (unistd.h), so it is excluded from the Windows build.
if(NOT WIN32)
    add_executable(test_shm_wire_fallback test_shm_wire_fallback.cpp)

    target_include_directories(test_shm_wire_fallback PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR})

    target_link_libraries(test_shm_wire_fallback PRIVATE
        plexus::plexus
        plexus::shm
        plexus::core
        plexus::native
        plexus::wire
        plexus::inproc
        Catch2::Catch2WithMain)

    catch_discover_tests(test_shm_wire_fallback TEST_PREFIX "shm.")
endif()

# The wire_fallback PRODUCTION-SEAM proof: the same-host medium decision a node reaches
# through node.advertise(...) is the multiplexer's prefer_upgradeable_hook over the shm-bearing
# pack, gated by the now MODE-AWARE shm_mux_member::can_acquire. A wire_fallback topic
# (provisioned by the SAME set_topic_geometry the node declare path calls) declines the
# ring so its same-host channel is the WIRE (the fail-safe); the two reliable-ring modes
# prefer SHM unchanged. The declined probe holds no refcount, and an over-cap message
# crosses a real process boundary byte-exact (repeated). Links plexus::native (the POSIX
# broker) + plexus::wire (the fqn-hash region naming).
# Drives fork() + the POSIX broker directly (unistd.h), so it is excluded from the Windows build.
if(NOT WIN32)
    add_executable(test_shm_wire_fallback_node test_shm_wire_fallback_node.cpp)

    target_include_directories(test_shm_wire_fallback_node PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR})

    target_link_libraries(test_shm_wire_fallback_node PRIVATE
        plexus::shm
        plexus::core
        plexus::native
        plexus::wire
        Catch2::Catch2WithMain)

    catch_discover_tests(test_shm_wire_fallback_node TEST_PREFIX "shm.")
endif()
