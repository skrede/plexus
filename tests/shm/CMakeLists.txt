if(NOT PLEXUS_ENABLE_SHM_BACKEND)
    return()
endif()

set(PLEXUS_TEST_PREFIX "shm.")

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

plexus_add_catch_test(test_shm_scaffold
    SOURCES test_shm_scaffold.cpp
    LINKS plexus::native
    DEFINES PLEXUS_TEST_PROJECT_VERSION="${PROJECT_VERSION}")

plexus_add_catch_test(test_shm_ring_core
    SOURCES test_shm_ring_core.cpp test_shm_ring_core_capacity.cpp test_shm_ring_magic.cpp
        test_shm_ring_attach_bounds.cpp test_shm_ring_tampered.cpp test_shm_cursor_generation.cpp
    LINKS plexus::shm plexus::core)

plexus_add_catch_test(test_shm_ring_backpressure
    SOURCES test_shm_ring_backpressure.cpp test_shm_ring_backpressure_recovery.cpp
    LINKS plexus::shm plexus::core)

find_package(Threads REQUIRED)

plexus_add_catch_test(test_shm_ring_dekker
    SOURCES test_shm_ring_dekker.cpp
    LINKS plexus::shm plexus::core Threads::Threads)

plexus_add_catch_test(test_shm_ring_overtake
    SOURCES test_shm_ring_overtake.cpp
    LINKS plexus::shm plexus::core)

plexus_add_catch_test(test_shm_loan_abandon
    SOURCES test_shm_loan_abandon.cpp
    LINKS plexus::shm plexus::core)

plexus_add_catch_test(test_shm_zerocopy_take
    SOURCES test_shm_zerocopy_take.cpp
    LINKS plexus::shm plexus::core plexus::wire)

plexus_add_catch_test(test_shm_loan_lifetime
    SOURCES test_shm_loan_lifetime.cpp
    LINKS plexus::shm plexus::core plexus::wire)

plexus_add_catch_test(test_shm_dispatch_hint
    SOURCES test_shm_dispatch_hint.cpp
    LINKS plexus::core)

plexus_add_catch_test(test_shm_same_host
    SOURCES test_shm_same_host.cpp
    LINKS plexus::shm plexus::core plexus::wire)

plexus_add_catch_test(test_region_naming_length
    SOURCES test_region_naming_length.cpp
    LINKS plexus::shm plexus::wire)

plexus_add_catch_test(test_shm_doorbell_naming
    SOURCES test_shm_doorbell_naming.cpp
    LINKS plexus::shm)

plexus_add_catch_test(test_shm_selection
    SOURCES test_shm_selection.cpp
    LINKS plexus::shm plexus::core plexus::wire)

plexus_add_catch_test(test_shm_seams
    SOURCES test_shm_seams.cpp
    LINKS plexus::shm plexus::core)

# Drives the POSIX broker directly, so it is excluded from the Windows build.
if(NOT WIN32)
    plexus_add_catch_test(test_shm_region_broker
        SOURCES test_shm_region_broker.cpp
        LINKS plexus::shm plexus::native)
endif()

# Both reach the region through /dev/shm, the tmpfs view of POSIX shm that only Linux has; macOS shm_open objects
# are not path-visible.
if(NOT WIN32 AND NOT APPLE)
    plexus_add_catch_test(linux_test_shm_region_broker
        SOURCES linux_test_shm_region_broker.cpp
        LINKS plexus::shm plexus::native)

    plexus_add_catch_test(linux_test_shm_slot_lock
        SOURCES linux_test_shm_slot_lock.cpp
        LINKS plexus::shm plexus::native plexus::testing)
endif()

# Seccomp user notification, which pauses the create inside its syscall, is Linux-only.
if(NOT WIN32 AND NOT APPLE)
    plexus_add_catch_test(linux_test_shm_create_window
        SOURCES linux_test_shm_create_window.cpp
        LINKS plexus::plexus plexus::shm plexus::native plexus::core plexus::testing Threads::Threads)
endif()

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

# Drives fork() and the POSIX broker directly, so it is excluded from the Windows build.
if(NOT WIN32)
    plexus_add_catch_test(test_shm_same_host_roundtrip
        SOURCES test_shm_same_host_roundtrip.cpp test_shm_same_host_fingerprint.cpp
        LINKS plexus::plexus plexus::shm plexus::native plexus::wire)
endif()

# Drives fork() and the POSIX broker directly, so it is excluded from the Windows build.
if(NOT WIN32)
    plexus_add_catch_test(test_shm_region_namespace
        SOURCES test_shm_region_namespace.cpp
        LINKS plexus::plexus plexus::shm plexus::native plexus::wire)
endif()

plexus_add_catch_test(test_shm_channel
    SOURCES test_shm_channel.cpp
    LINKS plexus::shm plexus::core plexus::wire)

plexus_add_catch_test(test_shm_spin_park
    SOURCES test_shm_spin_park.cpp
    LINKS plexus::shm plexus::core plexus::wire Threads::Threads)

plexus_add_catch_test(test_shm_congested
    SOURCES test_shm_congested.cpp test_shm_congested_alloc.cpp
    LINKS plexus::shm plexus::core plexus::wire Threads::Threads)

plexus_add_catch_test(test_shm_registry
    SOURCES test_shm_registry.cpp
    LINKS plexus::shm plexus::core plexus::wire)

plexus_add_catch_test(test_shm_registry_join
    SOURCES test_shm_registry_join.cpp
    LINKS plexus::plexus plexus::shm plexus::core plexus::testing)

plexus_add_catch_test(test_shm_ring_lifetime
    SOURCES test_shm_ring_lifetime.cpp test_shm_ring_recreate.cpp
    LINKS plexus::plexus plexus::shm plexus::native plexus::core plexus::testing)

if(NOT WIN32)
    plexus_add_catch_test(posix_test_shm_sidecar_leak
        SOURCES posix_test_shm_sidecar_leak.cpp
        LINKS plexus::plexus plexus::shm plexus::native plexus::core plexus::testing)
endif()

# On Windows a name ends with its last handle, so no retired name outlives its holders.
if(NOT WIN32)
    plexus_add_catch_test(test_shm_retired_name_identity
        SOURCES test_shm_retired_name_identity.cpp test_shm_retired_name_order.cpp test_shm_retired_name_stress.cpp
        LINKS plexus::plexus plexus::shm plexus::native plexus::core plexus::testing)
endif()

if(NOT WIN32)
    plexus_add_catch_test(posix_test_shm_mint_takeover
        SOURCES posix_test_shm_mint_takeover.cpp
        LINKS plexus::plexus plexus::shm plexus::native plexus::core plexus::testing)
endif()

if(NOT WIN32)
    plexus_add_catch_test(posix_test_shm_creator_retake
        SOURCES posix_test_shm_creator_retake.cpp
        LINKS plexus::plexus plexus::shm plexus::native plexus::core plexus::testing)
endif()

plexus_add_catch_test(test_shm_take_pin_mapping
    SOURCES test_shm_take_pin_mapping.cpp
    LINKS plexus::plexus plexus::shm plexus::native plexus::core plexus::testing)

plexus_add_catch_test(test_shm_registry_roles
    SOURCES test_shm_registry_roles.cpp
    LINKS plexus::plexus plexus::shm plexus::core plexus::testing)

plexus_add_catch_test(test_shm_cursor_reclaim
    SOURCES test_shm_cursor_reclaim.cpp
    LINKS plexus::plexus plexus::shm plexus::native plexus::core plexus::testing)

plexus_add_catch_test(test_shm_registry_capacity
    SOURCES test_shm_registry_capacity.cpp
    LINKS plexus::plexus plexus::shm plexus::core plexus::testing)

plexus_add_catch_test(test_shm_region_universe
    SOURCES test_shm_region_universe.cpp
    LINKS plexus::shm plexus::plexus plexus::testing)

plexus_add_catch_test(test_shm_registry_arm
    SOURCES test_shm_registry_arm.cpp
    LINKS plexus::plexus plexus::shm plexus::native plexus::core plexus::testing)

if(TARGET plexus::asio)
    plexus_add_catch_test(test_shm_registry_loss
        SOURCES test_shm_registry_loss.cpp
        LINKS plexus::plexus plexus::shm plexus::asio plexus::native plexus::core plexus::testing)
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

plexus_add_catch_test(test_shm_ring_memory_query
    SOURCES test_shm_ring_memory_query.cpp
    LINKS plexus::shm plexus::core plexus::wire)

plexus_add_catch_test(test_shm_geometry_defaults
    SOURCES test_shm_geometry_defaults.cpp
    LINKS plexus::shm plexus::core plexus::wire)

find_package(PkgConfig QUIET)
if(PkgConfig_FOUND)
    pkg_check_modules(LIBURING QUIET IMPORTED_TARGET liburing)
endif()

# io_uring_prep_futex_wait needs liburing 2.6 and the spike calls it directly, so it checks the capability, not the
# presence. The bridge tests stay under LIBURING_FOUND: ring_notifier falls back to its threaded floor without it.
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

    # The full member set includes the TLS and DTLS members, so it needs the crypto backend and takes its objects
    # directly (the COMDAT-fold object pattern).
    if(PLEXUS_ENABLE_CRYPTO_BACKEND)
        plexus_add_catch_test(test_shm_mux_composition
            SOURCES test_shm_mux_composition.cpp $<TARGET_OBJECTS:plexus-crypto>
            LINKS plexus::shm plexus::core plexus::wire plexus::asio plexus::asio::shm plexus::native plexus::crypto)
    endif()

    plexus_add_catch_test(test_shm_node_composition
        SOURCES test_shm_node_composition.cpp
        LINKS plexus::shm plexus::plexus plexus::core plexus::wire plexus::asio plexus::asio::shm plexus::native)

    plexus_add_catch_test(test_transport_set
        SOURCES test_transport_set.cpp
        LINKS plexus::shm plexus::plexus plexus::core plexus::wire plexus::asio plexus::asio::shm plexus::native)

    plexus_add_catch_test(test_same_host_transports
        SOURCES test_same_host_transports.cpp
        LINKS plexus::shm plexus::plexus plexus::core plexus::wire plexus::asio plexus::asio::shm plexus::native)

    plexus_add_catch_test(test_shm_switch_marker
        SOURCES test_shm_switch_marker.cpp
        LINKS plexus::plexus plexus::core plexus::wire plexus::asio plexus::asio::shm plexus::native)

    plexus_add_catch_test(test_shm_drain_reentry
        SOURCES test_shm_drain_reentry.cpp
        LINKS plexus::plexus plexus::core plexus::wire plexus::asio plexus::asio::shm plexus::native)

    # The cases time a loaded producer and reader, so they run alone.
    plexus_add_catch_test(test_shm_switch_hold
        SOURCES test_shm_switch_hold.cpp
        LINKS plexus::plexus plexus::core plexus::wire plexus::asio plexus::asio::shm plexus::native
        PROPERTIES RUN_SERIAL TRUE)

    plexus_add_catch_test(test_shm_offer_scope
        SOURCES test_shm_offer_scope.cpp
        LINKS plexus::plexus plexus::core plexus::wire plexus::asio plexus::asio::shm plexus::native)

    plexus_add_catch_test(test_same_host_fingerprint_node
        SOURCES test_same_host_fingerprint_node.cpp
        LINKS plexus::shm plexus::plexus plexus::core plexus::wire plexus::asio plexus::asio::shm plexus::native)

    if(PLEXUS_SHM_TESTS_HAVE_FUTEX)
        message(STATUS "plexus shm tests: liburing ${LIBURING_VERSION} found with futex-wait — io_uring spike + reactor bridge enabled")
    else()
        message(STATUS "plexus shm tests: liburing ${LIBURING_VERSION} found without futex-wait (< 2.6) — reactor bridge on the threaded floor, io_uring spike skipped")
    endif()
else()
    message(STATUS "plexus shm tests: liburing absent — io_uring futex spike + reactor bridge skipped (non-gating)")
endif()

plexus_add_catch_test(test_shm_mux
    SOURCES test_shm_mux.cpp
    LINKS plexus::shm plexus::core plexus::wire)

plexus_add_catch_test(test_shm_byte_channel_retire
    SOURCES test_shm_byte_channel_retire.cpp
    LINKS plexus::shm plexus::core plexus::native plexus::testing)

plexus_add_catch_test(test_medium_coordinator
    SOURCES test_medium_coordinator.cpp
    LINKS plexus::shm plexus::core plexus::wire plexus::inproc)

plexus_add_catch_test(test_coordinator_medium_lost
    SOURCES test_coordinator_medium_lost.cpp
    LINKS plexus::core)

plexus_add_catch_test(test_medium_coordinator_retry
    SOURCES test_medium_coordinator_retry.cpp
    LINKS plexus::core)

plexus_add_catch_test(test_medium_coordinator_reasons
    SOURCES test_medium_coordinator_reasons.cpp
    LINKS plexus::core)

plexus_add_catch_test(test_medium_coordinator_engagement
    SOURCES test_medium_coordinator_engagement.cpp
    LINKS plexus::core)

plexus_add_catch_test(test_medium_coordinator_offers
    SOURCES test_medium_coordinator_offers.cpp
    LINKS plexus::core)

plexus_add_catch_test(test_medium_coordinator_member_reasons
    SOURCES test_medium_coordinator_member_reasons.cpp
    LINKS plexus::plexus plexus::shm plexus::core plexus::testing)

plexus_add_catch_test(test_shm_switch_order
    SOURCES test_shm_switch_order.cpp
    LINKS plexus::core plexus::wire)

plexus_add_catch_test(test_medium_coordinator_lanes
    SOURCES test_medium_coordinator_lanes.cpp
    LINKS plexus::core)

plexus_add_catch_test(test_shm_write_once
    SOURCES test_shm_write_once.cpp
    LINKS plexus::core plexus::wire)

plexus_add_catch_test(test_shm_claim_switch
    SOURCES test_shm_claim_switch.cpp
    LINKS plexus::core plexus::wire)

# Drives fork() and the POSIX broker directly, so it is excluded from the Windows build.
if(NOT WIN32)
    plexus_add_catch_test(test_shm_bilateral
        SOURCES test_shm_bilateral.cpp
        LINKS plexus::plexus plexus::shm plexus::native plexus::wire)
endif()

# Drives the Linux futex primitive directly, so it is excluded from Windows and macOS.
if(NOT WIN32 AND NOT APPLE)
    plexus_add_catch_test(test_shm_large_message
        SOURCES test_shm_large_message.cpp
        LINKS plexus::plexus plexus::shm plexus::native plexus::wire)
endif()

# Both run their own main(): it dispatches a re-exec'd child's role before the Catch2 session.
plexus_add_catch_test(test_shm_stale_reclaim
    NO_MAIN
    SOURCES test_shm_stale_reclaim.cpp
    LINKS plexus::plexus plexus::shm plexus::native plexus::core plexus::testing)

plexus_add_catch_test(test_shm_xproc_process
    NO_MAIN
    SOURCES test_shm_xproc_process.cpp
    LINKS plexus::shm plexus::native plexus::testing)

# Drives fork() and the POSIX broker directly, so it is excluded from the Windows build.
if(NOT WIN32)
    plexus_add_catch_test(test_shm_wire_fallback
        SOURCES test_shm_wire_fallback.cpp
        LINKS plexus::plexus plexus::shm plexus::core plexus::native plexus::wire plexus::inproc)
endif()

# Drives fork() and the POSIX broker directly, so it is excluded from the Windows build.
if(NOT WIN32)
    plexus_add_catch_test(test_shm_wire_fallback_node
        SOURCES test_shm_wire_fallback_node.cpp
        LINKS plexus::shm plexus::core plexus::native plexus::wire)
endif()
