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

plexus_add_catch_test(test_shm_ring_memory_query
    SOURCES test_shm_ring_memory_query.cpp
    LINKS plexus::shm plexus::core plexus::wire)

plexus_add_catch_test(test_shm_geometry_defaults
    SOURCES test_shm_geometry_defaults.cpp
    LINKS plexus::shm plexus::core plexus::wire)

# It runs its own main(), which dispatches a re-exec'd child's role before the Catch2 session.
plexus_add_catch_test(test_shm_stale_reclaim
    NO_MAIN
    SOURCES test_shm_stale_reclaim.cpp
    LINKS plexus::plexus plexus::shm plexus::native plexus::core plexus::testing)
