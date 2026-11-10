plexus_add_catch_test(test_shm_dispatch_hint
    SOURCES test_shm_dispatch_hint.cpp
    LINKS plexus::core)

plexus_add_catch_test(test_shm_selection
    SOURCES test_shm_selection.cpp
    LINKS plexus::shm plexus::core plexus::wire)

if(LIBURING_FOUND)
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
endif()

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
