if(NOT PLEXUS_BUILD_TESTS)
    return()
endif()

set(PLEXUS_TEST_PREFIX "discovery.")

plexus_add_catch_test(test_contact_card
    SOURCES test_contact_card.cpp test_contact_card_static_discovery.cpp
    LINKS plexus::core plexus::discovery)

plexus_add_catch_test(test_universe
    SOURCES test_universe.cpp
    LINKS plexus::core plexus::discovery)

# The header-only discovery template is instantiated against a fake datagram socket, so these need no asio.
plexus_add_catch_test(test_multicast_discovery
    SOURCES test_multicast_discovery.cpp
    LINKS plexus::core plexus::wire plexus::stream plexus::discovery)

plexus_add_catch_test(test_multicast_discovery_universe
    SOURCES test_multicast_discovery_universe.cpp
    LINKS plexus::core plexus::wire plexus::stream plexus::discovery)

plexus_add_catch_test(test_multicast_discovery_flood_cap
    SOURCES test_multicast_discovery_flood_cap.cpp
    LINKS plexus::core plexus::wire plexus::stream plexus::discovery)

plexus_add_catch_test(test_flood_cap_aging
    SOURCES test_flood_cap_aging.cpp
    LINKS plexus::core plexus::wire plexus::stream plexus::discovery)

# The default_discovery factory cases drive the real asio socket.
if(PLEXUS_ENABLE_ASIO_BACKEND)
    plexus_add_catch_test(test_discovery_self_probe
        SOURCES test_discovery_self_probe.cpp
        LINKS plexus::core plexus::wire plexus::stream plexus::discovery plexus::asio)
endif()
