plexus_add_catch_test(test_node_discovery
    SOURCES test_node_discovery.cpp test_node_discovery_awareness.cpp
    LINKS plexus::plexus plexus::inproc plexus::discovery)

plexus_add_catch_test(test_node_discovery_aging
    SOURCES test_node_discovery_aging.cpp test_node_discovery_aging_wired.cpp
    LINKS plexus::core plexus::wire plexus::inproc plexus::testing)

plexus_add_catch_test(test_known_peer_pruning
    SOURCES test_known_peer_pruning.cpp
    LINKS plexus::plexus plexus::inproc plexus::discovery)

plexus_add_catch_test(test_late_join
    SOURCES test_late_join.cpp
    LINKS plexus::plexus plexus::inproc plexus::discovery)

if(PLEXUS_ENABLE_ASIO_BACKEND)
    plexus_add_catch_test(test_node_native_discovery
        SOURCES test_node_native_discovery.cpp
        LINKS plexus::plexus plexus::core plexus::wire plexus::stream plexus::asio plexus::discovery)

    plexus_add_catch_test(test_liveliness_tcp
        SOURCES test_liveliness_tcp.cpp
        LINKS plexus::plexus plexus::asio plexus::inproc plexus::testing plexus::discovery)

    plexus_add_catch_test(test_liveliness_udp
        SOURCES test_liveliness_udp.cpp
        LINKS plexus::plexus plexus::asio plexus::inproc plexus::testing plexus::discovery)
endif()
