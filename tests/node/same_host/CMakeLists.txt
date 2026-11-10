plexus_add_catch_test(test_shm_self_carrier
    SOURCES test_shm_self_carrier.cpp
    LINKS plexus::shm plexus::plexus plexus::core plexus::wire plexus::asio plexus::asio::shm plexus::native)

plexus_add_catch_test(test_node_same_host_identity
    SOURCES test_node_same_host_identity.cpp
    LINKS plexus::shm plexus::plexus plexus::core plexus::asio plexus::asio::shm plexus::native)

plexus_add_catch_test(test_node_same_host_fallbacks
    SOURCES test_node_same_host_fallbacks.cpp
    LINKS plexus::shm plexus::plexus plexus::core plexus::asio plexus::asio::shm plexus::native)
