if(PLEXUS_ENABLE_ASIO_BACKEND)
    # RUN_SERIAL and the loopback lock keep the real-multicast leg off the other socket legs.
    plexus_add_catch_test(test_default_discovery_lan
        SOURCES test_default_discovery_lan.cpp test_default_discovery_lan_sweep.cpp
        LINKS plexus::plexus plexus::core plexus::wire plexus::discovery plexus::asio plexus::testing
        PROPERTIES RUN_SERIAL TRUE RESOURCE_LOCK tcp-loopback LABELS "live-multicast")

    # RUN_SERIAL and the loopback lock keep the real-multicast leg off the other socket legs.
    plexus_add_catch_test(test_default_discovery_universe
        SOURCES test_default_discovery_universe.cpp
        LINKS plexus::plexus plexus::core plexus::wire plexus::discovery plexus::asio plexus::testing
        PROPERTIES RUN_SERIAL TRUE RESOURCE_LOCK tcp-loopback LABELS "live-multicast")

    # Its two nodes run in forked processes, which is POSIX-only. RUN_SERIAL and the loopback lock keep the
    # real-multicast leg off the other socket legs.
    if(NOT WIN32)
        plexus_add_catch_test(test_default_discovery_xproc
            SOURCES test_default_discovery_xproc.cpp
            LINKS plexus::plexus plexus::core plexus::wire plexus::discovery plexus::asio plexus::testing
            PROPERTIES RUN_SERIAL TRUE RESOURCE_LOCK tcp-loopback LABELS "live-multicast")
    endif()
endif()

# An alloc gate over the single-TU operator-new counter; concurrent test processes perturb it, so it runs alone.
plexus_add_catch_test(test_discovery_noalloc
    SOURCES test_discovery_noalloc.cpp
    LINKS plexus::core plexus::discovery plexus::wire plexus::stream
    PROPERTIES RUN_SERIAL TRUE)
