if(PLEXUS_ENABLE_CRYPTO_BACKEND)
    # A real-loopback leg with a fixed wall-clock pump deadline, so it runs alone and holds the loopback lock.
    plexus_add_catch_test(test_tls_channel_backpressure_asio
        SOURCES test_tls_channel_backpressure_asio.cpp $<TARGET_OBJECTS:plexus-crypto>
        LINKS plexus::core plexus::wire plexus::asio plexus::crypto
        PROPERTIES RUN_SERIAL TRUE RESOURCE_LOCK tcp-loopback)

    # A real-loopback leg with a fixed wall-clock pump deadline, so it runs alone and holds the loopback lock.
    plexus_add_catch_test(test_tls_dial_teardown
        SOURCES test_tls_dial_teardown.cpp $<TARGET_OBJECTS:plexus-crypto>
        LINKS plexus::core plexus::wire plexus::asio plexus::crypto
        PROPERTIES RUN_SERIAL TRUE RESOURCE_LOCK tcp-loopback)

    # A real-loopback leg with a fixed wall-clock pump deadline, so it runs alone and holds the loopback lock.
    plexus_add_catch_test(test_tls_retire_drain
        SOURCES test_tls_retire_drain.cpp $<TARGET_OBJECTS:plexus-crypto>
        LINKS plexus::core plexus::wire plexus::asio plexus::crypto
        PROPERTIES RUN_SERIAL TRUE RESOURCE_LOCK tcp-loopback)
endif()
