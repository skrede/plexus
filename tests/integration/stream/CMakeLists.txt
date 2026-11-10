plexus_add_catch_test(test_read_buffer_partial_read
    SOURCES test_read_buffer_partial_read.cpp
    LINKS plexus::core plexus::wire)

if(PLEXUS_ENABLE_ASIO_BACKEND)
    # Real-loopback legs flaked under contended parallel ctest, which squeezed each test's fixed wall-clock pump
    # deadline. RUN_SERIAL keeps them off each other; the loopback RESOURCE_LOCK keeps them off any other socket leg.
    plexus_add_catch_test(test_asio_roundtrip
        SOURCES test_asio_roundtrip.cpp
        LINKS plexus::core plexus::wire plexus::asio
        PROPERTIES RUN_SERIAL TRUE RESOURCE_LOCK tcp-loopback)

    plexus_add_catch_test(test_asio_frame_contract
        SOURCES test_asio_frame_contract.cpp
        LINKS plexus::core plexus::wire plexus::asio)

    plexus_add_catch_test(test_reqres_roundtrip_asio
        SOURCES test_reqres_roundtrip_asio.cpp test_reqres_roundtrip_asio_concurrent.cpp
        LINKS plexus::core plexus::wire plexus::asio)

    plexus_add_catch_test(test_reqres_timeout_asio
        SOURCES test_reqres_timeout_asio.cpp
        LINKS plexus::core plexus::wire plexus::asio)

    plexus_add_catch_test(test_latch_replay_asio
        SOURCES test_latch_replay_asio.cpp
        LINKS plexus::core plexus::wire plexus::asio)

    plexus_add_catch_test(test_peer_session_asio
        SOURCES test_peer_session_asio.cpp test_peer_session_asio_staleness.cpp
        LINKS plexus::core plexus::wire plexus::asio plexus::inproc)

    plexus_add_catch_test(test_transport_dial_asio
        SOURCES test_transport_dial_asio.cpp
        LINKS plexus::core plexus::wire plexus::asio plexus::inproc)

    # A real-loopback leg with a fixed wall-clock pump deadline, so it runs alone and holds the loopback lock.
    plexus_add_catch_test(test_routing_engine_asio
        SOURCES test_routing_engine_asio.cpp test_routing_engine_asio_identity.cpp
        LINKS plexus::core plexus::wire plexus::asio plexus::inproc
        PROPERTIES RUN_SERIAL TRUE RESOURCE_LOCK tcp-loopback)

    # A real-loopback leg with a fixed wall-clock pump deadline, so it runs alone and holds the loopback lock.
    plexus_add_catch_test(test_peer_observer_asio
        SOURCES test_peer_observer_asio.cpp test_peer_observer_asio_readiness.cpp test_peer_observer_asio_reentrancy.cpp
        LINKS plexus::core plexus::wire plexus::asio plexus::inproc
        PROPERTIES RUN_SERIAL TRUE RESOURCE_LOCK tcp-loopback)

    plexus_add_catch_test(test_reconnect_asio
        SOURCES test_reconnect_asio.cpp test_reconnect_asio_coldstart.cpp
        LINKS plexus::core plexus::wire plexus::asio plexus::inproc)

    # A real-loopback leg with a fixed wall-clock pump deadline, so it runs alone and holds the loopback lock.
    plexus_add_catch_test(test_multipeer_reconnect_asio
        SOURCES test_multipeer_reconnect_asio.cpp test_multipeer_reconnect_asio_surrender.cpp
        LINKS plexus::core plexus::wire plexus::asio plexus::inproc
        PROPERTIES RUN_SERIAL TRUE RESOURCE_LOCK tcp-loopback)

    plexus_add_catch_test(test_stream_channel_asio
        SOURCES test_stream_channel_asio.cpp test_stream_channel_asio_backpressure.cpp
            test_stream_channel_asio_largeframe.cpp
        LINKS plexus::core plexus::wire plexus::asio plexus::inproc)

    plexus_add_catch_test(test_stream_channel_asio_teardown
        SOURCES test_stream_channel_asio_teardown.cpp
        LINKS plexus::core plexus::wire plexus::asio plexus::inproc)

    # The allocation watch replaces operator new, so the allocation cases share this executable only.
    plexus_add_catch_test(test_stream_retire_drain
        SOURCES test_stream_retire_drain.cpp test_stream_retire_drain_alloc.cpp
        LINKS plexus::core plexus::wire plexus::asio)

    plexus_add_catch_test(test_transport_dial_unix
        SOURCES test_transport_dial_unix.cpp test_transport_dial_unix_flow.cpp
        LINKS plexus::core plexus::wire plexus::asio plexus::inproc)

    # Unlike the other AF_UNIX legs it reaches no raw POSIX, so it also builds and runs on Windows.
    plexus_add_catch_test(test_af_unix_capability
        SOURCES test_af_unix_capability.cpp
        LINKS plexus::core plexus::wire plexus::asio)

    plexus_add_catch_test(test_reconnect_unix
        SOURCES test_reconnect_unix.cpp test_reconnect_unix_coldstart.cpp
        LINKS plexus::core plexus::wire plexus::asio plexus::inproc)

    # Its raw hostile-client socket writes reach unistd.h directly, so it is excluded from Windows.
    if(NOT WIN32)
        plexus_add_catch_test(test_stream_channel_unix
            SOURCES test_stream_channel_unix.cpp test_stream_channel_unix_largeframe.cpp
            LINKS plexus::core plexus::wire plexus::asio plexus::inproc)
    endif()

    plexus_add_catch_test(test_egress_saturation_bounded
        SOURCES test_egress_saturation_bounded.cpp
        LINKS plexus::core plexus::wire plexus::asio)

    if(NOT WIN32)
        # The descriptor exhaustion lowers RLIMIT_NOFILE, which is POSIX-only.
        plexus_add_catch_test(test_listener_accept_retry
            SOURCES test_listener_accept_retry.cpp
            LINKS plexus::core plexus::wire plexus::asio)
        # UBSan's vptr check opens a pipe on a type-cache miss, which aborts inside the exhausted table.
        if(CMAKE_CXX_FLAGS MATCHES "-fsanitize=[^ ]*undefined")
            target_compile_options(test_listener_accept_retry PRIVATE -fno-sanitize=vptr)
        endif()
    endif()

    # AF_UNIX with SO_NOSIGPIPE is an Apple platform-ism, so the target is macOS-only. A real-loopback leg with a
    # fixed wall-clock pump deadline, so it runs alone and holds the loopback lock.
    if(APPLE)
        plexus_add_catch_test(macos_test_sigpipe_send_to_closed_peer
            SOURCES macos_test_sigpipe_send_to_closed_peer.cpp
            LINKS plexus::core plexus::wire plexus::asio
            PROPERTIES RUN_SERIAL TRUE RESOURCE_LOCK tcp-loopback)
    endif()
endif()
