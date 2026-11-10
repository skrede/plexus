if(NOT PLEXUS_ENABLE_ASIO_BACKEND)
    return()
endif()

set(PLEXUS_TEST_PREFIX "udp.")

plexus_add_catch_test(test_asio_policy
    PREFIX "asio."
    SOURCES test_asio_policy.cpp
    LINKS plexus::asio)

plexus_add_catch_test(test_executor_driver
    PREFIX "asio."
    SOURCES test_executor_driver.cpp
    LINKS plexus::asio)

plexus_add_catch_test(test_stream_read_buffer
    PREFIX "asio."
    SOURCES test_stream_read_buffer.cpp
    LINKS plexus::asio)

plexus_add_catch_test(test_udp_transport
    SOURCES test_udp_transport.cpp test_udp_transport_mux.cpp test_udp_publish_ceiling.cpp
    LINKS plexus::asio)
# The multiplexing transport header pulls in plexus::crypto for its secure member, so with the crypto backend on the
# executable also compiles the mux-route legs; PLEXUS_HAVE_TLS_MUX keeps the base UDP legs building with crypto off.
if(PLEXUS_ENABLE_CRYPTO_BACKEND)
    target_sources(test_udp_transport PRIVATE $<TARGET_OBJECTS:plexus-crypto>)
    target_link_libraries(test_udp_transport PRIVATE plexus::crypto)
    target_compile_definitions(test_udp_transport PRIVATE PLEXUS_HAVE_TLS_MUX)
endif()

plexus_add_catch_test(test_udp_handshake_arq
    SOURCES test_udp_handshake_arq.cpp test_udp_handshake_arq_component.cpp
    LINKS plexus::asio)

plexus_add_catch_test(test_udp_teardown
    SOURCES test_udp_teardown.cpp
    LINKS plexus::asio)

plexus_add_catch_test(test_udp_retire_drain
    SOURCES test_udp_retire_drain.cpp test_udp_retire_drain_dial.cpp
    LINKS plexus::asio)

plexus_add_catch_test(test_udp_server_retire_drain
    SOURCES test_udp_server_retire_drain.cpp
    LINKS plexus::asio)

plexus_add_catch_test(test_udp_reorder_buffer
    SOURCES test_udp_reorder_buffer.cpp
    LINKS plexus::asio)

plexus_add_catch_test(test_udp_rto_estimator
    SOURCES test_udp_rto_estimator.cpp
    LINKS plexus::asio)

plexus_add_catch_test(test_udp_reliable_arq
    SOURCES test_udp_reliable_arq.cpp
    LINKS plexus::asio)

plexus_add_catch_test(test_udp_arq_window_wrap
    SOURCES test_udp_arq_window_wrap.cpp
    LINKS plexus::asio)

plexus_add_catch_test(test_udp_fragment_admission
    SOURCES test_udp_fragment_admission.cpp
    LINKS plexus::asio)

plexus_add_catch_test(test_udp_arq_sweep
    SOURCES test_udp_arq_sweep.cpp
    LINKS plexus::asio)

plexus_add_catch_test(test_udp_selector
    SOURCES test_udp_selector.cpp test_udp_selector_dispatch.cpp
    LINKS plexus::asio)

plexus_add_catch_test(test_udp_congestion
    SOURCES test_udp_congestion.cpp
    LINKS plexus::asio)

plexus_add_catch_test(test_udp_reliable_datagram
    SOURCES test_udp_reliable_datagram.cpp test_udp_reliable_datagram_loss.cpp
    LINKS plexus::asio)

plexus_add_catch_test(test_udp_isn
    SOURCES test_udp_isn.cpp test_udp_isn_spoof.cpp test_udp_isn_frag.cpp
    LINKS plexus::asio)

plexus_add_catch_test(test_udp_fragmentation
    SOURCES test_udp_fragmentation.cpp test_udp_fragmentation_reliable.cpp
    LINKS plexus::asio)

plexus_add_catch_test(test_spin_run_loop
    PREFIX "asio."
    SOURCES test_spin_run_loop.cpp
    LINKS plexus::asio)

plexus_add_catch_test(test_udp_multicast_socket
    PREFIX "asio."
    SOURCES test_udp_multicast_socket.cpp
    LINKS plexus::asio)

plexus_add_catch_test(test_accept_retry
    PREFIX "asio."
    SOURCES test_accept_retry.cpp
    LINKS plexus::asio)
