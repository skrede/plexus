set(PLEXUS_TEST_PREFIX "integration.")

# The alloc gate snapshots the single-TU global operator-new counter across a steady-state loop and asserts a zero
# delta; concurrent test processes perturb that window, so it runs alone.
plexus_add_catch_test(test_hot_path_alloc
    SOURCES test_hot_path_alloc.cpp
    LINKS plexus::core plexus::wire plexus::inproc
    PROPERTIES RUN_SERIAL TRUE)

# An alloc gate over the single-TU operator-new counter; concurrent test processes perturb it, so it runs alone.
plexus_add_catch_test(test_typed_inproc_alloc
    SOURCES test_typed_inproc_alloc.cpp
    LINKS plexus::plexus plexus::inproc plexus::discovery
    PROPERTIES RUN_SERIAL TRUE)

# An alloc gate over the single-TU operator-new counter; concurrent test processes perturb it, so it runs alone.
plexus_add_catch_test(test_value_logger
    SOURCES test_value_logger.cpp
    LINKS plexus::plexus plexus::inproc plexus::discovery
    PROPERTIES RUN_SERIAL TRUE)

plexus_add_catch_test(test_recorder_defaults_sweep
    SOURCES test_recorder_defaults_sweep.cpp
    LINKS plexus::plexus plexus::inproc plexus::discovery)

plexus_add_catch_test(test_recording_qos_api
    SOURCES test_recording_qos_api.cpp test_recording_qos_api_override.cpp
    LINKS plexus::plexus plexus::inproc plexus::discovery)

plexus_add_catch_test(test_payload_fidelity_capture
    SOURCES test_payload_fidelity_capture.cpp
    LINKS plexus::plexus plexus::inproc plexus::discovery)

plexus_add_catch_test(test_self_describing_capture
    SOURCES test_self_describing_capture.cpp test_self_describing_capture_schema.cpp
    LINKS plexus::plexus plexus::inproc plexus::discovery)

plexus_add_catch_test(test_type_identity_carry
    SOURCES test_type_identity_carry.cpp
    LINKS plexus::plexus plexus::inproc plexus::discovery)

plexus_add_catch_test(test_inproc_stamp_demand
    SOURCES test_inproc_stamp_demand.cpp test_inproc_stamp_demand_latch.cpp
    LINKS plexus::plexus plexus::wire plexus::inproc plexus::discovery)

plexus_add_catch_test(test_reassembler_dos
    SOURCES test_reassembler_dos.cpp test_reassembler_dos_eviction.cpp
    LINKS plexus::core plexus::wire plexus::datagram plexus::testing)

plexus_add_catch_test(test_reqres_roundtrip_inproc
    SOURCES test_reqres_roundtrip_inproc.cpp test_reqres_roundtrip_inproc_noalloc.cpp
    LINKS plexus::core plexus::wire plexus::inproc)

plexus_add_catch_test(test_latch_replay_inproc
    SOURCES test_latch_replay_inproc.cpp test_latch_replay_inproc_noalloc.cpp
    LINKS plexus::core plexus::wire plexus::inproc)

plexus_add_catch_test(test_peer_session_inproc
    SOURCES test_peer_session_inproc.cpp test_peer_session_inproc_flow.cpp test_peer_session_inproc_staleness.cpp
        test_peer_session_inproc_lifecycle.cpp
    LINKS plexus::core plexus::wire plexus::inproc)

plexus_add_catch_test(test_peer_session_same_host_inproc
    SOURCES test_peer_session_same_host_inproc.cpp
    LINKS plexus::core plexus::shm plexus::wire plexus::inproc)

plexus_add_catch_test(test_transport_dial_inproc
    SOURCES test_transport_dial_inproc.cpp
    LINKS plexus::core plexus::wire plexus::inproc)

# An alloc gate over the single-TU operator-new counter; concurrent test processes perturb it, so it runs alone.
plexus_add_catch_test(test_reconnect_inproc
    SOURCES test_reconnect_inproc.cpp
    LINKS plexus::core plexus::wire plexus::inproc
    PROPERTIES RUN_SERIAL TRUE)

plexus_add_catch_test(test_reconnect_observer_inproc
    SOURCES test_reconnect_observer_inproc.cpp
    LINKS plexus::core plexus::wire plexus::inproc)

plexus_add_catch_test(test_routing_engine_inproc
    SOURCES test_routing_engine_inproc.cpp test_routing_engine_inproc_isolation.cpp
    LINKS plexus::core plexus::wire plexus::inproc)

plexus_add_catch_test(test_shared_receive_route
    SOURCES test_shared_receive_route.cpp
    LINKS plexus::core plexus::wire plexus::inproc)

plexus_add_catch_test(test_object_route_inproc
    SOURCES test_object_route_inproc.cpp
    LINKS plexus::core plexus::wire plexus::inproc)

plexus_add_catch_test(test_drop_seam_inproc
    SOURCES test_drop_seam_inproc.cpp
    LINKS plexus::core plexus::shm plexus::wire plexus::inproc)

plexus_add_catch_test(test_multipeer_reconnect_inproc
    SOURCES test_multipeer_reconnect_inproc.cpp
    LINKS plexus::core plexus::wire plexus::inproc)

plexus_add_catch_test(test_peer_observer_inproc
    SOURCES test_peer_observer_inproc.cpp
    LINKS plexus::core plexus::wire plexus::inproc)

plexus_add_catch_test(test_graph_change_reentrancy_inproc
    SOURCES test_graph_change_reentrancy_inproc.cpp
    LINKS plexus::core plexus::wire plexus::inproc)

# An alloc gate over the single-TU operator-new counter; concurrent test processes perturb it, so it runs alone.
plexus_add_catch_test(test_graph_change_alloc
    SOURCES test_graph_change_alloc.cpp
    LINKS plexus::core plexus::wire plexus::inproc
    PROPERTIES RUN_SERIAL TRUE)

plexus_add_catch_test(test_obs_tap_points
    SOURCES test_obs_tap_points.cpp test_obs_tap_points_rpc.cpp
    LINKS plexus::core plexus::wire plexus::inproc)

plexus_add_catch_test(test_spine_lifecycle_inproc
    SOURCES test_spine_lifecycle_inproc.cpp test_spine_lifecycle_inproc_edges.cpp
    LINKS plexus::plexus plexus::wire plexus::inproc plexus::discovery)

plexus_add_catch_test(test_unsubscribe_e2e_inproc
    SOURCES test_unsubscribe_e2e_inproc.cpp
    LINKS plexus::plexus plexus::wire plexus::inproc plexus::discovery)

plexus_add_catch_test(test_dispatch_hint_retention_inproc
    SOURCES test_dispatch_hint_retention_inproc.cpp
    LINKS plexus::plexus plexus::wire plexus::inproc plexus::discovery)

plexus_add_catch_test(test_unsolicited_data_inproc
    SOURCES test_unsolicited_data_inproc.cpp
    LINKS plexus::core plexus::wire plexus::inproc)

plexus_add_catch_test(test_capture_gate
    SOURCES test_capture_gate.cpp
    LINKS plexus::core plexus::wire plexus::inproc)

plexus_add_catch_test(test_read_buffer_partial_read
    SOURCES test_read_buffer_partial_read.cpp
    LINKS plexus::core plexus::wire)

# Its live AF_UNIX leg reaches raw POSIX (unistd.h), so the whole target is excluded from the Windows build.
if(NOT WIN32)
plexus_add_catch_test(test_locality_confinement
    SOURCES test_locality_confinement.cpp test_locality_confinement_wire.cpp test_locality_confinement_live.cpp
    LINKS plexus::core plexus::wire plexus::inproc)
endif()

plexus_add_catch_test(test_mux_select_hook
    SOURCES test_mux_select_hook.cpp
    LINKS plexus::core plexus::wire)

plexus_add_catch_test(test_transport_steering
    SOURCES test_transport_steering.cpp
    LINKS plexus::core plexus::wire)

plexus_add_catch_test(test_deadline_liveliness
    SOURCES test_deadline_liveliness.cpp test_deadline_liveliness_lease.cpp
    LINKS plexus::core plexus::wire plexus::inproc)

plexus_add_catch_test(test_liveliness_verdict_inproc
    SOURCES test_liveliness_verdict_inproc.cpp
    LINKS plexus::core plexus::wire plexus::inproc)

plexus_add_catch_test(test_liveliness_arbitration_inproc
    SOURCES test_liveliness_arbitration_inproc.cpp
    LINKS plexus::core plexus::wire plexus::inproc)

plexus_add_catch_test(test_rxo_compatibility
    SOURCES test_rxo_compatibility.cpp test_rxo_compatibility_soft.cpp
    LINKS plexus::core plexus::wire plexus::inproc)

plexus_add_catch_test(test_subscribe_qos_wire
    SOURCES test_subscribe_qos_wire.cpp
    LINKS plexus::core plexus::wire)

plexus_add_catch_test(test_security_attach_inproc
    SOURCES test_security_attach_inproc.cpp
    LINKS plexus::core plexus::wire plexus::inproc)

plexus_add_catch_test(test_downgrade_inproc
    SOURCES test_downgrade_inproc.cpp
    LINKS plexus::core plexus::wire plexus::inproc)

# An alloc gate over the single-TU operator-new counter; concurrent test processes perturb it, so it runs alone.
plexus_add_catch_test(test_byte_ring
    SOURCES test_byte_ring.cpp
    LINKS plexus::core plexus::wire
    PROPERTIES RUN_SERIAL TRUE)

plexus_add_catch_test(test_record_stream
    SOURCES test_record_stream.cpp test_record_stream_offline.cpp test_record_stream_schema.cpp
    LINKS plexus::core plexus::wire)

plexus_add_catch_test(test_record_projection
    SOURCES test_record_projection.cpp
    LINKS plexus::core plexus::wire plexus::inproc)

plexus_add_catch_test(test_recording_host_sink
    SOURCES test_recording_host_sink.cpp test_recording_host_sink_qos.cpp
    LINKS plexus::core plexus::wire plexus::recording-host)

plexus_add_catch_test(test_recording_channel
    SOURCES test_recording_channel.cpp test_recording_channel_sink.cpp
    LINKS plexus::core plexus::wire plexus::inproc)

plexus_add_catch_test(test_wire_loss_join
    SOURCES test_wire_loss_join.cpp
    LINKS plexus::core plexus::wire)

plexus_add_catch_test(test_wire_public_api
    SOURCES test_wire_public_api.cpp test_wire_public_api_absence.cpp
    LINKS plexus::plexus plexus::core plexus::wire plexus::inproc plexus::discovery)

# A default build must hold no recording_channel instantiation symbol. test_routing_engine_inproc composes a full
# inproc node without wire capture, so nm over its binary is the witness.
add_test(NAME integration.wire_inert_no_decorator_symbol
    COMMAND ${CMAKE_COMMAND}
        -DBINARY=$<TARGET_FILE:test_routing_engine_inproc>
        -P ${CMAKE_CURRENT_SOURCE_DIR}/wire_inert_symbol_guard.cmake)

# An alloc gate over the single-TU operator-new counter; concurrent test processes perturb it, so it runs alone.
plexus_add_catch_test(test_recorder_capture
    SOURCES test_recorder_capture.cpp test_recorder_capture_fdr.cpp test_recorder_capture_endtoend.cpp
        test_recorder_capture_teardown.cpp
    LINKS plexus::plexus plexus::core plexus::wire plexus::inproc plexus::discovery
    PROPERTIES RUN_SERIAL TRUE)

# An alloc gate over the single-TU operator-new counter; concurrent test processes perturb it, so it runs alone.
plexus_add_catch_test(test_recorder_attached_alloc
    SOURCES test_recorder_attached_alloc.cpp
    LINKS plexus::core plexus::wire
    PROPERTIES RUN_SERIAL TRUE)

plexus_add_catch_test(test_host_identity_inproc
    SOURCES test_host_identity_inproc.cpp test_host_identity_inproc_plaintext.cpp
    LINKS plexus::core plexus::wire plexus::inproc)

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

    plexus_add_catch_test(test_serial_atomic_admission
        SOURCES test_serial_atomic_admission.cpp
        LINKS plexus::core plexus::wire plexus::stream plexus::asio)

    plexus_add_catch_test(test_udp_reproducibility
        PREFIX "udp."
        SOURCES test_udp_reproducibility.cpp test_udp_reproducibility_reliable.cpp
        LINKS plexus::asio)

    plexus_add_catch_test(test_udp_send_burst
        PREFIX "udp."
        SOURCES test_udp_send_burst.cpp test_udp_send_burst_relay.cpp
        LINKS plexus::asio)

    plexus_add_catch_test(test_udp_large_payload
        PREFIX "udp."
        SOURCES test_udp_large_payload.cpp test_udp_large_payload_roundtrip.cpp test_udp_large_payload_probe.cpp
            test_udp_large_payload_fragment.cpp
        LINKS plexus::asio)

    # Its shared fixture reaches raw POSIX (unistd.h), so it is excluded from Windows. A real-loopback leg with a
    # fixed wall-clock pump deadline, so it runs alone and holds the loopback lock.
    if(NOT WIN32)
    plexus_add_catch_test(test_outofbox_message_ceiling
        SOURCES test_outofbox_message_ceiling.cpp test_outofbox_message_ceiling_udp.cpp
            test_outofbox_message_ceiling_tls.cpp
        LINKS plexus::core plexus::wire plexus::asio
        PROPERTIES RUN_SERIAL TRUE RESOURCE_LOCK tcp-loopback)
    endif()

    plexus_add_catch_test(test_transport_dial_unix
        SOURCES test_transport_dial_unix.cpp test_transport_dial_unix_flow.cpp
        LINKS plexus::core plexus::wire plexus::asio plexus::inproc)

    plexus_add_catch_test(test_transport_dial_cancel
        SOURCES test_transport_dial_cancel.cpp
        LINKS plexus::core plexus::wire plexus::asio)
    if(NOT WIN32)
        target_sources(test_transport_dial_cancel PRIVATE test_transport_dial_cancel_unix.cpp)
    endif()
    if(PLEXUS_ENABLE_CRYPTO_BACKEND)
        target_sources(test_transport_dial_cancel PRIVATE test_transport_dial_cancel_tls.cpp
            test_transport_dial_cancel_dtls.cpp
            $<TARGET_OBJECTS:plexus-crypto>)
        target_link_libraries(test_transport_dial_cancel PRIVATE plexus::crypto)
    endif()

    plexus_add_catch_test(test_dial_deadline_asio
        SOURCES test_dial_deadline_asio.cpp
        LINKS plexus::plexus plexus::asio plexus::discovery)
    if(PLEXUS_ENABLE_CRYPTO_BACKEND)
        target_sources(test_dial_deadline_asio PRIVATE test_dial_deadline_tls.cpp
            $<TARGET_OBJECTS:plexus-crypto>)
        target_link_libraries(test_dial_deadline_asio PRIVATE plexus::crypto)
    endif()

    plexus_add_catch_test(test_unclaimed_retire_stream
        SOURCES test_unclaimed_retire_stream.cpp
        LINKS plexus::core plexus::wire plexus::asio plexus::testing)
    if(NOT WIN32)
        target_link_libraries(test_unclaimed_retire_stream PRIVATE util)
    endif()
    plexus_add_catch_test(test_unclaimed_retire_wrap
        SOURCES test_unclaimed_retire_wrap.cpp
        LINKS plexus::core plexus::wire plexus::asio plexus::inproc plexus::testing)
    if(PLEXUS_ENABLE_CRYPTO_BACKEND)
        foreach(unclaimed_target test_unclaimed_retire_stream test_unclaimed_retire_wrap)
            target_sources(${unclaimed_target} PRIVATE $<TARGET_OBJECTS:plexus-crypto>)
            target_link_libraries(${unclaimed_target} PRIVATE plexus::crypto)
            target_compile_definitions(${unclaimed_target} PRIVATE PLEXUS_HAVE_CRYPTO)
        endforeach()
    endif()
    if(PLEXUS_ENABLE_SHM_BACKEND AND TARGET plexus::native)
        target_include_directories(test_unclaimed_retire_wrap PRIVATE ${PROJECT_SOURCE_DIR}/tests/shm)
        target_link_libraries(test_unclaimed_retire_wrap PRIVATE plexus::shm plexus::native)
        target_compile_definitions(test_unclaimed_retire_wrap PRIVATE PLEXUS_HAVE_SHM)
    endif()

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

    # openpty(3) lives in libutil and is POSIX-only, hence the util link and the Windows exclusion.
    if(NOT WIN32)
    plexus_add_catch_test(test_serial_pty_roundtrip
        SOURCES test_serial_pty_roundtrip.cpp
        LINKS plexus::core plexus::wire plexus::asio plexus::inproc util)
    endif()

    # openpty(3) lives in libutil and is POSIX-only, hence the util link and the Windows exclusion.
    if(NOT WIN32)
    plexus_add_catch_test(test_serial_resync
        SOURCES test_serial_resync.cpp
        LINKS plexus::core plexus::wire plexus::asio plexus::inproc util)
    endif()

    # openpty(3) lives in libutil and is POSIX-only, hence the util link and the Windows exclusion.
    if(NOT WIN32)
    plexus_add_catch_test(test_serial_retire_drain
        SOURCES test_serial_retire_drain.cpp
        LINKS plexus::core plexus::wire plexus::asio plexus::inproc util)
    endif()

    # openpty(3) lives in libutil and is POSIX-only, hence the util link and the Windows exclusion.
    if(NOT WIN32)
    plexus_add_catch_test(test_serial_egress_kat
        SOURCES test_serial_egress_kat.cpp
        LINKS plexus::core plexus::wire plexus::asio plexus::inproc util)
    endif()

    # openpty(3) lives in libutil and is POSIX-only, hence the util link and the Windows exclusion.
    if(NOT WIN32)
    plexus_add_catch_test(test_serial_header_drop
        SOURCES test_serial_header_drop.cpp
        LINKS plexus::core plexus::wire plexus::asio plexus::inproc util)
    endif()

    # openpty(3) lives in libutil and is POSIX-only, hence the util link and the Windows exclusion.
    if(NOT WIN32)
    plexus_add_catch_test(test_serial_oversize
        SOURCES test_serial_oversize.cpp
        LINKS plexus::core plexus::wire plexus::asio plexus::inproc util)
    endif()

    # openpty(3) lives in libutil and is POSIX-only, hence the util link and the Windows exclusion.
    if(NOT WIN32)
    plexus_add_catch_test(test_serial_cap_refusal
        SOURCES test_serial_cap_refusal.cpp
        LINKS plexus::core plexus::wire plexus::asio plexus::inproc util)
    endif()

    # openpty(3) lives in libutil and is POSIX-only, hence the util link and the Windows exclusion.
    if(NOT WIN32)
    plexus_add_catch_test(test_serial_send_refusal
        SOURCES test_serial_send_refusal.cpp
        LINKS plexus::core plexus::wire plexus::asio plexus::inproc util)
    endif()

    # openpty(3) lives in libutil and is POSIX-only, hence the util link and the Windows exclusion.
    if(NOT WIN32)
    plexus_add_catch_test(test_serial_qos_ceiling
        SOURCES test_serial_qos_ceiling.cpp
        LINKS plexus::core plexus::wire plexus::asio plexus::inproc util)
    endif()

    # openpty(3) lives in libutil and is POSIX-only, hence the util link and the Windows exclusion.
    if(NOT WIN32)
    plexus_add_catch_test(test_serial_options
        SOURCES test_serial_options.cpp
        LINKS plexus::core plexus::wire plexus::asio plexus::inproc util)
    endif()

    # Paced openpty measurement rigs (the serial payload-cap sweep and the hold after a dropped header byte), run by
    # hand and POSIX-only: they report numbers and assert nothing, so no ctest is registered.
    if(NOT WIN32)
    add_executable(test_serial_cap_sweep test_serial_cap_sweep.cpp)
    target_link_libraries(test_serial_cap_sweep PRIVATE
        plexus::core
        plexus::wire
        plexus::asio
        plexus::inproc
        Catch2::Catch2WithMain
        util)
    target_include_directories(test_serial_cap_sweep PRIVATE
        ${PROJECT_SOURCE_DIR}/tests)
    endif()

    # openpty(3) lives in libutil and is POSIX-only, hence the util link and the Windows exclusion.
    if(NOT WIN32)
    plexus_add_catch_test(test_relay_serial_e2e
        SOURCES test_relay_serial_e2e.cpp
        LINKS plexus::plexus plexus::core plexus::wire plexus::asio plexus::inproc util)

    plexus_add_catch_test(test_forward_serial_e2e
        SOURCES test_forward_serial_e2e.cpp
        LINKS plexus::plexus plexus::core plexus::wire plexus::asio plexus::inproc util)

    plexus_add_catch_test(test_forward_serial_kill_origin
        SOURCES test_forward_serial_kill_origin.cpp
        LINKS plexus::plexus plexus::core plexus::wire plexus::asio plexus::inproc util)

    plexus_add_catch_test(test_forward_reqres_robustness
        SOURCES test_forward_reqres_robustness.cpp
        LINKS plexus::plexus plexus::core plexus::wire plexus::asio plexus::inproc util)
    endif()

    # openpty(3) lives in libutil and is POSIX-only, hence the util link and the Windows exclusion.
    if(NOT WIN32)
    plexus_add_catch_test(test_forward_envelope_gate
        SOURCES test_forward_envelope_gate.cpp
        LINKS plexus::core plexus::wire plexus::asio plexus::inproc util)
    endif()

    plexus_add_catch_test(test_egress_saturation_bounded
        SOURCES test_egress_saturation_bounded.cpp
        LINKS plexus::core plexus::wire plexus::asio)

    # The AF_UNIX filesystem permissions model (chmod/stat via unistd.h) is POSIX-only; Windows AF_UNIX carries no
    # mode bits.
    if(NOT WIN32)
    plexus_add_catch_test(test_unix_permissions
        SOURCES test_unix_permissions.cpp
        LINKS plexus::core plexus::wire plexus::asio)

    # It interposes umask through dlsym(RTLD_NEXT), hence the dl link.
    plexus_add_catch_test(test_unix_bind_umask
        SOURCES test_unix_bind_umask.cpp
        LINKS plexus::core plexus::wire plexus::asio ${CMAKE_DL_LIBS})

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

    # The multiplexing transport composes a TLS member, so the mux oracles here need the crypto backend; that member
    # stays inert, as no tls channel is dialed or accepted.
    if(PLEXUS_ENABLE_CRYPTO_BACKEND)
        plexus_add_catch_test(test_drop_coverage
            SOURCES test_drop_coverage.cpp $<TARGET_OBJECTS:plexus-crypto>
            LINKS plexus::core plexus::wire plexus::asio plexus::crypto plexus::inproc)

        plexus_add_catch_test(test_mux_select_unix
            SOURCES test_mux_select_unix.cpp $<TARGET_OBJECTS:plexus-crypto>
            LINKS plexus::core plexus::wire plexus::asio plexus::crypto)

        plexus_add_catch_test(test_mux_select_asio
            SOURCES test_mux_select_asio.cpp $<TARGET_OBJECTS:plexus-crypto>
            LINKS plexus::core plexus::wire plexus::asio plexus::crypto)

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

        plexus_add_catch_test(test_aead_fragment
            SOURCES test_aead_fragment.cpp $<TARGET_OBJECTS:plexus-crypto>
            LINKS plexus::core plexus::wire plexus::crypto plexus::testing)

        if(NOT WIN32)
            target_sources(test_outofbox_message_ceiling PRIVATE $<TARGET_OBJECTS:plexus-crypto>)
            target_link_libraries(test_outofbox_message_ceiling PRIVATE plexus::crypto)
            target_compile_definitions(test_outofbox_message_ceiling PRIVATE PLEXUS_HAVE_TLS_OUTOFBOX)
        endif()
    endif()

    plexus_add_catch_test(test_mux_minimal_composition
        SOURCES test_mux_minimal_composition.cpp
        LINKS plexus::core plexus::wire plexus::asio)

    plexus_add_catch_test(test_mux_node_composition
        SOURCES test_mux_node_composition.cpp
        LINKS plexus::plexus plexus::asio)

    plexus_add_catch_test(test_serial_mux_member
        SOURCES test_serial_mux_member.cpp
        LINKS plexus::plexus plexus::asio)

    target_link_libraries(test_hot_path_alloc PRIVATE plexus::asio)
    target_compile_definitions(test_hot_path_alloc PRIVATE PLEXUS_HAVE_ASIO_MUX)

    if(PLEXUS_ENABLE_CRYPTO_BACKEND)
        target_sources(test_hot_path_alloc PRIVATE $<TARGET_OBJECTS:plexus-crypto>)
        target_link_libraries(test_hot_path_alloc PRIVATE plexus::crypto)
        target_compile_definitions(test_hot_path_alloc PRIVATE PLEXUS_HAVE_CRYPTO_DATAGRAM)
    endif()

    if(NOT WIN32)
        target_link_libraries(test_locality_confinement PRIVATE plexus::asio)
        target_compile_definitions(test_locality_confinement PRIVATE PLEXUS_HAVE_ASIO_MUX)
    endif()

    # It forks its two nodes and maps POSIX shared memory, so it is excluded from Windows.
    if(PLEXUS_ENABLE_SHM_BACKEND AND TARGET plexus::asio::shm AND NOT WIN32)
        plexus_add_catch_test(test_node_xproc_shm
            SOURCES test_node_xproc_shm.cpp
            LINKS plexus::plexus plexus::core plexus::wire plexus::asio plexus::asio::shm plexus::native
            INCLUDES ${PROJECT_SOURCE_DIR}/tests/shm)
    endif()

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

    plexus_add_catch_test(test_subscribe_retry_udp
        SOURCES test_subscribe_retry_udp.cpp
        LINKS plexus::plexus plexus::asio plexus::testing plexus::discovery)
endif()

if(PLEXUS_BUILD_MCAP_TRANSCODE)
    plexus_add_catch_test(test_mcap_transcode
        SOURCES test_mcap_transcode.cpp
        LINKS plexus::plexus plexus::core plexus::wire plexus::inproc plexus::discovery plexus::hints-robotics
            plexus::recording-host mcap_transcode_lib plexus_mcap_dep
        DEFINES PLEXUS_SOURCE_DIR="${PROJECT_SOURCE_DIR}")
endif()

if(PLEXUS_BUILD_PCAP_TRANSCODE)
    # It writes the captured streams to these build-dir paths as the fixtures the dissector QA test reads.
    plexus_add_catch_test(test_pcap_transcode
        SOURCES test_pcap_transcode.cpp
        LINKS plexus::plexus plexus::core plexus::wire plexus::inproc plexus::discovery pcap_transcode_lib
        DEFINES PLEXUS_QA_CAPTURE_PATH="${CMAKE_CURRENT_BINARY_DIR}/plexus_qa_capture.plxr"
            PLEXUS_QA_CIPHER_CAPTURE_PATH="${CMAKE_CURRENT_BINARY_DIR}/plexus_qa_capture_ciphertext.plxr")

    # Runs the projector's pcapng through the Lua dissector under tshark, skipping where tshark is absent, and reads
    # the fixtures the transcode test writes, so it depends on that test.
    add_test(NAME integration.pcap_dissector_qa
             COMMAND ${CMAKE_COMMAND} -E env sh
                     ${CMAKE_CURRENT_SOURCE_DIR}/test_pcap_dissector_qa.sh
                     $<TARGET_FILE:pcap_transcode>
                     ${PROJECT_SOURCE_DIR}
                     ${CMAKE_CURRENT_BINARY_DIR}/plexus_qa_capture.plxr
                     ${CMAKE_CURRENT_BINARY_DIR}/plexus_qa_capture_ciphertext.plxr)
    set_tests_properties(integration.pcap_dissector_qa PROPERTIES
        DEPENDS "integration.pcap transcode round-trips a captured session through a parsed pcapng")
endif()

if(PLEXUS_ENABLE_ASIO_BACKEND)
    # RUN_SERIAL and the loopback lock keep its asio transport off the other real-TCP legs.
    plexus_add_catch_test(test_native_shared_executor
        SOURCES test_native_shared_executor.cpp
        LINKS plexus::core plexus::wire plexus::asio
        PROPERTIES RUN_SERIAL TRUE RESOURCE_LOCK tcp-loopback)
endif()

# An alloc gate over the single-TU operator-new counter; concurrent test processes perturb it, so it runs alone.
plexus_add_catch_test(test_route_note_noalloc
    SOURCES test_route_note_noalloc.cpp
    LINKS plexus::core
    PROPERTIES RUN_SERIAL TRUE)

# An alloc gate over the single-TU operator-new counter; concurrent test processes perturb it, so it runs alone.
plexus_add_catch_test(test_discovery_noalloc
    SOURCES test_discovery_noalloc.cpp
    LINKS plexus::core plexus::discovery plexus::wire plexus::stream
    PROPERTIES RUN_SERIAL TRUE)

plexus_add_catch_test(test_peer_report_oracle
    SOURCES test_peer_report_oracle.cpp
    LINKS plexus::core plexus::wire)

plexus_add_catch_test(test_forwarded_frame_oracle
    SOURCES test_forwarded_frame_oracle.cpp
    LINKS plexus::core plexus::wire)

plexus_add_catch_test(test_forwarded_dedup_delivery
    SOURCES test_forwarded_dedup_delivery.cpp
    LINKS plexus::core plexus::wire plexus::inproc)

plexus_add_catch_test(test_heartbeat_decline_gate
    SOURCES test_heartbeat_decline_gate.cpp
    LINKS plexus::core plexus::wire plexus::inproc)

plexus_add_catch_test(test_session_gate
    SOURCES test_session_gate_matrix.cpp test_session_gate_object_lane.cpp test_session_gate_latch.cpp
    LINKS plexus::core plexus::wire plexus::inproc)

plexus_add_catch_test(test_session_identity
    SOURCES test_session_identity.cpp
    LINKS plexus::core plexus::wire plexus::inproc)

plexus_add_catch_test(test_session_teardown_in_callback
    SOURCES test_session_teardown_in_callback.cpp
    LINKS plexus::core plexus::wire plexus::inproc)

plexus_add_catch_test(test_subscribe_refusal_set
    SOURCES test_subscribe_refusal_set.cpp
    LINKS plexus::core plexus::wire plexus::inproc)

# An alloc gate over the single-TU operator-new counter; concurrent test processes perturb it, so it runs alone.
plexus_add_catch_test(test_session_malformed_logging
    SOURCES test_session_malformed_logging.cpp test_session_malformed_clock.cpp test_session_malformed_sites.cpp
    LINKS plexus::core plexus::wire plexus::inproc
    PROPERTIES RUN_SERIAL TRUE)

plexus_add_catch_test(test_dialed_identity
    SOURCES test_dialed_identity_inproc.cpp test_dialed_identity_redial_inproc.cpp
        test_dialed_identity_binding_inproc.cpp
    LINKS plexus::core plexus::wire plexus::inproc)

plexus_add_catch_test(test_slot_reclaim_inproc
    SOURCES test_slot_reclaim_inproc.cpp
    LINKS plexus::core plexus::wire plexus::inproc)

plexus_add_catch_test(test_session_soak_inproc
    SOURCES test_session_soak_inproc.cpp
    LINKS plexus::core plexus::wire plexus::inproc)
# std::getenv is portable, but MSVC's CRT deprecates it (C4996).
if(MSVC)
    target_compile_definitions(test_session_soak_inproc PRIVATE _CRT_SECURE_NO_WARNINGS)
endif()

plexus_add_catch_test(test_subscribe_retry_inproc
    SOURCES test_subscribe_retry_inproc.cpp
    LINKS plexus::core plexus::wire plexus::inproc)

plexus_add_catch_test(test_forward_splice_pubsub
    SOURCES test_forward_splice_pubsub.cpp
    LINKS plexus::core plexus::wire plexus::inproc)

plexus_add_catch_test(test_forward_splice_hardening
    SOURCES test_forward_splice_hardening.cpp
    LINKS plexus::core plexus::wire plexus::inproc)

plexus_add_catch_test(test_forward_splice_no_hol
    SOURCES test_forward_splice_no_hol.cpp
    LINKS plexus::core plexus::wire plexus::inproc)

plexus_add_catch_test(test_forward_reqres
    SOURCES test_forward_reqres.cpp
    LINKS plexus::core plexus::wire plexus::inproc)

plexus_add_catch_test(test_forward_pubsub
    SOURCES test_forward_pubsub.cpp
    LINKS plexus::core plexus::wire plexus::inproc)

plexus_add_catch_test(test_forward_dedup_pruning_inproc
    SOURCES test_forward_dedup_pruning_inproc.cpp
    LINKS plexus::core plexus::wire plexus::inproc)

plexus_add_catch_test(test_forward_reply_reresolve
    SOURCES test_forward_reply_reresolve.cpp
    LINKS plexus::core plexus::wire plexus::inproc)

plexus_add_catch_test(test_forwarded_rpc_tables
    SOURCES test_forwarded_rpc_tables.cpp
    LINKS plexus::core plexus::wire plexus::inproc)

plexus_add_catch_test(test_forward_demand_propagation
    SOURCES test_forward_demand_propagation.cpp
    LINKS plexus::core plexus::wire plexus::inproc)

plexus_add_catch_test(test_crossed_dial_inproc
    SOURCES test_crossed_dial_common.cpp test_crossed_dial_inproc.cpp test_crossed_dial_quiet_inproc.cpp
        test_crossed_dial_order_inproc.cpp test_crossed_dial_remote_inproc.cpp
    LINKS plexus::core plexus::wire plexus::inproc)

plexus_add_catch_test(test_proven_identity_inproc
    SOURCES test_proven_identity_common.cpp test_proven_identity_inproc.cpp
    LINKS plexus::core plexus::wire plexus::inproc)
target_sources(test_proven_identity_inproc PRIVATE
    test_crossed_dial_common.cpp
    test_proven_identity_crossed_inproc.cpp)

plexus_add_catch_test(test_same_kind_duplicate_inproc
    SOURCES test_crossed_dial_common.cpp test_same_kind_duplicate_inproc.cpp
    LINKS plexus::core plexus::wire plexus::inproc)

plexus_add_catch_test(test_identity_edges_inproc
    SOURCES test_crossed_dial_common.cpp test_identity_edges_inproc.cpp
    LINKS plexus::core plexus::wire plexus::inproc)

plexus_add_catch_test(test_session_stale_inproc
    SOURCES test_session_stale_inproc.cpp test_session_stale_edge_inproc.cpp test_session_stale_timeout_inproc.cpp
    LINKS plexus::core plexus::wire plexus::inproc)

plexus_add_catch_test(test_reach_guard_inproc
    SOURCES test_reach_guard_inproc.cpp test_reach_guard_backoff_inproc.cpp
    LINKS plexus::core plexus::wire plexus::inproc)

plexus_add_catch_test(test_idle_dialed_slot_inproc
    SOURCES test_idle_dialed_slot_inproc.cpp test_idle_dialed_slot_accept_inproc.cpp
    LINKS plexus::core plexus::wire plexus::inproc)
