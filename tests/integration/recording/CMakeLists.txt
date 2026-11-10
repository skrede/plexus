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

plexus_add_catch_test(test_capture_gate
    SOURCES test_capture_gate.cpp
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
