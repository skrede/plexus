# libFuzzer harnesses over the untrusted-input surfaces, each linking the libraries its decoder's headers reach.
if(NOT CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    message(WARNING
        "plexus fuzz: PLEXUS_BUILD_FUZZ_TESTS is ON but the compiler is "
        "${CMAKE_CXX_COMPILER_ID}; libFuzzer needs Clang. Skipping harnesses.")
    return()
endif()

function(plexus_add_fuzz_binary target source)
    add_executable(${target} ${source})
    target_link_libraries(${target} PRIVATE plexus::wire)
    target_compile_options(${target} PRIVATE
        -fsanitize=fuzzer,address,undefined
        -fno-sanitize-recover=all
        -fno-omit-frame-pointer
        -g
        -O1)
    target_link_options(${target} PRIVATE -fsanitize=fuzzer,address,undefined -fno-sanitize-recover=all)
endfunction()

# libFuzzer writes every coverage-increasing input into the directory it is given, so runs only ever see this
# build-tree corpus, seeded from the encoders, and never the source tree.
set(plexus_fuzz_corpus_dir ${CMAKE_CURRENT_BINARY_DIR}/corpus)

add_subdirectory(seeds)
add_custom_target(fuzz_seed_corpus COMMAND plexus_fuzz_seeds ${plexus_fuzz_corpus_dir} VERBATIM)
add_test(NAME fuzz_seed_corpus COMMAND plexus_fuzz_seeds ${plexus_fuzz_corpus_dir})
set_tests_properties(fuzz_seed_corpus PROPERTIES FIXTURES_SETUP fuzz_corpus)

function(plexus_add_fuzz_replay name)
    add_test(NAME ${name}_replay
             COMMAND ${CMAKE_COMMAND}
                     -DTARGET=$<TARGET_FILE:${name}>
                     -DCORPUS=${plexus_fuzz_corpus_dir}/${name}
                     -DMINIMUM=1
                     -P ${CMAKE_CURRENT_SOURCE_DIR}/replay.cmake)
    set_tests_properties(${name}_replay PROPERTIES
        FIXTURES_REQUIRED fuzz_corpus
        ENVIRONMENT "UBSAN_OPTIONS=print_stacktrace=1:halt_on_error=1")
endfunction()

function(plexus_add_fuzz_target name)
    plexus_add_fuzz_binary(${name} ${name}.cpp)
    plexus_add_fuzz_replay(${name})
endfunction()

plexus_add_fuzz_target(fuzz_frame_reassembler)
plexus_add_fuzz_target(fuzz_data_frame)
plexus_add_fuzz_target(fuzz_varint)
plexus_add_fuzz_target(fuzz_rpc_frame)
plexus_add_fuzz_target(fuzz_handshake_frame)
plexus_add_fuzz_target(fuzz_subscribe_frame)
plexus_add_fuzz_target(fuzz_fetch_latched_frame)
plexus_add_fuzz_target(fuzz_heartbeat_frame)
plexus_add_fuzz_target(fuzz_topic_declaration_frame)
plexus_add_fuzz_target(fuzz_companion_frame)

plexus_add_fuzz_target(fuzz_announcement_frame)
target_link_libraries(fuzz_announcement_frame PRIVATE plexus::core)

plexus_add_fuzz_target(fuzz_peer_report_frame)
target_link_libraries(fuzz_peer_report_frame PRIVATE plexus::core)

plexus_add_fuzz_target(fuzz_forwarded_frame)
target_link_libraries(fuzz_forwarded_frame PRIVATE plexus::core)

plexus_add_fuzz_target(fuzz_reassembler)
target_link_libraries(fuzz_reassembler PRIVATE plexus::core plexus::datagram plexus::inproc)

plexus_add_fuzz_target(fuzz_key_pattern)
target_link_libraries(fuzz_key_pattern PRIVATE plexus::core)

plexus_add_fuzz_target(fuzz_ring_attach)
target_link_libraries(fuzz_ring_attach PRIVATE plexus::shm plexus::core)
target_include_directories(fuzz_ring_attach PRIVATE ${PROJECT_SOURCE_DIR}/tests/shm)

plexus_add_fuzz_target(fuzz_udp_envelope)
target_link_libraries(fuzz_udp_envelope PRIVATE plexus::datagram plexus::core plexus::inproc)

plexus_add_fuzz_target(fuzz_stream_inbound)
target_link_libraries(fuzz_stream_inbound PRIVATE plexus::core plexus::stream plexus::inproc)

plexus_add_fuzz_target(fuzz_session_sequence)
target_link_libraries(fuzz_session_sequence PRIVATE plexus::core plexus::inproc)

plexus_add_fuzz_target(fuzz_crc_serial)
target_link_libraries(fuzz_crc_serial PRIVATE plexus::stream)

# The same harness with one oracle leg inverted, kept out of the replay tests; it must fail.
plexus_add_fuzz_binary(fuzz_crc_serial_planted fuzz_crc_serial.cpp)
target_link_libraries(fuzz_crc_serial_planted PRIVATE plexus::stream)
target_compile_definitions(fuzz_crc_serial_planted PRIVATE PLEXUS_FUZZ_PLANT_VIOLATION)
