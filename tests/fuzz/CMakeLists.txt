# Wire attack-surface libFuzzer harnesses. Each harness drives a header-only
# decoder over fuzzer-controlled spans; the blast radius is just plexus::wire.
# libFuzzer requires a Clang toolchain — skip with a warning otherwise.
if(NOT CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    message(WARNING
        "plexus fuzz: PLEXUS_BUILD_FUZZ_TESTS is ON but the compiler is "
        "${CMAKE_CXX_COMPILER_ID}; libFuzzer needs Clang. Skipping harnesses.")
    return()
endif()

function(plexus_add_fuzz_target name)
    add_executable(${name} ${name}.cpp)
    target_link_libraries(${name} PRIVATE plexus::wire)
    target_compile_options(${name} PRIVATE
        -fsanitize=fuzzer,address
        -fno-sanitize-recover=all
        -fno-omit-frame-pointer
        -g
        -O1)
    target_link_options(${name} PRIVATE -fsanitize=fuzzer,address)
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

# The announcement decoder's header reaches plexus/node_id.h in plexus::core, so this
# harness links core on top of the plexus::wire blast radius the others share.
plexus_add_fuzz_target(fuzz_announcement_frame)
target_link_libraries(fuzz_announcement_frame PRIVATE plexus::core)

# The peer_report decoder's header reaches plexus/node_id.h and the match key-pattern bounds in
# plexus::core, so this harness links core on top of the plexus::wire blast radius.
plexus_add_fuzz_target(fuzz_peer_report_frame)
target_link_libraries(fuzz_peer_report_frame PRIVATE plexus::core)

# The forwarded_frame decoder's header reaches plexus/node_id.h in plexus::core, so this harness
# links core on top of the plexus::wire blast radius.
plexus_add_fuzz_target(fuzz_forwarded_frame)
target_link_libraries(fuzz_forwarded_frame PRIVATE plexus::core)

# The datagram reassembler harness drives the header-only reassembler in the
# plexus::datagram family instantiated against the plexus::inproc executor/timer,
# beyond the plexus::wire blast radius the other harnesses share -- so it links those
# as well.
plexus_add_fuzz_target(fuzz_reassembler)
target_link_libraries(fuzz_reassembler PRIVATE plexus::core plexus::datagram plexus::inproc)

# The key-pattern matcher lives in plexus::core, so this harness links core on top of
# the plexus::wire blast radius the others share.
plexus_add_fuzz_target(fuzz_key_pattern)
target_link_libraries(fuzz_key_pattern PRIVATE plexus::core)
