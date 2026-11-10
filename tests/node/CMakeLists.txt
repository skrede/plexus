set(PLEXUS_TEST_PREFIX "node.")

# Linking only core and inproc keeps the include path to those two trees, so a core header reaching for a facade or
# backend header fails to compile. wire comes through core: linking it here would mask a broken core-to-wire link.
add_executable(core_standalone core_standalone.cpp)
target_link_libraries(core_standalone PRIVATE plexus::core plexus::inproc)
add_test(NAME node.core_standalone COMMAND core_standalone)

# With no shm library linked the shm include tree is off the path, so a facade header reaching for an shm header, or
# naming an shm:: symbol on this plain pack, fails to compile. The proof is the build; it runs only to be tracked.
add_executable(core_only_node_compile test_core_only_node_compile.cpp)
target_link_libraries(core_only_node_compile PRIVATE plexus::plexus plexus::core plexus::discovery)
add_test(NAME node.core_only_node_compile COMMAND core_only_node_compile)

# A real install and an out-of-tree find_package consumer; the components mirror this build's enabled backends so the
# check proves exactly the export sets that shipped.
set(_consume_components "")
if(PLEXUS_ENABLE_ASIO_BACKEND)
    list(APPEND _consume_components asio)
endif()
if(PLEXUS_ENABLE_CRYPTO_BACKEND)
    list(APPEND _consume_components crypto)
endif()
if(PLEXUS_ENABLE_SHM_BACKEND)
    list(APPEND _consume_components native)
endif()
# The parent's toolchain is forwarded because cmake's platform default differs: on Windows it is a multi-config Visual
# Studio generator, which does not match this Ninja + cl build and would not inherit the vcvars environment.
add_test(NAME node.install_consume
    COMMAND ${CMAKE_COMMAND}
        -DPLEXUS_BUILD_DIR=${CMAKE_BINARY_DIR}
        -DPLEXUS_CONSUME_DIR=${CMAKE_CURRENT_SOURCE_DIR}/install_consume
        "-DPLEXUS_CONSUME_COMPONENTS=${_consume_components}"
        "-DPLEXUS_CONSUME_CXX_FLAGS=${CMAKE_CXX_FLAGS}"
        "-DPLEXUS_CONSUME_GENERATOR=${CMAKE_GENERATOR}"
        "-DPLEXUS_CONSUME_MAKE_PROGRAM=${CMAKE_MAKE_PROGRAM}"
        "-DPLEXUS_CONSUME_C_COMPILER=${CMAKE_C_COMPILER}"
        "-DPLEXUS_CONSUME_CXX_COMPILER=${CMAKE_CXX_COMPILER}"
        -P ${CMAKE_CURRENT_SOURCE_DIR}/install_consume.cmake)

plexus_add_catch_test(test_vocabulary
    SOURCES test_vocabulary.cpp
    LINKS plexus::plexus)

plexus_add_catch_test(test_identity_fnv1a_kat
    SOURCES test_identity_fnv1a_kat.cpp
    LINKS plexus::plexus)

plexus_add_catch_test(test_node_construction
    SOURCES test_node_construction.cpp
    LINKS plexus::plexus plexus::inproc plexus::discovery)

plexus_add_catch_test(test_node_options_defaults
    SOURCES test_node_options_defaults.cpp
    LINKS plexus::plexus plexus::inproc plexus::discovery)

plexus_add_catch_test(test_node_discovery
    SOURCES test_node_discovery.cpp test_node_discovery_awareness.cpp
    LINKS plexus::plexus plexus::inproc plexus::discovery)

plexus_add_catch_test(test_node_discovery_aging
    SOURCES test_node_discovery_aging.cpp test_node_discovery_aging_wired.cpp
    LINKS plexus::core plexus::wire plexus::inproc plexus::testing)

plexus_add_catch_test(test_known_peer_pruning
    SOURCES test_known_peer_pruning.cpp
    LINKS plexus::plexus plexus::inproc plexus::discovery)

plexus_add_catch_test(test_handles
    SOURCES test_handles.cpp test_handles_retire.cpp
    LINKS plexus::plexus plexus::inproc plexus::discovery)

plexus_add_catch_test(test_handle_move_assign
    SOURCES test_handle_move_assign.cpp
    LINKS plexus::plexus plexus::inproc plexus::discovery)

plexus_add_catch_test(test_handle_moved_from
    SOURCES test_handle_moved_from.cpp test_handle_moved_from_call.cpp test_handle_moved_from_recorder.cpp
    LINKS plexus::plexus plexus::inproc plexus::discovery)

plexus_add_catch_test(test_bytes_pubsub
    SOURCES test_bytes_pubsub.cpp test_bytes_pubsub_fan.cpp
    LINKS plexus::plexus plexus::inproc plexus::discovery)

plexus_add_catch_test(test_call_family
    SOURCES test_call_family.cpp test_call_family_spelling.cpp
    LINKS plexus::plexus plexus::inproc plexus::discovery
    DEFINES PLEXUS_HAS_FAMILY_SPELLING)

plexus_add_catch_test(test_caller_errc
    SOURCES test_caller_errc.cpp
    LINKS plexus::plexus plexus::inproc plexus::discovery)

plexus_add_catch_test(test_typed_reqres
    SOURCES test_typed_reqres.cpp test_typed_reqres_interop.cpp
    LINKS plexus::plexus plexus::inproc plexus::discovery
    DEFINES PLEXUS_HAS_FAMILY_SPELLING)

plexus_add_catch_test(test_serve_deduction
    SOURCES test_serve_deduction.cpp
    LINKS plexus::plexus plexus::inproc plexus::discovery)

plexus_add_catch_test(test_typed_codec_concepts
    SOURCES test_typed_codec_concepts.cpp
    LINKS plexus::plexus)

plexus_add_catch_test(test_with_identity
    SOURCES test_with_identity.cpp
    LINKS plexus::plexus plexus::inproc plexus::discovery)

# Each endpoint over a codec without a type identity must fail to compile: the ctest builds the excluded target and
# passes on the endpoint's own diagnostic, not on the build's exit code, and fails on a second, shared one.
foreach(_endpoint publisher subscriber value_logger)
    add_executable(codec_identity_fail_${_endpoint} EXCLUDE_FROM_ALL codec_identity_fail_${_endpoint}.cpp)
    target_link_libraries(codec_identity_fail_${_endpoint} PRIVATE
        plexus::plexus
        plexus::inproc
        plexus::discovery)
    add_test(NAME node.codec_identity_fail_${_endpoint}
        COMMAND ${CMAKE_COMMAND} --build ${CMAKE_BINARY_DIR} --target codec_identity_fail_${_endpoint} --config $<CONFIG>)
    # RUN_SERIAL keeps the nested build in this tree from racing another.
    set_tests_properties(node.codec_identity_fail_${_endpoint} PROPERTIES
        PASS_REGULAR_EXPRESSION "give the ${_endpoint}'s codec"
        FAIL_REGULAR_EXPRESSION "There is no typeid fallback"
        RUN_SERIAL TRUE)
endforeach()

plexus_add_catch_test(test_late_join
    SOURCES test_late_join.cpp
    LINKS plexus::plexus plexus::inproc plexus::discovery)

plexus_add_catch_test(test_borrowed_executor_bridge
    SOURCES test_borrowed_executor_bridge.cpp
    LINKS plexus::plexus plexus::inproc plexus::discovery)

plexus_add_catch_test(test_loan_pool
    SOURCES test_loan_pool.cpp test_loan_pool_move.cpp
    LINKS plexus::plexus plexus::inproc)

plexus_add_catch_test(test_loan_pool_defer_free
    SOURCES test_loan_pool_defer_free.cpp
    LINKS plexus::plexus plexus::inproc)

plexus_add_catch_test(test_typed_pubsub
    SOURCES test_typed_pubsub.cpp test_typed_pubsub_posture.cpp
    LINKS plexus::plexus plexus::inproc plexus::discovery)

plexus_add_catch_test(test_typed_interop_matrix
    SOURCES test_typed_interop_matrix.cpp test_typed_interop_matrix_refusal.cpp
    LINKS plexus::plexus plexus::inproc plexus::discovery)

plexus_add_catch_test(test_typed_fastpath
    SOURCES test_typed_fastpath.cpp
    LINKS plexus::plexus plexus::inproc plexus::discovery)

plexus_add_catch_test(test_self_loopback
    SOURCES test_self_loopback.cpp
    LINKS plexus::plexus plexus::inproc plexus::discovery)

plexus_add_catch_test(test_self_loopback_mixed
    SOURCES test_self_loopback_mixed.cpp
    LINKS plexus::plexus plexus::inproc plexus::discovery)

plexus_add_catch_test(test_self_loopback_latch
    SOURCES test_self_loopback_latch.cpp
    LINKS plexus::plexus plexus::inproc plexus::discovery)

plexus_add_catch_test(test_self_loopback_recorder
    SOURCES test_self_loopback_recorder.cpp
    LINKS plexus::plexus plexus::inproc plexus::discovery)

plexus_add_catch_test(test_recorder_move
    SOURCES test_recorder_move.cpp
    LINKS plexus::plexus plexus::inproc plexus::discovery)

plexus_add_catch_test(test_self_loopback_lifetime
    SOURCES test_self_loopback_lifetime.cpp
    LINKS plexus::plexus plexus::inproc plexus::discovery)

plexus_add_catch_test(test_node_close
    SOURCES test_node_close.cpp
    LINKS plexus::plexus plexus::inproc plexus::discovery)

plexus_add_catch_test(test_node_hang_up
    SOURCES test_node_hang_up.cpp
    LINKS plexus::plexus plexus::inproc plexus::discovery)

plexus_add_catch_test(test_node_close_quiescence
    SOURCES test_node_close_quiescence.cpp
    LINKS plexus::plexus plexus::inproc plexus::discovery)

plexus_add_catch_test(test_node_close_relay
    SOURCES test_node_close_relay.cpp
    LINKS plexus::plexus plexus::inproc plexus::discovery)

plexus_add_catch_test(test_dispatch_reentrancy
    SOURCES test_dispatch_reentrancy.cpp
    LINKS plexus::plexus plexus::inproc plexus::discovery)

# The shm self-carrier's self-ring wake rides the Linux io_uring/futex reactor bridge, so it is excluded from Windows.
if(PLEXUS_ENABLE_SHM_BACKEND AND NOT WIN32)
    plexus_add_catch_test(test_shm_self_carrier
        SOURCES test_shm_self_carrier.cpp
        LINKS plexus::shm plexus::plexus plexus::core plexus::wire plexus::asio plexus::asio::shm plexus::native)

    plexus_add_catch_test(test_node_same_host_identity
        SOURCES test_node_same_host_identity.cpp
        LINKS plexus::shm plexus::plexus plexus::core plexus::asio plexus::asio::shm plexus::native)

    plexus_add_catch_test(test_node_same_host_fallbacks
        SOURCES test_node_same_host_fallbacks.cpp
        LINKS plexus::shm plexus::plexus plexus::core plexus::asio plexus::asio::shm plexus::native)
endif()

if(PLEXUS_ENABLE_ASIO_BACKEND)
    plexus_add_catch_test(test_node_native_discovery
        SOURCES test_node_native_discovery.cpp
        LINKS plexus::plexus plexus::core plexus::wire plexus::stream plexus::asio plexus::discovery)

    plexus_add_catch_test(test_node_close_asio
        SOURCES test_node_close_asio.cpp
        LINKS plexus::plexus plexus::asio plexus::inproc plexus::testing plexus::discovery)

    plexus_add_catch_test(test_liveliness_tcp
        SOURCES test_liveliness_tcp.cpp
        LINKS plexus::plexus plexus::asio plexus::inproc plexus::testing plexus::discovery)

    plexus_add_catch_test(test_liveliness_udp
        SOURCES test_liveliness_udp.cpp
        LINKS plexus::plexus plexus::asio plexus::inproc plexus::testing plexus::discovery)

    plexus_add_catch_test(test_post_from_thread
        SOURCES test_post_from_thread.cpp
        LINKS plexus::plexus plexus::asio plexus::inproc plexus::discovery)

    # The timeout bounds a blocking helper that would otherwise wait forever on the refused post.
    # The asio and inproc cases sit in their own translation unit so the host shim's global names never meet asio's.
    plexus_add_catch_test(test_post_refused
        SOURCES test_post_refused.cpp test_post_refused_hosts.cpp
        LINKS plexus::plexus plexus::asio plexus::inproc plexus::freertos plexus::discovery
        PROPERTIES TIMEOUT 60)

    # Each misuse runs in a child process that defines the check itself, so the abort is exercised on every platform
    # and build type; the test reads the child's exit status and the message it wrote.
    # The FreeRTOS roles sit in their own translation unit so the host shim's global names never meet asio's.
    add_executable(affinity_trip_child
        affinity_trip_child.cpp affinity_handover_roles.cpp affinity_freertos_roles.cpp affinity_endpoint_roles.cpp)
    target_include_directories(affinity_trip_child PRIVATE
        ${PROJECT_SOURCE_DIR}/tests ${PROJECT_SOURCE_DIR}/tests/integration)
    target_link_libraries(affinity_trip_child PRIVATE
        plexus::plexus
        plexus::asio
        plexus::inproc
        plexus::freertos
        plexus::discovery)
    target_compile_definitions(affinity_trip_child PRIVATE PLEXUS_AFFINITY_CHECKS)

    plexus_add_catch_test(test_affinity_check
        SOURCES test_affinity_check.cpp
        LINKS plexus::testing
        DEFINES PLEXUS_AFFINITY_TRIP_CHILD="$<TARGET_FILE:affinity_trip_child>")
    add_dependencies(test_affinity_check affinity_trip_child)
endif()
