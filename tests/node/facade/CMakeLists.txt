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
    target_include_directories(codec_identity_fail_${_endpoint} PRIVATE ${PLEXUS_TEST_INCLUDES})
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
