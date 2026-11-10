plexus_add_catch_test(test_borrowed_executor_bridge
    SOURCES test_borrowed_executor_bridge.cpp
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

if(PLEXUS_ENABLE_ASIO_BACKEND)
    plexus_add_catch_test(test_node_close_asio
        SOURCES test_node_close_asio.cpp
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
