set(PLEXUS_TEST_PREFIX "seam.")

plexus_add_catch_test(test_seam_concepts
    SOURCES test_seam_concepts.cpp
    LINKS plexus::core plexus::wire)

plexus_add_catch_test(test_freertos_seam
    SOURCES test_freertos_seam.cpp
    LINKS plexus::core plexus::wire plexus::freertos)

plexus_add_catch_test(test_uart_channel_seam
    SOURCES test_uart_channel_seam.cpp
    LINKS plexus::core plexus::wire plexus::freertos)

plexus_add_catch_test(test_uart_channel_cap
    SOURCES test_uart_channel_cap.cpp
    LINKS plexus::core plexus::wire plexus::freertos)

plexus_add_catch_test(test_device_runtime
    SOURCES test_device_runtime.cpp
    LINKS plexus::core plexus::wire plexus::freertos)

plexus_add_catch_test(test_lwip_channel_compile
    SOURCES test_lwip_channel_compile.cpp
    LINKS plexus::core plexus::wire plexus::freertos)

# The host loopback pair is a genuine POSIX stream socket (host_tcp_socket.h pulls netinet/in.h), so these slices
# are excluded from the Windows build.
if(NOT WIN32)
    plexus_add_catch_test(test_lwip_channel_seam
        SOURCES test_lwip_channel_seam.cpp
        LINKS plexus::core plexus::wire plexus::freertos)

    plexus_add_catch_test(test_lwip_rx_task_seam
        SOURCES test_lwip_rx_task_seam.cpp
        LINKS plexus::core plexus::wire plexus::freertos)

    plexus_add_catch_test(test_lwip_rx_task_backpressure
        SOURCES test_lwip_rx_task_backpressure.cpp
        LINKS plexus::core plexus::wire plexus::freertos)

    plexus_add_catch_test(test_lwip_rx_task_lifecycle
        SOURCES test_lwip_rx_task_lifecycle.cpp
        LINKS plexus::core plexus::wire plexus::freertos)

    plexus_add_catch_test(test_lwip_rx_task_teardown
        SOURCES test_lwip_rx_task_teardown.cpp
        LINKS plexus::core plexus::wire plexus::freertos)

    plexus_add_catch_test(test_lwip_rx_task_transport_seam
        SOURCES test_lwip_rx_task_transport_seam.cpp
        LINKS plexus::core plexus::wire plexus::freertos)

    plexus_add_catch_test(test_lwip_rx_task_adopt_seam
        SOURCES test_lwip_rx_task_adopt_seam.cpp
        LINKS plexus::core plexus::wire plexus::freertos)

    plexus_add_catch_test(test_lwip_drop_errno_seam
        SOURCES test_lwip_drop_errno_seam.cpp
        LINKS plexus::core plexus::wire plexus::freertos)

    plexus_add_catch_test(test_lwip_dial_seam
        SOURCES test_lwip_dial_seam.cpp
        LINKS plexus::core plexus::wire plexus::freertos)

    # The node's dial timeout reaches the lwIP dialer through the same probe the node runs, so the deadline cases link
    # the API layer.
    plexus_add_catch_test(test_lwip_dial_deadline_seam
        SOURCES test_lwip_dial_deadline_seam.cpp
        LINKS plexus::core plexus::wire plexus::freertos plexus::plexus plexus::discovery)

    plexus_add_catch_test(test_lwip_accept_seam
        SOURCES test_lwip_accept_seam.cpp
        LINKS plexus::core plexus::wire plexus::freertos)

    plexus_add_catch_test(test_lwip_dial_capacity_seam
        SOURCES test_lwip_dial_capacity_seam.cpp
        LINKS plexus::core plexus::wire plexus::freertos)

    plexus_add_catch_test(test_lwip_backpressure_seam
        SOURCES test_lwip_backpressure_seam.cpp
        LINKS plexus::core plexus::wire plexus::freertos)

    plexus_add_catch_test(test_lwip_transport_congestion_seam
        SOURCES test_lwip_transport_congestion_seam.cpp
        LINKS plexus::core plexus::wire plexus::freertos)

    plexus_add_catch_test(test_freertos_retire_quiescence
        SOURCES test_freertos_retire_quiescence.cpp test_freertos_retire_quiescence_uart.cpp
        LINKS plexus::core plexus::wire plexus::freertos)
endif()

plexus_add_catch_test(test_freertos_executor_post
    SOURCES test_freertos_executor_post.cpp
    LINKS plexus::core plexus::wire plexus::freertos)

# The program turns the expected abort into exit status 1, because WILL_FAIL does not invert a signal.
add_executable(test_freertos_live_task_abort test_freertos_live_task_abort.cpp)
target_link_libraries(test_freertos_live_task_abort PRIVATE
    plexus::core
    plexus::wire
    plexus::freertos)
add_test(NAME seam.freertos_live_task_abort.executor COMMAND test_freertos_live_task_abort executor)
set_tests_properties(seam.freertos_live_task_abort.executor PROPERTIES WILL_FAIL TRUE)
add_test(NAME seam.freertos_live_task_abort.channel COMMAND test_freertos_live_task_abort channel)
set_tests_properties(seam.freertos_live_task_abort.channel PROPERTIES WILL_FAIL TRUE)

# RUN_SERIAL keeps the allocation count clean.
plexus_add_catch_test(test_freertos_ingress
    SOURCES test_freertos_ingress.cpp
    LINKS plexus::core plexus::freertos
    PROPERTIES RUN_SERIAL TRUE)

# The udp. prefix lets `ctest -R udp` select it with the other reliability tests.
plexus_add_catch_test(test_reliability_enforcement
    PREFIX "udp."
    SOURCES test_reliability_enforcement.cpp test_reliability_enforcement_classifier.cpp
    LINKS plexus::core plexus::wire plexus::inproc)

plexus_add_catch_test(test_lwip_multicast_endpoint
    SOURCES test_lwip_multicast_endpoint.cpp
    LINKS plexus::core plexus::freertos)

if(PLEXUS_ENABLE_ASIO_BACKEND)
    plexus_add_catch_test(test_datagram_socket_concept
        SOURCES test_datagram_socket_concept.cpp
        LINKS plexus::asio)
endif()

# A CMake script test: it asserts the discovery module's link layering from the source tree, so it needs no build.
add_test(NAME seam.discovery_layering
    COMMAND ${CMAKE_COMMAND} -DPLEXUS_SOURCE_DIR=${PROJECT_SOURCE_DIR}
            -P ${CMAKE_CURRENT_SOURCE_DIR}/discovery_layering_check.cmake)
