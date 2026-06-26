# Seam-concept smoke test: compile-time proof the byte_channel / Policy concepts
# are satisfiable (maintainability evidence) plus the wire_bytes
# owner-keeps-bytes-alive guarantee. Links the header-only plexus core and the
# wire layer; the `seam.` ctest prefix lets `ctest -R seam` select it by name.
add_executable(test_seam_concepts test_seam_concepts.cpp)
target_link_libraries(test_seam_concepts PRIVATE
    plexus::core
    plexus::wire
    Catch2::Catch2WithMain)
catch_discover_tests(test_seam_concepts TEST_PREFIX "seam.")

# Constrained-target seam proof: fires static_assert(Policy<freertos_policy>) at
# compile time and instantiates the generic core (peer_context) against the
# substrate to prove it binds unchanged. Links the constrained-target substrate
# alongside the core and wire layers; registered under the same `seam.` prefix.
add_executable(test_freertos_seam test_freertos_seam.cpp)
target_link_libraries(test_freertos_seam PRIVATE
    plexus::core
    plexus::wire
    plexus::freertos
    Catch2::Catch2WithMain)
catch_discover_tests(test_freertos_seam TEST_PREFIX "seam.")

# Host seam proof for the constrained-target UART byte_channel: fires
# static_assert(byte_channel<uart_channel>) at compile time and drives synthetic bytes
# through its reused crc_serial decorator path over a test-local host UART shim (no real
# device). Links the constrained-target substrate alongside the core and wire layers;
# the current source dir is on the include path so the shim header resolves. Registered
# under the same `seam.` prefix.
add_executable(test_uart_channel_seam test_uart_channel_seam.cpp)
target_include_directories(test_uart_channel_seam PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(test_uart_channel_seam PRIVATE
    plexus::core
    plexus::wire
    plexus::freertos
    Catch2::Catch2WithMain)
catch_discover_tests(test_uart_channel_seam TEST_PREFIX "seam.")

# Host proof for the constrained-target device facade: drives the borrow-only tick/run
# loop discipline (poll -> drain -> watchdog-safe park) over a fake pollable and a
# host freertos_executor. The host shim's no-op vTaskDelay makes the facade run
# off-target. Links the constrained-target substrate alongside the core and wire
# layers; registered under the same `seam.` prefix.
add_executable(test_device_runtime test_device_runtime.cpp)
target_link_libraries(test_device_runtime PRIVATE
    plexus::core
    plexus::wire
    plexus::freertos
    Catch2::Catch2WithMain)
catch_discover_tests(test_device_runtime TEST_PREFIX "seam.")

# Host compile-check for the constrained-target lwIP byte_channel: forces lwip_channel.h
# through the host compiler so its in-header byte_channel proof and the stream_inbound /
# send_queue reuse are validated by the PC build (the lwIP socket is ESP_PLATFORM-gated and
# never reached). Links the constrained-target substrate alongside the core and wire layers;
# registered under the same `seam.` prefix.
add_executable(test_lwip_channel_compile test_lwip_channel_compile.cpp)
target_link_libraries(test_lwip_channel_compile PRIVATE
    plexus::core
    plexus::wire
    plexus::freertos
    Catch2::Catch2WithMain)
catch_discover_tests(test_lwip_channel_compile TEST_PREFIX "seam.")

# Host slice for the constrained-target lwIP byte_channel + dial transport: fires
# static_assert(byte_channel<lwip_channel<host_tcp_socket>>) and
# static_assert(transport_backend<lwip_transport, lwip_policy>) at compile time, then round-trips a
# framed message over a REAL localhost-TCP loopback pair (a genuine POSIX stream_socket declared
# test-side, no hardware and no mock) exercising real partial reads / EWOULDBLOCK + the P1 poll-drive
# + the dial path. The current source dir is on the include path so the host socket header resolves.
# Registered under the same `seam.` prefix.
add_executable(test_lwip_channel_seam test_lwip_channel_seam.cpp)
target_include_directories(test_lwip_channel_seam PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(test_lwip_channel_seam PRIVATE
    plexus::core
    plexus::wire
    plexus::freertos
    Catch2::Catch2WithMain)
catch_discover_tests(test_lwip_channel_seam TEST_PREFIX "seam.")

# Host slice for the constrained-target lwIP P2 RX-task receive policy: drives the channel's
# acquire-slot -> recv_into_slot -> post_from_task step inline as the RX context (the spawned task
# is a no-op stub off-target), then ex.drain() as the executor, proving delivery is POSTED and
# exactly-once with reassembly over a REAL localhost-TCP loopback pair, and a peer close surfaced as
# on_error. The current source dir is on the include path so the host socket header resolves.
# Registered under the same `seam.` prefix.
add_executable(test_lwip_rx_task_seam test_lwip_rx_task_seam.cpp)
target_include_directories(test_lwip_rx_task_seam PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(test_lwip_rx_task_seam PRIVATE
    plexus::core
    plexus::wire
    plexus::freertos
    Catch2::Catch2WithMain)
catch_discover_tests(test_lwip_rx_task_seam TEST_PREFIX "seam.")

# Host slice for the constrained-target lwIP listen/accept transport: fires
# static_assert(transport_backend<lwip_transport<host_tcp_socket, host_tcp_listener>>) at compile
# time, then over a REAL localhost-TCP loopback (a genuine POSIX listener test-side, no hardware and
# no mock) proves accept -> on_accepted -> a poll-able channel, the compile-time accept cap bound
# (default 1 refuses the second client; cap 2 accepts both), and view invalidation on channel close
# (poll-all never touches the freed view; the asan tree is the witness). The current source dir is on
# the include path so the host listener/socket headers resolve. Registered under the `seam.` prefix.
add_executable(test_lwip_accept_seam test_lwip_accept_seam.cpp)
target_include_directories(test_lwip_accept_seam PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(test_lwip_accept_seam PRIVATE
    plexus::core
    plexus::wire
    plexus::freertos
    Catch2::Catch2WithMain)
catch_discover_tests(test_lwip_accept_seam TEST_PREFIX "seam.")

# Host slice for the constrained-target lwIP egress backpressure / error-class split: over a REAL
# localhost-TCP loopback pair (a genuine POSIX stream_socket, no hardware and no mock) it proves a full
# egress queue drives the congestion QoS (block backpressures via would_block, drop_newest sheds +
# counts) and NEVER fires on_error / re-dials, a transient short send re-arms with the unsent tail left
# queued (no success-of-zero-bytes) and flushes once cleared, and a hard send drop fires
# on_error(connection_reset) DISTINCT from on_protocol_close. The current source dir is on the include
# path so the host socket header resolves. Registered under the same `seam.` prefix.
add_executable(test_lwip_backpressure_seam test_lwip_backpressure_seam.cpp)
target_include_directories(test_lwip_backpressure_seam PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(test_lwip_backpressure_seam PRIVATE
    plexus::core
    plexus::wire
    plexus::freertos
    Catch2::Catch2WithMain)
catch_discover_tests(test_lwip_backpressure_seam TEST_PREFIX "seam.")

# Host-shim proof for the constrained-target executor's task-safe cross-context post:
# enqueues caller-owned posted_work PODs through the FreeRTOS host queue (widened to
# carry the whole item by value) and proves pump() invokes them in FIFO order, with
# m_posted serviced first and no per-post allocation (the asan tree is the witness).
# Links the constrained-target substrate alongside the core and wire layers; registered
# under the same `seam.` prefix.
add_executable(test_freertos_executor_post test_freertos_executor_post.cpp)
target_link_libraries(test_freertos_executor_post PRIVATE
    plexus::core
    plexus::wire
    plexus::freertos
    Catch2::Catch2WithMain)
catch_discover_tests(test_freertos_executor_post TEST_PREFIX "seam.")

# The permissive/strict reliability enforcement gate on the routing_engine subscribe
# demand path — a deterministic inproc seam test (no backend socket). Links the inproc
# Policy substrate for the engine harness. Registered under the `udp.` prefix so
# `ctest -R udp` selects it with the rest of the phase's reliability legs.
add_executable(test_reliability_enforcement
    test_reliability_enforcement.cpp
    test_reliability_enforcement_classifier.cpp)
target_link_libraries(test_reliability_enforcement PRIVATE
    plexus::core
    plexus::wire
    plexus::inproc
    Catch2::Catch2WithMain)
catch_discover_tests(test_reliability_enforcement TEST_PREFIX "udp.")
