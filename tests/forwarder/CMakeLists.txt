# Forwarder behavioral oracle: the refcount-gated subscribe handshake,
# drain_for resurrection, byte-identical frame-once fan-out, detach_all,
# unsubscribe, resolve-by-topic_hash receive path, demand-driven no-send, and the
# warn-and-drop receive tail through an injected logger. A focused forwarder-level
# alloc-delta gate runs over a non-allocating sink Policy. Links the header-only
# core, the wire layer, and the inproc backend; the `forwarder.` ctest prefix lets
# `ctest -R forwarder` select it. Auto-discovered by the tests glob.
add_executable(test_message_forwarder test_message_forwarder.cpp)

# tests/ is on the include path so the test can pull in support/alloc_counter.h.
target_include_directories(test_message_forwarder PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/..)

target_link_libraries(test_message_forwarder PRIVATE
    plexus::plexus
    plexus::wire
    plexus::inproc
    Catch2::Catch2WithMain)

catch_discover_tests(test_message_forwarder TEST_PREFIX "forwarder.")

# The frame_router demux oracle: each msg_type routes to its registered consumer
# with the correct inner (header-off) span; short / bad-magic / unregistered
# frames are warn-and-dropped through an injected counting logger. The router is
# backend-agnostic (spans + move_only_function), so this needs only the core +
# wire layers. Selected by the same `forwarder.` ctest prefix.
add_executable(test_frame_router test_frame_router.cpp)

target_link_libraries(test_frame_router PRIVATE
    plexus::plexus
    plexus::wire
    Catch2::Catch2WithMain)

catch_discover_tests(test_frame_router TEST_PREFIX "forwarder.")

# The procedure (req/res) forwarder behavioral oracle: the refcount-gated procedure
# subscribe handshake, drain_for resurrection, the call/serve roundtrip recovering
# exact opaque param/return bytes matched by correlation_id, the no_handler and
# orphan-drop failure legs, detach_all failing every outstanding call with
# peer_disconnected, concurrent corr_ids resolving without cross-talk, and a
# steady-state provider-dispatch alloc-delta gate over a non-allocating sink Policy.
# Parity-strict: no timeout test (there is no per-call timer to exercise). Links the
# core, wire, and inproc layers; selected by the same `forwarder.` ctest prefix.
add_executable(test_procedure_forwarder test_procedure_forwarder.cpp)

target_include_directories(test_procedure_forwarder PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/..)

target_link_libraries(test_procedure_forwarder PRIVATE
    plexus::plexus
    plexus::wire
    plexus::inproc
    Catch2::Catch2WithMain)

catch_discover_tests(test_procedure_forwarder TEST_PREFIX "forwarder.")

# The latch behavioral oracle: the registry-seam qos assertion, replay of a value
# published with ZERO subscribers, the non-latched no-replay negative, empty
# retention, depth=1 last-value-only, replay-targets-only-the-new-peer, retention
# across subscriber churn, multi-publisher last-writer-wins, and a delta-zero
# LATCH-NOALLOC retention gate over a non-allocating sink Policy. Links the core,
# wire, and inproc layers; selected by the `forwarder.` ctest prefix and by `latch`
# Catch2 tags so `ctest -R latch` and `ctest -R forwarder` both pick it up.
add_executable(test_latch_forwarder test_latch_forwarder.cpp)

target_include_directories(test_latch_forwarder PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/..)

target_link_libraries(test_latch_forwarder PRIVATE
    plexus::plexus
    plexus::wire
    plexus::inproc
    Catch2::Catch2WithMain)

catch_discover_tests(test_latch_forwarder TEST_PREFIX "forwarder.")
