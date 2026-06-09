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

# The message_info delivery scaffold: exercises the existing 2-arg deliver and pins
# the not-yet-built metadata-overload contract RED-now (flipped GREEN by the owning
# plan). Links the core, wire, and inproc layers; selected by the `forwarder.` ctest
# prefix and by `ctest -R message_info`.
add_executable(test_message_info test_message_info.cpp)

target_link_libraries(test_message_info PRIVATE
    plexus::plexus
    plexus::wire
    plexus::inproc
    Catch2::Catch2WithMain)

catch_discover_tests(test_message_info TEST_PREFIX "forwarder.")

# The subscribe-time type_id match scaffold: drives attach_for_fanout and decodes the
# subscribe_response status. The matching case is GREEN; the mismatch case is RED-now
# (the type_id compare is activated by the owning plan). Links the core, wire, and
# inproc layers; selected by the `forwarder.` ctest prefix and by `ctest -R type_id_match`.
add_executable(test_type_id_match test_type_id_match.cpp)

target_link_libraries(test_type_id_match PRIVATE
    plexus::plexus
    plexus::wire
    plexus::inproc
    Catch2::Catch2WithMain)

catch_discover_tests(test_type_id_match TEST_PREFIX "forwarder.")

# The subscriber-durability replay oracle: a none-durability subscriber receives
# ZERO retained frames on subscribe, latest receives EXACTLY one, the friendly
# default behaves as latest, and all replays as latest against the depth-1 slot
# pending the retained ring. The none/latest delivery effects are looped for
# reproducibility. Links the core, wire, and inproc layers; selected by the
# `forwarder.` ctest prefix and by `ctest -R durability`.
add_executable(test_durability_replay test_durability_replay.cpp)

target_link_libraries(test_durability_replay PRIVATE
    plexus::plexus
    plexus::wire
    plexus::inproc
    Catch2::Catch2WithMain)

catch_discover_tests(test_durability_replay TEST_PREFIX "forwarder.")

# The KEEP_LAST-N history-ring oracle: durability=all on a depth-N latched topic
# published N+k times replays EXACTLY the last N oldest->newest; fewer than N
# retained replays all of them; replay_depth caps to the most-recent frames; and a
# depth-1 ring stays byte-identical to last-writer-wins (all/latest/none). The
# delivery effects are looped for reproducibility. Links the core, wire, and inproc
# layers; selected by the `forwarder.` ctest prefix and by `ctest -R history_ring`.
add_executable(test_history_ring test_history_ring.cpp)

target_link_libraries(test_history_ring PRIVATE
    plexus::plexus
    plexus::wire
    plexus::inproc
    Catch2::Catch2WithMain)

catch_discover_tests(test_history_ring TEST_PREFIX "forwarder.")

# The consumer-paced PULL (fetch_latched) oracle: a none+pull subscriber receives
# ZERO retained frames on subscribe, then a direct fetch_latched(max) replays the
# most-recent min(max, count) frames oldest->newest to the requesting peer only; a
# fetch against a never-declared topic replays nothing. The delivery effects are
# looped for reproducibility. Links the core, wire, and inproc layers; selected by
# the `forwarder.` ctest prefix and by `ctest -R fetch_latched`.
add_executable(test_fetch_latched test_fetch_latched.cpp)

target_link_libraries(test_fetch_latched PRIVATE
    plexus::plexus
    plexus::wire
    plexus::inproc
    Catch2::Catch2WithMain)

catch_discover_tests(test_fetch_latched TEST_PREFIX "forwarder.")
