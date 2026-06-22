# Integration proofs for the vertical slice: the live TCP loopback round-trip and
# the shared-executor test (asio + mdnspp on one io_context) are gated; the
# hot-path allocation audit is a deterministic inproc test and needs no backend.
# Auto-discovered by the tests glob; the `integration.` ctest prefix lets
# `ctest -R integration` select the whole area. The root tests/CMakeLists.txt is
# never edited.

# The hot-path allocation audit is deterministic (inproc Policy) — always built.
add_executable(test_hot_path_alloc test_hot_path_alloc.cpp)
target_link_libraries(test_hot_path_alloc PRIVATE
    plexus::core
    plexus::wire
    plexus::inproc
    Catch2::Catch2WithMain)
# tests/ is on the include path so the test can pull in support/alloc_counter.h.
target_include_directories(test_hot_path_alloc PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/..)
# RUN_SERIAL: the alloc gate snapshots the single-TU global operator-new counter across a
# steady-state loop and asserts a zero delta. Run concurrently with other test processes,
# CPU contention can perturb the measurement window; serializing keeps the count immune.
catch_discover_tests(test_hot_path_alloc TEST_PREFIX "integration."
    PROPERTIES RUN_SERIAL TRUE)

# The typed in-process fast-path alloc gate: a warmed steady-state borrow->publish->drain
# loop over the node facade allocates zero (the zero-serialization lane), with a forced-
# fallback variant documenting the serialize path's allocation for contrast. Links the
# facade (plexus::plexus) for the typed endpoints; tests/ is on the include path for
# support/alloc_counter.h.
add_executable(test_typed_inproc_alloc test_typed_inproc_alloc.cpp)
target_link_libraries(test_typed_inproc_alloc PRIVATE
    plexus::plexus
    plexus::inproc
    Catch2::Catch2WithMain)
target_include_directories(test_typed_inproc_alloc PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/..)
# RUN_SERIAL: alloc gate over the single-TU global operator-new counter (see test_hot_path_alloc).
catch_discover_tests(test_typed_inproc_alloc TEST_PREFIX "integration."
    PROPERTIES RUN_SERIAL TRUE)

# The typed value_logger: the codec decodes one topic at the handle, a projection (or the
# operator<< floor) formats CSV/JSON-Lines/text into a reused buffer. The alloc-gate
# section needs the replaceable new/delete, so tests/ is on the include path for
# support/alloc_counter.h (the test_typed_inproc_alloc precedent).
add_executable(test_value_logger test_value_logger.cpp)
target_link_libraries(test_value_logger PRIVATE
    plexus::plexus
    plexus::inproc
    Catch2::Catch2WithMain)
target_include_directories(test_value_logger PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/..)
# RUN_SERIAL: alloc gate over the single-TU global operator-new counter (see test_hot_path_alloc).
catch_discover_tests(test_value_logger TEST_PREFIX "integration."
    PROPERTIES RUN_SERIAL TRUE)

# The recorder numeric-defaults chosen-cell assertion: the recorded-sweep precedent
# (tests/crypto/test_rekey_threshold_sweep) pinned for the recorder ring budget. Drives a
# saturating burst at the SHIPPED ring_bytes default and asserts full recall at every
# measured payload (a regression below the knee fails), plus the keep-all + exact-1/N
# decimation guarantees on the typed object lane. Links the facade for the typed endpoints;
# tests/ is on the include path for in_memory_byte_sink.h.
add_executable(test_recorder_defaults_sweep test_recorder_defaults_sweep.cpp)
target_link_libraries(test_recorder_defaults_sweep PRIVATE
    plexus::plexus
    plexus::inproc
    Catch2::Catch2WithMain)
target_include_directories(test_recorder_defaults_sweep PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/..)
catch_discover_tests(test_recorder_defaults_sweep TEST_PREFIX "integration.")

# The public recording-QoS declaration gate: drive the capture decision through the node
# facade alone (node_options.capture + typed_publisher_options.capture), a counting codec
# witnessing the per-publish encode decision. Links the facade for the typed endpoints;
# tests/ is on the include path (unused here, kept for the area convention).
add_executable(test_recording_qos_api
    test_recording_qos_api.cpp
    test_recording_qos_api_override.cpp)
target_link_libraries(test_recording_qos_api PRIVATE
    plexus::plexus
    plexus::inproc
    Catch2::Catch2WithMain)
target_include_directories(test_recording_qos_api PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/..)
catch_discover_tests(test_recording_qos_api TEST_PREFIX "integration.")

# The payload-fidelity capture oracle: a payload-fidelity topic's recorded sample carries
# the BARE codec bytes (byte-identical to encode(), no frame prefix), proving the producer
# surfaces the bare span at the tap. Links the facade for the typed endpoints; tests/ is on
# the include path for in_memory_byte_sink.h. Not gated on mcap — it links plexus + inproc only.
add_executable(test_payload_fidelity_capture test_payload_fidelity_capture.cpp)
target_link_libraries(test_payload_fidelity_capture PRIVATE
    plexus::plexus
    plexus::inproc
    Catch2::Catch2WithMain)
target_include_directories(test_payload_fidelity_capture PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/..)
catch_discover_tests(test_payload_fidelity_capture TEST_PREFIX "integration.")

# The self-describing capture oracle: a declared type_schema + crypto position + producer
# type_id round-trip through the version-2 preamble, an empty-schema recorder still recovers,
# and a schema blob larger than the writer's default scratch round-trips. Links the facade
# for the typed endpoints; tests/ is on the include path for in_memory_byte_sink.h. Not gated
# on mcap — it links plexus + inproc only.
add_executable(test_self_describing_capture
    test_self_describing_capture.cpp
    test_self_describing_capture_schema.cpp)
target_link_libraries(test_self_describing_capture PRIVATE
    plexus::plexus
    plexus::inproc
    Catch2::Catch2WithMain)
target_include_directories(test_self_describing_capture PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/..)
catch_discover_tests(test_self_describing_capture TEST_PREFIX "integration.")

# The inproc stamp-demand behavioral gate: a 3-arg subscriber sees populated message_info
# timestamps; a 3-arg subscriber that opts out through subscriber_qos sees a documented 0;
# the per-topic latch OR-reduces over local subscriber demand. Links the facade for the
# typed endpoints; tests/ is on the include path for support headers.
add_executable(test_inproc_stamp_demand
    test_inproc_stamp_demand.cpp
    test_inproc_stamp_demand_latch.cpp)
target_link_libraries(test_inproc_stamp_demand PRIVATE
    plexus::plexus
    plexus::wire
    plexus::inproc
    Catch2::Catch2WithMain)
target_include_directories(test_inproc_stamp_demand PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/..)
catch_discover_tests(test_inproc_stamp_demand TEST_PREFIX "integration.")

# The reassembly-DoS bound: each reassembler bound (total-memory cap, per-message
# ceiling, malformed rejection, timeout eviction) is forced + looped and the in-flight
# state asserted bounded, plus the demux peer-cap bounding the spoofed-source channel
# count. Deterministic (inproc test clock via plexus::testing, no backend); tests/ is on
# the include path for support headers.
add_executable(test_reassembler_dos
    test_reassembler_dos.cpp
    test_reassembler_dos_eviction.cpp)
target_link_libraries(test_reassembler_dos PRIVATE
    plexus::core
    plexus::wire
    plexus::testing
    Catch2::Catch2WithMain)
target_include_directories(test_reassembler_dos PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/..)
catch_discover_tests(test_reassembler_dos TEST_PREFIX "integration.")

# The deterministic inproc req/res roundtrip + concurrent-outstanding + zero-alloc
# rpc-dispatch gate — always built (inproc Policy, no backend needed).
add_executable(test_reqres_roundtrip_inproc
    test_reqres_roundtrip_inproc.cpp
    test_reqres_roundtrip_inproc_noalloc.cpp)
target_link_libraries(test_reqres_roundtrip_inproc PRIVATE
    plexus::core
    plexus::wire
    plexus::inproc
    Catch2::Catch2WithMain)
target_include_directories(test_reqres_roundtrip_inproc PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/..)
catch_discover_tests(test_reqres_roundtrip_inproc TEST_PREFIX "integration.")

# The deterministic inproc latch-replay roundtrip: a late subscriber recovers the
# retained value through a real frame_router::on_unidirectional, plus the
# non-latched negative and the LATCH-NOALLOC retention gate — always built (inproc
# Policy, no backend needed). tests/ is on the include path for support/alloc_counter.h.
add_executable(test_latch_replay_inproc
    test_latch_replay_inproc.cpp
    test_latch_replay_inproc_noalloc.cpp)
target_link_libraries(test_latch_replay_inproc PRIVATE
    plexus::core
    plexus::wire
    plexus::inproc
    Catch2::Catch2WithMain)
target_include_directories(test_latch_replay_inproc PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/..)
catch_discover_tests(test_latch_replay_inproc TEST_PREFIX "integration.")

# The deterministic peer_session bridge oracle: two paired peer_sessions handshake
# over the inproc backend, install the node-shared forwarders once, flow a real
# published message AND a real RPC, prove the data-path staleness gate fires on a
# mismatched epoch, and prove the handshake timeout (virtual clock) aborts while a
# completed handshake cancels the timer — always built (inproc Policy, no backend).
add_executable(test_peer_session_inproc
    test_peer_session_inproc.cpp
    test_peer_session_inproc_flow.cpp
    test_peer_session_inproc_staleness.cpp
    test_peer_session_inproc_lifecycle.cpp)
target_link_libraries(test_peer_session_inproc PRIVATE
    plexus::core
    plexus::wire
    plexus::inproc
    Catch2::Catch2WithMain)
target_include_directories(test_peer_session_inproc PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/..)
catch_discover_tests(test_peer_session_inproc TEST_PREFIX "integration.")

# The same-host eligibility verdict: after the handshake the session records
# whether the peer's advertised fingerprint equals its own (the fingerprint rides the
# widened frame), and a non-same-host pair never reaches the shared-memory medium.
add_executable(test_peer_session_same_host_inproc test_peer_session_same_host_inproc.cpp)
target_link_libraries(test_peer_session_same_host_inproc PRIVATE
    plexus::core
    plexus::wire
    plexus::inproc
    Catch2::Catch2WithMain)
target_include_directories(test_peer_session_same_host_inproc PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/..)
catch_discover_tests(test_peer_session_same_host_inproc TEST_PREFIX "integration.")

# The DIALED inproc transport oracle: two paired peer_sessions stood up entirely
# through the bus-mediated listen/dial rendezvous (no hand-dial), handshake to
# completion, mint non-zero epochs, and a dial to an unregistered endpoint surfaces
# connection_refused — always built (inproc Policy, no backend needed).
add_executable(test_transport_dial_inproc test_transport_dial_inproc.cpp)
target_link_libraries(test_transport_dial_inproc PRIVATE
    plexus::core
    plexus::wire
    plexus::inproc
    Catch2::Catch2WithMain)
target_include_directories(test_transport_dial_inproc PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/..)
catch_discover_tests(test_transport_dial_inproc TEST_PREFIX "integration.")

# The deterministic single-connection reconnect oracle on the virtual clock:
# refused-dial → backoff → re-dial, surrender bounds, fresh epoch per reconnect —
# always built (inproc Policy, no backend needed).
add_executable(test_reconnect_inproc test_reconnect_inproc.cpp)
target_link_libraries(test_reconnect_inproc PRIVATE
    plexus::core
    plexus::wire
    plexus::inproc
    Catch2::Catch2WithMain)
target_include_directories(test_reconnect_inproc PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/..)
# RUN_SERIAL: alloc gate over the single-TU global operator-new counter (see test_hot_path_alloc).
catch_discover_tests(test_reconnect_inproc TEST_PREFIX "integration."
    PROPERTIES RUN_SERIAL TRUE)

# The reconnect-survives-observer oracle on the virtual clock: an observer installed
# once on a node still receives the post-reconnect lifecycle edge after a forced
# down-then-up cycle, proving the registry re-threads its observer wiring on every
# reconnect rebuild. The shared recording_observer.h resolves via the tests/integration
# include dir — always built (inproc Policy, no backend needed).
add_executable(test_reconnect_observer_inproc test_reconnect_observer_inproc.cpp)
target_link_libraries(test_reconnect_observer_inproc PRIVATE
    plexus::core
    plexus::wire
    plexus::inproc
    Catch2::Catch2WithMain)
target_include_directories(test_reconnect_observer_inproc PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/..
    ${CMAKE_CURRENT_SOURCE_DIR})
catch_discover_tests(test_reconnect_observer_inproc TEST_PREFIX "integration.")

# The deterministic two-node routing oracle on the virtual clock: two engines over
# one inproc bus prove lazy-no-dial-until-demand, eager-dial-on-awareness, awareness-
# without-connect, receive-path identity, single-slot reconnect isolation, and
# publish-does-not-dial — always built (inproc Policy, no backend needed).
add_executable(test_routing_engine_inproc
    test_routing_engine_inproc.cpp
    test_routing_engine_inproc_isolation.cpp)
target_link_libraries(test_routing_engine_inproc PRIVATE
    plexus::core
    plexus::wire
    plexus::inproc
    Catch2::Catch2WithMain)
target_include_directories(test_routing_engine_inproc PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/..)
catch_discover_tests(test_routing_engine_inproc TEST_PREFIX "integration.")

# The node-shared receive-route oracle on the virtual clock: a route installed once
# via routing_engine::on_message_route delivers a published message with a populated
# message_info, SURVIVES a forced reconnect rebuild with no re-install (looped over
# several reconnects), and yields to a per-session on_message when both are set —
# always built (inproc Policy, no backend needed).
add_executable(test_shared_receive_route test_shared_receive_route.cpp)
target_link_libraries(test_shared_receive_route PRIVATE
    plexus::core
    plexus::wire
    plexus::inproc
    Catch2::Catch2WithMain)
target_include_directories(test_shared_receive_route PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/..)
catch_discover_tests(test_shared_receive_route TEST_PREFIX "integration.")

# The process-tier object-route oracle: an object published on one engine's forwarder
# (publish_object) reaches the peer engine's object route in-process with zero
# serialization (the route observes the SAME slot object address the publisher handed
# off), an unresolvable topic_hash releases without delivering, and the node-shared
# object route survives a forced reconnect rebuild with no re-install (looped). Every
# delivered reference is released. Always built (inproc Policy, no backend needed).
add_executable(test_object_route_inproc test_object_route_inproc.cpp)
target_link_libraries(test_object_route_inproc PRIVATE
    plexus::core
    plexus::wire
    plexus::inproc
    Catch2::Catch2WithMain)
target_include_directories(test_object_route_inproc PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/..)
catch_discover_tests(test_object_route_inproc TEST_PREFIX "integration.")

# The deterministic drop+surrender oracle on the virtual clock: an established
# session's REAL transport drop (the partner's accepted end closes) drives an
# automatic per-slot re-dial through the production receive path with no injection;
# a clean tear_down drives none; crossing a surrender bound marks is_dead(id) while
# a co-resident live peer is untouched — always built (inproc Policy, no backend).
add_executable(test_drop_seam_inproc test_drop_seam_inproc.cpp)
target_link_libraries(test_drop_seam_inproc PRIVATE
    plexus::core
    plexus::wire
    plexus::inproc
    Catch2::Catch2WithMain)
target_include_directories(test_drop_seam_inproc PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/..)
catch_discover_tests(test_drop_seam_inproc TEST_PREFIX "integration.")

# The deterministic N>=3-peer concurrent-drop oracle on the virtual clock: one
# dialer engine holds N established peers, several drop AT ONCE (the remote accepted
# ends close before the backoff fires), and each dropped slot re-dials independently
# while every survivor is left bit-for-bit undisturbed; one peer crossing a surrender
# bound is is_dead with the live peers untouched; a reconnected peer's previous-epoch
# straggler is dropped by the staleness gate — always built (inproc Policy, no backend).
add_executable(test_multipeer_reconnect_inproc test_multipeer_reconnect_inproc.cpp)
target_link_libraries(test_multipeer_reconnect_inproc PRIVATE
    plexus::core
    plexus::wire
    plexus::inproc
    Catch2::Catch2WithMain)
target_include_directories(test_multipeer_reconnect_inproc PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/..)
catch_discover_tests(test_multipeer_reconnect_inproc TEST_PREFIX "integration.")

# The deterministic peer-observer behavior oracle: a recording observer registered on
# a real routing_engine forces the full lifecycle + readiness matrix over the inproc
# backend — connected/reconnected/disconnected/rejected/dead plus the readiness loop
# (ready fires once per cycle, re-arms across a reconnect, the zero-subscribe immediate
# fire, the premature-ready window, the forged-frame underflow guard). The shared
# recording_observer.h resolves via the tests/integration include dir — always built
# (inproc Policy, no backend needed).
add_executable(test_peer_observer_inproc test_peer_observer_inproc.cpp)
target_link_libraries(test_peer_observer_inproc PRIVATE
    plexus::core
    plexus::wire
    plexus::inproc
    Catch2::Catch2WithMain)
target_include_directories(test_peer_observer_inproc PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/..
    ${CMAKE_CURRENT_SOURCE_DIR})
catch_discover_tests(test_peer_observer_inproc TEST_PREFIX "integration.")

# The data-path tap-point oracle: a recording observer on a publishing routing_engine
# forces the message-fan-out taps — one publish to N subscribers fires on_message_published
# once and on_message_delivered once per destination, both posted, and the delivered view
# borrows the surfaced owner (a shared addref, not a copy). The runtime assertions activate
# once the data-path emit sites land; this target is BUILT as the compile gate but its ctest
# discovery is intentionally withheld until the taps make it green. The shared
# recording_observer.h resolves via the tests/integration include dir.
add_executable(test_obs_tap_points
    test_obs_tap_points.cpp
    test_obs_tap_points_rpc.cpp)
target_link_libraries(test_obs_tap_points PRIVATE
    plexus::core
    plexus::wire
    plexus::inproc
    Catch2::Catch2WithMain)
target_include_directories(test_obs_tap_points PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/..
    ${CMAKE_CURRENT_SOURCE_DIR})
catch_discover_tests(test_obs_tap_points TEST_PREFIX "integration.")

# The declaration-lifecycle timeline oracle: a custom observer reconstructs a node's
# create/destroy + endpoint declare/drop/register/retire over a single-node inproc
# composition, witnessing the posted edges by pumping the fixture-owned executor. Uses the
# node facade (plexus::plexus); the shared recording_observer.h resolves via the integration
# include dir.
add_executable(test_spine_lifecycle_inproc
    test_spine_lifecycle_inproc.cpp
    test_spine_lifecycle_inproc_edges.cpp)
target_link_libraries(test_spine_lifecycle_inproc PRIVATE
    plexus::plexus
    plexus::wire
    plexus::inproc
    Catch2::Catch2WithMain)
target_include_directories(test_spine_lifecycle_inproc PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/..
    ${CMAKE_CURRENT_SOURCE_DIR})
catch_discover_tests(test_spine_lifecycle_inproc TEST_PREFIX "integration.")

# The per-topic capture-gate oracle: a counting encode thunk witnesses the gate's pre-encode
# decision over a connected inproc node pair (publish_object), proving metadata-only forces no
# encode, payload forces one, wire degrades to payload, both decimation mechanisms drop the
# expected fraction, the folded gate matches the observer x selected matrix, and a lifecycle-only
# observer never admits a payload post. The shared recording_observer.h resolves via the
# integration include dir.
add_executable(test_capture_gate test_capture_gate.cpp)
target_link_libraries(test_capture_gate PRIVATE
    plexus::core
    plexus::wire
    plexus::inproc
    Catch2::Catch2WithMain)
target_include_directories(test_capture_gate PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/..
    ${CMAKE_CURRENT_SOURCE_DIR})
catch_discover_tests(test_capture_gate TEST_PREFIX "integration.")

# The read-buffer partial-read oracle: one framed wire message fed to the frame_reassembler
# in many small sub-header and mid-payload chunks still produces exactly one byte-identical
# complete_frame — the accumulation behavior any later read-buffer resize must preserve.
# Header-only wire codec (no backend needed) — always built.
add_executable(test_read_buffer_partial_read test_read_buffer_partial_read.cpp)
target_link_libraries(test_read_buffer_partial_read PRIVATE
    plexus::core
    plexus::wire
    Catch2::Catch2WithMain)
catch_discover_tests(test_read_buffer_partial_read TEST_PREFIX "integration.")

# The locality confinement matrix: the deterministic fan-out gate (synthetic schemes)
# + the engine demand-side gate (inproc bus) are always built (no backend); the live
# AF_UNIX/TCP confinement legs compile in only when the asio backend is on (guarded by
# PLEXUS_HAVE_ASIO_MUX, wired below inside the asio gate). tests/ is on the include path
# for support headers and the shared make_cfg helpers.
add_executable(test_locality_confinement
    test_locality_confinement.cpp
    test_locality_confinement_wire.cpp
    test_locality_confinement_live.cpp)
target_link_libraries(test_locality_confinement PRIVATE
    plexus::core
    plexus::wire
    plexus::inproc
    Catch2::Catch2WithMain)
target_include_directories(test_locality_confinement PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/..)
catch_discover_tests(test_locality_confinement TEST_PREFIX "integration.")

# The selection-hook substitutability proof: dummy members composed over the header-only
# core multiplexer (no backend) prove the DEFAULT hook routes to the first candidate and an
# INJECTED hook overrides that to a different candidate — the behavioral substitutability of
# the type-erased selection seam. Always built (pure core, no backend needed).
add_executable(test_mux_select_hook test_mux_select_hook.cpp)
target_link_libraries(test_mux_select_hook PRIVATE
    plexus::core
    plexus::wire
    Catch2::Catch2WithMain)
catch_discover_tests(test_mux_select_hook TEST_PREFIX "integration.")

# The transport-steering gate: a PURE value-object proof that a publisher's declared
# topic_qos.reliability and dispatch observably steer the transport_selector's class
# verdict (the no-silent-downgrade rule + the dispatch-preference consultation), and that
# the selector verdict mirrors the engine reliability gate for every scheme. Header-only
# core (no backend) — always built; the `integration.` ctest prefix selects it, the
# "transport steering" case substrings let `ctest -R 'transport steering'` match.
add_executable(test_transport_steering test_transport_steering.cpp)
target_link_libraries(test_transport_steering PRIVATE
    plexus::core
    plexus::wire
    Catch2::Catch2WithMain)
catch_discover_tests(test_transport_steering TEST_PREFIX "integration.")

# The deterministic deadline/liveliness behavioral gate on the virtual clock: a
# subscriber drives subscribe(id, fqn, qos) with its requested periods, the engine
# arms the one router-level monitor at the ready edge, a data gap fires exactly one
# missed-deadline, a presence (data+heartbeat) silence fires exactly one lease-expiry,
# and a continuing keepalive heartbeat keeps the lease alive while data lapses (the
# two-stamp distinction end-to-end). Each timing leg is looped over the virtual clock.
# Always built (inproc Policy, no backend needed); the `integration.` ctest prefix and
# the "deadline liveliness" case substrings let `ctest -R 'deadline liveliness'` match.
add_executable(test_deadline_liveliness
    test_deadline_liveliness.cpp
    test_deadline_liveliness_lease.cpp)
target_link_libraries(test_deadline_liveliness PRIVATE
    plexus::core
    plexus::wire
    plexus::inproc
    Catch2::Catch2WithMain)
target_include_directories(test_deadline_liveliness PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/..)
catch_discover_tests(test_deadline_liveliness TEST_PREFIX "integration.")

# The end-to-end request-vs-offered compatibility gate over the inproc rendezvous: a
# subscriber demands a producer's topic carrying its requested QoS + chosen rxo_mode;
# the producer runs the compatibility check at the fan-out gate and the outcome is
# surfaced subscriber-side. Per soft field a strict incompatible pair is refused (no
# data) while a compatible pair communicates, the same pair under permissive connects
# and surfaces a NON-EMPTY degraded set, and a requires-source-identity subscriber is
# refused regardless of mode against a non-offering producer. Each behavioral leg is
# looped. Always built (inproc Policy, no backend needed); the `integration.` ctest
# prefix and the "rxo compatibility" case substrings let `ctest -R 'rxo compatibility'`
# match.
add_executable(test_rxo_compatibility
    test_rxo_compatibility.cpp
    test_rxo_compatibility_soft.cpp)
target_link_libraries(test_rxo_compatibility PRIVATE
    plexus::core
    plexus::wire
    plexus::inproc
    Catch2::Catch2WithMain)
target_include_directories(test_rxo_compatibility PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/..)
catch_discover_tests(test_rxo_compatibility TEST_PREFIX "integration.")

# The subscriber-QoS wire round-trip identity unit: from_wire_region(to_wire_region(q))
# reproduces q over every requested_flags bit, the scalar/enum fields, and a couple of
# combinations. Pure header-only core (no backend needed) — always built; the
# `integration.` ctest prefix and the "subscribe qos wire" case substrings let
# `ctest -R subscribe_qos_wire` and `ctest -R 'subscribe qos wire'` both match.
add_executable(test_subscribe_qos_wire test_subscribe_qos_wire.cpp)
target_link_libraries(test_subscribe_qos_wire PRIVATE
    plexus::core
    plexus::wire
    Catch2::Catch2WithMain)
catch_discover_tests(test_subscribe_qos_wire TEST_PREFIX "integration.")

# The security attach-gate bridge oracle: two peer_sessions over the inproc backend
# drive the end-to-end attach surface — the dedicated security_event, the lifecycle
# rejected edge, the AEAD install hook, the stream-tamper teardown, and the auth-only
# + datagram posture refusal. No OpenSSL (the transcript seam is a deterministic fake,
# the attach_policy a test double) — always built (inproc Policy, no backend needed).
add_executable(test_security_attach_inproc test_security_attach_inproc.cpp)
target_link_libraries(test_security_attach_inproc PRIVATE
    plexus::core
    plexus::wire
    plexus::inproc
    Catch2::Catch2WithMain)
target_include_directories(test_security_attach_inproc PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/..)
catch_discover_tests(test_security_attach_inproc TEST_PREFIX "integration.")

# The downgrade + posture-mismatch refusal oracle: a transcript-bound proof failure (a
# forced cipher/version downgrade) is refused with downgrade_refused BOTH ways, and a
# secured-vs-plain posture mismatch is refused with posture_mismatch BOTH ways —
# fail-closed, no silent plaintext fallback. No OpenSSL (test policy + fake transcript
# seam) — always built (inproc Policy, no backend needed).
add_executable(test_downgrade_inproc test_downgrade_inproc.cpp)
target_link_libraries(test_downgrade_inproc PRIVATE
    plexus::core
    plexus::wire
    plexus::inproc
    Catch2::Catch2WithMain)
target_include_directories(test_downgrade_inproc PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/..)
catch_discover_tests(test_downgrade_inproc TEST_PREFIX "integration.")

# The recorder write-path spine oracle: the bounded grown-once SPSC byte-ring's
# hard invariants — backing store allocated once (the alloc counter delta is zero
# across a saturating push loop), never-block + drop-newest on a full ring, the
# exact dropout accounting (sum(dropout.count)+drained==produced, looped), the
# byte-budget residency bound, the no-thread cooperative drain into the in-memory
# byte_sink, and the drop-oldest overwrite discipline. Deterministic header-only
# core (no backend). tests/ is on the include path for support/alloc_counter.h and
# the current dir for in_memory_byte_sink.h.
add_executable(test_byte_ring test_byte_ring.cpp)
target_link_libraries(test_byte_ring PRIVATE
    plexus::core
    plexus::wire
    Catch2::Catch2WithMain)
target_include_directories(test_byte_ring PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/..
    ${CMAKE_CURRENT_SOURCE_DIR})
# RUN_SERIAL: alloc gate over the single-TU global operator-new counter (see test_hot_path_alloc).
catch_discover_tests(test_byte_ring TEST_PREFIX "integration."
    PROPERTIES RUN_SERIAL TRUE)

# The flat record-stream format oracle: the canonical encoding round-trips through the
# reader (mixed categories, a payload sample byte-identical with no codec in the path),
# the envelope union reserves the wire-record variant, a mid-write truncation recovers
# every complete record and loses only the trailing partial, and the recording_sink ->
# flat_recorder -> byte_ring -> byte_sink chain captures synthetic observer edges that
# decode offline. Deterministic header-only core (no backend). tests/ is on the include
# path and the current dir for in_memory_byte_sink.h.
add_executable(test_record_stream
    test_record_stream.cpp
    test_record_stream_offline.cpp
    test_record_stream_schema.cpp)
target_link_libraries(test_record_stream PRIVATE
    plexus::core
    plexus::wire
    Catch2::Catch2WithMain)
target_include_directories(test_record_stream PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/..
    ${CMAKE_CURRENT_SOURCE_DIR})
catch_discover_tests(test_record_stream TEST_PREFIX "integration.")

# The projector-seam lifetime oracle: read_projection_input decodes the shared
# (definitions, records, recovery) every host projector consumes and OWNS its payloads —
# the drop-input-first case frees + overwrites the source bytes before reading a recovered
# payload and asserts byte-identity (a borrowed span would be a use-after-free the mcap
# round-trip cannot catch), plus a non-flat buffer returns nullopt. Deterministic header-only
# core (no backend); not gated on mcap. tests/ + the current dir for in_memory_byte_sink.h.
add_executable(test_record_projection test_record_projection.cpp)
target_link_libraries(test_record_projection PRIVATE
    plexus::core
    plexus::wire
    plexus::inproc
    Catch2::Catch2WithMain)
target_include_directories(test_record_projection PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/..
    ${CMAKE_CURRENT_SOURCE_DIR})
catch_discover_tests(test_record_projection TEST_PREFIX "integration.")

# The lossless recording_channel decorator oracle: send/recv byte-fidelity (forwarded ==
# input on both directions), the OUT/IN taps fire once each with verbatim bytes, the inert
# (no-tap) edge, and a captured frame reaching the recorder as a wire_frame record through
# recording_sink. Deterministic header-only core (no backend). tests/ is on the include path
# and the current dir for in_memory_byte_sink.h.
add_executable(test_recording_channel
    test_recording_channel.cpp
    test_recording_channel_sink.cpp)
target_link_libraries(test_recording_channel PRIVATE
    plexus::core
    plexus::wire
    plexus::inproc
    Catch2::Catch2WithMain)
target_include_directories(test_recording_channel PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/..
    ${CMAKE_CURRENT_SOURCE_DIR})
catch_discover_tests(test_recording_channel TEST_PREFIX "integration.")

# The two-node cross-node loss-join oracle: a sender's recording_channel stamps a per-direction
# OUT sequence and a receiver's an IN sequence; a relay drops one frame in transit, and decoding
# both recorded streams offline identifies the lost packet by sequence arithmetic alone (present
# in the sender OUT run, absent in the receiver IN run) with no cross-node clock — proven over
# several runs on deliberately skewed recorder clocks. Deterministic header-only core (no
# backend). tests/ is on the include path and the current dir for in_memory_byte_sink.h.
add_executable(test_wire_loss_join test_wire_loss_join.cpp)
target_link_libraries(test_wire_loss_join PRIVATE
    plexus::core
    plexus::wire
    Catch2::Catch2WithMain)
target_include_directories(test_wire_loss_join PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/..
    ${CMAKE_CURRENT_SOURCE_DIR})
catch_discover_tests(test_wire_loss_join TEST_PREFIX "integration.")

# The construction-time per-transport wire-capture public-API oracle: a node composed over the
# wire-capturing policy + transport (node_options.wire opted in) mints the recording_channel
# decorator and a make_recorder attach captures wire_frame records; a default node mints bare
# channels and records none (the structural-absence witness holds). Public node facade only.
# Links the facade for the typed endpoints + recorder; tests/ + the current dir on the include
# path for in_memory_byte_sink.h.
add_executable(test_wire_public_api
    test_wire_public_api.cpp
    test_wire_public_api_absence.cpp)
target_link_libraries(test_wire_public_api PRIVATE
    plexus::plexus
    plexus::core
    plexus::wire
    plexus::inproc
    Catch2::Catch2WithMain)
target_include_directories(test_wire_public_api PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/..
    ${CMAKE_CURRENT_SOURCE_DIR})
catch_discover_tests(test_wire_public_api TEST_PREFIX "integration.")

# The structural-absence symbol guard: a default (non-wire) build artifact must contain NO
# recording_channel template instantiation symbol. test_routing_engine_inproc composes a full
# inproc node WITHOUT wire capture, so its binary is the witness — nm must find no
# recording_channel symbol. A ctest-runnable guard over the known default-build artifact.
add_test(NAME integration.wire_inert_no_decorator_symbol
    COMMAND ${CMAKE_COMMAND}
        -DBINARY=$<TARGET_FILE:test_routing_engine_inproc>
        -P ${CMAKE_CURRENT_SOURCE_DIR}/wire_inert_symbol_guard.cmake)

# The recorder oracle, in two halves: the pre-buffer / FDR mode (drop-oldest, freeze =
# two indices with zero allocation, three trigger sources, captured_span) AND the public-
# API end-to-end capture (node.make_recorder attach + the RAII handle over a live multi-
# endpoint inproc session, offline recovery, dropout accounting closed, a post-dtor-pump
# teardown section). Links the public facade + inproc for the end-to-end half. tests/ is on
# the include path for support/alloc_counter.h and the current dir for in_memory_byte_sink.h.
add_executable(test_recorder_capture
    test_recorder_capture.cpp
    test_recorder_capture_fdr.cpp
    test_recorder_capture_endtoend.cpp
    test_recorder_capture_teardown.cpp)
target_link_libraries(test_recorder_capture PRIVATE
    plexus::plexus
    plexus::core
    plexus::wire
    plexus::inproc
    Catch2::Catch2WithMain)
target_include_directories(test_recorder_capture PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/..
    ${CMAKE_CURRENT_SOURCE_DIR})
# RUN_SERIAL: alloc gate over the single-TU global operator-new counter (see test_hot_path_alloc).
catch_discover_tests(test_recorder_capture TEST_PREFIX "integration."
    PROPERTIES RUN_SERIAL TRUE)

# The recorder-attached alloc gate: with a recorder draining, the steady-state push ->
# encode -> ring -> drain path allocates ZERO after attach (grown-once ring + reused encode
# scratch, a pre-grown fixed-capacity drain target isolating the recorder's own path). A
# contrast cell shows the inert baseline is itself zero on the same loop. The replaceable
# global new/delete constrains it to its own TU. tests/ is on the include path for
# support/alloc_counter.h.
add_executable(test_recorder_attached_alloc test_recorder_attached_alloc.cpp)
target_link_libraries(test_recorder_attached_alloc PRIVATE
    plexus::core
    plexus::wire
    Catch2::Catch2WithMain)
target_include_directories(test_recorder_attached_alloc PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/..
    ${CMAKE_CURRENT_SOURCE_DIR})
# RUN_SERIAL: alloc gate over the single-TU global operator-new counter (see test_hot_path_alloc).
catch_discover_tests(test_recorder_attached_alloc TEST_PREFIX "integration."
    PROPERTIES RUN_SERIAL TRUE)

# The host-identity accessor oracle: the authenticated peer's host identity is
# read from the security step (SPKI-derived for TLS, attach-bound for PSK), never a
# self-asserted TXT/wire claim. Pure free-function legs + a bridge accessor leg — always
# built (inproc Policy, no backend needed).
add_executable(test_host_identity_inproc
    test_host_identity_inproc.cpp
    test_host_identity_inproc_plaintext.cpp)
target_link_libraries(test_host_identity_inproc PRIVATE
    plexus::core
    plexus::wire
    plexus::inproc
    Catch2::Catch2WithMain)
target_include_directories(test_host_identity_inproc PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/..)
catch_discover_tests(test_host_identity_inproc TEST_PREFIX "integration.")

# The live TCP round-trip needs the asio backend.
if(PLEXUS_ENABLE_ASIO_BACKEND)
    add_executable(test_asio_roundtrip test_asio_roundtrip.cpp)
    target_link_libraries(test_asio_roundtrip PRIVATE
        plexus::core
        plexus::wire
        plexus::asio
        Catch2::Catch2WithMain)
    # Serialized + RESOURCE_LOCK on the shared loopback resource: this real-TCP leg
    # (and the other *over asio* legs below) flaked only under contended parallel ctest
    # -- many real-loopback handshake/reconnect tests piling onto the kernel TCP stack at
    # once squeezed each test's fixed wall-clock pump deadline. RUN_SERIAL keeps them off
    # each other; the shared loopback RESOURCE_LOCK keeps them off any other socket leg.
    catch_discover_tests(test_asio_roundtrip TEST_PREFIX "integration."
        PROPERTIES RUN_SERIAL TRUE RESOURCE_LOCK tcp-loopback)

    # The asio control-frame demux smoke test: a frame_header-wrapped subscribe
    # pushed through the real asio reassembler + frame_router over TCP loopback
    # demuxes to the subscribe consumer — proving the frame_header.type byte
    # survives the asio receive path (the receive-contract proof, at the point of
    # change). Same asio gate + `integration.` ctest prefix.
    add_executable(test_asio_frame_contract test_asio_frame_contract.cpp)
    target_link_libraries(test_asio_frame_contract PRIVATE
        plexus::core
        plexus::wire
        plexus::asio
        Catch2::Catch2WithMain)
    catch_discover_tests(test_asio_frame_contract TEST_PREFIX "integration.")

    # The live TCP loopback req/res roundtrip: a server-side provider and a
    # client-side caller over a real bidirectional asio connection, each on_data
    # wired straight to a frame_router (the settled header-on receive contract).
    # Loops the FULL roundtrip N>=100 in-body; the ctest invocation is re-run
    # >=3 process runs for cross-process reproducibility. Includes the
    # concurrent-outstanding stress (distinct correlation_ids in flight at once).
    add_executable(test_reqres_roundtrip_asio
        test_reqres_roundtrip_asio.cpp
        test_reqres_roundtrip_asio_concurrent.cpp)
    target_link_libraries(test_reqres_roundtrip_asio PRIVATE
        plexus::core
        plexus::wire
        plexus::asio
        Catch2::Catch2WithMain)
    catch_discover_tests(test_reqres_roundtrip_asio TEST_PREFIX "integration.")

    # The live TCP loopback req/res TIMEOUT proof: a real provider that receives the
    # request but never replies; the caller's asio steady_timer fires the per-call
    # deadline as rpc_status::timeout over the production receive path. Loops the
    # behavior in-body; the ctest invocation is re-run >=3 process runs for
    # cross-process reproducibility (a live-timing claim is never made from one run).
    add_executable(test_reqres_timeout_asio test_reqres_timeout_asio.cpp)
    target_link_libraries(test_reqres_timeout_asio PRIVATE
        plexus::core
        plexus::wire
        plexus::asio
        Catch2::Catch2WithMain)
    catch_discover_tests(test_reqres_timeout_asio TEST_PREFIX "integration.")

    # The live TCP loopback latch-replay: a publisher latches + publishes, then a
    # SECOND client connects and subscribes and receives the RETAINED value over the
    # real reassembler-framed stream through frame_router::on_unidirectional (the
    # replay is byte-identical to a normal data frame, so it must). Carries the
    # non-latched negative over asio. Loops the full roundtrip N>=100 in-body; the
    # ctest invocation is re-run >=3 process runs for cross-process reproducibility.
    add_executable(test_latch_replay_asio test_latch_replay_asio.cpp)
    target_link_libraries(test_latch_replay_asio PRIVATE
        plexus::core
        plexus::wire
        plexus::asio
        Catch2::Catch2WithMain)
    catch_discover_tests(test_latch_replay_asio TEST_PREFIX "integration.")

    # The live peer_session bridge oracle over real TCP loopback: a hand-dialed
    # client and an accepted server end of ONE bidirectional connection stand up two
    # peer_sessions that complete the handshake (the simultaneous-connect path), mint
    # non-zero epochs, flow a real published message over the reassembler-framed TCP
    # stream, and prove the data-path staleness gate fires over TCP (a mismatched
    # non-zero session_id is dropped; the latched epoch is delivered). Loops the full
    # scenario N>=100 in-body; the ctest invocation is re-run >=3 process runs for
    # cross-process reproducibility (a live networking claim is never made from one
    # run). Links plexus::inproc too: the synthetic stale gate-frame is produced by
    # the production framing path over an inproc capture link, then fed verbatim to
    # the asio receiver's on_receive.
    add_executable(test_peer_session_asio
        test_peer_session_asio.cpp
        test_peer_session_asio_staleness.cpp)
    target_link_libraries(test_peer_session_asio PRIVATE
        plexus::core
        plexus::wire
        plexus::asio
        plexus::inproc
        Catch2::Catch2WithMain)
    catch_discover_tests(test_peer_session_asio TEST_PREFIX "integration.")

    # The DIALED asio transport oracle over real TCP loopback: the transport's
    # listen + async-connect dial stand up two peer_sessions (no hand-dial,
    # no manual socket().connect / poll loop) that handshake to completion, mint
    # non-zero epochs, and flow a real published message carrying the minted epoch.
    # Loops the full scenario N>=100 in-body; the ctest invocation is re-run >=3
    # process runs for cross-process reproducibility. Links plexus::inproc for the
    # forwarder framing path the oracle reuses.
    add_executable(test_transport_dial_asio test_transport_dial_asio.cpp)
    target_link_libraries(test_transport_dial_asio PRIVATE
        plexus::core
        plexus::wire
        plexus::asio
        plexus::inproc
        Catch2::Catch2WithMain)
    catch_discover_tests(test_transport_dial_asio TEST_PREFIX "integration.")

    # The gated real-TCP routing oracle: the routing_engine over asio_transport (no
    # hand-dial) proves BOTH dial knobs over loopback — lazy (no connection until a
    # demand call, then dial+handshake) and eager (dial on awareness) — flows a real
    # published message carrying the minted epoch, and proves the two-peer receive-
    # path source identity leg over the wire (one node dialing two peers near-
    # simultaneously resolves each frame to its own source, no cross-attribution; the
    # overlapping in-flight dials also exercise the dial-completion correlation).
    # Loops the behavioral paths N>=100 in-body; the ctest invocation is re-run >=3
    # process runs. Links plexus::inproc for the forwarder framing path.
    add_executable(test_routing_engine_asio
        test_routing_engine_asio.cpp
        test_routing_engine_asio_identity.cpp)
    target_link_libraries(test_routing_engine_asio PRIVATE
        plexus::core
        plexus::wire
        plexus::asio
        plexus::inproc
        Catch2::Catch2WithMain)
    catch_discover_tests(test_routing_engine_asio TEST_PREFIX "integration."
        PROPERTIES RUN_SERIAL TRUE RESOURCE_LOCK tcp-loopback)

    # The gated real-TCP peer-observer behavior oracle: a recording observer on a
    # routing_engine over asio_transport proves the lifecycle + readiness matrix over a
    # real socket — connected/reconnected/disconnected/rejected/dead plus the readiness
    # loop (ready once per cycle over the real subscribe->ack loop, double-fire
    # prevention across a real reconnect, the premature-ready window, the accepted-peer
    # edge subset, posted re-entrant delivery). Reuses the shared recording_observer.h
    # via the tests/integration include dir. Loops N>=100 in-body; re-run >=3 process
    # runs. Links plexus::inproc for the forwarder framing path the peers reuse.
    add_executable(test_peer_observer_asio
        test_peer_observer_asio.cpp
        test_peer_observer_asio_readiness.cpp
        test_peer_observer_asio_reentrancy.cpp)
    target_link_libraries(test_peer_observer_asio PRIVATE
        plexus::core
        plexus::wire
        plexus::asio
        plexus::inproc
        Catch2::Catch2WithMain)
    target_include_directories(test_peer_observer_asio PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/..
        ${CMAKE_CURRENT_SOURCE_DIR})
    catch_discover_tests(test_peer_observer_asio TEST_PREFIX "integration."
        PROPERTIES RUN_SERIAL TRUE RESOURCE_LOCK tcp-loopback)

    # The gated real-TCP reconnect oracle: an established session whose channel
    # drops re-dials + re-handshakes over real loopback; the initial-refused path
    # re-dials a closed port. Links plexus::inproc for the forwarder framing path.
    add_executable(test_reconnect_asio
        test_reconnect_asio.cpp
        test_reconnect_asio_coldstart.cpp)
    target_link_libraries(test_reconnect_asio PRIVATE
        plexus::core
        plexus::wire
        plexus::asio
        plexus::inproc
        Catch2::Catch2WithMain)
    catch_discover_tests(test_reconnect_asio TEST_PREFIX "integration.")

    # The gated real-TCP N>=3-peer concurrent-drop oracle: one dialer engine over
    # asio_transport holds N established peers; several drop AT ONCE by closing the
    # accepted sockets, and the engine's OWN production drop route re-dials each slot
    # independently while every survivor is undisturbed; one peer crossing a surrender
    # bound is is_dead with the live peers connected. Every drop is a real socket().close
    # on the accepted end (no injection). Loops in-body; re-run >=3 process runs. Links
    # plexus::inproc for the forwarder framing path the peers reuse.
    add_executable(test_multipeer_reconnect_asio
        test_multipeer_reconnect_asio.cpp
        test_multipeer_reconnect_asio_surrender.cpp)
    target_link_libraries(test_multipeer_reconnect_asio PRIVATE
        plexus::core
        plexus::wire
        plexus::asio
        plexus::inproc
        Catch2::Catch2WithMain)
    catch_discover_tests(test_multipeer_reconnect_asio TEST_PREFIX "integration."
        PROPERTIES RUN_SERIAL TRUE RESOURCE_LOCK tcp-loopback)

    # The gated real-TCP stream-channel hardening leg: a raw client socket writes
    # hostile bytes (a bad-magic run; a header with a withheld payload) into a live
    # asio_channel stamped with a SHORT-floor wire::stream_inbound_config, and the
    # channel's on_protocol_close fires with the matching close_cause (invalid_magic;
    # no_progress_timeout) — the byte-stream slowloris/garbage defense proven over a
    # real socket. Loops each path in-body; re-run >=3 process runs.
    add_executable(test_stream_channel_asio
        test_stream_channel_asio.cpp
        test_stream_channel_asio_backpressure.cpp
        test_stream_channel_asio_largeframe.cpp)
    target_link_libraries(test_stream_channel_asio PRIVATE
        plexus::core
        plexus::wire
        plexus::asio
        plexus::inproc
        Catch2::Catch2WithMain)
    catch_discover_tests(test_stream_channel_asio TEST_PREFIX "integration.")

    # The phase-close cross-class UDP reproducibility gate: a best_effort clean-path flow
    # AND a reliable-datagram-over-loss flow, each looped 100x in-body; the ctest
    # invocation is re-run across >=3 process runs (a timing/transport claim is never made
    # from a single run). Header-only inside plexus::asio; the `udp.` ctest prefix selects
    # it alongside the asio-dir udp legs.
    add_executable(test_udp_reproducibility
        test_udp_reproducibility.cpp
        test_udp_reproducibility_reliable.cpp)
    target_link_libraries(test_udp_reproducibility PRIVATE
        plexus::asio
        Catch2::Catch2WithMain)
    catch_discover_tests(test_udp_reproducibility TEST_PREFIX "udp.")

    # The outbound-send-lifetime gate: a BURST of sends issued in one turn (no drain
    # between them) each transmit their OWN bytes — the proof that the shared udp_server
    # owns each in-flight datagram across its async_send_to (a serial owned queue), not a
    # reused caller scratch. Covers a best_effort burst, a two-peer same-turn overlap, and
    # a reliable retransmit interleaved with fresh sends. Looped 100x in-body; re-run >=3
    # process runs. Header-only inside plexus::asio; the `udp.` ctest prefix selects it.
    add_executable(test_udp_send_burst
        test_udp_send_burst.cpp
        test_udp_send_burst_relay.cpp)
    target_link_libraries(test_udp_send_burst PRIVATE
        plexus::asio
        Catch2::Catch2WithMain)
    catch_discover_tests(test_udp_send_burst TEST_PREFIX "udp.")

    # The large-payload datagram capability gate: a 1 MB and a 4 MB message round-trip
    # byte-identically through the existing fragmenter + reassembler over BOTH best-effort
    # ("udp") and reliable ("udpr") legs, looped in-body and re-run across >=3 process runs.
    # The deterministic loss/reorder shim (tests/support, reused by the fragment-scale sweep
    # and the lossy benchmark cell) drives the injected-loss legs and records the measured
    # loss policy (best-effort drops the whole message; reliable retransmit-reassembles); the
    # shim's own cross-run determinism is asserted first. Header-only inside plexus::asio;
    # tests/ is on the include path for support/loss_reorder_shim.h.
    add_executable(test_udp_large_payload
        test_udp_large_payload.cpp
        test_udp_large_payload_roundtrip.cpp
        test_udp_large_payload_probe.cpp
        test_udp_large_payload_fragment.cpp)
    target_link_libraries(test_udp_large_payload PRIVATE
        plexus::asio
        Catch2::Catch2WithMain)
    target_include_directories(test_udp_large_payload PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/..)
    catch_discover_tests(test_udp_large_payload TEST_PREFIX "udp.")

    # The out-of-box message-ceiling regression guard: an 8 MiB message (the shipped default
    # per-message ceiling) round-trips byte-identically over tcp / unix / udpr at FULL SHIPPED
    # DEFAULTS — no write-queue / back-pressure / ceiling / budget bump — proving the local
    # back-pressure caps can never silently strangle the shipped ceiling. The TLS leg is added
    # inside the crypto gate below (it needs the secure backend + OpenSSL). Looped in-body;
    # re-run >=3 process runs. tests/ is on the include path for shared headers.
    add_executable(test_outofbox_message_ceiling
        test_outofbox_message_ceiling.cpp
        test_outofbox_message_ceiling_udp.cpp
        test_outofbox_message_ceiling_tls.cpp)
    target_link_libraries(test_outofbox_message_ceiling PRIVATE
        plexus::core
        plexus::wire
        plexus::asio
        Catch2::Catch2WithMain)
    target_include_directories(test_outofbox_message_ceiling PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/..)
    catch_discover_tests(test_outofbox_message_ceiling TEST_PREFIX "integration."
        PROPERTIES RUN_SERIAL TRUE RESOURCE_LOCK tcp-loopback)

    # The DIALED AF_UNIX transport oracle over a real local socket: the transport's
    # listen + async-connect dial stand up two peer_sessions (no hand-dial) that
    # handshake to completion, mint non-zero epochs, and flow a real published
    # message carrying the minted epoch. Loops N>=100 in-body; re-run >=3 process
    # runs. Links plexus::inproc for the forwarder framing path the oracle reuses.
    add_executable(test_transport_dial_unix
        test_transport_dial_unix.cpp
        test_transport_dial_unix_flow.cpp)
    target_link_libraries(test_transport_dial_unix PRIVATE
        plexus::core
        plexus::wire
        plexus::asio
        plexus::inproc
        Catch2::Catch2WithMain)
    catch_discover_tests(test_transport_dial_unix TEST_PREFIX "integration.")

    # The gated real-AF_UNIX reconnect oracle: an established session whose channel
    # drops re-dials + re-handshakes over a real local socket; the cold-start
    # missing-socket path re-dials until a listener appears (the same-host demand
    # resurrection). Links plexus::inproc for the forwarder framing path.
    add_executable(test_reconnect_unix
        test_reconnect_unix.cpp
        test_reconnect_unix_coldstart.cpp)
    target_link_libraries(test_reconnect_unix PRIVATE
        plexus::core
        plexus::wire
        plexus::asio
        plexus::inproc
        Catch2::Catch2WithMain)
    catch_discover_tests(test_reconnect_unix TEST_PREFIX "integration.")

    # The gated real-AF_UNIX stream-channel hardening leg: a raw client socket writes
    # hostile bytes (a bad-magic run; a header with a withheld payload) into a live
    # unix_channel stamped with a SHORT-floor wire::stream_inbound_config, and the
    # channel's on_protocol_close fires with the matching close_cause (invalid_magic;
    # no_progress_timeout) — the byte-stream slowloris/garbage defense proven over a
    # real local socket. Loops each path in-body; re-run >=3 process runs.
    add_executable(test_stream_channel_unix
        test_stream_channel_unix.cpp
        test_stream_channel_unix_largeframe.cpp)
    target_link_libraries(test_stream_channel_unix PRIVATE
        plexus::core
        plexus::wire
        plexus::asio
        plexus::inproc
        Catch2::Catch2WithMain)
    catch_discover_tests(test_stream_channel_unix TEST_PREFIX "integration.")

    # The hardware-free host serial loopback oracle over an openpty pair: two serial_channels,
    # one adopting each pty fd on one io_context, prove a production-framed message round-trips
    # host<->host (SER-01) and a peer_session pair completes the point-at-port bring-up handshake
    # over the framed serial link, reusing the handshake_fsm unchanged (SER-04). Loops each path
    # in-body; the ctest invocation is re-run >=3 process runs. Links util for openpty(3).
    add_executable(test_serial_pty_roundtrip test_serial_pty_roundtrip.cpp)
    target_link_libraries(test_serial_pty_roundtrip PRIVATE
        plexus::core
        plexus::wire
        plexus::asio
        plexus::inproc
        Catch2::Catch2WithMain
        util)
    catch_discover_tests(test_serial_pty_roundtrip TEST_PREFIX "integration.")

    # The SER-02 corruption->resync recovery oracle over the same openpty pair: corrupt bytes
    # (a flipped byte AND a dropped byte) are written RAW onto the master fd, and the slave-end
    # serial_channel's CRC decorator must drop them, fire the non-fatal on_frame_dropped seam,
    # magic-resync, and still deliver the following valid frame — with on_protocol_close /
    # on_closed NEVER fired. Each leg loops in-body; the ctest invocation is re-run >=3 process
    # runs. Links util for openpty(3).
    add_executable(test_serial_resync test_serial_resync.cpp)
    target_link_libraries(test_serial_resync PRIVATE
        plexus::core
        plexus::wire
        plexus::asio
        plexus::inproc
        Catch2::Catch2WithMain
        util)
    catch_discover_tests(test_serial_resync TEST_PREFIX "integration.")

    # The saturating-publisher memory-bound gate: a producer outrunning a never-reading
    # peer drives each stream channel's bounded write queue to its cap and no further — the
    # byte budget is the structural memory bound. Looped per tcp/unix transport; re-run >=3
    # process runs. The byte cap IS the residency bound (no RSS sampling needed).
    add_executable(test_egress_saturation_bounded test_egress_saturation_bounded.cpp)
    target_link_libraries(test_egress_saturation_bounded PRIVATE
        plexus::core
        plexus::wire
        plexus::asio
        Catch2::Catch2WithMain)
    catch_discover_tests(test_egress_saturation_bounded TEST_PREFIX "integration.")

    # The af_unix permissions model gate: a real unix_listener over a per-test owner-only
    # temp dir, stat the on-disk socket inode for the configured mode (0700 default + a
    # widened knob), and exercise the accept-time peer-credential gate (admit/refuse fail-
    # closed) plus the closed unlink-before-bind TOCTOU window.
    add_executable(test_unix_permissions test_unix_permissions.cpp)
    target_link_libraries(test_unix_permissions PRIVATE
        plexus::core
        plexus::wire
        plexus::asio
        Catch2::Catch2WithMain)
    catch_discover_tests(test_unix_permissions TEST_PREFIX "integration.")

    # The multiplexing transport now composes a secure (TLS) member alongside the local
    # and remote ones, so its header pulls in plexus::crypto and these mux oracles link it
    # (and only build when the secure backend is ON). The secure member stays inert on
    # these unix/tcp routes — its credential is a default (invalid) one, never touched
    # because no "tls" channel is ever dialed/accepted here.
    if(PLEXUS_ENABLE_CRYPTO_BACKEND)
        # The drop-observability coverage oracle: each datagram-path drop cause is forced on
        # a real component, the engine's posted drop-observer hook delivers it, and the
        # per-cause occupancy counter is asserted. Needs crypto (the AEAD datagram channel)
        # and asio (the udp channel/transport drop sites); the engine + reassembler ride the
        # inproc virtual-clock executor. Loops each leg.
        add_executable(test_drop_coverage test_drop_coverage.cpp
            $<TARGET_OBJECTS:plexus-crypto>)
        target_link_libraries(test_drop_coverage PRIVATE
            plexus::core
            plexus::wire
            plexus::asio
            plexus::crypto
            plexus::inproc
            Catch2::Catch2WithMain)
        target_include_directories(test_drop_coverage PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}/..)
        catch_discover_tests(test_drop_coverage TEST_PREFIX "integration.")

        # The live per-peer mux-selection oracle over real AF_UNIX: a multiplexing_transport
        # over a unix + tcp + tls triple, dialing a same-host ("unix") endpoint, completes
        # over the local stream — the dialed AND accepted mux_channels report the "unix"
        # scheme through the erasure, and the dialed endpoint is passed back unchanged. Loops
        # N>=100 in-body; the ctest invocation is re-run >=3 process runs.
        add_executable(test_mux_select_unix test_mux_select_unix.cpp
            $<TARGET_OBJECTS:plexus-crypto>)
        target_link_libraries(test_mux_select_unix PRIVATE
            plexus::core
            plexus::wire
            plexus::asio
            plexus::crypto
            Catch2::Catch2WithMain)
        catch_discover_tests(test_mux_select_unix TEST_PREFIX "integration.")

        # The live per-peer mux-selection oracle over real TCP loopback: a
        # multiplexing_transport over a unix + tcp + tls triple, dialing a remote ("tcp")
        # endpoint, completes over the network stream — the dialed AND accepted mux_channels
        # report the "tcp" scheme through the erasure, and the dialed endpoint is passed back
        # unchanged. Loops N>=100 in-body; the ctest invocation is re-run >=3 process runs.
        add_executable(test_mux_select_asio test_mux_select_asio.cpp
            $<TARGET_OBJECTS:plexus-crypto>)
        target_link_libraries(test_mux_select_asio PRIVATE
            plexus::core
            plexus::wire
            plexus::asio
            plexus::crypto
            Catch2::Catch2WithMain)
        catch_discover_tests(test_mux_select_asio TEST_PREFIX "integration.")

        # The gated real-TLS bounded-outbox hardening leg: the SECOND composition of the
        # shared io::detail::stream_send_queue block (the plaintext asio_channel pins the
        # first). A real mutual-TLS pair handshakes over loopback; the server end never
        # reads, the encrypted async_write stalls, and the dialed channel's outbox cap
        # engages — congestion=drop sheds, congestion=block surfaces would_block, the
        # outbox never grows past the cap. Serialized + RESOURCE_LOCK on the shared
        # loopback resource alongside the other real-socket legs.
        add_executable(test_tls_channel_backpressure_asio test_tls_channel_backpressure_asio.cpp
            $<TARGET_OBJECTS:plexus-crypto>)
        target_link_libraries(test_tls_channel_backpressure_asio PRIVATE
            plexus::core
            plexus::wire
            plexus::asio
            plexus::crypto
            Catch2::Catch2WithMain)
        catch_discover_tests(test_tls_channel_backpressure_asio TEST_PREFIX "integration."
            PROPERTIES RUN_SERIAL TRUE RESOURCE_LOCK tcp-loopback)

        # The TLS pending-dial teardown asan proof: an in-flight TLS dial whose io_context
        # (and owning transport) is destroyed mid-handshake leaks nothing — the transport-
        # owned pending_dial_registry frees the half-open channel/socket/timer with no
        # leak/UAF (a verify-and-prove rider). Serialized + RESOURCE_LOCK on the
        # shared loopback resource alongside the other real-socket legs.
        add_executable(test_tls_dial_teardown test_tls_dial_teardown.cpp
            $<TARGET_OBJECTS:plexus-crypto>)
        target_link_libraries(test_tls_dial_teardown PRIVATE
            plexus::core
            plexus::wire
            plexus::asio
            plexus::crypto
            Catch2::Catch2WithMain)
        catch_discover_tests(test_tls_dial_teardown TEST_PREFIX "integration."
            PROPERTIES RUN_SERIAL TRUE RESOURCE_LOCK tcp-loopback)

        # The AEAD x fragment composition proof: the per-fragment seal composes with
        # io::split + the reassembler end to end — a sealed-fragment large message
        # round-trips byte-identically in order and under fragment-scale reorder (the
        # deterministic loss/reorder scheduler), and a forged fragment dies at the tag
        # check before reaching the reassembler. Deterministic (no socket, no wall clock:
        # the receiver reassembler runs on the inproc test clock via plexus::testing);
        # tests/ is on the include path for support/loss_reorder_shim.h.
        add_executable(test_aead_fragment test_aead_fragment.cpp
            $<TARGET_OBJECTS:plexus-crypto>)
        target_link_libraries(test_aead_fragment PRIVATE
            plexus::core
            plexus::wire
            plexus::crypto
            plexus::testing
            Catch2::Catch2WithMain)
        target_include_directories(test_aead_fragment PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}/..)
        catch_discover_tests(test_aead_fragment TEST_PREFIX "integration.")

        # The out-of-box ceiling guard gains its mutual-TLS leg when the secure backend is on:
        # the same 8 MiB shipped-ceiling round-trip over a live mutual-TLS loopback pair at full
        # defaults. Linking crypto + the crypto objects and defining PLEXUS_HAVE_TLS_OUTOFBOX
        # compiles in the guarded TLS test case (the cert fixture needs OpenSSL).
        target_sources(test_outofbox_message_ceiling PRIVATE $<TARGET_OBJECTS:plexus-crypto>)
        target_link_libraries(test_outofbox_message_ceiling PRIVATE plexus::crypto)
        target_compile_definitions(test_outofbox_message_ceiling PRIVATE PLEXUS_HAVE_TLS_OUTOFBOX)
    endif()

    # The minimal-composition proof: the core multiplexer instantiated DIRECTLY over a
    # unix + tcp member subset (NOT the all_backends_mux alias) compiles, constructs, and
    # satisfies the erased transport_backend surface. It links plexus::asio but NEITHER
    # plexus::crypto NOR OpenSSL — the composability payoff: a node that omits the secure
    # member pulls in none of the crypto stack.
    add_executable(test_mux_minimal_composition test_mux_minimal_composition.cpp)
    target_link_libraries(test_mux_minimal_composition PRIVATE
        plexus::core
        plexus::wire
        plexus::asio
        Catch2::Catch2WithMain)
    catch_discover_tests(test_mux_minimal_composition TEST_PREFIX "integration.")

    # The companion node-level proof: the variadic node ctor that builds the mux itself from
    # the borrowed leaves (the consumer-facing node<Policy, T1, T2, ...> shape). Links the
    # node facade; the unix+tcp pack still pulls in no secure member.
    add_executable(test_mux_node_composition test_mux_node_composition.cpp)
    target_link_libraries(test_mux_node_composition PRIVATE
        plexus::plexus
        plexus::asio
        Catch2::Catch2WithMain)
    catch_discover_tests(test_mux_node_composition TEST_PREFIX "integration.")

    # The hot-path allocation audit gains a type-erased polymorphic_byte_channel variant when the asio
    # backend is on: the same steady-state 0-alloc invariant must hold across the single
    # virtual hop the erasure adds. The mux headers are header-only inside plexus::asio, so
    # the variant is compiled in (guarded by PLEXUS_HAVE_ASIO_MUX) only here. The same gate
    # arms the live-loopback UDP best-effort send variants (the idle fast-path and the warm
    # pooled queued path both proven 0-alloc over a real bound udp_server).
    target_link_libraries(test_hot_path_alloc PRIVATE plexus::asio)
    target_compile_definitions(test_hot_path_alloc PRIVATE PLEXUS_HAVE_ASIO_MUX)

    # The AEAD datagram lane inherits the UDP send path's 0-alloc when the crypto backend is
    # on: datagram_authenticated_channel seals ABOVE send_to into reused scratch, so the
    # encrypted datagram boundary adds no steady-state heap. Linked + guarded only here.
    if(PLEXUS_ENABLE_CRYPTO_BACKEND)
        target_sources(test_hot_path_alloc PRIVATE $<TARGET_OBJECTS:plexus-crypto>)
        target_link_libraries(test_hot_path_alloc PRIVATE plexus::crypto)
        target_compile_definitions(test_hot_path_alloc PRIVATE PLEXUS_HAVE_CRYPTO_DATAGRAM)
    endif()

    # The locality confinement test gains its LIVE AF_UNIX/TCP legs when the asio backend
    # is on: real local + remote channels prove the same fan-out confinement over the wire.
    target_link_libraries(test_locality_confinement PRIVATE plexus::asio)
    target_compile_definitions(test_locality_confinement PRIVATE PLEXUS_HAVE_ASIO_MUX)

    # The whole-stack co-host shared-memory proof through the public facade: two forked
    # same_host_transports nodes rendezvous over static_discovery and auto-engage a /dev/shm
    # ring, the parent asserting the MEDIUM (the region maps iff co-host) plus byte-exact
    # delivery over >=2 runs and the forced-distinct-fingerprint wire fallback. Gated to the
    # Linux shared-memory node composition (the /dev/shm region naming + the shm member are
    # Linux-only); the broker + the region naming come from plexus::shm/plexus::asio::shm.
    if(PLEXUS_ENABLE_SHM_BACKEND AND TARGET plexus::asio::shm)
        add_executable(test_node_xproc_shm test_node_xproc_shm.cpp)
        target_link_libraries(test_node_xproc_shm PRIVATE
            plexus::plexus
            plexus::core
            plexus::wire
            plexus::asio
            plexus::asio::shm
            plexus::shm
            Catch2::Catch2WithMain)
        catch_discover_tests(test_node_xproc_shm TEST_PREFIX "integration.")
    endif()
endif()

# The host-side flat-stream to MCAP transcode round-trip: a live wire-capturing inproc
# session captured into an in-memory flat stream, transcoded to an MCAP file, read back with
# the mcap reader, and the channel/schema/message mapping asserted (per-topic sample channel,
# synthetic plexus-events channels, the wire channel). Registered ONLY when the transcode tool
# is enabled (it links mcap_transcode_lib, which pulls in the host-only mcap dependency); the
# default suite never builds or registers it. tests/ + the current dir on the include path for
# in_memory_byte_sink.h, and mcap_transcode_lib carries the transcode + mcap include paths.
if(PLEXUS_BUILD_MCAP_TRANSCODE)
    add_executable(test_mcap_transcode test_mcap_transcode.cpp)
    target_link_libraries(test_mcap_transcode PRIVATE
        plexus::plexus
        plexus::core
        plexus::wire
        plexus::inproc
        mcap_transcode_lib
        plexus_mcap_dep
        Catch2::Catch2WithMain)
    target_include_directories(test_mcap_transcode PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/..
        ${CMAKE_CURRENT_SOURCE_DIR})
    catch_discover_tests(test_mcap_transcode TEST_PREFIX "integration.")
endif()

# The host-side flat-stream to pcapng transcode round-trip: a live wire-capturing inproc
# session captured into an in-memory flat stream, transcoded to a pcapng file, parsed back
# in-test (no external dependency), and the block structure asserted (SHB byte-order magic +
# crypto-position comment, IDB DLT_USER0, one EPB per wire frame with direction flags, ns
# timestamps, verbatim packet bytes). Registered ONLY when the pcap tool is enabled; the
# default suite never builds it. The test also writes the captured flat stream to a
# deterministic build-dir path (PLEXUS_QA_CAPTURE_PATH) as the fixture the dependent QA gate
# consumes. pcapng is hand-rolled, so the test links pcap_transcode_lib and pulls no third party.
if(PLEXUS_BUILD_PCAP_TRANSCODE)
    add_executable(test_pcap_transcode test_pcap_transcode.cpp)
    target_link_libraries(test_pcap_transcode PRIVATE
        plexus::plexus
        plexus::core
        plexus::wire
        plexus::inproc
        pcap_transcode_lib
        Catch2::Catch2WithMain)
    target_include_directories(test_pcap_transcode PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/..
        ${CMAKE_CURRENT_SOURCE_DIR})
    target_compile_definitions(test_pcap_transcode PRIVATE
        PLEXUS_QA_CAPTURE_PATH="${CMAKE_CURRENT_BINARY_DIR}/plexus_qa_capture.plxr"
        PLEXUS_QA_CIPHER_CAPTURE_PATH="${CMAKE_CURRENT_BINARY_DIR}/plexus_qa_capture_ciphertext.plxr")
    catch_discover_tests(test_pcap_transcode TEST_PREFIX "integration.")

    # The tshark QA gate: load the projector's pcapng under the Lua dissector and assert a
    # decoded field VALUE matches a known fixture input (a big-endian byte-order guard) plus a
    # sealed-blob branch for the ciphertext capture. It consumes the deterministic build-dir
    # fixtures the C++ test above produces, so it is ordered after it. Skips gracefully where
    # tshark is absent.
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

# The shared-executor proof needs both the asio backend and the mdnspp adapter.
if(PLEXUS_ENABLE_ASIO_BACKEND AND PLEXUS_ENABLE_MDNSPP_DISCOVERY)
    add_executable(test_shared_executor
        test_shared_executor.cpp
        test_shared_executor_readvertise.cpp)
    target_link_libraries(test_shared_executor PRIVATE
        plexus::core
        plexus::wire
        plexus::asio
        plexus::mdnspp
        Catch2::Catch2WithMain)
    # The mdnspp adapter surface (and the io_context it shares) compiles at C++23.
    set_target_properties(test_shared_executor PROPERTIES
        CXX_STANDARD 23
        CXX_STANDARD_REQUIRED ON
        CXX_EXTENSIONS OFF)
    # The live-mDNS tests share the host's real multicast medium -- parallel ctest
    # workers contend on advertise/browse convergence and squeeze the pump deadlines.
    catch_discover_tests(test_shared_executor TEST_PREFIX "integration."
        PROPERTIES RUN_SERIAL TRUE RESOURCE_LOCK live-mdns)
endif()
