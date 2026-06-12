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
catch_discover_tests(test_hot_path_alloc TEST_PREFIX "integration.")

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
catch_discover_tests(test_typed_inproc_alloc TEST_PREFIX "integration.")

# The reassembly-DoS bound: each reassembler bound (total-memory cap, per-message
# ceiling, malformed rejection, timeout eviction) is forced + looped and the in-flight
# state asserted bounded, plus the demux peer-cap bounding the spoofed-source channel
# count. Deterministic (inproc test clock via plexus::testing, no backend); tests/ is on
# the include path for support headers.
add_executable(test_reassembler_dos test_reassembler_dos.cpp)
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
add_executable(test_reqres_roundtrip_inproc test_reqres_roundtrip_inproc.cpp)
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
add_executable(test_latch_replay_inproc test_latch_replay_inproc.cpp)
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
add_executable(test_peer_session_inproc test_peer_session_inproc.cpp)
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
catch_discover_tests(test_reconnect_inproc TEST_PREFIX "integration.")

# The deterministic two-node routing oracle on the virtual clock: two engines over
# one inproc bus prove lazy-no-dial-until-demand, eager-dial-on-awareness, awareness-
# without-connect, receive-path identity, single-slot reconnect isolation, and
# publish-does-not-dial — always built (inproc Policy, no backend needed).
add_executable(test_routing_engine_inproc test_routing_engine_inproc.cpp)
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

# The locality confinement matrix: the deterministic fan-out gate (synthetic schemes)
# + the engine demand-side gate (inproc bus) are always built (no backend); the live
# AF_UNIX/TCP confinement legs compile in only when the asio backend is on (guarded by
# PLEXUS_HAVE_ASIO_MUX, wired below inside the asio gate). tests/ is on the include path
# for support headers and the shared make_cfg helpers.
add_executable(test_locality_confinement test_locality_confinement.cpp)
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
add_executable(test_deadline_liveliness test_deadline_liveliness.cpp)
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
add_executable(test_rxo_compatibility test_rxo_compatibility.cpp)
target_link_libraries(test_rxo_compatibility PRIVATE
    plexus::core
    plexus::wire
    plexus::inproc
    Catch2::Catch2WithMain)
target_include_directories(test_rxo_compatibility PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/..)
catch_discover_tests(test_rxo_compatibility TEST_PREFIX "integration.")

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

# The host-identity accessor oracle: the authenticated peer's host identity is
# read from the security step (SPKI-derived for TLS, attach-bound for PSK), never a
# self-asserted TXT/wire claim. Pure free-function legs + a bridge accessor leg — always
# built (inproc Policy, no backend needed).
add_executable(test_host_identity_inproc test_host_identity_inproc.cpp)
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
    add_executable(test_reqres_roundtrip_asio test_reqres_roundtrip_asio.cpp)
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
    add_executable(test_peer_session_asio test_peer_session_asio.cpp)
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
    add_executable(test_routing_engine_asio test_routing_engine_asio.cpp)
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
    add_executable(test_peer_observer_asio test_peer_observer_asio.cpp)
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
    add_executable(test_reconnect_asio test_reconnect_asio.cpp)
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
    add_executable(test_multipeer_reconnect_asio test_multipeer_reconnect_asio.cpp)
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
    add_executable(test_stream_channel_asio test_stream_channel_asio.cpp)
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
    add_executable(test_udp_reproducibility test_udp_reproducibility.cpp)
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
    add_executable(test_udp_send_burst test_udp_send_burst.cpp)
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
    add_executable(test_udp_large_payload test_udp_large_payload.cpp)
    target_link_libraries(test_udp_large_payload PRIVATE
        plexus::asio
        Catch2::Catch2WithMain)
    target_include_directories(test_udp_large_payload PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/..)
    catch_discover_tests(test_udp_large_payload TEST_PREFIX "udp.")

    # The DIALED AF_UNIX transport oracle over a real local socket: the transport's
    # listen + async-connect dial stand up two peer_sessions (no hand-dial) that
    # handshake to completion, mint non-zero epochs, and flow a real published
    # message carrying the minted epoch. Loops N>=100 in-body; re-run >=3 process
    # runs. Links plexus::inproc for the forwarder framing path the oracle reuses.
    add_executable(test_transport_dial_unix test_transport_dial_unix.cpp)
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
    add_executable(test_reconnect_unix test_reconnect_unix.cpp)
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
    add_executable(test_stream_channel_unix test_stream_channel_unix.cpp)
    target_link_libraries(test_stream_channel_unix PRIVATE
        plexus::core
        plexus::wire
        plexus::asio
        plexus::inproc
        Catch2::Catch2WithMain)
    catch_discover_tests(test_stream_channel_unix TEST_PREFIX "integration.")

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

    # The hot-path allocation audit gains a type-erased polymorphic_byte_channel variant when the asio
    # backend is on: the same steady-state 0-alloc invariant must hold across the single
    # virtual hop the erasure adds. The mux headers are header-only inside plexus::asio, so
    # the variant is compiled in (guarded by PLEXUS_HAVE_ASIO_MUX) only here.
    target_link_libraries(test_hot_path_alloc PRIVATE plexus::asio)
    target_compile_definitions(test_hot_path_alloc PRIVATE PLEXUS_HAVE_ASIO_MUX)

    # The locality confinement test gains its LIVE AF_UNIX/TCP legs when the asio backend
    # is on: real local + remote channels prove the same fan-out confinement over the wire.
    target_link_libraries(test_locality_confinement PRIVATE plexus::asio)
    target_compile_definitions(test_locality_confinement PRIVATE PLEXUS_HAVE_ASIO_MUX)
endif()

# The shared-executor proof needs both the asio backend and the mdnspp adapter.
if(PLEXUS_ENABLE_ASIO_BACKEND AND PLEXUS_ENABLE_MDNSPP_DISCOVERY)
    add_executable(test_shared_executor test_shared_executor.cpp)
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
