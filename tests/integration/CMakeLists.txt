# Integration proofs for the vertical slice: the live TCP loopback round-trip and
# the shared-executor test (asio + mdnspp on one io_context) are gated; the
# hot-path allocation audit is a deterministic inproc test and needs no backend.
# Auto-discovered by the tests glob; the `integration.` ctest prefix lets
# `ctest -R integration` select the whole area. The root tests/CMakeLists.txt is
# never edited.

# The hot-path allocation audit is deterministic (inproc Policy) — always built.
add_executable(test_hot_path_alloc test_hot_path_alloc.cpp)
target_link_libraries(test_hot_path_alloc PRIVATE
    plexus::plexus
    plexus::wire
    plexus::inproc
    Catch2::Catch2WithMain)
# tests/ is on the include path so the test can pull in support/alloc_counter.h.
target_include_directories(test_hot_path_alloc PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/..)
catch_discover_tests(test_hot_path_alloc TEST_PREFIX "integration.")

# The deterministic inproc req/res roundtrip + concurrent-outstanding + zero-alloc
# rpc-dispatch gate — always built (inproc Policy, no backend needed).
add_executable(test_reqres_roundtrip_inproc test_reqres_roundtrip_inproc.cpp)
target_link_libraries(test_reqres_roundtrip_inproc PRIVATE
    plexus::plexus
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
    plexus::plexus
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
    plexus::plexus
    plexus::wire
    plexus::inproc
    Catch2::Catch2WithMain)
target_include_directories(test_peer_session_inproc PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/..)
catch_discover_tests(test_peer_session_inproc TEST_PREFIX "integration.")

# The DIALED inproc transport oracle: two paired peer_sessions stood up entirely
# through the bus-mediated listen/dial rendezvous (no hand-dial), handshake to
# completion, mint non-zero epochs, and a dial to an unregistered endpoint surfaces
# connection_refused — always built (inproc Policy, no backend needed).
add_executable(test_transport_dial_inproc test_transport_dial_inproc.cpp)
target_link_libraries(test_transport_dial_inproc PRIVATE
    plexus::plexus
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
    plexus::plexus
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
    plexus::plexus
    plexus::wire
    plexus::inproc
    Catch2::Catch2WithMain)
target_include_directories(test_routing_engine_inproc PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/..)
catch_discover_tests(test_routing_engine_inproc TEST_PREFIX "integration.")

# The deterministic drop+surrender oracle on the virtual clock: an established
# session's REAL transport drop (the partner's accepted end closes) drives an
# automatic per-slot re-dial through the production receive path with no injection;
# a clean tear_down drives none; crossing a surrender bound marks is_dead(id) while
# a co-resident live peer is untouched — always built (inproc Policy, no backend).
add_executable(test_drop_seam_inproc test_drop_seam_inproc.cpp)
target_link_libraries(test_drop_seam_inproc PRIVATE
    plexus::plexus
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
    plexus::plexus
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
    plexus::plexus
    plexus::wire
    plexus::inproc
    Catch2::Catch2WithMain)
target_include_directories(test_peer_observer_inproc PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/..
    ${CMAKE_CURRENT_SOURCE_DIR})
catch_discover_tests(test_peer_observer_inproc TEST_PREFIX "integration.")

# The live TCP round-trip needs the asio backend.
if(PLEXUS_ENABLE_ASIO_BACKEND)
    add_executable(test_asio_roundtrip test_asio_roundtrip.cpp)
    target_link_libraries(test_asio_roundtrip PRIVATE
        plexus::plexus
        plexus::wire
        plexus::asio
        Catch2::Catch2WithMain)
    catch_discover_tests(test_asio_roundtrip TEST_PREFIX "integration.")

    # The asio control-frame demux smoke test: a frame_header-wrapped subscribe
    # pushed through the real asio reassembler + frame_router over TCP loopback
    # demuxes to the subscribe consumer — proving the frame_header.type byte
    # survives the asio receive path (the receive-contract proof, at the point of
    # change). Same asio gate + `integration.` ctest prefix.
    add_executable(test_asio_frame_contract test_asio_frame_contract.cpp)
    target_link_libraries(test_asio_frame_contract PRIVATE
        plexus::plexus
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
        plexus::plexus
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
        plexus::plexus
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
        plexus::plexus
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
        plexus::plexus
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
        plexus::plexus
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
        plexus::plexus
        plexus::wire
        plexus::asio
        plexus::inproc
        Catch2::Catch2WithMain)
    catch_discover_tests(test_routing_engine_asio TEST_PREFIX "integration.")

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
        plexus::plexus
        plexus::wire
        plexus::asio
        plexus::inproc
        Catch2::Catch2WithMain)
    target_include_directories(test_peer_observer_asio PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/..
        ${CMAKE_CURRENT_SOURCE_DIR})
    catch_discover_tests(test_peer_observer_asio TEST_PREFIX "integration.")

    # The gated real-TCP reconnect oracle: an established session whose channel
    # drops re-dials + re-handshakes over real loopback; the initial-refused path
    # re-dials a closed port. Links plexus::inproc for the forwarder framing path.
    add_executable(test_reconnect_asio test_reconnect_asio.cpp)
    target_link_libraries(test_reconnect_asio PRIVATE
        plexus::plexus
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
        plexus::plexus
        plexus::wire
        plexus::asio
        plexus::inproc
        Catch2::Catch2WithMain)
    catch_discover_tests(test_multipeer_reconnect_asio TEST_PREFIX "integration.")

    # The gated real-TCP stream-channel hardening leg: a raw client socket writes
    # hostile bytes (a bad-magic run; a header with a withheld payload) into a live
    # asio_channel stamped with a SHORT-floor wire::stream_inbound_config, and the
    # channel's on_protocol_close fires with the matching close_cause (invalid_magic;
    # no_progress_timeout) — the byte-stream slowloris/garbage defense proven over a
    # real socket. Loops each path in-body; re-run >=3 process runs.
    add_executable(test_stream_channel_asio test_stream_channel_asio.cpp)
    target_link_libraries(test_stream_channel_asio PRIVATE
        plexus::plexus
        plexus::wire
        plexus::asio
        plexus::inproc
        Catch2::Catch2WithMain)
    catch_discover_tests(test_stream_channel_asio TEST_PREFIX "integration.")

    # The DIALED AF_UNIX transport oracle over a real local socket: the transport's
    # listen + async-connect dial stand up two peer_sessions (no hand-dial) that
    # handshake to completion, mint non-zero epochs, and flow a real published
    # message carrying the minted epoch. Loops N>=100 in-body; re-run >=3 process
    # runs. Links plexus::inproc for the forwarder framing path the oracle reuses.
    add_executable(test_transport_dial_unix test_transport_dial_unix.cpp)
    target_link_libraries(test_transport_dial_unix PRIVATE
        plexus::plexus
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
        plexus::plexus
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
        plexus::plexus
        plexus::wire
        plexus::asio
        plexus::inproc
        Catch2::Catch2WithMain)
    catch_discover_tests(test_stream_channel_unix TEST_PREFIX "integration.")

    # The live per-peer mux-selection oracle over real AF_UNIX: a multiplexing_transport
    # over a unix + tcp pair, dialing a same-host ("unix") endpoint, completes over the
    # local stream — the dialed AND accepted mux_channels report the "unix" scheme through
    # the erasure, and the dialed endpoint is passed back unchanged. Loops N>=100 in-body;
    # the ctest invocation is re-run >=3 process runs for cross-process reproducibility.
    add_executable(test_mux_select_unix test_mux_select_unix.cpp)
    target_link_libraries(test_mux_select_unix PRIVATE
        plexus::plexus
        plexus::wire
        plexus::asio
        Catch2::Catch2WithMain)
    catch_discover_tests(test_mux_select_unix TEST_PREFIX "integration.")

    # The live per-peer mux-selection oracle over real TCP loopback: a multiplexing_transport
    # over a unix + tcp pair, dialing a remote ("tcp") endpoint, completes over the network
    # stream — the dialed AND accepted mux_channels report the "tcp" scheme through the
    # erasure, and the dialed endpoint is passed back unchanged. Loops N>=100 in-body; the
    # ctest invocation is re-run >=3 process runs for cross-process reproducibility.
    add_executable(test_mux_select_asio test_mux_select_asio.cpp)
    target_link_libraries(test_mux_select_asio PRIVATE
        plexus::plexus
        plexus::wire
        plexus::asio
        Catch2::Catch2WithMain)
    catch_discover_tests(test_mux_select_asio TEST_PREFIX "integration.")

    # The hot-path allocation audit gains a type-erased mux_channel variant when the asio
    # backend is on: the same steady-state 0-alloc invariant must hold across the single
    # virtual hop the erasure adds. The mux headers are header-only inside plexus::asio, so
    # the variant is compiled in (guarded by PLEXUS_HAVE_ASIO_MUX) only here.
    target_link_libraries(test_hot_path_alloc PRIVATE plexus::asio)
    target_compile_definitions(test_hot_path_alloc PRIVATE PLEXUS_HAVE_ASIO_MUX)
endif()

# The shared-executor proof needs both the asio backend and the mdnspp adapter.
if(PLEXUS_ENABLE_ASIO_BACKEND AND PLEXUS_ENABLE_MDNSPP_DISCOVERY)
    add_executable(test_shared_executor test_shared_executor.cpp)
    target_link_libraries(test_shared_executor PRIVATE
        plexus::plexus
        plexus::wire
        plexus::asio
        plexus::mdnspp
        Catch2::Catch2WithMain)
    # The mdnspp adapter surface (and the io_context it shares) compiles at C++23.
    set_target_properties(test_shared_executor PROPERTIES
        CXX_STANDARD 23
        CXX_STANDARD_REQUIRED ON
        CXX_EXTENSIONS OFF)
    catch_discover_tests(test_shared_executor TEST_PREFIX "integration.")
endif()
