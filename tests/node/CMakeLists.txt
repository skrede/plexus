# The structural build-discipline checks (no Catch2): a zero-facade standalone link
# check and a real install + out-of-tree find_package(plexus) consume check. Both run
# as ordinary ctest entries under the `node.` prefix. Auto-discovered by the tests glob;
# the nested install_consume/ consumer project is NOT globbed (the glob is one level deep).

# The zero-facade / MCU-minimal link check: links ONLY plexus::core + plexus::inproc, so
# its include path is exactly those two trees. Instantiating the routing_engine compiles
# the core's templates; a core header reaching for a facade or backend header would fail
# header-not-found. plexus::wire arrives transitively through core — linking it explicitly
# would mask a broken core->wire link.
add_executable(core_standalone core_standalone.cpp)
target_link_libraries(core_standalone PRIVATE plexus::core plexus::inproc)
add_test(NAME node.core_standalone COMMAND core_standalone)

# The UART-only node compile gate: a single non-shm transport leaf composed into the full
# node facade, linking ONLY plexus::plexus + plexus::core so the shm include tree is off the
# path. A facade header reaching for an shm header — or naming an shm:: symbol on this plain
# pack — would fail header-not-found. The proof is that it BUILDS and LINKS shm-free; it runs
# as an ordinary ctest entry so the gate is tracked.
add_executable(core_only_node_compile test_core_only_node_compile.cpp)
target_link_libraries(core_only_node_compile PRIVATE plexus::plexus plexus::core)
add_test(NAME node.core_only_node_compile COMMAND core_only_node_compile)

# The install + out-of-tree consume check: a real install to a throwaway prefix, then a
# fresh find_package(plexus CONFIG REQUIRED COMPONENTS ...) + compile + link + RUN of a
# consumer. The component set mirrors this build's enabled backends so the check proves
# exactly the export sets that shipped; crypto additionally drives a sealed/opened AEAD
# round-trip so the imported OBJECT files are exercised at runtime, not just linked.
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
add_test(NAME node.install_consume
    COMMAND ${CMAKE_COMMAND}
        -DPLEXUS_BUILD_DIR=${CMAKE_BINARY_DIR}
        -DPLEXUS_CONSUME_DIR=${CMAKE_CURRENT_SOURCE_DIR}/install_consume
        "-DPLEXUS_CONSUME_COMPONENTS=${_consume_components}"
        "-DPLEXUS_CONSUME_CXX_FLAGS=${CMAKE_CXX_FLAGS}"
        -P ${CMAKE_CURRENT_SOURCE_DIR}/install_consume.cmake)

# The facade vocabulary unit test — the one place that links plexus::plexus (the facade
# library), exercising the public expected alias and the call_errc error category.
add_executable(test_vocabulary test_vocabulary.cpp)
target_link_libraries(test_vocabulary PRIVATE
    plexus::plexus
    Catch2::Catch2WithMain)
catch_discover_tests(test_vocabulary TEST_PREFIX "node.")

# The node facade tests: construction/identity/escape-hatch surface and the discovery
# wiring, over the deterministic inproc backend (no real networking, no owned loop).
add_executable(test_node_construction test_node_construction.cpp)
target_link_libraries(test_node_construction PRIVATE
    plexus::plexus
    plexus::inproc
    Catch2::Catch2WithMain)
catch_discover_tests(test_node_construction TEST_PREFIX "node.")

add_executable(test_node_discovery test_node_discovery.cpp test_node_discovery_awareness.cpp)
target_link_libraries(test_node_discovery PRIVATE
    plexus::plexus
    plexus::inproc
    Catch2::Catch2WithMain)
catch_discover_tests(test_node_discovery TEST_PREFIX "node.")

# The discovery-aging proof: the recorded (announce_period, TTL) empirical sweep and the wired
# engine-pair aging case, both on the manual virtual clock (the plexus::testing test_clock) — so it
# links core/wire/inproc/testing rather than the facade.
add_executable(test_node_discovery_aging
    test_node_discovery_aging.cpp
    test_node_discovery_aging_wired.cpp)
target_link_libraries(test_node_discovery_aging PRIVATE
    plexus::core
    plexus::wire
    plexus::inproc
    plexus::testing
    Catch2::Catch2WithMain)
catch_discover_tests(test_node_discovery_aging TEST_PREFIX "node.")

# The pub/sub endpoint handles: move-only RAII semantics + ctor-canonical registration,
# and the end-to-end bytes family including the standing-demand fan.
add_executable(test_handles test_handles.cpp test_handles_retire.cpp)
target_link_libraries(test_handles PRIVATE
    plexus::plexus
    plexus::inproc
    Catch2::Catch2WithMain)
catch_discover_tests(test_handles TEST_PREFIX "node.")

# The typed endpoint handles reject move-assignment (a compile-time gate against the
# free-decode-state-while-adapters-registered use-after-free) yet still deliver after a
# move-construction into a local.
add_executable(test_handle_move_assign test_handle_move_assign.cpp)
target_link_libraries(test_handle_move_assign PRIVATE
    plexus::plexus
    plexus::inproc
    Catch2::Catch2WithMain)
catch_discover_tests(test_handle_move_assign TEST_PREFIX "node.")

add_executable(test_bytes_pubsub test_bytes_pubsub.cpp test_bytes_pubsub_fan.cpp)
target_link_libraries(test_bytes_pubsub PRIVATE
    plexus::plexus
    plexus::inproc
    Catch2::Catch2WithMain)
catch_discover_tests(test_bytes_pubsub TEST_PREFIX "node.")

# The call family (caller/procedure handles): the byte-identical round-trip, the
# double-serve refusal and retire-to-no_handler cases, and the no-provider errc proof
# (posted, never inline; never hangs).
add_executable(test_call_family test_call_family.cpp test_call_family_spelling.cpp)
target_compile_definitions(test_call_family PRIVATE PLEXUS_HAS_FAMILY_SPELLING)
target_link_libraries(test_call_family PRIVATE
    plexus::plexus
    plexus::inproc
    Catch2::Catch2WithMain)
catch_discover_tests(test_call_family TEST_PREFIX "node.")

add_executable(test_caller_errc test_caller_errc.cpp)
target_link_libraries(test_caller_errc PRIVATE
    plexus::plexus
    plexus::inproc
    Catch2::Catch2WithMain)
catch_discover_tests(test_caller_errc TEST_PREFIX "node.")

# The typed request/response family: a typed caller/procedure round-trip through real
# encode/decode, the provider-errc value carriage under provider_category, the
# request/reply decode-failure legs, and the bytes-procedure interop fallback.
add_executable(test_typed_reqres test_typed_reqres.cpp test_typed_reqres_interop.cpp)
target_compile_definitions(test_typed_reqres PRIVATE PLEXUS_HAS_FAMILY_SPELLING)
target_link_libraries(test_typed_reqres PRIVATE
    plexus::plexus
    plexus::inproc
    Catch2::Catch2WithMain)
catch_discover_tests(test_typed_reqres TEST_PREFIX "node.")

# The deduced-serve negative-compile probe: a concrete handler deduces, a generic lambda
# and an overloaded operator() are rejected (the "spell Sig explicitly" diagnostic), proven
# by static_assert on the real detail::deducible_handler, plus a deduced serve/caller
# round-trip over the inproc node facade.
add_executable(test_serve_deduction test_serve_deduction.cpp)
target_link_libraries(test_serve_deduction PRIVATE
    plexus::plexus
    plexus::inproc
    Catch2::Catch2WithMain)
catch_discover_tests(test_serve_deduction TEST_PREFIX "node.")

# The injected-codec concept vocabulary: typed_codec / identity_bearing trait proofs and
# the resolve_identity precedence (explicit argument wins, else the codec's type_info).
add_executable(test_typed_codec_concepts test_typed_codec_concepts.cpp)
target_link_libraries(test_typed_codec_concepts PRIVATE
    plexus::plexus
    Catch2::Catch2WithMain)
catch_discover_tests(test_typed_codec_concepts TEST_PREFIX "node.")

# The late-joiner gate: a peer that joins after the demand is in place exchanges data with
# no user action on join timing, looped over both join orders, every iteration asserted.
add_executable(test_late_join test_late_join.cpp)
target_link_libraries(test_late_join PRIVATE
    plexus::plexus
    plexus::inproc
    Catch2::Catch2WithMain)
catch_discover_tests(test_late_join TEST_PREFIX "node.")

# The borrowed-executor bridge smoke: a vagus-shaped host owns the io substrate and two
# embedded nodes borrow it; no thread is spawned and nothing progresses unless the host
# pumps. The milestone's named correctness gate for the node facade.
add_executable(test_borrowed_executor_bridge test_borrowed_executor_bridge.cpp)
target_link_libraries(test_borrowed_executor_bridge PRIVATE
    plexus::plexus
    plexus::inproc
    Catch2::Catch2WithMain)
catch_discover_tests(test_borrowed_executor_bridge TEST_PREFIX "node.")

# The fixed-slot loan pool backing the typed publisher: borrow/publish lifecycle,
# alloc-free slot reuse, and graceful counted exhaustion.
add_executable(test_loan_pool test_loan_pool.cpp test_loan_pool_move.cpp)
target_link_libraries(test_loan_pool PRIVATE
    plexus::plexus
    plexus::inproc
    Catch2::Catch2WithMain)
catch_discover_tests(test_loan_pool TEST_PREFIX "node.")

# Defer-free: a slot whose release obligation rides an in-flight carrier past the pool's own
# destruction still releases safely, because the slab storage lives in a heap control block freed
# only when the last outstanding slot returns — not when the pool handle dies. asan is the gate.
add_executable(test_loan_pool_defer_free test_loan_pool_defer_free.cpp)
target_link_libraries(test_loan_pool_defer_free PRIVATE
    plexus::plexus
    plexus::inproc
    Catch2::Catch2WithMain)
catch_discover_tests(test_loan_pool_defer_free TEST_PREFIX "node.")

# The typed pub/sub family end to end: the zero-serialization fast path (address witness,
# zero encodes), the byte fallback, the decode-failure knobs, and the strict posture.
add_executable(test_typed_pubsub test_typed_pubsub.cpp test_typed_pubsub_posture.cpp)
target_link_libraries(test_typed_pubsub PRIVATE
    plexus::plexus
    plexus::inproc
    Catch2::Catch2WithMain)
catch_discover_tests(test_typed_pubsub TEST_PREFIX "node.")

# The typed↔bytes interop matrix: every boundary cell between a typed and a bytes endpoint
# (and a declared-mismatch typed pair) asserting its observable outcome, never UB.
add_executable(test_typed_interop_matrix test_typed_interop_matrix.cpp
    test_typed_interop_matrix_refusal.cpp)
target_link_libraries(test_typed_interop_matrix PRIVATE
    plexus::plexus
    plexus::inproc
    Catch2::Catch2WithMain)
catch_discover_tests(test_typed_interop_matrix TEST_PREFIX "node.")

# The fast-path gates: the identity witness (cell 6) and the LOOPED fast/fallback flip
# (cell 7) — fresh nodes per iteration, every message's path witnessed, every iteration
# asserted, alternating eligible/ineligible sets plus pool-exhaustion bursts.
add_executable(test_typed_fastpath test_typed_fastpath.cpp)
target_link_libraries(test_typed_fastpath PRIVATE
    plexus::plexus
    plexus::inproc
    Catch2::Catch2WithMain)
catch_discover_tests(test_typed_fastpath TEST_PREFIX "node.")

# The single-node self-loopback slice: a node built with no caller transport delivers its own
# publications to its own subscribers — the bytes lane (posted), the typed zero-copy lane (same
# address, zero encodes), and the no-wire-subscribe witness for the self-route.
add_executable(test_self_loopback test_self_loopback.cpp)
target_link_libraries(test_self_loopback PRIVATE
    plexus::plexus
    plexus::inproc
    Catch2::Catch2WithMain)
catch_discover_tests(test_self_loopback TEST_PREFIX "node.")

# The intra-node self-route composed with a network transport on one node: the multi-transport
# framed-lane self-delivery (the erased channel has no zero-copy hop) and exactly-once delivery
# across the self-publish and remote-receive entry points.
add_executable(test_self_loopback_mixed test_self_loopback_mixed.cpp)
target_link_libraries(test_self_loopback_mixed PRIVATE
    plexus::plexus
    plexus::inproc
    Catch2::Catch2WithMain)
catch_discover_tests(test_self_loopback_mixed TEST_PREFIX "node.")

# A late-joining local subscriber that requests durability receives the latched value over the
# self-route (replay_if_latched wired into attach_local); a default-qos subscriber gets no replay.
add_executable(test_self_loopback_latch test_self_loopback_latch.cpp)
target_link_libraries(test_self_loopback_latch PRIVATE
    plexus::plexus
    plexus::inproc
    Catch2::Catch2WithMain)
catch_discover_tests(test_self_loopback_latch TEST_PREFIX "node.")

# Recorder/capture taps observe self-delivery: a public-API recorder over a byte_sink captures the
# bytes-lane self message (recovered sample's framed tail = the payload) and records the typed-lane
# self-delivery as an event.
add_executable(test_self_loopback_recorder test_self_loopback_recorder.cpp)
target_link_libraries(test_self_loopback_recorder PRIVATE
    plexus::plexus
    plexus::inproc
    Catch2::Catch2WithMain)
catch_discover_tests(test_self_loopback_recorder TEST_PREFIX "node.")

# The recorder move-construction lifetime: the shared drain block's rearm closure owns its executor,
# so a handle relocated by a container reallocation still drains to a live executor rather than a
# freed member of the moved-from source.
add_executable(test_recorder_move test_recorder_move.cpp)
target_link_libraries(test_recorder_move PRIVATE
    plexus::plexus
    plexus::inproc
    Catch2::Catch2WithMain)
catch_discover_tests(test_recorder_move TEST_PREFIX "node.")

# The self-route lifetime contract: retire-then-posted is a safe no-op (the re-entry re-scans
# m_subscriptions) and drain-before-destroy holds with no per-callback liveness guard — asan/ubsan
# is the load-bearing gate.
add_executable(test_self_loopback_lifetime test_self_loopback_lifetime.cpp)
target_link_libraries(test_self_loopback_lifetime PRIVATE
    plexus::plexus
    plexus::inproc
    Catch2::Catch2WithMain)
catch_discover_tests(test_self_loopback_lifetime TEST_PREFIX "node.")

# The dispatch re-entrancy contract: a callback that subscribes a new endpoint (forcing the
# subscription table to grow) or drops its own handle mid-fan is safe — delivery fans from a frozen
# view and defers the edit to the outermost fan's exit. asan/ubsan is the load-bearing gate.
add_executable(test_dispatch_reentrancy test_dispatch_reentrancy.cpp)
target_link_libraries(test_dispatch_reentrancy PRIVATE
    plexus::plexus
    plexus::inproc
    Catch2::Catch2WithMain)
catch_discover_tests(test_dispatch_reentrancy TEST_PREFIX "node.")

# The shm self-carrier fallback: a node with NO intra-node transport but WITH an shm member
# self-delivers a same-node publish→subscribe over a node-local shm self-ring. Built only when
# the shm backend is on (plexus::asio::shm exists only under the gate); links the full shm node
# surface (the asio reactor drives the ring's wake). Off-gate hosts skip the cell entirely.
if(PLEXUS_ENABLE_SHM_BACKEND)
    add_executable(test_shm_self_carrier test_shm_self_carrier.cpp)
    target_include_directories(test_shm_self_carrier PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
    target_link_libraries(test_shm_self_carrier PRIVATE
        plexus::shm
        plexus::plexus
        plexus::core
        plexus::wire
        plexus::asio
        plexus::asio::shm
        plexus::native
        Catch2::Catch2WithMain)
    catch_discover_tests(test_shm_self_carrier TEST_PREFIX "node.")
endif()

# The two-node native multicast-discovery host slice: two real basic_node instances, each with a
# multicast_discovery over a datagram_socket, discover each other with NO static table and NO
# mdnspp. The deterministic wired-pair case runs in the default set; the live udp_multicast_socket
# case is hidden behind [.multicast] (real multicast loopback is environment-sensitive). Built only
# when the asio backend is on (the real socket + transport live there); links plexus::native (the
# discovery) and plexus::stream (the datagram_socket concept).
if(PLEXUS_ENABLE_ASIO_BACKEND)
    add_executable(test_node_native_discovery test_node_native_discovery.cpp)
    target_include_directories(test_node_native_discovery PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
    target_link_libraries(test_node_native_discovery PRIVATE
        plexus::plexus
        plexus::core
        plexus::wire
        plexus::stream
        plexus::asio
        plexus::native
        Catch2::Catch2WithMain)
    catch_discover_tests(test_node_native_discovery TEST_PREFIX "node.")
endif()
