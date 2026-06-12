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
    list(APPEND _consume_components shm)
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

add_executable(test_node_discovery test_node_discovery.cpp)
target_link_libraries(test_node_discovery PRIVATE
    plexus::plexus
    plexus::inproc
    Catch2::Catch2WithMain)
catch_discover_tests(test_node_discovery TEST_PREFIX "node.")

# The pub/sub endpoint handles: move-only RAII semantics + ctor-canonical registration,
# and the end-to-end bytes family including the standing-demand fan.
add_executable(test_handles test_handles.cpp)
target_link_libraries(test_handles PRIVATE
    plexus::plexus
    plexus::inproc
    Catch2::Catch2WithMain)
catch_discover_tests(test_handles TEST_PREFIX "node.")

add_executable(test_bytes_pubsub test_bytes_pubsub.cpp)
target_link_libraries(test_bytes_pubsub PRIVATE
    plexus::plexus
    plexus::inproc
    Catch2::Catch2WithMain)
catch_discover_tests(test_bytes_pubsub TEST_PREFIX "node.")

# The call family (caller/procedure handles): the byte-identical round-trip, the
# double-serve refusal and retire-to-no_handler cases, and the no-provider errc proof
# (posted, never inline; never hangs).
add_executable(test_call_family test_call_family.cpp)
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
add_executable(test_typed_reqres test_typed_reqres.cpp)
target_link_libraries(test_typed_reqres PRIVATE
    plexus::plexus
    plexus::inproc
    Catch2::Catch2WithMain)
catch_discover_tests(test_typed_reqres TEST_PREFIX "node.")

# The deduced-serve negative-compile probe: a concrete handler deduces, a generic lambda
# and an overloaded operator() are rejected (the "spell Sig explicitly" diagnostic), proven
# by static_assert on a local deducibility concept the family wave repoints at the real one.
add_executable(test_serve_deduction test_serve_deduction.cpp)
target_link_libraries(test_serve_deduction PRIVATE
    plexus::plexus
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
add_executable(test_loan_pool test_loan_pool.cpp)
target_link_libraries(test_loan_pool PRIVATE
    plexus::plexus
    plexus::inproc
    Catch2::Catch2WithMain)
catch_discover_tests(test_loan_pool TEST_PREFIX "node.")

# The typed pub/sub family end to end: the zero-serialization fast path (address witness,
# zero encodes), the byte fallback, the decode-failure knobs, and the strict posture.
add_executable(test_typed_pubsub test_typed_pubsub.cpp)
target_link_libraries(test_typed_pubsub PRIVATE
    plexus::plexus
    plexus::inproc
    Catch2::Catch2WithMain)
catch_discover_tests(test_typed_pubsub TEST_PREFIX "node.")

# The typed↔bytes interop matrix: every boundary cell between a typed and a bytes endpoint
# (and a declared-mismatch typed pair) asserting its observable outcome, never UB.
add_executable(test_typed_interop_matrix test_typed_interop_matrix.cpp)
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
