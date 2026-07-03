# Shared-memory backend tests: the cross-process (re-exec, open-by-name) xproc
# harness, the MAP_SHARED round-trip behavioral-oracle placeholder, and the
# io_uring FUTEX_WAIT ⇄ reactor de-risking spike. Only built when PLEXUS_ENABLE_SHM_BACKEND
# is ON (plexus::native exists only under the gate); the `shm.` ctest prefix lets
# `ctest -R shm` select it. Auto-discovered by the tests glob.
if(NOT PLEXUS_ENABLE_SHM_BACKEND)
    return()
endif()

# The liburing usage-requirement proof: links ONLY plexus::asio::shm (+ plexus::asio for the
# policy header path) and NO manual liburing, then drives ring_notifier's io_uring calls. A
# green link proves plexus::asio::shm carries liburing transitively — the symptom a real
# consumer hit (undefined reference to io_uring_*). Built only when the shm backend exposes
# its bridge target.
if(TARGET plexus::asio::shm)
    add_executable(test_shm_liburing_link test_shm_liburing_link.cpp)

    target_link_libraries(test_shm_liburing_link PRIVATE
        plexus::asio::shm
        plexus::asio
        Catch2::Catch2WithMain)

    catch_discover_tests(test_shm_liburing_link TEST_PREFIX "shm.")
endif()

# The scaffold oracle: forks a child, round-trips a value through an anonymous
# MAP_SHARED page, and asserts equality looped N>=100 — proving fork +
# MAP_SHARED actually crosses the process boundary on this host. The later waves
# fill it with the real ring/notifier xproc cases.
add_executable(test_shm_scaffold test_shm_scaffold.cpp)

target_include_directories(test_shm_scaffold PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(test_shm_scaffold PRIVATE
    plexus::native
    Catch2::Catch2WithMain)

catch_discover_tests(test_shm_scaffold TEST_PREFIX "shm.")

# The header-only broadcast ring + loan/take family. These TUs include ONLY the
# family headers under plexus/shm/ — they need no plexus::native backend link
# (the ring drives a caller-supplied anonymous mapped span), proving the family is
# POSIX/asio-free. They link plexus::shm for the family include path, plexus::core
# for the engine, and the xproc harness header path.
add_executable(test_shm_ring_core test_shm_ring_core.cpp test_shm_ring_core_capacity.cpp)

target_include_directories(test_shm_ring_core PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(test_shm_ring_core PRIVATE
    plexus::shm
    plexus::core
    Catch2::Catch2WithMain)

catch_discover_tests(test_shm_ring_core TEST_PREFIX "shm.")

# Ring backpressure: best_effort overwrite-skips a pinned slot, the full-lap
# congested fallback, and the reliable cursor gate (lossless).
add_executable(test_shm_ring_backpressure test_shm_ring_backpressure.cpp
    test_shm_ring_backpressure_recovery.cpp)

target_link_libraries(test_shm_ring_backpressure PRIVATE
    plexus::shm
    plexus::core
    Catch2::Catch2WithMain)

catch_discover_tests(test_shm_ring_backpressure TEST_PREFIX "shm.")

# The overwrite-vs-pin Dekker race, driven across a producer + consumer thread
# (the looped-logic proof; tsan is the phase-boundary gate). The two driver
# threads are a test-local concern (the ring itself spawns none) -- the SHM test
# tree is the sole consumer of pthreads, so the dependency is resolved here in the
# fragment, never in the shared root.
find_package(Threads REQUIRED)

add_executable(test_shm_ring_dekker test_shm_ring_dekker.cpp)

target_link_libraries(test_shm_ring_dekker PRIVATE
    plexus::shm
    plexus::core
    Threads::Threads
    Catch2::Catch2WithMain)

catch_discover_tests(test_shm_ring_dekker TEST_PREFIX "shm.")

# The loan reshape: the move-only intrusive shm_slot_owner, the zero-copy
# take() returning wire_bytes<shm_slot_owner> (no shared_ptr), and the move-only
# loaned_buffer / taken_message lifetime (inert moved-from, unpin exactly once).
add_executable(test_shm_zerocopy_take test_shm_zerocopy_take.cpp)

target_include_directories(test_shm_zerocopy_take PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(test_shm_zerocopy_take PRIVATE
    plexus::shm
    plexus::core
    plexus::wire
    Catch2::Catch2WithMain)

catch_discover_tests(test_shm_zerocopy_take TEST_PREFIX "shm.")

add_executable(test_shm_loan_lifetime test_shm_loan_lifetime.cpp)

target_include_directories(test_shm_loan_lifetime PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(test_shm_loan_lifetime PRIVATE
    plexus::shm
    plexus::core
    plexus::wire
    Catch2::Catch2WithMain)

catch_discover_tests(test_shm_loan_lifetime TEST_PREFIX "shm.")

# The generic selection + identity layer (header-only, POSIX/asio-free): the
# dispatch_hint FLAGS type wired into topic_qos + the selector's locality x hint
# SHM-eligibility decision. The TU includes only core headers, proving the
# selection layer carries no backend dependency.
add_executable(test_shm_dispatch_hint test_shm_dispatch_hint.cpp)

target_link_libraries(test_shm_dispatch_hint PRIVATE
    plexus::core
    Catch2::Catch2WithMain)

catch_discover_tests(test_shm_dispatch_hint TEST_PREFIX "shm.")

# The same-host identity layer: the null-guarded fingerprint compare + the
# deterministic, direction-discriminated region naming (the generic half; the
# machine-id read is deferred to the backend).
add_executable(test_shm_same_host test_shm_same_host.cpp)

target_link_libraries(test_shm_same_host PRIVATE
    plexus::shm
    plexus::core
    plexus::wire
    Catch2::Catch2WithMain)

catch_discover_tests(test_shm_same_host TEST_PREFIX "shm.")

# The naming-budget invariant: the region name is a fixed 16 hex chars for any fqn/direction/
# namespace, and the composed macOS canonical/slab/notifier names all fit the Darwin 31-char
# limit. Platform-neutral naming math (no Darwin symbol needed); links plexus::wire for the fqn
# hash the naming routes through.
add_executable(test_region_naming_length test_region_naming_length.cpp)

target_link_libraries(test_region_naming_length PRIVATE
    plexus::shm
    plexus::wire
    Catch2::Catch2WithMain)

catch_discover_tests(test_region_naming_length TEST_PREFIX "shm.")

# The shared-memory selection decision (locality x hint, bilateral) + the
# per-forwarder acquired-ring refcount set (no singleton).
add_executable(test_shm_selection test_shm_selection.cpp)

target_link_libraries(test_shm_selection PRIVATE
    plexus::shm
    plexus::core
    plexus::wire
    Catch2::Catch2WithMain)

catch_discover_tests(test_shm_selection TEST_PREFIX "shm.")

# The two core seams (region_broker + notifier): stub models prove the concepts are
# satisfiable and the notifier's drain is a move-only callback, with no POSIX/asio in
# the core translation unit.
add_executable(test_shm_seams test_shm_seams.cpp)

target_link_libraries(test_shm_seams PRIVATE
    plexus::shm
    plexus::core
    Catch2::Catch2WithMain)

catch_discover_tests(test_shm_seams TEST_PREFIX "shm.")

# The POSIX region broker: create mints a 0600 /dev/shm region (creator owns the
# unlink), attach maps an existing one, unlink_stale_on_create reclaims an orphan,
# and an oversize create fast-fails before any syscall. Links plexus::native for the
# compiled broker bodies (shm_open/mmap/ftruncate/shm_unlink).
add_executable(test_shm_region_broker test_shm_region_broker.cpp)

target_include_directories(test_shm_region_broker PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(test_shm_region_broker PRIVATE
    plexus::shm
    plexus::native
    Catch2::Catch2WithMain)

catch_discover_tests(test_shm_region_broker TEST_PREFIX "shm.")

# The cross-process futex notifier: the wake crosses address spaces (the
# no-PRIVATE-flag proof, a forked producer waking a forked consumer on a shared
# MAP_SHARED word) and the drain-before-wait protocol delivers an interleaved
# publish (lost-wakeup-safe). Links plexus::native for the raw SYS_futex bodies.
add_executable(test_shm_notifier_lostwakeup test_shm_notifier_lostwakeup.cpp)

target_include_directories(test_shm_notifier_lostwakeup PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(test_shm_notifier_lostwakeup PRIVATE
    plexus::native
    Catch2::Catch2WithMain)

catch_discover_tests(test_shm_notifier_lostwakeup TEST_PREFIX "shm.")

# The parked-flag-gated wake: the wake decision is gated off the prior park-state (a
# pure predicate, so zero-wake-when-spinning is asserted off the syscall), a spinning
# consumer leaves the gated signal NOTIFIED parking nobody, and a forked parked
# consumer is still woken across address spaces by the two-arg gated signal (the
# lost-wakeup guard on the gated path). Links plexus::native for the raw SYS_futex bodies.
add_executable(test_shm_wake_gating test_shm_wake_gating.cpp)

target_include_directories(test_shm_wake_gating PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(test_shm_wake_gating PRIVATE
    plexus::native
    Catch2::Catch2WithMain)

catch_discover_tests(test_shm_wake_gating TEST_PREFIX "shm.")

# The first whole-stack cross-process proof: two processes converge on the same
# deterministically-named regions with no exchange (the machine fingerprint + the
# direction-discriminated region naming), one creates + publishes, the other
# attaches + consumes, and a real value round-trips byte-equal over the MAP_SHARED
# broadcast ring. Links plexus::native (broker + fingerprint) + plexus::wire (the fqn
# hash the region naming routes through).
add_executable(test_shm_same_host_roundtrip test_shm_same_host_roundtrip.cpp
    test_shm_same_host_fingerprint.cpp)

target_include_directories(test_shm_same_host_roundtrip PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(test_shm_same_host_roundtrip PRIVATE
    plexus::shm
    plexus::native
    plexus::wire
    Catch2::Catch2WithMain)

catch_discover_tests(test_shm_same_host_roundtrip TEST_PREFIX "shm.")

# The shm-region namespace cross-process proof: a producer mints regions under one
# namespace; a consumer with the SAME namespace receives the value (convergence), a
# consumer with a DIFFERENT namespace cannot attach the producer's region and sees
# nothing (isolation — two unrelated co-host apps never collide on a shared region).
add_executable(test_shm_region_namespace test_shm_region_namespace.cpp)

target_include_directories(test_shm_region_namespace PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(test_shm_region_namespace PRIVATE
    plexus::shm
    plexus::native
    plexus::wire
    Catch2::Catch2WithMain)

catch_discover_tests(test_shm_region_namespace TEST_PREFIX "shm.")

# The core pub/sub endpoints (header-only, asio/POSIX-free): the shm_channel
# send -> drain round-trip over an anonymous mapped span (loan -> one memcpy ->
# publish -> signal), the notifier signaling exactly once per successful send via
# a recording stub, and the oversize fallback (a payload over the slot
# capacity returns rejected, no publish, no signal). The TU includes only core
# headers, proving the endpoints carry no backend dependency.
add_executable(test_shm_channel test_shm_channel.cpp)

target_include_directories(test_shm_channel PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(test_shm_channel PRIVATE
    plexus::shm
    plexus::core
    plexus::wire
    Catch2::Catch2WithMain)

catch_discover_tests(test_shm_channel TEST_PREFIX "shm.")

# Adaptive spin-then-park consumer policy at the slot_subscriber take seam (header-only
# core); drives a real publisher/subscriber over a heap-backed ring and a producer thread.
add_executable(test_shm_spin_park test_shm_spin_park.cpp)
target_include_directories(test_shm_spin_park PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(test_shm_spin_park PRIVATE
    plexus::shm
    plexus::core
    plexus::wire
    Threads::Threads
    Catch2::Catch2WithMain)
catch_discover_tests(test_shm_spin_park TEST_PREFIX "shm.")

# Backpressure + observable congestion (header-only): a best_effort ring with every
# slot pinned returns congested off send() (OBSERVABLE, never a silent drop); a
# reliable producer blocks losslessly on a lagging consumer (every value arrives in
# order, none skipped, across a reader + writer thread); and the reliable blocking
# spin/yield path allocates nothing across N sends (the alloc-counter idiom from
# tests/support). The two driver threads are a test-local concern (the channel
# spawns none).
add_executable(test_shm_congested test_shm_congested.cpp test_shm_congested_alloc.cpp)

target_include_directories(test_shm_congested PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR}/tests)

target_link_libraries(test_shm_congested PRIVATE
    plexus::shm
    plexus::core
    plexus::wire
    Threads::Threads
    Catch2::Catch2WithMain)

# RUN_SERIAL: the congested-alloc gate asserts a zero delta on the single-TU global operator-new
# counter; serializing keeps the count immune to the CPU contention of concurrent test processes.
catch_discover_tests(test_shm_congested TEST_PREFIX "shm."
    PROPERTIES RUN_SERIAL TRUE)

# The demand-driven topic registry (header-only, asio/strand/std::function-free): a
# stub broker over heap-backed regions + a recording stub notifier prove the
# acquire/release refcount lifecycle (created -> attached -> teardown+unlink), the
# max_payload ring sizing with a subscriber-only default fallback, and the teardown
# ordering (disarm BEFORE the subscriber the drain touches is destroyed -- a
# recording notifier brackets the order). The registry borrows the broker + notifier
# seams by reference; the TU includes only core headers.
add_executable(test_shm_registry test_shm_registry.cpp)

target_include_directories(test_shm_registry PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(test_shm_registry PRIVATE
    plexus::shm
    plexus::core
    plexus::wire
    Catch2::Catch2WithMain)

catch_discover_tests(test_shm_registry TEST_PREFIX "shm.")

# The ring memory cost query (header-only core): ring_memory_for equals the layout
# helpers AND the bytes the registry actually requests from the broker for a
# (payload, capacity) table, pinning the depth-32 16 MiB figure (512 MiB) so a
# depth-17 / deep-tier regression is caught. A recording stub broker captures the
# requested slab sizes; the TU includes only core headers.
add_executable(test_shm_ring_memory_query test_shm_ring_memory_query.cpp)

target_include_directories(test_shm_ring_memory_query PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(test_shm_ring_memory_query PRIVATE
    plexus::shm
    plexus::core
    plexus::wire
    Catch2::Catch2WithMain)

catch_discover_tests(test_shm_ring_memory_query TEST_PREFIX "shm.")

# The safe-defaults regression + the unified fail-closed path (header-only core): the
# small-payload default geometry is byte-identical to the shipped values, and a
# registration over the tunable ceiling AND one the broker/OS cannot map BOTH fail
# closed naming which bound was hit (ceiling vs OS allocator) with the ask vs available
# — never a silent downgrade. A heap-backed stub broker drives both legs.
add_executable(test_shm_geometry_defaults test_shm_geometry_defaults.cpp)

target_include_directories(test_shm_geometry_defaults PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(test_shm_geometry_defaults PRIVATE
    plexus::shm
    plexus::core
    plexus::wire
    Catch2::Catch2WithMain)

catch_discover_tests(test_shm_geometry_defaults TEST_PREFIX "shm.")

# The thread-free notifier spike (NON-GATING): probes whether an
# IORING_OP_FUTEX_WAIT CQE on a MAP_SHARED word can be reaped alongside an asio
# reactor without a plexus-spawned thread. Built only when the host distro's
# liburing is found (the io_uring path's optional dependency, the OpenSSL trust
# class — a system lib, NOT a language-registry package). When liburing is
# absent the spike is silently skipped; it never gates the suite.
find_package(PkgConfig QUIET)
if(PkgConfig_FOUND)
    pkg_check_modules(LIBURING QUIET IMPORTED_TARGET liburing)
endif()

if(LIBURING_FOUND)
    add_executable(spike_iouring_futex spike_iouring_futex.cpp)

    # The spike links plexus::asio for the asio include path ONLY — it probes
    # whether an io_uring CQE can be reaped THROUGH the user's asio reactor (the
    # spike question). The backend target plexus-native itself stays asio-free;
    # this asio dependency lives in the spike test executable, never in the lib.
    target_link_libraries(spike_iouring_futex PRIVATE
        plexus::native
        plexus::asio
        PkgConfig::LIBURING
        Catch2::Catch2WithMain)

    catch_discover_tests(spike_iouring_futex TEST_PREFIX "shm.")

    # The wakeup -> reactor bridge functional proof: a forked producer publishes +
    # signals the in-region notify word; the parent's io_uring-futex bridge, armed on
    # a user-owned io_context, reaps the CQE on a reactor turn and posts the drain.
    # Links plexus::asio (the bridge + its asio include path), plexus::native (the broker
    # + the futex primitive the bridge's signal() delegates to), plexus::wire (the fqn
    # hash the region naming routes through), and liburing (the io_uring the bridge
    # drives). The bridge is the only asio-coupled shared-memory piece (the core +
    # the compiled backend stay asio-free).
    add_executable(test_shm_notifier_bridge test_shm_notifier_bridge.cpp)

    target_include_directories(test_shm_notifier_bridge PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR})

    target_link_libraries(test_shm_notifier_bridge PRIVATE
    plexus::shm
        plexus::asio
        plexus::asio::shm
        plexus::native
        plexus::wire
        Catch2::Catch2WithMain)

    catch_discover_tests(test_shm_notifier_bridge TEST_PREFIX "shm.")

    # The teardown-race proof (asan-targeted): a producer process signals at the
    # moment the consumer tears the channel + bridge down, repeatedly, to drive the
    # wake-vs-teardown race. The PASS criterion is asan/ubsan-clean -- no
    # heap-use-after-free on the posted-drain path during teardown (the
    # stop -> wake -> close ordering). Same link set as the bridge proof.
    add_executable(test_shm_teardown_race test_shm_teardown_race.cpp)

    target_include_directories(test_shm_teardown_race PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR})

    target_link_libraries(test_shm_teardown_race PRIVATE
    plexus::shm
        plexus::asio
        plexus::asio::shm
        plexus::native
        plexus::wire
        Catch2::Catch2WithMain)

    catch_discover_tests(test_shm_teardown_race TEST_PREFIX "shm.")

    # The dual-delivery xproc proof: for ONE topic, a same-host SHM ring round-trips a
    # value over the io_uring-futex reactor bridge AND a cross-host loopback wire path
    # delivers the same bytes — the SHM upgrade ADDS the fast path, it does not suppress
    # the wire attach. Same link set as the bridge proof (the bridge + the posix broker +
    # the futex primitive + the fqn-hash region naming + the io_uring the bridge drives).
    add_executable(test_shm_dual_delivery test_shm_dual_delivery.cpp)

    target_include_directories(test_shm_dual_delivery PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR})

    target_link_libraries(test_shm_dual_delivery PRIVATE
    plexus::shm
        plexus::asio
        plexus::asio::shm
        plexus::native
        plexus::wire
        Catch2::Catch2WithMain)

    catch_discover_tests(test_shm_dual_delivery TEST_PREFIX "shm.")

    # The composition compile-gate: the shm-bearing all-backends multiplexer (shm as the
    # second local member alongside AF_UNIX) satisfies transport_backend AND the existing
    # no-shm composition stays intact. The full member set pulls in the secure (TLS/DTLS)
    # members, so it links plexus::crypto via the COMDAT-fold object pattern and builds only
    # when the secure backend is ON.
    if(PLEXUS_ENABLE_CRYPTO_BACKEND)
        add_executable(test_shm_mux_composition test_shm_mux_composition.cpp
            $<TARGET_OBJECTS:plexus-crypto>)

        target_link_libraries(test_shm_mux_composition PRIVATE
    plexus::shm
            plexus::core
            plexus::wire
            plexus::asio
            plexus::asio::shm
            plexus::native
            plexus::crypto
            Catch2::Catch2WithMain)

        catch_discover_tests(test_shm_mux_composition TEST_PREFIX "shm.")
    endif()

    # The variadic-leaf shm node composition: a live node<asio_policy, shm_member,
    # unix_transport, asio_transport> constructs + brings up a same-host listener, and the
    # lean crypto-free local_shm_mux alias satisfies transport_backend. This is the consumer's
    # composition path (the node builds its own mux from the borrowed leaves). It needs the
    # node/api surface (plexus::plexus) + the shm bridge (liburing) but NOT crypto — the lean
    # alias deliberately omits the secure members, so it builds with the crypto backend off.
    add_executable(test_shm_node_composition test_shm_node_composition.cpp)

    target_include_directories(test_shm_node_composition PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR})

    target_link_libraries(test_shm_node_composition PRIVATE
    plexus::shm
        plexus::plexus
        plexus::core
        plexus::wire
        plexus::asio
        plexus::asio::shm
        plexus::native
        Catch2::Catch2WithMain)

    catch_discover_tests(test_shm_node_composition TEST_PREFIX "shm.")

    # The owning composition bundle: a transport_set names the leaves once and mints a node
    # that borrows them — covering both the shm-bearing {io, broker} form and the lean no-shm
    # {io} form in one TU. Same dependency surface as the node-composition proof.
    add_executable(test_transport_set test_transport_set.cpp)

    target_include_directories(test_transport_set PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR})

    target_link_libraries(test_transport_set PRIVATE
    plexus::shm
        plexus::plexus
        plexus::core
        plexus::wire
        plexus::asio
        plexus::asio::shm
        plexus::native
        Catch2::Catch2WithMain)

    catch_discover_tests(test_transport_set TEST_PREFIX "shm.")

    # The portable same-host composition: one consumer surface on every platform that resolves
    # to shm + AF_UNIX + TCP on Linux. Proves the portable type stands up a live node AND that
    # two independent per-instance brokers (the model the type owns) share an shm ring by name.
    add_executable(test_same_host_transports test_same_host_transports.cpp)

    target_include_directories(test_same_host_transports PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR})

    target_link_libraries(test_same_host_transports PRIVATE
    plexus::shm
        plexus::plexus
        plexus::core
        plexus::wire
        plexus::asio
        plexus::asio::shm
        plexus::native
        Catch2::Catch2WithMain)

    catch_discover_tests(test_same_host_transports TEST_PREFIX "shm.")

    # The node-level same-host fingerprint fix: make_node default-fills the node's local
    # fingerprint from the real machine read when the caller left it null, and respects a
    # caller-supplied value. Reads the effective fingerprint off the minted node's engine.
    # Same dependency surface as the portable-composition proof.
    add_executable(test_same_host_fingerprint_node test_same_host_fingerprint_node.cpp)

    target_include_directories(test_same_host_fingerprint_node PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR})

    target_link_libraries(test_same_host_fingerprint_node PRIVATE
    plexus::shm
        plexus::plexus
        plexus::core
        plexus::wire
        plexus::asio
        plexus::asio::shm
        plexus::native
        Catch2::Catch2WithMain)

    catch_discover_tests(test_same_host_fingerprint_node TEST_PREFIX "shm.")

    message(STATUS "plexus shm tests: liburing ${LIBURING_VERSION} found — io_uring futex spike + reactor bridge enabled")
else()
    message(STATUS "plexus shm tests: liburing absent — io_uring futex spike + reactor bridge skipped (non-gating)")
endif()

# The mux-join selection oracle (header-only, asio/POSIX-free): the shm member composed
# over the core multiplexer alongside a stream member proves the member satisfies
# mux_member + byte_channel, the preference hook routes a same-host dial to the shm member
# when the ring acquires, and the SAME dial falls back to the stream member on a forced
# broker failure. A heap-backed stub broker + a no-op notifier keep it single-process and
# deterministic; the TU includes only core headers (the real cross-process data path is the
# dual_delivery xproc proof).
add_executable(test_shm_mux test_shm_mux.cpp)

target_link_libraries(test_shm_mux PRIVATE
    plexus::shm
    plexus::core
    plexus::wire
    Catch2::Catch2WithMain)

catch_discover_tests(test_shm_mux TEST_PREFIX "shm.")

# The medium-coordinator oracle (header-only core + inproc): the engine-owned coordinator
# consumes the forwarder's demand transition, reads same_host, runs the upgrade policy, and
# acquires/releases the per-topic ring THROUGH the injected gate (the can_acquire/prefer_upgradeable_hook
# path), never a direct registry acquire — with refcount 0->1/1->0 gating, last-unsubscribe and
# peer-dead teardown, the bilateral-OR hint, and dual delivery (the wire never suppressed). It also
# drives the forwarder's real on_demand_transition emit seam (boundary-only fire, no-op when unset).
add_executable(test_medium_coordinator test_medium_coordinator.cpp)

target_link_libraries(test_medium_coordinator PRIVATE
    plexus::shm
    plexus::core
    plexus::wire
    plexus::inproc
    Catch2::Catch2WithMain)

catch_discover_tests(test_medium_coordinator TEST_PREFIX "shm.")

# The bilateral, consumer-sovereign upgrade via demand-driven convergence (xproc):
# two real processes each decide INDEPENDENTLY whether to attempt the shared-memory
# acquire from their OWN dispatch hint (no hint on the wire), converge on the same
# deterministically-named ring, and round-trip a value over it; the publisher sizes
# the ring via max_payload, a no-max_payload upgrade falls back to the default
# geometry. Links plexus::native (the POSIX broker + the ring) + plexus::wire (the fqn
# hash the region naming routes through).
add_executable(test_shm_bilateral test_shm_bilateral.cpp)

target_include_directories(test_shm_bilateral PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(test_shm_bilateral PRIVATE
    plexus::shm
    plexus::native
    plexus::wire
    Catch2::Catch2WithMain)

catch_discover_tests(test_shm_bilateral TEST_PREFIX "shm.")

# The large-message live-mux round-trip (xproc): a publisher declares a sizing
# shm_geometry and mints the ring through shm_mux_member's ACQUIRE path (not the unit
# registry); a >1 MiB then a 16 MiB payload crosses byte-exact to a converged
# subscriber, per notifier variant (futex-wake shmn + reactor-reaped/spin shmr;
# busy-poll shm excluded). The large cells run >= 2 times per ctest invocation. The
# probe-higher cell proves reliable_preserving fails closed above the ceiling while
# explicit best_effort_large round-trips. Links plexus::native (the POSIX broker + the
# futex primitive) + plexus::wire (the fqn-hash region naming).
add_executable(test_shm_large_message test_shm_large_message.cpp)

target_include_directories(test_shm_large_message PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(test_shm_large_message PRIVATE
    plexus::shm
    plexus::native
    plexus::wire
    Catch2::Catch2WithMain)

catch_discover_tests(test_shm_large_message TEST_PREFIX "shm.")

# The crashed-creator orphan-reclaim proof (xproc): a child creates a region,
# stamps a sentinel, and _exits WITHOUT unlinking (the create-owns-unlink dtor
# skipped), leaving an orphan in /dev/shm; the parent's create with
# unlink_stale_on_create reclaims the orphan and mints a FRESH (zero-filled)
# region, proving the orphan's stale bytes do not leak through. The bounded
# crash-orphan mitigation; looped N>=100, the binary re-run >=3 times. Links
# plexus::native for the compiled broker (shm_open/ftruncate/shm_unlink).
# A custom main() runs the re-exec child dispatcher BEFORE the Catch2 session, so
# it links the main-less Catch2 target and pulls plexus::testing for the portable
# process-id / shared-object-name helper the re-exec harness routes through.
add_executable(test_shm_stale_reclaim test_shm_stale_reclaim.cpp)

target_include_directories(test_shm_stale_reclaim PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(test_shm_stale_reclaim PRIVATE
    plexus::shm
    plexus::native
    plexus::testing
    Catch2::Catch2)

catch_discover_tests(test_shm_stale_reclaim TEST_PREFIX "shm.")

# The wire_fallback per-message routing proof (xproc): a topic in wire_fallback mode
# mints a CAPPED ring through shm_mux_member's acquire path AND routes per message at the
# message_forwarder publish fan-out — a message that fits the cap rides the SHM ring
# (verified byte-exact cross-process), a message over the cap rides the same-host wire.
# The carrying medium is ASSERTED per message via a per-channel tap (proven, not assumed),
# the over-cap cell repeats in one invocation, and a ring-less subscriber still receives
# both over the wire (the dual-delivery rescue). Links plexus::native (the POSIX broker + the
# futex primitive the live ring acquires) + plexus::wire (the fqn-hash region naming + the
# forwarder framing) + plexus::inproc (the test Policy's executor/timer substrate).
add_executable(test_shm_wire_fallback test_shm_wire_fallback.cpp)

target_include_directories(test_shm_wire_fallback PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(test_shm_wire_fallback PRIVATE
    plexus::shm
    plexus::core
    plexus::native
    plexus::wire
    plexus::inproc
    Catch2::Catch2WithMain)

catch_discover_tests(test_shm_wire_fallback TEST_PREFIX "shm.")

# The wire_fallback PRODUCTION-SEAM proof: the same-host medium decision a node reaches
# through node.advertise(...) is the multiplexer's prefer_upgradeable_hook over the shm-bearing
# pack, gated by the now MODE-AWARE shm_mux_member::can_acquire. A wire_fallback topic
# (provisioned by the SAME set_topic_geometry the node declare path calls) declines the
# ring so its same-host channel is the WIRE (the fail-safe); the two reliable-ring modes
# prefer SHM unchanged. The declined probe holds no refcount, and an over-cap message
# crosses a real process boundary byte-exact (repeated). Links plexus::native (the POSIX
# broker) + plexus::wire (the fqn-hash region naming).
add_executable(test_shm_wire_fallback_node test_shm_wire_fallback_node.cpp)

target_include_directories(test_shm_wire_fallback_node PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(test_shm_wire_fallback_node PRIVATE
    plexus::shm
    plexus::core
    plexus::native
    plexus::wire
    Catch2::Catch2WithMain)

catch_discover_tests(test_shm_wire_fallback_node TEST_PREFIX "shm.")
