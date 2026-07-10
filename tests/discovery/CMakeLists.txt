# Discovery unit tests: the node contact-card assembler and its inverse port read,
# proven over static_discovery (no mDNS, no socket). Header-only core only, so each
# links plexus::core + Catch2's main. The "discovery." ctest prefix selects the
# area; the TEST_CASE names carry "contact_card" so `-R contact_card` resolves.
# Auto-discovered by the tests glob (gated on PLEXUS_BUILD_TESTS by the parent).
if(NOT PLEXUS_BUILD_TESTS)
    return()
endif()

add_executable(test_contact_card
    test_contact_card.cpp
    test_contact_card_static_discovery.cpp)
target_link_libraries(test_contact_card PRIVATE plexus::core plexus::discovery Catch2::Catch2WithMain)
catch_discover_tests(test_contact_card TEST_PREFIX "discovery.")

# The universe vocabulary: the label hash goldens, the fmix32 group derivation range/avoidance
# sweep, and the effective_group scoping selection. Pure header-only core, no socket.
add_executable(test_universe test_universe.cpp)
target_link_libraries(test_universe PRIVATE plexus::core plexus::discovery Catch2::Catch2WithMain)
catch_discover_tests(test_universe TEST_PREFIX "discovery.")

# The multicast discovery over a fake datagram_socket: the byte-identical-metadata
# reuse, the immediate on-join announce, and the malformed-reject short-circuit. The
# header-only template is instantiated against the fake socket, so no asio is needed; it
# links plexus::discovery (the leaf), plexus::stream (the datagram_socket concept), and
# plexus::wire (the contact-card codec).
add_executable(test_multicast_discovery test_multicast_discovery.cpp)
target_link_libraries(test_multicast_discovery PRIVATE
    plexus::core
    plexus::wire
    plexus::stream
    plexus::discovery
    Catch2::Catch2WithMain)
catch_discover_tests(test_multicast_discovery TEST_PREFIX "discovery.")

# The universe partition over the same fake socket: foreign-universe isolation (announce and
# goodbye), browse-only filtering, same-universe resolve, configless default-universe rendezvous,
# and the self-echo latching the self-probe under a custom universe.
add_executable(test_multicast_discovery_universe test_multicast_discovery_universe.cpp)
target_link_libraries(test_multicast_discovery_universe PRIVATE
    plexus::core
    plexus::wire
    plexus::stream
    plexus::discovery
    Catch2::Catch2WithMain)
catch_discover_tests(test_multicast_discovery_universe TEST_PREFIX "discovery.")

# The consumer-tunable anti-flood admission cap over the same fake socket, with a virtual clock the
# cases advance by hand: the max-peers bound, established-peer protection, LRU eviction, the
# per-source rate window, the disabled mode, and the ungated goodbye path.
add_executable(test_multicast_discovery_flood_cap test_multicast_discovery_flood_cap.cpp)
target_link_libraries(test_multicast_discovery_flood_cap PRIVATE
    plexus::core
    plexus::wire
    plexus::stream
    plexus::discovery
    Catch2::Catch2WithMain)
catch_discover_tests(test_multicast_discovery_flood_cap TEST_PREFIX "discovery.")

# The self-exclusion invariant, the self-echo health probe, and the default_discovery factory's
# combined health surface + one-shot warning. The leaf cases run over the fake socket (a fake clock
# drives the no-self-echo window); the factory cases drive the real asio socket, so this executable
# needs the asio backend and is skipped when it is off.
if(PLEXUS_ENABLE_ASIO_BACKEND)
    add_executable(test_discovery_self_probe test_discovery_self_probe.cpp)
    target_link_libraries(test_discovery_self_probe PRIVATE
        plexus::core
        plexus::wire
        plexus::stream
        plexus::discovery
        plexus::asio
        Catch2::Catch2WithMain)
    catch_discover_tests(test_discovery_self_probe TEST_PREFIX "discovery.")
endif()
