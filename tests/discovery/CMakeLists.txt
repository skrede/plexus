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
target_link_libraries(test_contact_card PRIVATE plexus::core Catch2::Catch2WithMain)
catch_discover_tests(test_contact_card TEST_PREFIX "discovery.")

# The native multicast discovery over a fake datagram_socket: the byte-identical-metadata
# reuse, the immediate on-join announce, and the malformed-reject short-circuit. The
# header-only template is instantiated against the fake socket, so no asio is needed; it
# still links plexus::stream (the datagram_socket concept) and plexus::native (the discovery).
add_executable(test_multicast_discovery test_multicast_discovery.cpp)
target_link_libraries(test_multicast_discovery PRIVATE
    plexus::core
    plexus::wire
    plexus::stream
    plexus::native
    Catch2::Catch2WithMain)
catch_discover_tests(test_multicast_discovery TEST_PREFIX "discovery.")

# The consumer-tunable anti-flood admission cap over the same fake socket, with a virtual clock the
# cases advance by hand: the max-peers bound, established-peer protection, LRU eviction, the
# per-source rate window, the disabled mode, and the ungated goodbye path.
add_executable(test_multicast_discovery_flood_cap test_multicast_discovery_flood_cap.cpp)
target_link_libraries(test_multicast_discovery_flood_cap PRIVATE
    plexus::core
    plexus::wire
    plexus::stream
    plexus::native
    Catch2::Catch2WithMain)
catch_discover_tests(test_multicast_discovery_flood_cap TEST_PREFIX "discovery.")
