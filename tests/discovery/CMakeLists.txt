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
