# Discovery unit tests: the node contact-card assembler and its inverse port read,
# proven over static_discovery (no mDNS, no socket). Header-only core only, so each
# links plexus::plexus + Catch2's main. The "discovery." ctest prefix selects the
# area; the TEST_CASE names carry "contact_card" so `-R contact_card` resolves.
# Auto-discovered by the tests glob (gated on PLEXUS_BUILD_TESTS by the parent).
if(NOT PLEXUS_BUILD_TESTS)
    return()
endif()

set(PLEXUS_DISCOVERY_TESTS
    test_contact_card)

foreach(test ${PLEXUS_DISCOVERY_TESTS})
    add_executable(${test} ${test}.cpp)
    target_link_libraries(${test} PRIVATE plexus::plexus Catch2::Catch2WithMain)
    catch_discover_tests(${test} TEST_PREFIX "discovery.")
endforeach()
