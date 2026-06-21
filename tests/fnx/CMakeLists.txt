# The -fno-exceptions audit area. Two compile-gate translation units enumerate the
# implicit-throw surface and prove the seam pass under -fno-exceptions -fno-rtti (built in
# the throwaway build-fnx tree configured with those flags via CMAKE_CXX_FLAGS); plus the
# fail_closed throw-semantics oracle that runs as an ordinary ctest entry under the "fnx."
# prefix. The audit TUs are compile-only gates — they carry no ctest registration; their
# value is that they BUILD clean under -fno-exceptions. In the default build/ tree they
# simply compile the exceptions branch (inert, byte-identical to before).
# Auto-discovered by the tests glob (gated on PLEXUS_BUILD_TESTS by the parent).
if(NOT PLEXUS_BUILD_TESTS)
    return()
endif()

# The fail_closed oracle: header-only core, links plexus::core + Catch2's main.
add_executable(test_fail_closed test_fail_closed.cpp)
target_link_libraries(test_fail_closed PRIVATE plexus::core Catch2::Catch2WithMain)
catch_discover_tests(test_fail_closed TEST_PREFIX "fnx.")

# The MCU-floor audit TU: core + wire only (plexus::inproc supplies the concrete policy the
# routing_engine is instantiated over). No facade, no recorder/value_projection — FNX-03 is
# structural (those headers are simply not included).
add_executable(audit_mcu_floor audit_mcu_floor.cpp)
target_link_libraries(audit_mcu_floor PRIVATE plexus::core plexus::inproc)

# The api-facade audit TU: node.h + publisher.h over the inproc backend, exercising the
# converted serve_procedure_seam throw and the guarded catch(...) cleanup sites.
add_executable(audit_api_facade audit_api_facade.cpp)
target_link_libraries(audit_api_facade PRIVATE plexus::plexus plexus::inproc)
