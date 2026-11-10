if(NOT PLEXUS_BUILD_TESTS)
    return()
endif()

set(PLEXUS_TEST_PREFIX "fnx.")

plexus_add_catch_test(test_fail_closed
    SOURCES test_fail_closed.cpp
    LINKS plexus::core)

# The audit translation units are compile-only gates with no ctest entry, proven by building clean in a tree configured
# with -fno-exceptions -fno-rtti through CMAKE_CXX_FLAGS. The MCU-floor one links plexus::inproc only for the concrete
# policy its routing engine is instantiated over.
add_executable(audit_mcu_floor audit_mcu_floor.cpp)
target_link_libraries(audit_mcu_floor PRIVATE plexus::core plexus::stream plexus::inproc)

add_executable(audit_api_facade audit_api_facade.cpp)
target_link_libraries(audit_api_facade PRIVATE plexus::plexus plexus::inproc plexus::discovery)

add_executable(audit_node_shm_free audit_node_shm_free.cpp)
target_link_libraries(audit_node_shm_free PRIVATE plexus::plexus plexus::inproc plexus::discovery)
