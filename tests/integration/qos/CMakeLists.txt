plexus_add_catch_test(test_inproc_stamp_demand
    SOURCES test_inproc_stamp_demand.cpp test_inproc_stamp_demand_latch.cpp
    LINKS plexus::plexus plexus::wire plexus::inproc plexus::discovery)

plexus_add_catch_test(test_latch_replay_inproc
    SOURCES test_latch_replay_inproc.cpp test_latch_replay_inproc_noalloc.cpp
    LINKS plexus::core plexus::wire plexus::inproc)

# Its live AF_UNIX leg reaches raw POSIX (unistd.h), so the whole target is excluded from the Windows build.
if(NOT WIN32)
plexus_add_catch_test(test_locality_confinement
    SOURCES test_locality_confinement.cpp test_locality_confinement_wire.cpp test_locality_confinement_live.cpp
    LINKS plexus::core plexus::wire plexus::inproc)
endif()

plexus_add_catch_test(test_mux_select_hook
    SOURCES test_mux_select_hook.cpp
    LINKS plexus::core plexus::wire)

plexus_add_catch_test(test_transport_steering
    SOURCES test_transport_steering.cpp
    LINKS plexus::core plexus::wire)

plexus_add_catch_test(test_deadline_liveliness
    SOURCES test_deadline_liveliness.cpp test_deadline_liveliness_lease.cpp
    LINKS plexus::core plexus::wire plexus::inproc)

plexus_add_catch_test(test_liveliness_verdict_inproc
    SOURCES test_liveliness_verdict_inproc.cpp
    LINKS plexus::core plexus::wire plexus::inproc)

plexus_add_catch_test(test_liveliness_arbitration_inproc
    SOURCES test_liveliness_arbitration_inproc.cpp
    LINKS plexus::core plexus::wire plexus::inproc)

plexus_add_catch_test(test_rxo_compatibility
    SOURCES test_rxo_compatibility.cpp test_rxo_compatibility_soft.cpp
    LINKS plexus::core plexus::wire plexus::inproc)

plexus_add_catch_test(test_subscribe_qos_wire
    SOURCES test_subscribe_qos_wire.cpp
    LINKS plexus::core plexus::wire)

if(PLEXUS_ENABLE_ASIO_BACKEND)
    if(NOT WIN32)
        target_link_libraries(test_locality_confinement PRIVATE plexus::asio)
        target_compile_definitions(test_locality_confinement PRIVATE PLEXUS_HAVE_ASIO_MUX)
    endif()
endif()
