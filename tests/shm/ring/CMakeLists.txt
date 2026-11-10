plexus_add_catch_test(test_shm_scaffold
    SOURCES test_shm_scaffold.cpp
    LINKS plexus::native
    DEFINES PLEXUS_TEST_PROJECT_VERSION="${PROJECT_VERSION}")

plexus_add_catch_test(test_shm_ring_core
    SOURCES test_shm_ring_core.cpp test_shm_ring_core_capacity.cpp test_shm_ring_magic.cpp
        test_shm_ring_attach_bounds.cpp test_shm_ring_tampered.cpp test_shm_cursor_generation.cpp
    LINKS plexus::shm plexus::core)

plexus_add_catch_test(test_shm_ring_backpressure
    SOURCES test_shm_ring_backpressure.cpp test_shm_ring_backpressure_recovery.cpp
    LINKS plexus::shm plexus::core)

plexus_add_catch_test(test_shm_ring_dekker
    SOURCES test_shm_ring_dekker.cpp
    LINKS plexus::shm plexus::core Threads::Threads)

plexus_add_catch_test(test_shm_ring_overtake
    SOURCES test_shm_ring_overtake.cpp
    LINKS plexus::shm plexus::core)

plexus_add_catch_test(test_shm_loan_abandon
    SOURCES test_shm_loan_abandon.cpp
    LINKS plexus::shm plexus::core)

plexus_add_catch_test(test_shm_zerocopy_take
    SOURCES test_shm_zerocopy_take.cpp
    LINKS plexus::shm plexus::core plexus::wire)

plexus_add_catch_test(test_shm_loan_lifetime
    SOURCES test_shm_loan_lifetime.cpp
    LINKS plexus::shm plexus::core plexus::wire)

plexus_add_catch_test(test_region_naming_length
    SOURCES test_region_naming_length.cpp
    LINKS plexus::shm plexus::wire)

plexus_add_catch_test(test_shm_doorbell_naming
    SOURCES test_shm_doorbell_naming.cpp
    LINKS plexus::shm)

plexus_add_catch_test(test_shm_channel
    SOURCES test_shm_channel.cpp
    LINKS plexus::shm plexus::core plexus::wire)

plexus_add_catch_test(test_shm_spin_park
    SOURCES test_shm_spin_park.cpp
    LINKS plexus::shm plexus::core plexus::wire Threads::Threads)

plexus_add_catch_test(test_shm_congested
    SOURCES test_shm_congested.cpp test_shm_congested_alloc.cpp
    LINKS plexus::shm plexus::core plexus::wire Threads::Threads)

plexus_add_catch_test(test_shm_byte_channel_retire
    SOURCES test_shm_byte_channel_retire.cpp
    LINKS plexus::shm plexus::core plexus::native plexus::testing)

plexus_add_catch_test(test_shm_write_once
    SOURCES test_shm_write_once.cpp
    LINKS plexus::core plexus::wire)

plexus_add_catch_test(test_shm_claim_switch
    SOURCES test_shm_claim_switch.cpp
    LINKS plexus::core plexus::wire)
