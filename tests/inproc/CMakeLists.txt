set(PLEXUS_TEST_PREFIX "inproc.")

# RUN_SERIAL keeps the alloc-counter harness's global operator-new count immune to the contention of concurrent test
# processes.
plexus_add_catch_test(test_inproc_policy
    SOURCES test_inproc_policy.cpp
    LINKS plexus::core plexus::wire plexus::inproc plexus::discovery
    PROPERTIES RUN_SERIAL TRUE)

plexus_add_catch_test(test_inproc_object_lane
    SOURCES test_inproc_object_lane.cpp
    LINKS plexus::core plexus::wire plexus::inproc)

plexus_add_catch_test(test_inproc_retire_quiescence
    SOURCES test_inproc_retire_quiescence.cpp
    LINKS plexus::core plexus::inproc)

plexus_add_catch_test(test_inproc_ingress
    SOURCES test_inproc_ingress.cpp
    LINKS plexus::plexus plexus::inproc plexus::discovery)

plexus_add_catch_test(test_inproc_listen_set
    SOURCES test_inproc_listen_set.cpp
    LINKS plexus::core plexus::inproc)
