plexus_add_catch_test(test_self_loopback
    SOURCES test_self_loopback.cpp
    LINKS plexus::plexus plexus::inproc plexus::discovery)

plexus_add_catch_test(test_self_loopback_mixed
    SOURCES test_self_loopback_mixed.cpp
    LINKS plexus::plexus plexus::inproc plexus::discovery)

plexus_add_catch_test(test_self_loopback_latch
    SOURCES test_self_loopback_latch.cpp
    LINKS plexus::plexus plexus::inproc plexus::discovery)

plexus_add_catch_test(test_self_loopback_recorder
    SOURCES test_self_loopback_recorder.cpp
    LINKS plexus::plexus plexus::inproc plexus::discovery)

plexus_add_catch_test(test_recorder_move
    SOURCES test_recorder_move.cpp
    LINKS plexus::plexus plexus::inproc plexus::discovery)

plexus_add_catch_test(test_self_loopback_lifetime
    SOURCES test_self_loopback_lifetime.cpp
    LINKS plexus::plexus plexus::inproc plexus::discovery)

plexus_add_catch_test(test_dispatch_reentrancy
    SOURCES test_dispatch_reentrancy.cpp
    LINKS plexus::plexus plexus::inproc plexus::discovery)
