# Drives the POSIX broker directly, so it is excluded from the Windows build.
if(NOT WIN32)
    plexus_add_catch_test(test_shm_region_broker
        SOURCES test_shm_region_broker.cpp
        LINKS plexus::shm plexus::native)
endif()

# Both reach the region through /dev/shm, the tmpfs view of POSIX shm that only Linux has; macOS shm_open objects
# are not path-visible.
if(NOT WIN32 AND NOT APPLE)
    plexus_add_catch_test(linux_test_shm_region_broker
        SOURCES linux_test_shm_region_broker.cpp
        LINKS plexus::shm plexus::native)

    plexus_add_catch_test(linux_test_shm_slot_lock
        SOURCES linux_test_shm_slot_lock.cpp
        LINKS plexus::shm plexus::native plexus::testing)
endif()

# Seccomp user notification, which pauses the create inside its syscall, is Linux-only.
if(NOT WIN32 AND NOT APPLE)
    plexus_add_catch_test(linux_test_shm_create_window
        SOURCES linux_test_shm_create_window.cpp
        LINKS plexus::plexus plexus::shm plexus::native plexus::core plexus::testing Threads::Threads)
endif()

# Drives fork() and the POSIX broker directly, so it is excluded from the Windows build.
if(NOT WIN32)
    plexus_add_catch_test(test_shm_same_host_roundtrip
        SOURCES test_shm_same_host_roundtrip.cpp test_shm_same_host_fingerprint.cpp
        LINKS plexus::plexus plexus::shm plexus::native plexus::wire)
endif()

# Drives fork() and the POSIX broker directly, so it is excluded from the Windows build.
if(NOT WIN32)
    plexus_add_catch_test(test_shm_region_namespace
        SOURCES test_shm_region_namespace.cpp
        LINKS plexus::plexus plexus::shm plexus::native plexus::wire)
endif()
