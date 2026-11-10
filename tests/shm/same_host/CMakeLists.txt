plexus_add_catch_test(test_shm_same_host
    SOURCES test_shm_same_host.cpp
    LINKS plexus::shm plexus::core plexus::wire)

if(LIBURING_FOUND)
    # The full member set includes the TLS and DTLS members, so it needs the crypto backend and takes its objects
    # directly (the COMDAT-fold object pattern).
    if(PLEXUS_ENABLE_CRYPTO_BACKEND)
        plexus_add_catch_test(test_shm_mux_composition
            SOURCES test_shm_mux_composition.cpp $<TARGET_OBJECTS:plexus-crypto>
            LINKS plexus::shm plexus::core plexus::wire plexus::asio plexus::asio::shm plexus::native plexus::crypto)
    endif()

    plexus_add_catch_test(test_shm_node_composition
        SOURCES test_shm_node_composition.cpp
        LINKS plexus::shm plexus::plexus plexus::core plexus::wire plexus::asio plexus::asio::shm plexus::native)

    plexus_add_catch_test(test_transport_set
        SOURCES test_transport_set.cpp
        LINKS plexus::shm plexus::plexus plexus::core plexus::wire plexus::asio plexus::asio::shm plexus::native)

    plexus_add_catch_test(test_same_host_transports
        SOURCES test_same_host_transports.cpp
        LINKS plexus::shm plexus::plexus plexus::core plexus::wire plexus::asio plexus::asio::shm plexus::native)

    plexus_add_catch_test(test_same_host_fingerprint_node
        SOURCES test_same_host_fingerprint_node.cpp
        LINKS plexus::shm plexus::plexus plexus::core plexus::wire plexus::asio plexus::asio::shm plexus::native)
endif()

plexus_add_catch_test(test_shm_mux
    SOURCES test_shm_mux.cpp
    LINKS plexus::shm plexus::core plexus::wire)

# Drives fork() and the POSIX broker directly, so it is excluded from the Windows build.
if(NOT WIN32)
    plexus_add_catch_test(test_shm_bilateral
        SOURCES test_shm_bilateral.cpp
        LINKS plexus::plexus plexus::shm plexus::native plexus::wire)
endif()

# Drives the Linux futex primitive directly, so it is excluded from Windows and macOS.
if(NOT WIN32 AND NOT APPLE)
    plexus_add_catch_test(test_shm_large_message
        SOURCES test_shm_large_message.cpp
        LINKS plexus::plexus plexus::shm plexus::native plexus::wire)
endif()

# Drives fork() and the POSIX broker directly, so it is excluded from the Windows build.
if(NOT WIN32)
    plexus_add_catch_test(test_shm_wire_fallback
        SOURCES test_shm_wire_fallback.cpp
        LINKS plexus::plexus plexus::shm plexus::core plexus::native plexus::wire plexus::inproc)
endif()

# Drives fork() and the POSIX broker directly, so it is excluded from the Windows build.
if(NOT WIN32)
    plexus_add_catch_test(test_shm_wire_fallback_node
        SOURCES test_shm_wire_fallback_node.cpp
        LINKS plexus::shm plexus::core plexus::native plexus::wire)
endif()
