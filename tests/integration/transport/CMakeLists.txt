plexus_add_catch_test(test_transport_dial_cancel
    SOURCES test_transport_dial_cancel.cpp
    LINKS plexus::core plexus::wire plexus::asio)
if(NOT WIN32)
    target_sources(test_transport_dial_cancel PRIVATE test_transport_dial_cancel_unix.cpp)
endif()
if(PLEXUS_ENABLE_CRYPTO_BACKEND)
    target_sources(test_transport_dial_cancel PRIVATE test_transport_dial_cancel_tls.cpp
        test_transport_dial_cancel_dtls.cpp
        $<TARGET_OBJECTS:plexus-crypto>)
    target_link_libraries(test_transport_dial_cancel PRIVATE plexus::crypto)
endif()

plexus_add_catch_test(test_dial_deadline_asio
    SOURCES test_dial_deadline_asio.cpp
    LINKS plexus::plexus plexus::asio plexus::discovery)
if(PLEXUS_ENABLE_CRYPTO_BACKEND)
    target_sources(test_dial_deadline_asio PRIVATE test_dial_deadline_tls.cpp
        $<TARGET_OBJECTS:plexus-crypto>)
    target_link_libraries(test_dial_deadline_asio PRIVATE plexus::crypto)
endif()

plexus_add_catch_test(test_unclaimed_retire_stream
    SOURCES test_unclaimed_retire_stream.cpp
    LINKS plexus::core plexus::wire plexus::asio plexus::testing)
if(NOT WIN32)
    target_link_libraries(test_unclaimed_retire_stream PRIVATE util)
endif()
plexus_add_catch_test(test_unclaimed_retire_wrap
    SOURCES test_unclaimed_retire_wrap.cpp
    LINKS plexus::core plexus::wire plexus::asio plexus::inproc plexus::testing)
if(PLEXUS_ENABLE_CRYPTO_BACKEND)
    foreach(unclaimed_target test_unclaimed_retire_stream test_unclaimed_retire_wrap)
        target_sources(${unclaimed_target} PRIVATE $<TARGET_OBJECTS:plexus-crypto>)
        target_link_libraries(${unclaimed_target} PRIVATE plexus::crypto)
        target_compile_definitions(${unclaimed_target} PRIVATE PLEXUS_HAVE_CRYPTO)
    endforeach()
endif()
if(PLEXUS_ENABLE_SHM_BACKEND AND TARGET plexus::native)
    target_include_directories(test_unclaimed_retire_wrap PRIVATE ${PROJECT_SOURCE_DIR}/tests/shm)
    target_link_libraries(test_unclaimed_retire_wrap PRIVATE plexus::shm plexus::native)
    target_compile_definitions(test_unclaimed_retire_wrap PRIVATE PLEXUS_HAVE_SHM)
endif()

# The AF_UNIX filesystem permissions model (chmod/stat via unistd.h) is POSIX-only; Windows AF_UNIX carries no
# mode bits.
if(NOT WIN32)
plexus_add_catch_test(test_unix_permissions
    SOURCES test_unix_permissions.cpp
    LINKS plexus::core plexus::wire plexus::asio)

# It interposes umask through dlsym(RTLD_NEXT), hence the dl link.
plexus_add_catch_test(test_unix_bind_umask
    SOURCES test_unix_bind_umask.cpp
    LINKS plexus::core plexus::wire plexus::asio ${CMAKE_DL_LIBS})
endif()

# The multiplexing transport composes a TLS member, so the mux oracles here need the crypto backend; that member
# stays inert, as no tls channel is dialed or accepted.
if(PLEXUS_ENABLE_CRYPTO_BACKEND)
    plexus_add_catch_test(test_mux_select_unix
        SOURCES test_mux_select_unix.cpp $<TARGET_OBJECTS:plexus-crypto>
        LINKS plexus::core plexus::wire plexus::asio plexus::crypto)

    plexus_add_catch_test(test_mux_select_asio
        SOURCES test_mux_select_asio.cpp $<TARGET_OBJECTS:plexus-crypto>
        LINKS plexus::core plexus::wire plexus::asio plexus::crypto)
endif()

plexus_add_catch_test(test_mux_minimal_composition
    SOURCES test_mux_minimal_composition.cpp
    LINKS plexus::core plexus::wire plexus::asio)

plexus_add_catch_test(test_mux_node_composition
    SOURCES test_mux_node_composition.cpp
    LINKS plexus::plexus plexus::asio)

# It forks its two nodes and maps POSIX shared memory, so it is excluded from Windows.
if(PLEXUS_ENABLE_SHM_BACKEND AND TARGET plexus::asio::shm AND NOT WIN32)
    plexus_add_catch_test(test_node_xproc_shm
        SOURCES test_node_xproc_shm.cpp
        LINKS plexus::plexus plexus::core plexus::wire plexus::asio plexus::asio::shm plexus::native
        INCLUDES ${PROJECT_SOURCE_DIR}/tests/shm)
endif()

plexus_add_catch_test(test_subscribe_retry_udp
    SOURCES test_subscribe_retry_udp.cpp
    LINKS plexus::plexus plexus::asio plexus::testing plexus::discovery)

# RUN_SERIAL and the loopback lock keep its asio transport off the other real-TCP legs.
plexus_add_catch_test(test_native_shared_executor
    SOURCES test_native_shared_executor.cpp
    LINKS plexus::core plexus::wire plexus::asio
    PROPERTIES RUN_SERIAL TRUE RESOURCE_LOCK tcp-loopback)
