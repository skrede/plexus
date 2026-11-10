if(NOT PLEXUS_ENABLE_CRYPTO_BACKEND)
    return()
endif()

set(PLEXUS_TEST_PREFIX "tls.")

plexus_add_catch_test(test_tls_seam
    SOURCES test_tls_seam.cpp $<TARGET_OBJECTS:plexus-crypto>
    LINKS plexus::crypto)

# The TLS listener keeps accepting after an EMFILE accept once descriptors free; the exhaustion lowers RLIMIT_NOFILE,
# which is POSIX-only.
if(NOT WIN32)
    plexus_add_catch_test(test_tls_accept_retry
        SOURCES test_tls_accept_retry.cpp $<TARGET_OBJECTS:plexus-crypto>
        LINKS plexus::crypto)
    # UBSan's vptr check opens a pipe on a type-cache miss, which aborts inside the exhausted table.
    if(CMAKE_CXX_FLAGS MATCHES "-fsanitize=[^ ]*undefined")
        target_compile_options(test_tls_accept_retry PRIVATE -fno-sanitize=vptr)
    endif()
endif()

plexus_add_catch_test(test_tls_trust
    SOURCES test_tls_trust.cpp $<TARGET_OBJECTS:plexus-crypto>
    LINKS plexus::crypto)

plexus_add_catch_test(test_tls_mux
    SOURCES test_tls_mux.cpp $<TARGET_OBJECTS:plexus-crypto>
    LINKS plexus::asio plexus::crypto)

plexus_add_catch_test(test_tls_large_payload
    SOURCES test_tls_large_payload.cpp test_tls_large_payload_past_field.cpp $<TARGET_OBJECTS:plexus-crypto>
    LINKS plexus::crypto)
