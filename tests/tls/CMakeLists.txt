# TLS backend tests: the static_assert concept gates plus the live loopback
# mutual-TLS handshake proof and the COMDAT link+run proof. Only built when
# PLEXUS_ENABLE_TLS_BACKEND is ON (plexus::tls exists only under the gate); the
# `tls.` ctest prefix lets `ctest -R tls` select it. Auto-discovered by the
# tests glob.
if(NOT PLEXUS_ENABLE_TLS_BACKEND)
    return()
endif()

# The OBJECT library's objects are folded DIRECTLY into this executable's link
# unit via $<TARGET_OBJECTS:plexus-tls>; linking plexus::tls pulls only the
# INTERFACE usage requirements (include dirs + OpenSSL/asio deps), never a
# second asio-bearing archive. This is the COMDAT-safe wiring the run proof
# below validates at runtime.
add_executable(test_tls_seam test_tls_seam.cpp $<TARGET_OBJECTS:plexus-tls>)

target_link_libraries(test_tls_seam PRIVATE
    plexus::tls
    Catch2::Catch2WithMain)

catch_discover_tests(test_tls_seam TEST_PREFIX "tls.")
